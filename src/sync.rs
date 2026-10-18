//! Offline-first sync for the desktop POS.
//!
//! - `run_sync` uploads queued offline sales (POST /api/device/sales) and then downloads a
//!   fresh snapshot (GET /api/device/sync) into the local SQLite database.
//! - Everything is authenticated with the device token issued at activation (OS keychain).
//! - The POS may run offline for at most MAX_OFFLINE_HOURS (3 days) since the last successful
//!   snapshot sync; after that `create_local_sale` refuses new sales until the device syncs.

use crate::local_db::{self, Db};
use crate::staff_auth::SessionState;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tauri::{AppHandle, Manager, State};

pub const API_BASE: &str = "https://restropro-lite.vercel.app";
const MAX_OFFLINE_HOURS: i64 = 72;
const CLOCK_ROLLBACK_TOLERANCE_MINUTES: i64 = 5;
const BACKGROUND_SNAPSHOT_EVERY_MINUTES: i64 = 30;
const UPLOAD_BATCH: i64 = 50;

static SYNCING: AtomicBool = AtomicBool::new(false);

struct SyncGuard;
impl Drop for SyncGuard {
    fn drop(&mut self) {
        SYNCING.store(false, Ordering::SeqCst);
    }
}

#[derive(Serialize, Clone)]
pub struct SyncStatus {
    last_sync_at: Option<String>,
    hours_since_sync: Option<i64>,
    hours_left: Option<i64>,
    pending_sales: i64,
    rejected_sales: i64,
    locked: bool,
    lock_reason: Option<String>,
    last_error: Option<String>,
}

// ---------------------------------------------------------------- status / lock

fn status_from(conn: &Connection) -> Result<SyncStatus, String> {
    let (last_sync, max_clock, last_error, revoked): (Option<String>, Option<String>, Option<String>, i64) = conn
        .query_row(
            "SELECT last_sync_at, max_clock_seen, last_error, revoked FROM sync_state WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .map_err(|e| e.to_string())?;
    let pending: i64 = conn
        .query_row("SELECT COUNT(*) FROM sales_outbox WHERE status = 'pending'", [], |r| r.get(0))
        .map_err(|e| e.to_string())?;
    let rejected: i64 = conn
        .query_row("SELECT COUNT(*) FROM sales_outbox WHERE status = 'rejected'", [], |r| r.get(0))
        .map_err(|e| e.to_string())?;

    let now = Utc::now();
    let parse = |s: &Option<String>| s.as_deref().and_then(|v| DateTime::parse_from_rfc3339(v).ok()).map(|d| d.with_timezone(&Utc));
    let last_sync_dt = parse(&last_sync);
    let max_clock_dt = parse(&max_clock);

    let mut locked = false;
    let mut lock_reason: Option<String> = None;

    // Moving the PC clock back would stretch the 3-day window, so remember the latest time seen.
    if let Some(max_seen) = max_clock_dt {
        if now < max_seen - ChronoDuration::minutes(CLOCK_ROLLBACK_TOLERANCE_MINUTES) {
            locked = true;
            lock_reason = Some("The computer's clock was set back. Connect to the internet and sync to continue.".to_string());
        } else if now > max_seen {
            conn.execute("UPDATE sync_state SET max_clock_seen = ?1 WHERE id = 1", params![now.to_rfc3339()])
                .map_err(|e| e.to_string())?;
        }
    }

    let hours_since = last_sync_dt.map(|d| (now - d).num_hours().max(0));
    if revoked != 0 {
        locked = true;
        lock_reason = Some("This device is no longer authorised. Activate it again with the owner account.".to_string());
    } else if last_sync_dt.is_none() {
        locked = true;
        lock_reason = Some("This device has not synced yet. Connect to the internet and sync.".to_string());
    } else if !locked && hours_since.unwrap_or(0) >= MAX_OFFLINE_HOURS {
        locked = true;
        lock_reason = Some("This POS has been offline for 3 days. Connect to the internet and sync to continue selling.".to_string());
    }

    Ok(SyncStatus {
        last_sync_at: last_sync,
        hours_since_sync: hours_since,
        hours_left: hours_since.map(|h| (MAX_OFFLINE_HOURS - h).max(0)),
        pending_sales: pending,
        rejected_sales: rejected,
        locked,
        lock_reason,
        last_error,
    })
}

fn set_last_error(db: &Db, msg: &str, revoked: bool) {
    if let Ok(conn) = db.0.lock() {
        let _ = conn.execute(
            "UPDATE sync_state SET last_error = ?1, revoked = CASE WHEN ?2 THEN 1 ELSE revoked END WHERE id = 1",
            params![msg, revoked],
        );
    }
}

// ---------------------------------------------------------------- http helpers

fn http_client(timeout_secs: u64) -> Result<reqwest::blocking::Client, String> {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .build()
        .map_err(|e| e.to_string())
}

fn device_token() -> Result<String, String> {
    crate::secure_store::secure_get("device_token".to_string())?
        .ok_or_else(|| "This device is not activated yet".to_string())
}

/// Turn a non-2xx response into (message, device_revoked).
fn error_from(resp: reqwest::blocking::Response) -> (String, bool) {
    let status = resp.status();
    let body: Value = resp.json().unwrap_or_default();
    let msg = body["error"].as_str().map(str::to_string).unwrap_or_else(|| format!("Server error ({status})"));
    (msg, body["code"].as_str() == Some("device_revoked"))
}

// ---------------------------------------------------------------- upload

/// Upload pending sales in batches. Idempotent on the server (clientSaleId), so it is safe if
/// this runs twice at once or a response is lost. Returns Err only when the server could not be
/// reached or refused the device; per-sale rejections are recorded in the outbox instead.
fn upload_pending(db: &Db, client: &reqwest::blocking::Client, token: &str, only: Option<&str>) -> Result<Option<String>, (String, bool)> {
    // First "try again later" reason from the server (e.g. a database problem on its side). The
    // sale stays queued and the reason is shown in the sync bar so it is never silent.
    let mut note: Option<String> = None;
    loop {
        let rows: Vec<(String, String, i64)> = {
            let conn = db.0.lock().map_err(|e| (e.to_string(), false))?;
            let mut stmt = conn
                .prepare(
                    "SELECT client_sale_id, payload, seq FROM sales_outbox
                     WHERE status = 'pending' AND (?1 IS NULL OR client_sale_id = ?1)
                     ORDER BY seq LIMIT ?2",
                )
                .map_err(|e| (e.to_string(), false))?;
            let mapped = stmt
                .query_map(params![only, UPLOAD_BATCH], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .map_err(|e| (e.to_string(), false))?;
            mapped.collect::<Result<Vec<_>, _>>().map_err(|e| (e.to_string(), false))?
        };
        if rows.is_empty() {
            return Ok(note);
        }

        // Each sale carries this device's own counter; the server builds the D<n>-<counter> id from it.
        let sales: Vec<Value> = rows
            .iter()
            .filter_map(|(_, p, seq)| {
                let mut v: Value = serde_json::from_str(p).ok()?;
                v["deviceSeq"] = json!(seq);
                Some(v)
            })
            .collect();
        let resp = client
            .post(format!("{API_BASE}/api/device/sales"))
            .bearer_auth(token)
            .json(&json!({ "sales": sales }))
            .send()
            .map_err(|e| (format!("Could not reach the server — {e}"), false))?;
        if !resp.status().is_success() {
            return Err(error_from(resp));
        }
        let body: Value = resp.json().map_err(|e| (format!("Unexpected response — {e}"), false))?;

        let mut progressed = false;
        let conn = db.0.lock().map_err(|e| (e.to_string(), false))?;
        for r in body["results"].as_array().cloned().unwrap_or_default() {
            let Some(id) = r["clientSaleId"].as_str() else { continue };
            if r["ok"].as_bool() == Some(true) {
                let order_no = match &r["orderNo"] {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                conn.execute(
                    "UPDATE sales_outbox SET status = 'synced', order_no = ?2, server_sale_id = COALESCE(?3, server_sale_id), error = NULL WHERE client_sale_id = ?1",
                    params![id, order_no, r["saleId"].as_str()],
                )
                .map_err(|e| (e.to_string(), false))?;
                progressed = true;
            } else if r["retryable"].as_bool() == Some(true) {
                let err = r["error"].as_str().unwrap_or("The server asked to try again later");
                conn.execute("UPDATE sales_outbox SET error = ?2 WHERE client_sale_id = ?1", params![id, err])
                    .map_err(|e| (e.to_string(), false))?;
                note.get_or_insert_with(|| err.to_string());
            } else {
                let err = r["error"].as_str().unwrap_or("Rejected by the server");
                conn.execute(
                    "UPDATE sales_outbox SET status = 'rejected', error = ?2 WHERE client_sale_id = ?1",
                    params![id, err],
                )
                .map_err(|e| (e.to_string(), false))?;
                progressed = true;
            }
        }
        drop(conn);
        if !progressed || only.is_some() {
            return Ok(note); // only retryable results left (or a single-sale attempt): try again next sync
        }
    }
}

// ---------------------------------------------------------------- ticket rail moves

/// Upload Ticket Rail moves made offline. Only the latest move of each ticket is sent (earlier
/// ones are superseded). Moves on a sale that has not uploaded yet wait until it has. Network
/// trouble leaves everything queued for the next sync.
fn upload_kitchen_ops(db: &Db, client: &reqwest::blocking::Client, token: &str) -> Result<(), String> {
    let (sendable, superseded): (Vec<(String, String, Vec<i64>)>, Vec<i64>) = {
        let conn = db.0.lock().map_err(|e| e.to_string())?;
        let mut stmt = conn
            .prepare("SELECT op_id, sale_ref, kitchen_status FROM kitchen_ops WHERE status = 'pending' ORDER BY op_id")
            .map_err(|e| e.to_string())?;
        let rows: Vec<(i64, String, String)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .map_err(|e| e.to_string())?
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())?;

        // server sale id -> (status, op ids of every pending move of that sale)
        let mut order: Vec<String> = Vec::new();
        let mut by_sale: std::collections::HashMap<String, (String, Vec<i64>)> = std::collections::HashMap::new();
        for (op_id, sale_ref, status) in rows {
            let sale_id = if sale_ref.starts_with("local:") {
                match crate::offline_api::server_id_for_local(&conn, &sale_ref) {
                    Some(id) => id,
                    None => continue, // its sale has not uploaded yet
                }
            } else {
                sale_ref
            };
            let entry = by_sale.entry(sale_id.clone()).or_insert_with(|| {
                order.push(sale_id.clone());
                (status.clone(), Vec::new())
            });
            entry.0 = status;
            entry.1.push(op_id);
        }
        let mut sendable = Vec::new();
        let mut superseded = Vec::new();
        for sale_id in order {
            if let Some((status, mut ids)) = by_sale.remove(&sale_id) {
                let last = ids.pop().unwrap_or_default();
                superseded.extend(ids);
                sendable.push((sale_id, status, vec![last]));
            }
        }
        (sendable, superseded)
    };
    if sendable.is_empty() {
        return Ok(());
    }

    let ops: Vec<Value> = sendable
        .iter()
        .map(|(sale_id, status, ids)| json!({ "opId": ids[0], "saleId": sale_id, "kitchenStatus": status }))
        .collect();
    let resp = client
        .post(format!("{API_BASE}/api/device/kitchen-status"))
        .bearer_auth(token)
        .json(&json!({ "ops": ops }))
        .send()
        .map_err(|e| format!("Could not reach the server — {e}"))?;
    if !resp.status().is_success() {
        return Err(error_from(resp).0);
    }
    let body: Value = resp.json().map_err(|e| format!("Unexpected response — {e}"))?;

    let conn = db.0.lock().map_err(|e| e.to_string())?;
    for id in superseded {
        let _ = conn.execute("UPDATE kitchen_ops SET status = 'synced' WHERE op_id = ?", [id]);
    }
    for r in body["results"].as_array().cloned().unwrap_or_default() {
        let Some(op_id) = r["opId"].as_i64() else { continue };
        if r["ok"].as_bool() == Some(true) {
            let _ = conn.execute("UPDATE kitchen_ops SET status = 'synced', error = NULL WHERE op_id = ?", [op_id]);
            if let Some((sale_id, status, _)) = sendable.iter().find(|(_, _, ids)| ids[0] == op_id) {
                crate::offline_api::set_cache_kitchen(&conn, sale_id, status);
            }
        } else {
            let err = r["error"].as_str().unwrap_or("Rejected by the server");
            let _ = conn.execute("UPDATE kitchen_ops SET status = 'failed', error = ?2 WHERE op_id = ?1", params![op_id, err]);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- full sync

fn do_sync(db: &Db, retry_rejected: bool, force_snapshot: bool) -> Result<Option<String>, (String, bool)> {
    let token = device_token().map_err(|e| (e, false))?;
    let client = http_client(30).map_err(|e| (e, false))?;

    if retry_rejected {
        let conn = db.0.lock().map_err(|e| (e.to_string(), false))?;
        conn.execute("UPDATE sales_outbox SET status = 'pending', error = NULL WHERE status = 'rejected'", [])
            .map_err(|e| (e.to_string(), false))?;
    }

    // Sales first, so the snapshot we pull next already reflects their stock changes.
    let note = upload_pending(db, &client, &token, None)?;
    // Ticket Rail moves; a failure here must not stop the snapshot download below.
    let _ = upload_kitchen_ops(db, &client, &token);

    let due = {
        let conn = db.0.lock().map_err(|e| (e.to_string(), false))?;
        let last: Option<String> = conn
            .query_row("SELECT last_sync_at FROM sync_state WHERE id = 1", [], |r| r.get(0))
            .optional()
            .map_err(|e| (e.to_string(), false))?
            .flatten();
        match last.as_deref().and_then(|v| DateTime::parse_from_rfc3339(v).ok()) {
            Some(d) => Utc::now() - d.with_timezone(&Utc) >= ChronoDuration::minutes(BACKGROUND_SNAPSHOT_EVERY_MINUTES),
            None => true,
        }
    };
    if !force_snapshot && !due {
        return Ok(note);
    }

    let resp = client
        .get(format!("{API_BASE}/api/device/sync"))
        .bearer_auth(&token)
        .send()
        .map_err(|e| (format!("Could not reach the server — {e}"), false))?;
    if !resp.status().is_success() {
        return Err(error_from(resp));
    }
    let snap: Value = resp.json().map_err(|e| (format!("Unexpected response — {e}"), false))?;
    local_db::apply_snapshot(db, &snap).map_err(|e| (e, false))?;
    Ok(note)
}

/// Shared by the `sync_now` command and the background thread.
pub fn run_sync(app: &AppHandle, retry_rejected: bool, force_snapshot: bool) -> Result<SyncStatus, String> {
    if SYNCING.swap(true, Ordering::SeqCst) {
        return Err("A sync is already running".to_string());
    }
    let _guard = SyncGuard;
    let db = app.state::<Db>();

    // Changes made offline on the other screens go up first, so the snapshot below can't overwrite them.
    crate::api_bridge::replay_queue(app);

    match do_sync(&db, retry_rejected, force_snapshot) {
        Err((msg, revoked)) => {
            set_last_error(&db, &msg, revoked);
            return Err(msg);
        }
        Ok(Some(note)) => set_last_error(&db, &note, false),
        Ok(None) => {
            if let Ok(conn) = db.0.lock() {
                // Keep telling the user about offline changes the server refused, until they age out.
                let note = crate::offline_cache::rejected_summary(&conn);
                let _ = conn.execute("UPDATE sync_state SET last_error = ?1 WHERE id = 1", [note]);
            }
        }
    }
    let conn = db.0.lock().map_err(|e| e.to_string())?;
    status_from(&conn)
}

#[tauri::command]
pub async fn sync_now(app: AppHandle, retry_rejected: Option<bool>) -> Result<SyncStatus, String> {
    tauri::async_runtime::spawn_blocking(move || run_sync(&app, retry_rejected.unwrap_or(false), true))
        .await
        .map_err(|e| e.to_string())?
}

#[tauri::command]
pub fn get_sync_status(db: State<Db>) -> Result<SyncStatus, String> {
    let conn = db.0.lock().map_err(|e| e.to_string())?;
    status_from(&conn)
}

// ---------------------------------------------------------------- cached reads

/// Returns the last snapshot of one data kind, in the same JSON shape the web API returns:
/// products -> [..], tables -> [..], areas -> [..], payment_methods -> [..],
/// settings -> { restaurant, settings }, permissions -> { role: { module: bool } }, meta -> { subStatus }.
#[tauri::command]
pub fn get_cached_data(kind: String, db: State<Db>) -> Result<Value, String> {
    if !["products", "tables", "areas", "payment_methods", "settings", "permissions", "meta"].contains(&kind.as_str()) {
        return Err(format!("Unknown data kind: {kind}"));
    }
    let conn = db.0.lock().map_err(|e| e.to_string())?;
    let raw: Option<String> = conn
        .query_row("SELECT json FROM cache WHERE kind = ?", [&kind], |r| r.get(0))
        .optional()
        .map_err(|e| e.to_string())?;
    match raw {
        Some(s) => serde_json::from_str(&s).map_err(|e| e.to_string()),
        None => Ok(match kind.as_str() {
            "settings" => json!({ "restaurant": null, "settings": {} }),
            "permissions" | "meta" => json!({}),
            _ => json!([]),
        }),
    }
}

#[tauri::command]
pub fn search_local_customers(q: String, db: State<Db>) -> Result<Value, String> {
    let conn = db.0.lock().map_err(|e| e.to_string())?;
    let raw: Option<String> = conn
        .query_row("SELECT json FROM cache WHERE kind = 'customers'", [], |r| r.get(0))
        .optional()
        .map_err(|e| e.to_string())?;
    let all: Vec<Value> = raw.and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default();
    let needle = q.trim().to_lowercase();
    let hits: Vec<Value> = all
        .into_iter()
        .filter(|c| {
            c["name"].as_str().unwrap_or("").to_lowercase().contains(&needle)
                || c["phone"].as_str().unwrap_or("").to_lowercase().contains(&needle)
        })
        .take(50)
        .collect();
    Ok(json!(hits))
}

// ---------------------------------------------------------------- offline sale

/// Saves a sale locally (always succeeds while unlocked). The sale is saved to the local outbox and the cashier gets this device's own order id
/// (D<device>-<counter>, e.g. D2-0045) immediately; the upload happens in the background.
#[tauri::command]
pub async fn create_local_sale(app: AppHandle, payload: Value) -> Result<Value, String> {
    tauri::async_runtime::spawn_blocking(move || -> Result<Value, String> {
        let db = app.state::<Db>();

        let client_sale_id = payload["clientSaleId"].as_str().filter(|s| !s.is_empty()).ok_or("Missing sale id")?.to_string();
        if payload["items"].as_array().map_or(true, |a| a.is_empty()) {
            return Err("Order must have at least one item".to_string());
        }
        // The cashier is whoever is signed in on this device — never trusted from the UI.
        let (employee_id, employee_name) = app
            .state::<SessionState>()
            .0
            .lock()
            .map_err(|e| e.to_string())?
            .as_ref()
            .map(|s| (s.employee_id.clone(), s.name.clone()))
            .ok_or("Not signed in")?;

        let mut sale = payload.clone();
        sale["cashierEmployeeId"] = json!(employee_id);
        sale["cashierName"] = json!(employee_name);
        sale["soldAt"] = json!(Utc::now().to_rfc3339());

        let seq: i64 = {
            let conn = db.0.lock().map_err(|e| e.to_string())?;
            let status = status_from(&conn)?;
            if status.locked {
                return Err(status.lock_reason.unwrap_or_else(|| "Sync required before selling".to_string()));
            }
            // New sale -> queue it. Same id again: a sale still waiting ('pending') or already
            // uploaded ('synced') is left exactly as it is; one the server REJECTED is put back in
            // the queue with the current cart, so pressing Save/Print again after fixing the cause
            // really retries instead of replaying the old error forever.
            conn.execute(
                "INSERT INTO sales_outbox (client_sale_id, payload, created_at) VALUES (?1, ?2, ?3)
                 ON CONFLICT(client_sale_id) DO UPDATE
                   SET payload = excluded.payload, status = 'pending', error = NULL
                   WHERE sales_outbox.status = 'rejected'",
                params![client_sale_id, sale.to_string(), Utc::now().to_rfc3339()],
            )
            .map_err(|e| e.to_string())?;
            conn.query_row("SELECT seq FROM sales_outbox WHERE client_sale_id = ?", [&client_sale_id], |r| r.get(0))
                .map_err(|e| e.to_string())?
        };

        // An existing customer edited at checkout: show the new details in the saved customer list
        // now; the server record is updated when the sale uploads.
        if let (Some(cid), Some(update)) = (payload["customerId"].as_str(), payload.get("customerUpdate")) {
            if update.is_object() {
                if let Ok(conn) = db.0.lock() {
                    crate::offline_api::update_cached_customer(&conn, cid, update);
                }
            }
        }

        // Offline first: the sale is already safely stored in the outbox above, so the cashier is
        // released right now. The upload runs in the background (and the regular sync keeps retrying
        // it); a failure just means it waits in the queue.
        let order_id = {
            let conn = db.0.lock().map_err(|e| e.to_string())?;
            crate::local_db::display_id(&conn, seq)
        };

        let bg_app = app.clone();
        let bg_id = client_sale_id.clone();
        std::thread::spawn(move || {
            let db = bg_app.state::<Db>();
            if let (Ok(token), Ok(client)) = (device_token(), http_client(20)) {
                let _ = upload_pending(&db, &client, &token, Some(&bg_id));
            }
        });

        Ok(json!({ "orderNo": order_id, "offline": false }))
    })
    .await
    .map_err(|e| e.to_string())?
}
