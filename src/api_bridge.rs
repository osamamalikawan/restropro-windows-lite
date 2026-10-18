//! Lets the bundled screens keep calling `fetch("/api/...")`: the web layer hands each request to
//! `api_request`, which forwards it to the cloud server with this device's credentials and the
//! id of the cashier signed in on the device (PIN checked locally). Server-side, each request is
//! re-authorised from the database (employee active + role + same restaurant + device not revoked).
//!
//! Offline: Sales and the POS are answered from this device (offline_api.rs). Every other lite screen
//! (Dashboard, Menu, Expenses, Accounts, Settings, Notifications) shows the last data the cloud sent
//! (read cache), and changes made there are queued and uploaded when the cloud answers again
//! (offline_cache.rs). Anything else fails fast with a network error instead of hanging.

use crate::local_db::Db;
use crate::offline_api as offline;
use crate::offline_cache as oc;
use crate::staff_auth::SessionState;
use crate::sync::API_BASE;
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;
use tauri::{AppHandle, Manager};

/// Never reachable through the bridge, whatever the screen asks for.
const BLOCKED_PREFIXES: [&str; 4] = ["/api/device/", "/api/super-admin", "/api/cron", "/api/staff/login"];

/// When the cloud last failed to answer (epoch ms; 0 = it is answering). Requests made within a few
/// seconds of a failure skip the network and go straight to the offline path, so a dead connection
/// doesn't make every screen wait for a timeout.
static LAST_NET_FAIL: AtomicI64 = AtomicI64::new(0);
const OFFLINE_HINT_MS: i64 = 8_000;

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
fn recently_failed() -> bool {
    let t = LAST_NET_FAIL.load(Ordering::Relaxed);
    t != 0 && now_ms() - t < OFFLINE_HINT_MS
}
fn mark_failed() {
    LAST_NET_FAIL.store(now_ms(), Ordering::Relaxed);
}
fn mark_ok() {
    LAST_NET_FAIL.store(0, Ordering::Relaxed);
}

#[derive(Serialize)]
pub struct ApiReply {
    status: u16,
    content_type: Option<String>,
    body: String,
}

fn json_reply(status: u16, v: Value) -> ApiReply {
    ApiReply { status, content_type: Some("application/json".to_string()), body: v.to_string() }
}

pub fn check_path(path: &str) -> Result<(), String> {
    if !path.starts_with("/api/") || path.contains("..") || path.contains('\\') || path.contains("//") {
        return Err("Blocked request path".to_string());
    }
    let lower = path.to_lowercase();
    if BLOCKED_PREFIXES.iter().any(|p| lower.starts_with(p)) {
        return Err("This request is not available from the desktop app".to_string());
    }
    Ok(())
}

enum NetError {
    /// The cloud could not be reached (no internet, DNS, timeout).
    Offline(String),
    /// Something local (not activated, not signed in) — not a connectivity problem.
    Local(String),
}

fn http(app: &AppHandle, method: &reqwest::Method, path: &str, body: Option<&str>, content_type: Option<&str>) -> Result<ApiReply, NetError> {
    http_as(app, None, method, path, body, content_type)
}

/// `as_employee`: act for this cashier (an offline change is uploaded as whoever made it, whoever is
/// signed in by then). None = the cashier signed in right now.
fn http_as(app: &AppHandle, as_employee: Option<String>, method: &reqwest::Method, path: &str, body: Option<&str>, content_type: Option<&str>) -> Result<ApiReply, NetError> {
    let token = crate::secure_store::secure_get("device_token".to_string())
        .map_err(NetError::Local)?
        .ok_or_else(|| NetError::Local("This device is not activated yet".to_string()))?;
    let employee_id = match as_employee {
        Some(id) => id,
        None => app
            .state::<SessionState>()
            .0
            .lock()
            .map_err(|e| NetError::Local(e.to_string()))?
            .as_ref()
            .map(|s| s.employee_id.clone())
            .ok_or_else(|| NetError::Local("Not signed in".to_string()))?,
    };

    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(4))
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| NetError::Local(e.to_string()))?;

    let mut req = client
        .request(method.clone(), format!("{API_BASE}{path}"))
        .bearer_auth(token)
        .header("x-staff-id", employee_id);
    if let Some(b) = body {
        req = req.header("content-type", content_type.unwrap_or("application/json")).body(b.to_string());
    }

    let resp = req.send().map_err(|e| NetError::Offline(format!("Could not reach the server — {e}")))?;
    let status = resp.status().as_u16();
    let content_type = resp.headers().get("content-type").and_then(|v| v.to_str().ok()).map(|s| s.to_string());
    let body = resp.text().map_err(|e| NetError::Offline(format!("Could not read the server response — {e}")))?;
    Ok(ApiReply { status, content_type, body })
}

/// Try the cloud (unless it just failed). Ok = it answered (any HTTP status).
fn try_http(app: &AppHandle, method: &reqwest::Method, path: &str, body: Option<&str>, content_type: Option<&str>) -> Result<ApiReply, NetError> {
    if recently_failed() {
        return Err(NetError::Offline("You're offline — this needs an internet connection".to_string()));
    }
    match http(app, method, path, body, content_type) {
        Ok(r) => {
            mark_ok();
            Ok(r)
        }
        Err(NetError::Offline(e)) => {
            mark_failed();
            Err(NetError::Offline(e))
        }
        Err(e) => Err(e),
    }
}

fn net_msg(e: NetError) -> String {
    match e {
        NetError::Offline(m) | NetError::Local(m) => m,
    }
}

/// A body that names a sale rung up on this device ("local:<id>") is rewritten to the server's id
/// once that sale has uploaded; before that, the server can't act on it.
fn remap_local_sale(db: &Db, body: &str) -> Result<String, ApiReply> {
    let Ok(mut v) = serde_json::from_str::<Value>(body) else { return Ok(body.to_string()) };
    let Some(sale_id) = v["saleId"].as_str().filter(|s| s.starts_with("local:")).map(str::to_string) else {
        return Ok(body.to_string());
    };
    let conn = db.0.lock().map_err(|e| json_reply(500, json!({ "error": e.to_string() })))?;
    match offline::server_id_for_local(&conn, &sale_id) {
        Some(server_id) => {
            v["saleId"] = json!(server_id);
            Ok(v.to_string())
        }
        None => Err(json_reply(
            409,
            json!({ "error": "This order was rung up on this device and hasn't synced yet. Try again once the device is online and has synced." }),
        )),
    }
}

fn kitchen_status(app: &AppHandle, db: &Db, path: &str, body: Option<String>, content_type: Option<String>) -> Result<ApiReply, String> {
    let raw = body.unwrap_or_default();
    let v: Value = serde_json::from_str(&raw).unwrap_or(Value::Null);
    let (Some(sale_ref), Some(status)) = (v["saleId"].as_str().map(str::to_string), v["kitchenStatus"].as_str().map(str::to_string)) else {
        return Ok(json_reply(400, json!({ "error": "saleId and a valid kitchenStatus are required" })));
    };
    if !offline::KITCHEN_STATUSES.contains(&status.as_str()) {
        return Ok(json_reply(400, json!({ "error": "saleId and a valid kitchenStatus are required" })));
    }

    // A sale that is still only on this device: remember the move, the sync applies it later.
    let mut server_id = sale_ref.clone();
    if sale_ref.starts_with("local:") {
        let conn = db.0.lock().map_err(|e| e.to_string())?;
        match offline::server_id_for_local(&conn, &sale_ref) {
            Some(id) => server_id = id,
            None => {
                offline::record_kitchen_op(&conn, &sale_ref, &status)?;
                return Ok(json_reply(200, json!({ "success": true, "queued": true })));
            }
        }
    }

    let forwarded = json!({ "saleId": server_id, "kitchenStatus": status }).to_string();
    match try_http(app, &reqwest::Method::POST, path, Some(&forwarded), content_type.as_deref()) {
        Ok(reply) => {
            if reply.status == 200 {
                if let Ok(conn) = db.0.lock() {
                    offline::set_cache_kitchen(&conn, &server_id, &status);
                }
            }
            Ok(reply)
        }
        Err(NetError::Offline(_)) => {
            let conn = db.0.lock().map_err(|e| e.to_string())?;
            offline::record_kitchen_op(&conn, &server_id, &status)?;
            Ok(json_reply(200, json!({ "success": true, "queued": true })))
        }
        Err(NetError::Local(m)) => Err(m),
    }
}

/// The cashier signed in on this device, if any.
fn current_employee(app: &AppHandle) -> Option<String> {
    app.state::<SessionState>().0.lock().ok()?.as_ref().map(|s| s.employee_id.clone())
}

fn has_pending_changes(db: &Db) -> bool {
    db.0.lock().map(|c| oc::pending_count(&c) > 0).unwrap_or(false)
}

/// Upload the changes made offline, oldest first. Stops quietly at the first sign the cloud can't be
/// reached (they stay queued). A change the server refuses is kept as "rejected" with the reason.
pub fn replay_queue(app: &AppHandle) {
    let db = app.state::<Db>();
    let mut uploaded_any = false;
    loop {
        let next = match db.0.lock() {
            Ok(conn) => oc::next_pending(&conn),
            Err(_) => return,
        };
        let Some(p) = next else { break };
        let method = reqwest::Method::from_bytes(p.method.as_bytes()).unwrap_or(reqwest::Method::POST);
        match http_as(app, p.employee_id.clone(), &method, &p.path, Some(&p.body), None) {
            Ok(reply) => {
                mark_ok();
                let Ok(conn) = db.0.lock() else { return };
                if (200..300).contains(&reply.status) {
                    oc::finish(&conn, p.seq, None);
                    uploaded_any = true;
                } else if reply.status == 401 || reply.status >= 500 {
                    break; // not the change's fault: try again later
                } else {
                    let why = serde_json::from_str::<Value>(&reply.body)
                        .ok()
                        .and_then(|v| v["error"].as_str().map(str::to_string))
                        .unwrap_or_else(|| format!("HTTP {}", reply.status));
                    oc::finish(&conn, p.seq, Some(&why));
                }
            }
            Err(NetError::Offline(_)) => {
                mark_failed();
                break;
            }
            Err(NetError::Local(_)) => break,
        }
    }
    if let Ok(conn) = db.0.lock() {
        if let Some(msg) = oc::rejected_summary(&conn) {
            let _ = conn.execute("UPDATE sync_state SET last_error = ?1 WHERE id = 1", [msg]);
        }
    }
    if uploaded_any {
        LAST_WARM.store(0, Ordering::Relaxed); // the server now has new data: refresh the saved copies soon
    }
}

/// Fetch every screen's default data in the background (at most every 15 minutes) so each one has a
/// saved copy for offline use, including screens never opened while online.
static LAST_WARM: AtomicI64 = AtomicI64::new(0);
static WARMING: AtomicBool = AtomicBool::new(false);
const WARM_EVERY_MS: i64 = 15 * 60 * 1000;

fn maybe_warm(app: &AppHandle) {
    if now_ms() - LAST_WARM.load(Ordering::Relaxed) < WARM_EVERY_MS || WARMING.swap(true, Ordering::SeqCst) {
        return;
    }
    LAST_WARM.store(now_ms(), Ordering::Relaxed);
    let app = app.clone();
    std::thread::spawn(move || {
        for path in oc::warm_paths() {
            if recently_failed() {
                break;
            }
            if let Ok(reply) = try_http(&app, &reqwest::Method::GET, &path, None, None) {
                if reply.status == 200 {
                    if let Ok(conn) = app.state::<Db>().0.lock() {
                        oc::store_get(&conn, &path, &reply.body);
                    }
                }
            }
        }
        WARMING.store(false, Ordering::SeqCst);
    });
}

fn handle(app: AppHandle, method: reqwest::Method, path: String, body: Option<String>, content_type: Option<String>) -> Result<ApiReply, String> {
    let db = app.state::<Db>();
    let (base, qp) = offline::split_path(&path);

    // Changes made offline go up before anything else is asked of the server.
    if !recently_failed() && has_pending_changes(&db) {
        replay_queue(&app);
    }

    if method == reqwest::Method::POST && base == "/api/sales/kitchen-status" {
        return kitchen_status(&app, &db, &path, body, content_type);
    }

    // Other changes to a sale made on this device (cancel, take payment, edit): map its id, or say
    // plainly that it has to upload first.
    let body = match body {
        Some(b) if method != reqwest::Method::GET => match remap_local_sale(&db, &b) {
            Ok(b) => Some(b),
            Err(reply) => return Ok(reply),
        },
        other => other,
    };

    let local_get = method == reqwest::Method::GET && offline::is_local_capable_get(&path);
    let cacheable = method == reqwest::Method::GET && oc::cacheable_get(base);
    match try_http(&app, &method, &path, body.as_deref(), content_type.as_deref()) {
        Ok(mut reply) => {
            if cacheable && reply.status == 200 {
                if let Ok(conn) = db.0.lock() {
                    oc::store_get(&conn, &path, &reply.body);
                }
            }
            maybe_warm(&app);
            if local_get && reply.status == 200 && base == "/api/sales" {
                if let Ok(conn) = db.0.lock() {
                    if let Some(adjusted) = offline::adjust_online_sales(&conn, &reply.body, &qp) {
                        reply.body = adjusted;
                    }
                }
            }
            Ok(reply)
        }
        Err(NetError::Offline(msg)) => {
            if local_get {
                let conn = db.0.lock().map_err(|e| e.to_string())?;
                if let Some(v) = offline::serve_get_local(&conn, &path) {
                    return Ok(json_reply(200, v));
                }
            }
            if cacheable {
                let conn = db.0.lock().map_err(|e| e.to_string())?;
                if let Some(saved) = oc::lookup_get(&conn, &path) {
                    return Ok(ApiReply { status: 200, content_type: Some("application/json".to_string()), body: saved });
                }
            }
            if oc::queueable_write(method.as_str(), base) {
                let employee = current_employee(&app);
                let conn = db.0.lock().map_err(|e| e.to_string())?;
                return Ok(match oc::queue_write(&conn, method.as_str(), &path, body.as_deref().unwrap_or(""), employee.as_deref()) {
                    Ok(v) => json_reply(200, v),
                    Err(m) => json_reply(400, json!({ "error": m })),
                });
            }
            Err(msg)
        }
        Err(e) => Err(net_msg(e)),
    }
}

#[tauri::command]
pub async fn api_request(
    app: AppHandle,
    method: String,
    path: String,
    body: Option<String>,
    content_type: Option<String>,
) -> Result<ApiReply, String> {
    check_path(&path)?;
    let method = reqwest::Method::from_bytes(method.to_uppercase().as_bytes()).map_err(|_| "Bad method".to_string())?;
    if ![reqwest::Method::GET, reqwest::Method::POST, reqwest::Method::PUT, reqwest::Method::PATCH, reqwest::Method::DELETE].contains(&method) {
        return Err("Method not allowed".to_string());
    }
    tauri::async_runtime::spawn_blocking(move || handle(app, method, path, body, content_type))
        .await
        .map_err(|e| e.to_string())?
}

/// Is the cloud reachable right now? A quick TCP connect (no HTTP, no login), polled by the UI to
/// decide whether to show the "You're offline" page. Also resets/sets the fast-fail hint above.
#[tauri::command]
pub async fn check_online() -> bool {
    tauri::async_runtime::spawn_blocking(|| {
        use std::net::{TcpStream, ToSocketAddrs};
        let host = API_BASE.trim_start_matches("https://").trim_start_matches("http://").trim_end_matches('/');
        let addr = if host.contains(':') { host.to_string() } else { format!("{host}:443") };
        let ok = addr
            .to_socket_addrs()
            .ok()
            .and_then(|mut it| it.next())
            .map(|a| TcpStream::connect_timeout(&a, Duration::from_secs(3)).is_ok())
            .unwrap_or(false);
        if ok {
            mark_ok();
        } else {
            mark_failed();
        }
        ok
    })
    .await
    .unwrap_or(false)
}
