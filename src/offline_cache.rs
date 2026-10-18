//! Offline support for the lite screens that don't have their own local tables (Dashboard, Menu,
//! Expenses, Accounts, Settings, Notifications). Two pieces, both kept in the local SQLite file:
//!
//!  1. READ CACHE (`api_cache`): every successful `GET` of a screen's data is stored as the server
//!     sent it. With no internet the same URL is answered from that copy (or, if that exact URL was
//!     never fetched, the newest copy of the same endpoint), so a page opens with its last synced data.
//!  2. WRITE QUEUE (`write_queue`): a change made while offline (add/edit/delete a product, category,
//!     expense, payment method, setting ...) is stored and ALSO applied to the cached data, so the
//!     screen shows it straight away. `api_bridge` uploads the queue, oldest first, as soon as the
//!     cloud answers again (before anything else is requested), using the id of the cashier who made it.
//!
//! New rows get a client-made UUID (the server upserts on `id`), so uploading the same change twice
//! can't create a duplicate.
//!
//! This module only needs rusqlite + serde_json, so it can be unit-tested without Tauri.

use chrono::{Local, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};

const KEEP_PER_BASE: i64 = 30;

pub fn init(conn: &Connection) -> rusqlite::Result<()> {
    let r = init_tables(conn);
    // Refused changes are only kept for a week so the warning doesn't stay forever.
    let cutoff = (Utc::now() - chrono::Duration::days(7)).to_rfc3339();
    let _ = conn.execute("DELETE FROM write_queue WHERE status = 'rejected' AND created_at < ?1", [cutoff]);
    r
}

fn init_tables(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "
        -- Last answer of every cached GET, keyed by the full path + query.
        CREATE TABLE IF NOT EXISTS api_cache (
            key TEXT PRIMARY KEY,
            base TEXT NOT NULL,
            body TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS api_cache_base ON api_cache (base, updated_at);
        -- Changes made offline, uploaded oldest first. status: pending | rejected (the server said no).
        CREATE TABLE IF NOT EXISTS write_queue (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            method TEXT NOT NULL,
            path TEXT NOT NULL,
            body TEXT NOT NULL,
            employee_id TEXT,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            error TEXT
        );
        ",
    )
}

// ------------------------------------------------------------------ which endpoints

pub fn base_of(path: &str) -> &str {
    path.split_once('?').map(|(b, _)| b).unwrap_or(path)
}

fn query_of(path: &str) -> &str {
    path.split_once('?').map(|(_, q)| q).unwrap_or("")
}

fn qp(path: &str, name: &str) -> Option<String> {
    query_of(path)
        .split('&')
        .filter_map(|p| p.split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| crate::offline_api::percent_decode(v))
}

/// GETs whose answer is remembered for offline use.
pub fn cacheable_get(base: &str) -> bool {
    matches!(
        base,
        "/api/dashboard"
            | "/api/dashboard/charts"
            | "/api/products"
            | "/api/menu-categories"
            | "/api/expenses"
            | "/api/expense-categories"
            | "/api/accounts"
            | "/api/accounts/products"
            | "/api/notifications"
            | "/api/gallery-images"
            | "/api/shifts/current"
    )
}

/// Changes that may be made offline and queued.
pub fn queueable_write(method: &str, base: &str) -> bool {
    method == "POST"
        && matches!(
            base,
            "/api/products"
                | "/api/menu-categories"
                | "/api/expense-categories"
                | "/api/payment-methods"
                | "/api/expenses"
                | "/api/settings"
                | "/api/reorder"
                | "/api/notifications"
        )
}

/// The default GETs fetched in the background so every screen has something to show offline,
/// even one never opened while online.
pub fn warm_paths() -> Vec<String> {
    let tz = Local::now().offset().local_minus_utc() / 60;
    vec![
        "/api/dashboard".to_string(),
        format!("/api/dashboard/charts?tz={tz}"),
        "/api/products".to_string(),
        "/api/menu-categories".to_string(),
        "/api/expense-categories".to_string(),
        "/api/expenses?limit=50&offset=0".to_string(),
        "/api/accounts?limit=50&offset=0".to_string(),
        "/api/accounts/products".to_string(),
        "/api/notifications".to_string(),
        format!("/api/shifts/current?tz={tz}"),
    ]
}

// ------------------------------------------------------------------ read cache

pub fn store_get(conn: &Connection, path: &str, body: &str) {
    let base = base_of(path);
    if !cacheable_get(base) || serde_json::from_str::<Value>(body).is_err() {
        return;
    }
    let now = Utc::now().to_rfc3339();
    let _ = conn.execute(
        "INSERT INTO api_cache (key, base, body, updated_at) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(key) DO UPDATE SET body = excluded.body, updated_at = excluded.updated_at",
        params![path, base, body, now],
    );
    // Many filter combinations (date ranges, searches) pile up: keep the newest few per endpoint.
    let _ = conn.execute(
        "DELETE FROM api_cache WHERE base = ?1 AND key NOT IN
           (SELECT key FROM api_cache WHERE base = ?1 ORDER BY updated_at DESC LIMIT ?2)",
        params![base, KEEP_PER_BASE],
    );
}

/// Offline answer for a GET: this exact URL, or the newest copy of the same endpoint. A later page
/// of a list (offset > 0) is never guessed.
pub fn lookup_get(conn: &Connection, path: &str) -> Option<String> {
    let exact: Option<String> = conn.query_row("SELECT body FROM api_cache WHERE key = ?1", [path], |r| r.get(0)).optional().ok().flatten();
    if exact.is_some() {
        return exact;
    }
    if qp(path, "offset").map(|o| o != "0" && !o.is_empty()).unwrap_or(false) {
        return None;
    }
    conn.query_row("SELECT body FROM api_cache WHERE base = ?1 ORDER BY updated_at DESC LIMIT 1", [base_of(path)], |r| r.get(0))
        .optional()
        .ok()
        .flatten()
}

fn entries(conn: &Connection, base: &str) -> Vec<(String, Value)> {
    let Ok(mut st) = conn.prepare("SELECT key, body FROM api_cache WHERE base = ?1") else { return vec![] };
    let rows = st.query_map([base], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)));
    match rows {
        Ok(it) => it.filter_map(|r| r.ok()).filter_map(|(k, b)| serde_json::from_str::<Value>(&b).ok().map(|v| (k, v))).collect(),
        Err(_) => vec![],
    }
}

fn put_entry(conn: &Connection, key: &str, v: &Value) {
    let _ = conn.execute("UPDATE api_cache SET body = ?1 WHERE key = ?2", params![v.to_string(), key]);
}

/// Run `f` on the array under `field` of every cached copy of `base`, saving the ones it changed.
fn edit_lists(conn: &Connection, base: &str, field: &str, mut f: impl FnMut(&mut Vec<Value>, &Value) -> bool) {
    for (key, mut body) in entries(conn, base) {
        let key_ref = key.clone();
        let snapshot = body.clone();
        let Some(list) = body.get_mut(field).and_then(|v| v.as_array_mut()) else { continue };
        if f(list, &snapshot) {
            put_entry(conn, &key_ref, &body);
        }
    }
}

// Plain lists kept in the snapshot cache (payment methods, settings) by the regular sync.
fn kind_get(conn: &Connection, kind: &str) -> Option<Value> {
    conn.query_row("SELECT json FROM cache WHERE kind = ?1", [kind], |r| r.get::<_, String>(0)).ok().and_then(|j| serde_json::from_str(&j).ok())
}
fn kind_put(conn: &Connection, kind: &str, v: &Value) {
    let _ = conn.execute(
        "INSERT INTO cache (kind, json) VALUES (?1, ?2) ON CONFLICT(kind) DO UPDATE SET json = excluded.json",
        params![kind, v.to_string()],
    );
}

// ------------------------------------------------------------------ ids

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A random-looking v4 UUID. Not cryptographic, only needs to be unique across devices.
pub fn new_uuid() -> String {
    let mut h = Sha256::new();
    h.update(Utc::now().timestamp_nanos_opt().unwrap_or(0).to_le_bytes());
    h.update(COUNTER.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    h.update(std::process::id().to_le_bytes());
    h.update(Local::now().offset().local_minus_utc().to_le_bytes());
    let d = h.finalize();
    let mut b = [0u8; 16];
    b.copy_from_slice(&d[..16]);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32])
}

fn id_of(v: &Value) -> Option<&str> {
    v.get("id").and_then(|x| x.as_str())
}

/// Insert `row` into `list`, or merge its fields into the row with the same id.
fn upsert_row(list: &mut Vec<Value>, row: &Value, defaults: Value) {
    let Some(obj) = row.as_object() else { return };
    let id = obj.get("id").and_then(|x| x.as_str()).unwrap_or("");
    let target = list.iter_mut().find(|r| id_of(r) == Some(id) && !id.is_empty());
    match target {
        Some(existing) => {
            if let Some(eo) = existing.as_object_mut() {
                for (k, v) in obj {
                    if k != "restaurant_id" && k != "menu_categories" {
                        eo.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        None => {
            let mut fresh = defaults.as_object().cloned().unwrap_or_default();
            for (k, v) in obj {
                if k != "restaurant_id" && k != "menu_categories" {
                    fresh.insert(k.clone(), v.clone());
                }
            }
            list.push(Value::Object(fresh));
        }
    }
}

fn sort_by_sort_order(list: &mut [Value]) {
    list.sort_by_key(|r| r.get("sort_order").and_then(|x| x.as_i64()).unwrap_or(i64::MAX));
}

// ------------------------------------------------------------------ products / categories

fn category_join(conn: &Connection, category_id: &str) -> Value {
    for (_, body) in entries(conn, "/api/menu-categories") {
        if let Some(list) = body.get("categories").and_then(|v| v.as_array()) {
            if let Some(c) = list.iter().find(|c| id_of(c) == Some(category_id)) {
                return json!({
                    "name": c.get("name").cloned().unwrap_or(Value::Null),
                    "name_ur": c.get("name_ur").cloned().unwrap_or(Value::Null),
                    "sort_order": c.get("sort_order").cloned().unwrap_or(Value::Null),
                    "color": c.get("color").cloned().unwrap_or(Value::Null),
                });
            }
        }
    }
    Value::Null
}

/// After a category changed: refresh the category info shown on every cached product.
fn refresh_product_joins(conn: &Connection) {
    let cats: Vec<Value> = entries(conn, "/api/menu-categories")
        .into_iter()
        .next()
        .and_then(|(_, b)| b.get("categories").and_then(|v| v.as_array()).cloned())
        .unwrap_or_default();
    edit_lists(conn, "/api/products", "products", |list, _| {
        for p in list.iter_mut() {
            let cid = p.get("category_id").and_then(|x| x.as_str()).unwrap_or("").to_string();
            let join = cats
                .iter()
                .find(|c| id_of(c) == Some(cid.as_str()))
                .map(|c| {
                    json!({
                        "name": c.get("name").cloned().unwrap_or(Value::Null),
                        "name_ur": c.get("name_ur").cloned().unwrap_or(Value::Null),
                        "sort_order": c.get("sort_order").cloned().unwrap_or(Value::Null),
                        "color": c.get("color").cloned().unwrap_or(Value::Null),
                    })
                })
                .unwrap_or(Value::Null);
            if let Some(o) = p.as_object_mut() {
                o.insert("menu_categories".into(), join);
            }
        }
        true
    });
}

fn overlay_categories(conn: &Connection, body: &Value) {
    let op = body.get("op").and_then(|x| x.as_str()).unwrap_or("");
    match op {
        "delete" => {
            let id = body["row"]["id"].as_str().unwrap_or("").to_string();
            edit_lists(conn, "/api/menu-categories", "categories", |list, _| {
                list.retain(|c| id_of(c) != Some(id.as_str()));
                true
            });
        }
        "reorder" => {
            let ids: Vec<String> = body["ids"].as_array().map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect()).unwrap_or_default();
            edit_lists(conn, "/api/menu-categories", "categories", |list, _| {
                for (i, id) in ids.iter().enumerate() {
                    if let Some(c) = list.iter_mut().find(|c| id_of(c) == Some(id.as_str())) {
                        c["sort_order"] = json!(i);
                    }
                }
                sort_by_sort_order(list);
                true
            });
        }
        _ => {
            let row = &body["row"];
            edit_lists(conn, "/api/menu-categories", "categories", |list, _| {
                let next = list.len() as i64;
                upsert_row(list, row, json!({ "is_active": true, "sort_order": next, "color": null, "name_ur": null }));
                sort_by_sort_order(list);
                true
            });
        }
    }
    refresh_product_joins(conn);
}

fn overlay_products(conn: &Connection, body: &Value) {
    let op = body.get("op").and_then(|x| x.as_str()).unwrap_or("");
    if op == "delete" {
        let id = body["row"]["id"].as_str().unwrap_or("").to_string();
        edit_lists(conn, "/api/products", "products", |list, _| {
            list.retain(|p| id_of(p) != Some(id.as_str()));
            true
        });
        return;
    }
    let row = body["row"].clone();
    let cid = row.get("category_id").and_then(|x| x.as_str()).map(str::to_string);
    let join = cid.as_deref().map(|c| category_join(conn, c));
    edit_lists(conn, "/api/products", "products", |list, _| {
        upsert_row(list, &row, json!({ "is_available": true, "image_url": null, "name_ur": null, "price": 0 }));
        if let (Some(j), Some(id)) = (join.as_ref(), id_of(&row)) {
            if let Some(p) = list.iter_mut().find(|p| id_of(p) == Some(id)) {
                p["menu_categories"] = j.clone();
            }
        }
        true
    });
}

// ------------------------------------------------------------------ expenses

fn today() -> String {
    Local::now().format("%Y-%m-%d").to_string()
}

fn overlay_expense(conn: &Connection, path_body: &Value, id: &str) {
    let amount = path_body["amount"].as_f64().unwrap_or(0.0);
    let category = path_body["category"].as_str().unwrap_or("").to_string();
    let row = json!({
        "id": id,
        "category": category,
        "expense_type": path_body["expenseType"].as_str().unwrap_or("regular"),
        "amount": amount,
        "vendor": path_body.get("vendor").cloned().unwrap_or(Value::Null),
        "description": path_body.get("description").cloned().unwrap_or(Value::Null),
        "payment_method": path_body["paymentMethod"].as_str().unwrap_or("Cash"),
        "txn_date": today(),
    });
    let now = today();
    for (key, mut body) in entries(conn, "/api/expenses") {
        // Only lists that would contain it: first page, matching the search and the date range.
        if qp(&key, "offset").map(|o| o != "0" && !o.is_empty()).unwrap_or(false) {
            continue;
        }
        if let Some(q) = qp(&key, "q").filter(|q| !q.trim().is_empty()) {
            if !category.to_lowercase().contains(&q.trim().to_lowercase()) {
                continue;
            }
        }
        if qp(&key, "from").filter(|f| !f.is_empty()).map(|f| now.as_str() < f.as_str()).unwrap_or(false) {
            continue;
        }
        if qp(&key, "to").filter(|t| !t.is_empty()).map(|t| now.as_str() > t.as_str()).unwrap_or(false) {
            continue;
        }
        if let Some(list) = body.get_mut("expenses").and_then(|v| v.as_array_mut()) {
            list.insert(0, row.clone());
            let limit = qp(&key, "limit").and_then(|l| l.parse::<usize>().ok()).unwrap_or(50);
            if list.len() > limit {
                list.truncate(limit);
            }
        }
        if let Some(t) = body.get("total").and_then(|v| v.as_f64()) {
            body["total"] = json!(t + amount);
        }
        if let Some(c) = body.get("count").and_then(|v| v.as_i64()) {
            body["count"] = json!(c + 1);
        }
        put_entry(conn, &key, &body);
    }
}

// ------------------------------------------------------------------ payment methods, categories, settings, notifications

fn overlay_payment_methods(conn: &Connection, body: &Value) {
    let mut list = kind_get(conn, "payment_methods").and_then(|v| v.as_array().cloned()).unwrap_or_default();
    match body.get("op").and_then(|x| x.as_str()).unwrap_or("") {
        "delete" => {
            let id = body["row"]["id"].as_str().unwrap_or("");
            list.retain(|m| id_of(m) != Some(id));
        }
        _ => {
            let name = body["row"]["name"].as_str().unwrap_or("").trim().to_string();
            if !name.is_empty() && !list.iter().any(|m| m["name"].as_str().map(|n| n.eq_ignore_ascii_case(&name)).unwrap_or(false)) {
                list.push(json!({ "id": body["row"]["id"], "name": name }));
            }
        }
    }
    kind_put(conn, "payment_methods", &Value::Array(list));
}

fn overlay_reorder(conn: &Connection, body: &Value) {
    if body["kind"].as_str() != Some("payment_methods") {
        return;
    }
    let ids: Vec<&str> = body["ids"].as_array().map(|a| a.iter().filter_map(|x| x.as_str()).collect()).unwrap_or_default();
    let mut list = kind_get(conn, "payment_methods").and_then(|v| v.as_array().cloned()).unwrap_or_default();
    list.sort_by_key(|m| ids.iter().position(|i| Some(*i) == id_of(m)).unwrap_or(usize::MAX));
    for (i, m) in list.iter_mut().enumerate() {
        if let Some(o) = m.as_object_mut() {
            o.insert("sort_order".into(), json!(i + 1));
        }
    }
    kind_put(conn, "payment_methods", &Value::Array(list));
}

fn overlay_expense_categories(conn: &Connection, body: &Value) {
    let op = body.get("op").and_then(|x| x.as_str()).unwrap_or("");
    let row = body["row"].clone();
    edit_lists(conn, "/api/expense-categories", "categories", |list, _| {
        if op == "delete" {
            let id = row["id"].as_str().unwrap_or("");
            list.retain(|c| id_of(c) != Some(id));
        } else {
            let name = row["name"].as_str().unwrap_or("").trim().to_string();
            if name.is_empty() || list.iter().any(|c| c["name"].as_str().map(|n| n.eq_ignore_ascii_case(&name)).unwrap_or(false)) {
                return false;
            }
            list.push(json!({ "id": row["id"], "name": name }));
            list.sort_by_key(|c| c["name"].as_str().unwrap_or("").to_lowercase());
        }
        true
    });
}

/// POST /api/settings body key -> restaurant_settings column (same map as the server route).
const SETTINGS_FIELDS: [(&str, &str); 27] = [
    ("serviceChargeRate", "service_charge_rate"),
    ("shiftStart", "shift_start"),
    ("shiftEnd", "shift_end"),
    ("utcOffsetMinutes", "utc_offset_minutes"),
    ("singleDeviceLogin", "single_device_login"),
    ("idleLogoutMinutes", "idle_logout_minutes"),
    ("cashTaxRate", "cash_tax_rate"),
    ("cardTaxRate", "card_tax_rate"),
    ("posShowKitchenPrint", "pos_show_kitchen_print"),
    ("posShowPrintInvoice", "pos_show_print_invoice"),
    ("printerName", "printer_name"),
    ("paperWidth", "paper_width"),
    ("connection", "connection"),
    ("autoPrint", "auto_print"),
    ("receiptHeader", "receipt_header"),
    ("receiptFooter", "receipt_footer"),
    ("fbrEnabled", "fbr_enabled"),
    ("fbrNtn", "fbr_ntn"),
    ("fbrStrn", "fbr_strn"),
    ("fbrPosId", "fbr_pos_id"),
    ("fbrApiToken", "fbr_api_token"),
    ("fbrEnvironment", "fbr_environment"),
    ("fbrFee", "fbr_fee"),
    ("receiptTemplate", "receipt_template"),
    ("showDiscount", "show_discount"),
    ("notePresets", "note_presets"),
    ("urduEnabled", "urdu_enabled"),
];

fn overlay_settings(conn: &Connection, body: &Value) {
    let mut cur = kind_get(conn, "settings").unwrap_or_else(|| json!({ "restaurant": {}, "settings": {} }));
    if !cur["restaurant"].is_object() {
        cur["restaurant"] = json!({});
    }
    if !cur["settings"].is_object() {
        cur["settings"] = json!({});
    }
    for (src, dst) in [("restaurantName", "name"), ("address", "address"), ("phone", "phone")] {
        if let Some(v) = body.get(src).filter(|v| v.is_string()) {
            cur["restaurant"][dst] = v.clone();
        }
    }
    for (src, col) in SETTINGS_FIELDS {
        if let Some(v) = body.get(src) {
            cur["settings"][col] = v.clone();
        }
    }
    kind_put(conn, "settings", &cur);
    if let Some(name) = body.get("restaurantName").and_then(|v| v.as_str()) {
        let _ = conn.execute("UPDATE device_info SET restaurant_name = ?1 WHERE id = 1", [name]);
    }
}

fn overlay_notifications(conn: &Connection, body: &Value) {
    let action = body["action"].as_str().unwrap_or("").to_string();
    let rid = body["recipientId"].as_str().unwrap_or("").to_string();
    let now = Utc::now().to_rfc3339();
    for (key, mut b) in entries(conn, "/api/notifications") {
        let mut unread = 0;
        if let Some(list) = b.get_mut("notifications").and_then(|v| v.as_array_mut()) {
            for n in list.iter_mut() {
                let hit = action == "markAllRead" && n["readAt"].is_null() || n["recipientId"].as_str() == Some(rid.as_str());
                if hit {
                    if action == "markUnread" {
                        n["readAt"] = Value::Null;
                        n["readBy"] = Value::Null;
                    } else {
                        n["readAt"] = json!(now);
                    }
                }
                if n["readAt"].is_null() {
                    unread += 1;
                }
            }
        }
        b["unreadCount"] = json!(unread);
        put_entry(conn, &key, &b);
    }
}

// ------------------------------------------------------------------ queue

/// Remember an offline change and apply it to the cached screens. Returns the JSON to answer the
/// screen with, or an error message (bad request).
pub fn queue_write(conn: &Connection, method: &str, path: &str, body: &str, employee_id: Option<&str>) -> Result<Value, String> {
    let base = base_of(path);
    let mut v: Value = serde_json::from_str(body).map_err(|_| "Could not read the request".to_string())?;
    if !v.is_object() {
        return Err("Could not read the request".to_string());
    }

    match base {
        "/api/menu-categories" | "/api/products" => {
            let op = v["op"].as_str().unwrap_or("").to_string();
            if op != "delete" && op != "reorder" {
                if !v["row"].is_object() {
                    return Err("op and row are required".to_string());
                }
                if v["row"]["id"].as_str().map(|s| s.is_empty()).unwrap_or(true) {
                    v["row"]["id"] = json!(new_uuid()); // the server upserts on this id
                }
            }
            if base == "/api/products" {
                overlay_products(conn, &v);
            } else {
                overlay_categories(conn, &v);
            }
        }
        "/api/expense-categories" | "/api/payment-methods" => {
            let op = v["op"].as_str().unwrap_or("").to_string();
            if op != "delete" {
                if v["row"]["name"].as_str().map(|s| s.trim().is_empty()).unwrap_or(true) {
                    return Err("Enter a name".to_string());
                }
                if v["row"]["id"].as_str().map(|s| s.is_empty()).unwrap_or(true) {
                    v["row"]["id"] = json!(new_uuid());
                }
            }
            if base == "/api/payment-methods" {
                overlay_payment_methods(conn, &v);
            } else {
                overlay_expense_categories(conn, &v);
            }
        }
        "/api/expenses" => {
            if v["category"].as_str().map(|s| s.is_empty()).unwrap_or(true) || v["amount"].as_f64().unwrap_or(0.0) == 0.0 {
                return Err("category and amount are required".to_string());
            }
            overlay_expense(conn, &v, &new_uuid());
        }
        "/api/settings" => overlay_settings(conn, &v),
        "/api/reorder" => overlay_reorder(conn, &v),
        "/api/notifications" => overlay_notifications(conn, &v),
        _ => return Err("This change needs an internet connection".to_string()),
    }

    conn.execute(
        "INSERT INTO write_queue (method, path, body, employee_id, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![method, path, v.to_string(), employee_id, Utc::now().to_rfc3339()],
    )
    .map_err(|e| e.to_string())?;
    Ok(json!({ "success": true, "ok": true, "queued": true }))
}

pub struct Pending {
    pub seq: i64,
    pub method: String,
    pub path: String,
    pub body: String,
    pub employee_id: Option<String>,
}

pub fn pending_count(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM write_queue WHERE status = 'pending'", [], |r| r.get(0)).unwrap_or(0)
}

pub fn next_pending(conn: &Connection) -> Option<Pending> {
    conn.query_row(
        "SELECT seq, method, path, body, employee_id FROM write_queue WHERE status = 'pending' ORDER BY seq LIMIT 1",
        [],
        |r| Ok(Pending { seq: r.get(0)?, method: r.get(1)?, path: r.get(2)?, body: r.get(3)?, employee_id: r.get(4)? }),
    )
    .optional()
    .ok()
    .flatten()
}

/// Uploaded (done) or refused by the server (rejected, kept with the reason so nothing vanishes silently).
pub fn finish(conn: &Connection, seq: i64, rejected_with: Option<&str>) {
    match rejected_with {
        None => {
            let _ = conn.execute("DELETE FROM write_queue WHERE seq = ?1", [seq]);
        }
        Some(err) => {
            let _ = conn.execute("UPDATE write_queue SET status = 'rejected', error = ?2 WHERE seq = ?1", params![seq, err]);
        }
    }
}

/// One line about changes the server refused, for the sync status.
pub fn rejected_summary(conn: &Connection) -> Option<String> {
    let n: i64 = conn.query_row("SELECT COUNT(*) FROM write_queue WHERE status = 'rejected'", [], |r| r.get(0)).unwrap_or(0);
    if n == 0 {
        return None;
    }
    let first: Option<String> = conn.query_row("SELECT error FROM write_queue WHERE status = 'rejected' ORDER BY seq LIMIT 1", [], |r| r.get(0)).ok().flatten();
    Some(format!("{n} change(s) made offline were refused by the server: {}", first.unwrap_or_default()))
}


#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch("CREATE TABLE cache (kind TEXT PRIMARY KEY, json TEXT NOT NULL); CREATE TABLE device_info (id INTEGER PRIMARY KEY, restaurant_name TEXT);").unwrap();
        c.execute("INSERT INTO device_info (id, restaurant_name) VALUES (1, 'Old')", []).unwrap();
        init(&c).unwrap();
        c
    }
    fn get(c: &Connection, path: &str) -> Value {
        serde_json::from_str(&lookup_get(c, path).unwrap()).unwrap()
    }

    #[test]
    fn caches_and_serves_gets() {
        let c = db();
        store_get(&c, "/api/products", r#"{"products":[{"id":"p1","name":"A"}]}"#);
        assert_eq!(get(&c, "/api/products")["products"][0]["name"], "A");
        // same endpoint, different query: newest copy; later pages are never guessed
        store_get(&c, "/api/expenses?limit=50&offset=0", r#"{"expenses":[],"hasMore":false}"#);
        assert!(lookup_get(&c, "/api/expenses?limit=50&offset=0&from=2026-01-01").is_some());
        assert!(lookup_get(&c, "/api/expenses?limit=50&offset=50").is_none());
        // not cacheable / not JSON
        store_get(&c, "/api/sales", "{}");
        assert!(lookup_get(&c, "/api/sales").is_none());
        store_get(&c, "/api/accounts", "<html>");
        assert!(lookup_get(&c, "/api/accounts").is_none());
    }

    #[test]
    fn category_and_product_edits_show_up_and_get_ids() {
        let c = db();
        store_get(&c, "/api/menu-categories", r#"{"categories":[{"id":"c1","name":"Burgers","name_ur":null,"sort_order":0,"is_active":true,"color":null}]}"#);
        store_get(&c, "/api/products", r#"{"products":[{"id":"p1","name":"Zinger","price":500,"category_id":"c1","menu_categories":null}]}"#);

        let r = queue_write(&c, "POST", "/api/menu-categories", r#"{"op":"upsert","row":{"name":"Drinks","name_ur":"مشروبات"}}"#, Some("e1")).unwrap();
        assert_eq!(r["queued"], true);
        let cats = get(&c, "/api/menu-categories");
        assert_eq!(cats["categories"].as_array().unwrap().len(), 2);
        let new_id = cats["categories"][1]["id"].as_str().unwrap().to_string();
        assert_eq!(new_id.len(), 36);
        // the queued body carries the same id, so replay upserts the very same row
        let q = next_pending(&c).unwrap();
        assert!(q.body.contains(&new_id));
        assert_eq!(q.employee_id.as_deref(), Some("e1"));

        // rename Burgers: the product's category info follows
        queue_write(&c, "POST", "/api/menu-categories", r#"{"op":"upsert","row":{"id":"c1","name":"Grill","name_ur":"گرل"}}"#, None).unwrap();
        let p = get(&c, "/api/products");
        assert_eq!(p["products"][0]["menu_categories"]["name"], "Grill");

        // new product with a category
        queue_write(&c, "POST", "/api/products", &format!(r#"{{"op":"upsert","row":{{"name":"Cola","price":150,"category_id":"{new_id}"}}}}"#), None).unwrap();
        let p = get(&c, "/api/products");
        assert_eq!(p["products"].as_array().unwrap().len(), 2);
        assert_eq!(p["products"][1]["menu_categories"]["name"], "Drinks");
        assert_eq!(p["products"][1]["is_available"], true);

        // edit + delete
        queue_write(&c, "POST", "/api/products", r#"{"op":"upsert","row":{"id":"p1","price":550}}"#, None).unwrap();
        assert_eq!(get(&c, "/api/products")["products"][0]["price"], 550);
        queue_write(&c, "POST", "/api/products", r#"{"op":"delete","row":{"id":"p1"}}"#, None).unwrap();
        assert_eq!(get(&c, "/api/products")["products"].as_array().unwrap().len(), 1);

        // reorder
        queue_write(&c, "POST", "/api/menu-categories", &format!(r#"{{"op":"reorder","ids":["{new_id}","c1"]}}"#), None).unwrap();
        assert_eq!(get(&c, "/api/menu-categories")["categories"][0]["id"], new_id.as_str());
        assert_eq!(pending_count(&c), 6);
    }

    #[test]
    fn expenses_are_added_to_matching_lists_only() {
        let c = db();
        store_get(&c, "/api/expenses?limit=50&offset=0", r#"{"expenses":[{"id":"x","category":"Rent","amount":100}],"hasMore":false,"total":100,"count":1}"#);
        store_get(&c, "/api/expenses?limit=50&offset=0&q=gas", r#"{"expenses":[],"hasMore":false,"total":0,"count":0}"#);
        queue_write(&c, "POST", "/api/expenses", r#"{"category":"Utilities","amount":250,"paymentMethod":"Cash"}"#, None).unwrap();
        let all = get(&c, "/api/expenses?limit=50&offset=0");
        assert_eq!(all["expenses"][0]["category"], "Utilities");
        assert_eq!(all["total"], 350.0);
        assert_eq!(all["count"], 2);
        assert_eq!(get(&c, "/api/expenses?limit=50&offset=0&q=gas")["count"], 0);
        assert!(queue_write(&c, "POST", "/api/expenses", r#"{"category":"","amount":0}"#, None).is_err());
    }

    #[test]
    fn payment_methods_settings_and_notifications() {
        let c = db();
        kind_put(&c, "payment_methods", &json!([{"id":"a","name":"Cash"},{"id":"b","name":"Card"}]));
        kind_put(&c, "settings", &json!({"restaurant":{"name":"Old"},"settings":{"urdu_enabled":false}}));
        queue_write(&c, "POST", "/api/payment-methods", r#"{"op":"insert","row":{"name":"UPaisa"}}"#, None).unwrap();
        queue_write(&c, "POST", "/api/payment-methods", r#"{"op":"insert","row":{"name":"cash"}}"#, None).unwrap(); // duplicate name: not shown twice
        queue_write(&c, "POST", "/api/payment-methods", r#"{"op":"delete","row":{"id":"b"}}"#, None).unwrap();
        let list = kind_get(&c, "payment_methods").unwrap();
        let names: Vec<&str> = list.as_array().unwrap().iter().map(|m| m["name"].as_str().unwrap()).collect();
        assert_eq!(names, vec!["Cash", "UPaisa"]);
        let upaisa = list[1]["id"].as_str().unwrap().to_string();
        queue_write(&c, "POST", "/api/reorder", &format!(r#"{{"kind":"payment_methods","ids":["{upaisa}","a"]}}"#), None).unwrap();
        assert_eq!(kind_get(&c, "payment_methods").unwrap()[0]["name"], "UPaisa");

        queue_write(&c, "POST", "/api/settings", r#"{"restaurantName":"New","urduEnabled":true}"#, None).unwrap();
        let s = kind_get(&c, "settings").unwrap();
        assert_eq!(s["restaurant"]["name"], "New");
        assert_eq!(s["settings"]["urdu_enabled"], true);
        let n: String = c.query_row("SELECT restaurant_name FROM device_info", [], |r| r.get(0)).unwrap();
        assert_eq!(n, "New");

        store_get(&c, "/api/notifications", r#"{"notifications":[{"recipientId":"r1","readAt":null},{"recipientId":"r2","readAt":null}],"unreadCount":2,"canMarkRead":true}"#);
        queue_write(&c, "POST", "/api/notifications", r#"{"action":"markRead","recipientId":"r1"}"#, None).unwrap();
        assert_eq!(get(&c, "/api/notifications")["unreadCount"], 1);
        queue_write(&c, "POST", "/api/notifications", r#"{"action":"markAllRead"}"#, None).unwrap();
        assert_eq!(get(&c, "/api/notifications")["unreadCount"], 0);
    }

    #[test]
    fn expense_categories_and_queue_lifecycle() {
        let c = db();
        store_get(&c, "/api/expense-categories", r#"{"categories":[{"id":"1","name":"Rent"}]}"#);
        queue_write(&c, "POST", "/api/expense-categories", r#"{"op":"insert","row":{"name":"Gas"}}"#, None).unwrap();
        assert_eq!(get(&c, "/api/expense-categories")["categories"][0]["name"], "Gas");
        assert!(queue_write(&c, "POST", "/api/expense-categories", r#"{"op":"insert","row":{"name":" "}}"#, None).is_err());
        let a = next_pending(&c).unwrap();
        finish(&c, a.seq, Some("Not permitted"));
        assert_eq!(pending_count(&c), 0);
        assert!(rejected_summary(&c).unwrap().contains("Not permitted"));
        queue_write(&c, "POST", "/api/expense-categories", r#"{"op":"delete","row":{"id":"1"}}"#, None).unwrap();
        let b = next_pending(&c).unwrap();
        finish(&c, b.seq, None);
        assert!(next_pending(&c).is_none());
        assert_eq!(new_uuid().len(), 36);
        assert_ne!(new_uuid(), new_uuid());
        assert!(queueable_write("POST", "/api/expenses") && !queueable_write("GET", "/api/expenses") && !queueable_write("POST", "/api/sales"));
    }
}
