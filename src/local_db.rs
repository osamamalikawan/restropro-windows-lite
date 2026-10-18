use rusqlite::{params, Connection};
use std::sync::Mutex;

pub struct Db(pub Mutex<Connection>);

fn db_path() -> std::path::PathBuf {
    dirs::data_dir().unwrap().join("restropro-lite").join("local.db")
}

pub fn init() -> Db {
    let path = db_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let conn = Connection::open(&path).expect("failed to open local database");
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS device_info (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            restaurant_id TEXT NOT NULL,
            restaurant_name TEXT NOT NULL,
            slug TEXT NOT NULL,
            last_synced_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS staff_cache (
            employee_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            role TEXT NOT NULL,
            pin_hash TEXT NOT NULL,
            status TEXT NOT NULL
        );
        -- Last server snapshot, one JSON document per kind (products, tables, areas,
        -- payment_methods, settings, customers). Replaced wholesale on every sync.
        CREATE TABLE IF NOT EXISTS cache (
            kind TEXT PRIMARY KEY,
            json TEXT NOT NULL
        );
        -- Sales rung up on this device. 'pending' -> uploaded -> 'synced', or 'rejected' when
        -- the server refused it (reason in `error`; kept so nothing is silently lost).
        CREATE TABLE IF NOT EXISTS sales_outbox (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            client_sale_id TEXT NOT NULL UNIQUE,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            order_no TEXT,
            error TEXT
        );
        -- Ticket Rail moves made while offline (or on a sale that has not uploaded yet). Uploaded by
        -- the sync; sale_ref is the server sale id, or local:<clientSaleId> for a sale still on this device.
        CREATE TABLE IF NOT EXISTS kitchen_ops (
            op_id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_ref TEXT NOT NULL,
            kitchen_status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            error TEXT
        );
        CREATE TABLE IF NOT EXISTS sync_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_sync_at TEXT,
            max_clock_seen TEXT,
            last_error TEXT,
            revoked INTEGER NOT NULL DEFAULT 0
        );
        INSERT OR IGNORE INTO sync_state (id) VALUES (1);
        ",
    )
    .expect("failed to initialize local database schema");
    // Read cache + queued changes for the Dashboard / Menu / Expenses / Accounts / Settings / Notifications screens.
    crate::offline_cache::init(&conn).expect("failed to initialize the offline cache tables");
    // Added after the first release: the server's id of an uploaded sale (so Ticket Rail moves made
    // while it was only on this device can be applied to it). SQLite has no "ADD COLUMN IF NOT
    // EXISTS", so a "duplicate column" error on later starts is expected and ignored.
    let _ = conn.execute("ALTER TABLE sales_outbox ADD COLUMN server_sale_id TEXT", []);
    Db(Mutex::new(conn))
}

#[derive(serde::Deserialize)]
pub struct StaffCacheRow {
    pub id: String,
    pub name: String,
    pub role: String,
    pub pin_hash: String,
    pub status: String,
}

pub fn store_device_and_roster(
    db: &Db,
    restaurant_id: &str,
    restaurant_name: &str,
    slug: &str,
    roster: &[StaffCacheRow],
) -> Result<(), String> {
    let mut conn = db.0.lock().map_err(|e| e.to_string())?;
    let tx = conn.transaction().map_err(|e| e.to_string())?;

    let now = chrono::Utc::now().to_rfc3339();
    tx.execute(
        "INSERT INTO device_info (id, restaurant_id, restaurant_name, slug, last_synced_at)
         VALUES (1, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET restaurant_id=excluded.restaurant_id,
            restaurant_name=excluded.restaurant_name, slug=excluded.slug, last_synced_at=excluded.last_synced_at",
        params![restaurant_id, restaurant_name, slug, now],
    )
    .map_err(|e| e.to_string())?;

    tx.execute(
        "UPDATE sync_state SET last_sync_at = ?1, max_clock_seen = ?1, last_error = NULL, revoked = 0 WHERE id = 1",
        params![now],
    )
    .map_err(|e| e.to_string())?;

    tx.execute("DELETE FROM staff_cache", []).map_err(|e| e.to_string())?;
    for row in roster {
        tx.execute(
            "INSERT INTO staff_cache (employee_id, name, role, pin_hash, status) VALUES (?, ?, ?, ?, ?)",
            params![row.id, row.name, row.role, row.pin_hash, row.status],
        )
        .map_err(|e| e.to_string())?;
    }

    tx.commit().map_err(|e| e.to_string())
}

/// Replace the local copy of the server data with a fresh /api/device/sync response.
/// One transaction: either the whole snapshot lands or none of it does.
pub fn apply_snapshot(db: &Db, snap: &serde_json::Value) -> Result<(), String> {
    let roster: Vec<StaffCacheRow> = serde_json::from_value(snap["staffRoster"].clone())
        .map_err(|e| format!("Bad staff roster in sync response — {e}"))?;

    let mut conn = db.0.lock().map_err(|e| e.to_string())?;
    let tx = conn.transaction().map_err(|e| e.to_string())?;

    let kinds: [(&str, &serde_json::Value); 9] = [
        ("products", &snap["products"]),
        ("tables", &snap["tables"]),
        ("areas", &snap["areas"]),
        ("payment_methods", &snap["paymentMethods"]),
        ("customers", &snap["customers"]),
        // role -> module -> can_view, and subscription status: lets the sidebar, page guards and
        // top bar behave like the web app while offline.
        ("permissions", &snap["permissionMatrix"]),
        ("meta", &serde_json::json!({ "subStatus": snap["subStatus"] })),
        (
            "settings",
            &serde_json::json!({ "restaurant": snap["restaurant"], "settings": snap["settings"] }),
        ),
        // This device's number on the server (1, 2, 3 ...). Used for order ids like D2-0045.
        // Only overwritten when the server sends one, so an older server never erases it.
        ("device", &serde_json::json!({ "deviceNo": snap["deviceNo"] })),
    ];
    for (kind, value) in kinds {
        if kind == "device" && value["deviceNo"].is_null() {
            continue;
        }
        tx.execute(
            "INSERT INTO cache (kind, json) VALUES (?1, ?2) ON CONFLICT(kind) DO UPDATE SET json = excluded.json",
            params![kind, value.to_string()],
        )
        .map_err(|e| e.to_string())?;
    }

    if let Some(rows) = snap["recentSales"].as_array() {
        crate::offline_api::merge_sales_into_cache(&tx, rows)?;
    }

    tx.execute("DELETE FROM staff_cache", []).map_err(|e| e.to_string())?;
    for row in &roster {
        tx.execute(
            "INSERT INTO staff_cache (employee_id, name, role, pin_hash, status) VALUES (?, ?, ?, ?, ?)",
            params![row.id, row.name, row.role, row.pin_hash, row.status],
        )
        .map_err(|e| e.to_string())?;
    }

    if let Some(name) = snap["restaurant"]["name"].as_str() {
        tx.execute("UPDATE device_info SET restaurant_name = ? WHERE id = 1", params![name])
            .map_err(|e| e.to_string())?;
    }

    let now = chrono::Utc::now().to_rfc3339();
    tx.execute(
        "UPDATE sync_state SET last_sync_at = ?1, max_clock_seen = ?1, last_error = NULL, revoked = 0 WHERE id = 1",
        params![now],
    )
    .map_err(|e| e.to_string())?;
    tx.execute("UPDATE device_info SET last_synced_at = ? WHERE id = 1", params![now])
        .map_err(|e| e.to_string())?;

    tx.commit().map_err(|e| e.to_string())
}

/// This device's order id for a sale with local counter `seq`: `D<device number>-<seq, 4 digits>`
/// (for example D2-0045). The counter is the outbox row number, which only ever grows on this
/// device, so ids from different devices (and different offline sessions) can never collide.
/// Falls back to the old provisional `L<seq>` until the device has been numbered by a sync.
pub fn display_id(conn: &Connection, seq: i64) -> String {
    let device_no: Option<i64> = conn
        .query_row("SELECT json FROM cache WHERE kind = 'device'", [], |r| r.get::<_, String>(0))
        .ok()
        .and_then(|j| serde_json::from_str::<serde_json::Value>(&j).ok())
        .and_then(|v| v["deviceNo"].as_i64());
    match device_no {
        Some(n) if n > 0 => format!("D{n}-{seq:04}"),
        _ => format!("L{seq}"),
    }
}
