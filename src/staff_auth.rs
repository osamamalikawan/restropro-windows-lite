use crate::local_db::{Db, StaffCacheRow};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use tauri::State;

#[derive(Serialize, Clone)]
pub struct DeviceInfo {
    restaurant_id: String,
    restaurant_name: String,
    slug: String,
}

#[derive(Serialize)]
pub struct StaffListEntry {
    id: String,
    name: String,
    role: String,
}

#[derive(Serialize, Clone)]
pub struct LocalSession {
    pub employee_id: String,
    pub name: String,
    role: String,
    restaurant_id: String,
    restaurant_name: String,
}

#[derive(Default)]
pub struct SessionState(pub Mutex<Option<LocalSession>>);

#[tauri::command]
pub fn get_device_info(db: State<Db>) -> Result<Option<DeviceInfo>, String> {
    let conn = db.0.lock().map_err(|e| e.to_string())?;
    let result = conn.query_row(
        "SELECT restaurant_id, restaurant_name, slug FROM device_info WHERE id = 1",
        [],
        |r| {
            Ok(DeviceInfo {
                restaurant_id: r.get(0)?,
                restaurant_name: r.get(1)?,
                slug: r.get(2)?,
            })
        },
    );
    match result {
        Ok(info) => {
            // A device is only "activated" while its token is in the OS credential store. If the
            // token is gone (credential store cleared, or an old build that never saved it), report
            // "not activated" so the login screen offers activation again instead of leaving a
            // device that can never sync. A credential-store *error* is not treated as missing.
            if matches!(crate::secure_store::secure_get("device_token".to_string()), Ok(None)) {
                return Ok(None);
            }
            Ok(Some(info))
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

#[tauri::command]
pub fn get_cached_staff_list(db: State<Db>) -> Result<Vec<StaffListEntry>, String> {
    let conn = db.0.lock().map_err(|e| e.to_string())?;
    let mut stmt = conn
        .prepare("SELECT employee_id, name, role FROM staff_cache WHERE status = 'active' ORDER BY name")
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |r| {
            Ok(StaffListEntry {
                id: r.get(0)?,
                name: r.get(1)?,
                role: r.get(2)?,
            })
        })
        .map_err(|e| e.to_string())?;
    rows.collect::<Result<Vec<_>, _>>().map_err(|e| e.to_string())
}

#[tauri::command]
pub fn verify_staff_pin(
    employee_id: String,
    pin: String,
    db: State<Db>,
    session: State<SessionState>,
) -> Result<LocalSession, String> {
    let device = get_device_info(db.clone())?.ok_or("Device is not activated yet")?;

    let (name, role, pin_hash, status): (String, String, String, String) = {
        let conn = db.0.lock().map_err(|e| e.to_string())?;
        conn.query_row(
            "SELECT name, role, pin_hash, status FROM staff_cache WHERE employee_id = ?",
            [&employee_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .map_err(|_| "Employee not found".to_string())?
    };

    if status != "active" {
        return Err("This employee is not active".to_string());
    }

    let valid = bcrypt::verify(&pin, &pin_hash).map_err(|e| e.to_string())?;
    if !valid {
        return Err("Incorrect PIN — try again".to_string());
    }

    let local_session = LocalSession {
        employee_id,
        name,
        role,
        restaurant_id: device.restaurant_id,
        restaurant_name: device.restaurant_name,
    };

    *session.0.lock().map_err(|e| e.to_string())? = Some(local_session.clone());

    Ok(local_session)
}

#[tauri::command]
pub fn get_local_session(session: State<SessionState>) -> Result<Option<LocalSession>, String> {
    Ok(session.0.lock().map_err(|e| e.to_string())?.clone())
}

#[tauri::command]
pub fn staff_logout(session: State<SessionState>) -> Result<(), String> {
    *session.0.lock().map_err(|e| e.to_string())? = None;
    Ok(())
}

const ACTIVATE_URL: &str = "https://restropro-lite.vercel.app/api/device/activate";

#[derive(Deserialize)]
struct ActivateResponse {
    #[serde(rename = "deviceToken")]
    device_token: String,
    #[serde(rename = "restaurantId")]
    restaurant_id: String,
    #[serde(rename = "restaurantName")]
    restaurant_name: String,
    slug: String,
    #[serde(rename = "staffRoster")]
    staff_roster: Vec<RosterRow>,
}

#[derive(Deserialize)]
struct RosterRow {
    id: String,
    name: String,
    role: String,
    pin_hash: String,
    status: String,
}

#[tauri::command]
pub fn activate_device(email: String, password: String, db: State<Db>, app: tauri::AppHandle) -> Result<DeviceInfo, String> {
    let client = reqwest::blocking::Client::new();
    let resp = client
        .post(ACTIVATE_URL)
        .json(&serde_json::json!({ "email": email, "password": password }))
        .send()
        .map_err(|e| format!("Could not reach the server — {e}"))?;

    if !resp.status().is_success() {
        let body: serde_json::Value = resp.json().unwrap_or_default();
        let msg = body.get("error").and_then(|v| v.as_str()).unwrap_or("Activation failed");
        return Err(msg.to_string());
    }

    let data: ActivateResponse = resp.json().map_err(|e| format!("Unexpected response — {e}"))?;

    crate::secure_store::secure_set("device_token".to_string(), data.device_token)?;

    let roster: Vec<StaffCacheRow> = data
        .staff_roster
        .into_iter()
        .map(|r| StaffCacheRow {
            id: r.id,
            name: r.name,
            role: r.role,
            pin_hash: r.pin_hash,
            status: r.status,
        })
        .collect();

    crate::local_db::store_device_and_roster(&db, &data.restaurant_id, &data.restaurant_name, &data.slug, &roster)?;

    // First full download (menu, tables, settings, customers) so the POS opens with data.
    // Failure is fine — the background sync retries — so the error is deliberately ignored.
    let _ = crate::sync::run_sync(&app, false, true);

    Ok(DeviceInfo {
        restaurant_id: data.restaurant_id,
        restaurant_name: data.restaurant_name,
        slug: data.slug,
    })
}

#[tauri::command]
pub fn deactivate_device(db: State<Db>) -> Result<(), String> {
    let conn = db.0.lock().map_err(|e| e.to_string())?;
    conn.execute("DELETE FROM device_info", []).map_err(|e| e.to_string())?;
    conn.execute("DELETE FROM staff_cache", []).map_err(|e| e.to_string())?;
    drop(conn);
    crate::secure_store::secure_delete("device_token".to_string())
}