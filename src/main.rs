// Restro Pro POS desktop shell.
//
// This app has exactly one job beyond showing the web app in a window: give the page a way
// to send raw ESC/POS bytes to a thermal printer (network TCP, Windows print queue, or raw
// USB), since that is not possible from plain browser JavaScript. Everything else (UI,
// business logic, receipt formatting) stays in the Next.js app bundle, downloaded and
// verified by bundle_updater and served locally through the app:// scheme below.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::io::Write;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

use nusb::descriptors::TransferType;
use nusb::transfer::{Bulk, Direction, Out};
use nusb::MaybeFuture;
use serde::Serialize;

use windows::core::{PCWSTR, PWSTR};
use windows::Win32::Foundation::HANDLE;
use windows::Win32::Graphics::Printing::{
    ClosePrinter, EndDocPrinter, EndPagePrinter, EnumPrintersW, OpenPrinterW, StartDocPrinterW,
    StartPagePrinter, WritePrinter, DOC_INFO_1W, PRINTER_ENUM_LOCAL, PRINTER_INFO_4W,
};
use tauri_plugin_updater::UpdaterExt;
mod secure_store;
mod bundle_updater;
mod local_db;
mod offline_api;
mod offline_cache;
mod staff_auth;
mod sync;
mod api_bridge;
mod serial_print;

mod shell_updater;

use tauri::Manager;

fn to_wide(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------------------
// Network (TCP) printing
// ---------------------------------------------------------------------------------------

#[tauri::command]
fn print_raw(ip: String, port: u16, data: Vec<u8>) -> Result<(), String> {
    let addr: SocketAddr = format!("{ip}:{port}")
        .to_socket_addrs()
        .map_err(|e| format!("Could not resolve {ip}:{port} — {e}"))?
        .next()
        .ok_or_else(|| format!("No address found for {ip}:{port}"))?;

    let mut stream = TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT)
        .map_err(|e| format!("Could not reach printer at {ip}:{port} — {e}. Check the IP, that the printer is powered on, and that this PC is on the same network."))?;

    stream
        .set_write_timeout(Some(WRITE_TIMEOUT))
        .map_err(|e| format!("Could not configure the connection — {e}"))?;

    stream
        .write_all(&data)
        .map_err(|e| format!("Connected to {ip}:{port} but the print job failed partway — {e}"))?;

    Ok(())
}

#[tauri::command]
fn test_printer_connection(ip: String, port: u16) -> Result<(), String> {
    let addr: SocketAddr = format!("{ip}:{port}")
        .to_socket_addrs()
        .map_err(|e| format!("Could not resolve {ip}:{port} — {e}"))?
        .next()
        .ok_or_else(|| format!("No address found for {ip}:{port}"))?;

    TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT)
        .map(|_| ())
        .map_err(|e| format!("Could not reach printer at {ip}:{port} — {e}"))
}

// ---------------------------------------------------------------------------------------
// Windows print queue (RAW spooler) printing
// ---------------------------------------------------------------------------------------

#[tauri::command]
fn list_windows_printers() -> Result<Vec<String>, String> {
    unsafe {
        let mut needed = 0u32;
        let mut returned = 0u32;

        let _ = EnumPrintersW(
            PRINTER_ENUM_LOCAL,
            PCWSTR::null(),
            4,
            None,
            &mut needed,
            &mut returned,
        );

        if needed == 0 {
            return Ok(vec![]);
        }

        let mut backing = vec![0u64; (needed as usize + 7) / 8];
        let bytes = std::slice::from_raw_parts_mut(backing.as_mut_ptr() as *mut u8, needed as usize);

        EnumPrintersW(
            PRINTER_ENUM_LOCAL,
            PCWSTR::null(),
            4,
            Some(bytes),
            &mut needed,
            &mut returned,
        )
        .map_err(|e| format!("Could not list printers — {e}"))?;

        let infos = std::slice::from_raw_parts(
            backing.as_ptr() as *const PRINTER_INFO_4W,
            returned as usize,
        );

        Ok(infos
            .iter()
            .map(|info| {
                let name_ptr = info.pPrinterName.0;
                if name_ptr.is_null() {
                    return String::new();
                }
                let len = (0..).take_while(|&i| *name_ptr.add(i) != 0).count();
                String::from_utf16_lossy(std::slice::from_raw_parts(name_ptr, len))
            })
            .filter(|name| !name.is_empty())
            .collect())
    }
}

#[tauri::command]
fn print_raw_windows(printer_name: String, data: Vec<u8>) -> Result<(), String> {
    unsafe {
        let printer_name_w = to_wide(&printer_name);
        let mut handle = HANDLE::default();

        OpenPrinterW(PCWSTR(printer_name_w.as_ptr()), &mut handle, None).map_err(|e| {
            format!("Could not open printer \"{printer_name}\" — {e}. Check the name matches exactly what's shown in Printers & scanners.")
        })?;

        let mut doc_name_w = to_wide("Restro Pro Receipt");
        let mut datatype_w = to_wide("RAW");

        let doc_info = DOC_INFO_1W {
            pDocName: PWSTR(doc_name_w.as_mut_ptr()),
            pOutputFile: PWSTR::null(),
            pDatatype: PWSTR(datatype_w.as_mut_ptr()),
        };

        let job_id = StartDocPrinterW(handle, 1, &doc_info);
        if job_id == 0 {
            let _ = ClosePrinter(handle);
            return Err("Could not start the print job (StartDocPrinterW failed).".to_string());
        }

        if !StartPagePrinter(handle).as_bool() {
            let _ = EndDocPrinter(handle);
            let _ = ClosePrinter(handle);
            return Err("Could not start the print page (StartPagePrinter failed).".to_string());
        }

        let mut bytes_written: u32 = 0;
        let write_ok = WritePrinter(
            handle,
            data.as_ptr() as *const _,
            data.len() as u32,
            &mut bytes_written,
        )
        .as_bool();

        let _ = EndPagePrinter(handle);
        let _ = EndDocPrinter(handle);
        let _ = ClosePrinter(handle);

        if !write_ok || bytes_written as usize != data.len() {
            return Err(format!(
                "Print job failed partway — wrote {bytes_written} of {} bytes.",
                data.len()
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Raw USB printing (WinUSB via nusb) — only for printers with no usable Windows driver
// ---------------------------------------------------------------------------------------

#[derive(Serialize)]
struct UsbDeviceInfo {
    vendor_id: u16,
    product_id: u16,
    manufacturer: Option<String>,
    product: Option<String>,
    serial: Option<String>,
}

#[tauri::command]
fn list_usb_printers() -> Result<Vec<UsbDeviceInfo>, String> {
    let devices = nusb::list_devices()
        .wait()
        .map_err(|e| format!("Could not list USB devices — {e}"))?;
    Ok(devices
        .map(|d| UsbDeviceInfo {
            vendor_id: d.vendor_id(),
            product_id: d.product_id(),
            manufacturer: d.manufacturer_string().map(|s| s.to_string()),
            product: d.product_string().map(|s| s.to_string()),
            serial: d.serial_number().map(|s| s.to_string()),
        })
        .collect())
}

#[tauri::command]
fn print_raw_usb(vendor_id: u16, product_id: u16, data: Vec<u8>) -> Result<(), String> {
    let device_info = nusb::list_devices()
        .wait()
        .map_err(|e| format!("Could not list USB devices — {e}"))?
        .find(|d| d.vendor_id() == vendor_id && d.product_id() == product_id)
        .ok_or_else(|| format!("USB device {vendor_id:04x}:{product_id:04x} is not connected"))?;

    let device = device_info.open().wait().map_err(|e| {
        format!("Could not open the USB device — {e}. On Windows it usually needs to be bound to the generic WinUSB driver via Zadig first.")
    })?;

    let mut target: Option<(u8, u8)> = None;
    'search: for config in device.configurations() {
        for iface in config.interfaces() {
            for alt in iface.alt_settings() {
                for ep in alt.endpoints() {
                    if ep.transfer_type() == TransferType::Bulk && ep.direction() == Direction::Out {
                        target = Some((iface.interface_number(), ep.address()));
                        break 'search;
                    }
                }
            }
        }
    }
    let (interface_number, endpoint_address) = target.ok_or_else(|| {
        "Could not find a bulk OUT endpoint on this USB device — it may not be a printer, or its descriptors are non-standard.".to_string()
    })?;

    let interface = device.claim_interface(interface_number).wait().map_err(|e| {
        format!("Could not claim the USB interface — {e}. Another app (or a real driver still bound to this device) may be using it.")
    })?;

    let mut writer = interface
        .endpoint::<Bulk, Out>(endpoint_address)
        .map_err(|e| format!("Could not open endpoint 0x{endpoint_address:02x} — {e}"))?
        .writer(4096);

    writer.write_all(&data).map_err(|e| format!("USB write failed — {e}"))?;
    writer.flush().map_err(|e| format!("USB write failed to flush — {e}"))?;
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Static asset resolution for the app:// scheme
// ---------------------------------------------------------------------------------------

fn resolve_static_file(base: &std::path::Path, raw_path: &str) -> Option<std::path::PathBuf> {
    let candidates = [
        base.join(raw_path),
        base.join(format!("{raw_path}.html")),
        base.join(raw_path).join("index.html"),
    ];
    candidates.into_iter().find(|p| p.is_file())
}

fn main() {
    let db = local_db::init();

    tauri::Builder::default()
        // Shell application updater plugins
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_process::init())
        .manage(db)
        .manage(staff_auth::SessionState::default())
        .manage(shell_updater::ShellUpdateState::default())
        .register_uri_scheme_protocol("app", |_app, request| {
            let raw_path = request
                .uri()
                .path()
                .trim_start_matches('/')
                .to_string();

            let raw_path = if raw_path.is_empty() {
                "index.html".to_string()
            } else {
                raw_path
            };

            let base = bundle_updater::current_bundle_path();

            let Some(base) = base else {
                let mut msg = String::from(
                    "No app bundle downloaded yet \u{2014} connect to the internet once to finish setup.",
                );
                if let Some(err) = bundle_updater::last_error() {
                    msg.push_str("\n\nLast error: ");
                    msg.push_str(&err);
                }
                return tauri::http::Response::builder()
                    .status(503)
                    .header("Content-Type", "text/plain; charset=utf-8")
                    .body(msg.into_bytes())
                    .unwrap();
            };

            match resolve_static_file(&base, &raw_path) {
                Some(file_path) => {
                    let data = std::fs::read(&file_path).unwrap_or_default();
                    let mime =
                        mime_guess::from_path(&file_path).first_or_octet_stream();

                    tauri::http::Response::builder()
                        .header("Content-Type", mime.as_ref())
                        .body(data)
                        .unwrap()
                }
                None => {
                    eprintln!(
                        "404: no match for {raw_path:?} under {base:?}"
                    );

                    let not_found = base.join("404.html");

                    if not_found.is_file() {
                        let data =
                            std::fs::read(&not_found).unwrap_or_default();

                        tauri::http::Response::builder()
                            .status(404)
                            .header("Content-Type", "text/html")
                            .body(data)
                            .unwrap()
                    } else {
                        tauri::http::Response::builder()
                            .status(404)
                            .body(b"not found".to_vec())
                            .unwrap()
                    }
                }
            }
        })
        .setup(|app| {
            // Background sync: uploads queued offline sales and refreshes
            // the local snapshot whenever internet is reachable.
            let sync_handle = app.handle().clone();

            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_secs(20));

                loop {
                    let _ = sync::run_sync(&sync_handle, false, false);

                    std::thread::sleep(Duration::from_secs(300));
                }
            });

            // Existing web bundle updater.
            // This updates the web application assets, not the Tauri shell.
            let handle = app.handle().clone();

            std::thread::spawn(move || {
                // Retry until a bundle exists, so first run on a flaky connection (or a
                // transient failure) fixes itself instead of leaving the 503 page forever.
                let mut shown = false;
                loop {
                    let result = bundle_updater::check_and_update();
                    let had_bundle = bundle_updater::current_bundle_path().is_some();

                    match &result {
                        Ok(Some(v)) => println!("Updated bundle to {v}"),
                        Ok(None) => println!("Bundle already current"),
                        Err(e) => {
                            eprintln!("Update check failed (using cached bundle if any): {e}");
                            if !had_bundle {
                                // Surface the real reason instead of a generic message.
                                bundle_updater::set_last_error(e.clone());
                            }
                        }
                    }

                    if let Some(window) = handle.get_webview_window("main") {
                        if had_bundle {
                            let _ = window.eval("window.location.reload()");
                        }
                        if !shown {
                            let _ = window.show();
                            shown = true;
                        }
                    }

                    if had_bundle {
                        break;
                    }
                    std::thread::sleep(Duration::from_secs(30));
                }
            });

            // Shell updater: checks for a new desktop application release
            // every 30 minutes. This is separate from the web bundle updater.
            let shell_handle = app.handle().clone();

            std::thread::spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        eprintln!(
                            "Failed to initialize shell updater runtime: {e}"
                        );
                        return;
                    }
                };

                rt.block_on(async move {
                    loop {
                        if let Ok(updater) = shell_handle.updater() {
                            match updater.check().await {
                                Ok(Some(update)) => {
                                    let version = update.version.clone();

                                    let state = shell_handle
                                        .state::<shell_updater::ShellUpdateState>();

                                    *state.0.lock().unwrap() =
                                        Some(version.clone());

                                    println!(
                                        "Shell update available: {version}"
                                    );
                                }
                                Ok(None) => {
                                    println!(
                                        "Shell application is up to date"
                                    );
                                }
                                Err(e) => {
                                    eprintln!(
                                        "Shell update check failed: {e}"
                                    );
                                }
                            }
                        }

                        tokio::time::sleep(
                            std::time::Duration::from_secs(30 * 60),
                        )
                        .await;
                    }
                });
            });

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            print_raw,
            test_printer_connection,
            list_windows_printers,
            print_raw_windows,
            serial_print::list_serial_ports,
            serial_print::check_serial_port,
            serial_print::print_raw_serial,
            list_usb_printers,
            print_raw_usb,
            secure_store::secure_set,
            secure_store::secure_get,
            secure_store::secure_delete,
            staff_auth::get_device_info,
            staff_auth::get_cached_staff_list,
            staff_auth::verify_staff_pin,
            staff_auth::activate_device,
            staff_auth::get_local_session,
            staff_auth::staff_logout,
            staff_auth::deactivate_device,
            sync::sync_now,
            sync::get_sync_status,
            sync::get_cached_data,
            sync::search_local_customers,
            sync::create_local_sale,
            api_bridge::api_request,
            api_bridge::check_online,

            // Shell updater commands
            shell_updater::check_for_shell_update,
            shell_updater::get_shell_update_status,
            shell_updater::install_shell_update
        ])
        .run(tauri::generate_context!())
        .expect("error while running Restro Pro POS");
}