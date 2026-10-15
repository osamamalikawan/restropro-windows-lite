use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

const MANIFEST_URL: &str = "https://github.com/osamamalikawan/restropro/releases/latest/download/latest.json";
static LAST_ERROR: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

pub fn set_last_error(e: String) {
    if let Ok(mut g) = LAST_ERROR.lock() {
        *g = Some(e);
    }
}

pub fn last_error() -> Option<String> {
    LAST_ERROR.lock().ok().and_then(|g| g.clone())
}

const SHELL_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Deserialize)]
struct Manifest {
    version: String,
    min_shell_version: String,
    url: String,
    sha256: String,
}

fn base_dir() -> PathBuf {
    dirs::data_dir().unwrap().join("restropro").join("versions")
}

fn current_pointer() -> PathBuf {
    dirs::data_dir().unwrap().join("restropro").join("current.txt")
}

pub fn current_bundle_path() -> Option<PathBuf> {
    let ptr = current_pointer();
    let version = fs::read_to_string(&ptr).ok()?;
    let dir = base_dir().join(version.trim());
    if dir.join("index.html").exists() {
        Some(dir)
    } else {
        None
    }
}

pub fn check_and_update() -> Result<Option<String>, String> {
    let response_text = reqwest::blocking::get(MANIFEST_URL)
        .map_err(|e| format!("manifest fetch failed: {e}"))?
        .text()
        .map_err(|e| format!("manifest read failed: {e}"))?;
    let cleaned = response_text.trim_start_matches('\u{feff}');
    let manifest: Manifest = serde_json::from_str(cleaned)
        .map_err(|e| format!("manifest parse failed: {e} — raw body started with: {:?}", &cleaned.chars().take(80).collect::<String>()))?;

    if !shell_supports(&manifest.min_shell_version) {
        return Err(format!(
            "This app version is too old for bundle {} (needs shell >= {}). Update the desktop app itself.",
            manifest.version, manifest.min_shell_version
        ));
    }

    let target_dir = base_dir().join(&manifest.version);
    if target_dir.join("index.html").exists() {
        fs::write(current_pointer(), &manifest.version).ok();
        return Ok(None);
    }

    let response = reqwest::blocking::get(&manifest.url)
        .map_err(|e| format!("download failed: {e}"))?;
    if !response.status().is_success() {
        return Err(format!("download failed: server returned {}", response.status()));
    }
    let bytes = response.bytes().map_err(|e| format!("download read failed: {e}"))?;

    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let actual = format!("{:x}", hasher.finalize());
    if actual != manifest.sha256 {
        return Err(format!(
            "checksum mismatch (expected {}, got {}) — refusing to install",
            manifest.sha256, actual
        ));
    }

    let tmp_dir = base_dir().join(format!("{}.tmp", manifest.version));
    if tmp_dir.exists() {
        fs::remove_dir_all(&tmp_dir)
            .map_err(|e| format!("could not clear stale temp dir {tmp_dir:?}: {e}"))?;
    }
    fs::create_dir_all(&tmp_dir)
        .map_err(|e| format!("could not create temp dir {tmp_dir:?}: {e}"))?;

    unzip_to(&bytes, &tmp_dir)?;

    if !tmp_dir.join("index.html").exists() {
        fs::remove_dir_all(&tmp_dir).ok();
        return Err("downloaded bundle has no index.html — corrupt or wrong zip layout".into());
    }

    if target_dir.exists() {
        fs::remove_dir_all(&target_dir)
            .map_err(|e| format!("could not clear existing target dir {target_dir:?} before activating new version: {e}"))?;
    }
    fs::rename(&tmp_dir, &target_dir)
        .map_err(|e| format!("could not activate new version — rename {tmp_dir:?} -> {target_dir:?} failed: {e}"))?;
    fs::write(current_pointer(), &manifest.version)
        .map_err(|e| format!("could not write current-version pointer: {e}"))?;

    prune_old_versions(&manifest.version);

    Ok(Some(manifest.version))
}

/// Unzips `bytes` into `dest`. Normalizes every entry name to forward slashes before doing
/// anything else — `Compress-Archive` (PowerShell's built-in zip cmdlet) sometimes writes
/// directory entries using the Windows-native backslash separator (e.g.
/// `login\staff\__next.login\`) instead of the ZIP-spec forward slash. Without normalizing,
/// such an entry fails the `ends_with('/')` directory check, gets treated as a file, and
/// Windows refuses to create a file whose path ends in a trailing separator ("The directory
/// name is invalid").
fn unzip_to(bytes: &[u8], dest: &Path) -> Result<(), String> {
    let reader = std::io::Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(reader)
        .map_err(|e| format!("could not open zip archive: {e}"))?;

    for i in 0..archive.len() {
        let mut file = archive.by_index(i)
            .map_err(|e| format!("could not read zip entry #{i}: {e}"))?;
        let raw_name = file.name().to_string();
        let normalized = raw_name.replace('\\', "/");
        let outpath = dest.join(&normalized);

        if normalized.is_empty() || normalized.ends_with('/') {
            fs::create_dir_all(&outpath)
                .map_err(|e| format!("could not create directory for entry {raw_name:?} at {outpath:?}: {e}"))?;
        } else {
            if let Some(parent) = outpath.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| format!("could not create parent directory {parent:?} for entry {raw_name:?} ({outpath:?}): {e}"))?;
            }
            let mut out = fs::File::create(&outpath)
                .map_err(|e| format!("could not create file for entry {raw_name:?} at {outpath:?}: {e}"))?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)
                .map_err(|e| format!("could not read entry {raw_name:?} from zip: {e}"))?;
            use std::io::Write;
            out.write_all(&buf)
                .map_err(|e| format!("could not write entry {raw_name:?} to {outpath:?}: {e}"))?;
        }
    }
    Ok(())
}

fn prune_old_versions(keep_current: &str) {
    let dir = base_dir();
    let Ok(entries) = fs::read_dir(&dir) else { return };
    let mut versions: Vec<String> = entries
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| n != keep_current && !n.ends_with(".tmp"))
        .collect();
    versions.sort();
    if versions.len() > 1 {
        for old in &versions[..versions.len() - 1] {
            fs::remove_dir_all(dir.join(old)).ok();
        }
    }
}

fn shell_supports(min_required: &str) -> bool {
    let parse = |s: &str| -> Vec<u32> { s.split('.').filter_map(|p| p.parse().ok()).collect() };
    parse(SHELL_VERSION) >= parse(min_required)
}