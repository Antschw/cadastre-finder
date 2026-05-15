// Prevents additional console window on Windows in release, DO NOT REMOVE
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tauri::{AppHandle, Manager};
use tauri_plugin_opener::OpenerExt;

#[cfg(windows)]
use std::os::windows::process::CommandExt;

const BACKEND_HOST: &str = "127.0.0.1:8000";
const BACKEND_URL: &str = "http://127.0.0.1:8000";
const STARTUP_TIMEOUT_SECS: u64 = 30;

fn find_backend_exe() -> PathBuf {
    std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .join("backend")
        .join("server.exe")
}

fn start_backend(exe: &PathBuf) -> std::io::Result<Child> {
    let data_dir = exe
        .parent()  // backend/
        .and_then(|p| p.parent())  // racine distribution
        .map(|p| p.join("data"))
        .unwrap_or_else(|| PathBuf::from("data"));

    let mut cmd = Command::new(exe);
    cmd.env("CADASTRE_DATA_DIR", data_dir.to_str().unwrap_or("data"));

    #[cfg(target_os = "windows")]
    cmd.creation_flags(0x08000000); // CREATE_NO_WINDOW

    cmd.spawn()
}

fn wait_for_backend(timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect(BACKEND_HOST).is_ok() {
            thread::sleep(Duration::from_millis(300));
            return true;
        }
        thread::sleep(Duration::from_millis(300));
    }
    false
}

pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_window_state::Builder::default().build())
        .setup(|app| {
            let handle: AppHandle = app.handle().clone();
            let child_state: Arc<Mutex<Option<Child>>> = Arc::new(Mutex::new(None));
            let child_clone = child_state.clone();

            thread::spawn(move || {
                let exe = find_backend_exe();
                match start_backend(&exe) {
                    Ok(child) => *child_clone.lock().unwrap() = Some(child),
                    Err(e) => {
                        eprintln!("Échec démarrage backend : {e}");
                        return;
                    }
                }

                if wait_for_backend(Duration::from_secs(STARTUP_TIMEOUT_SECS)) {
                    if let Some(win) = handle.get_webview_window("main") {
                        let url: tauri::Url = BACKEND_URL.parse().unwrap();
                        let _ = win.navigate(url);
                        let _ = win.show();
                        win.open_devtools();
                    }
                } else {
                    eprintln!("Backend non disponible après {}s", STARTUP_TIMEOUT_SECS);
                }
            });

            app.manage(child_state);
            Ok(())
        })
        .on_window_event(|window, event| {
            if let tauri::WindowEvent::Destroyed = event {
                if let Some(arc) = window
                    .app_handle()
                    .try_state::<Arc<Mutex<Option<Child>>>>()
                {
                    if let Some(child) = arc.lock().unwrap().as_mut() {
                        let _ = child.kill();
                    }
                }
            }
        })
        .run(tauri::generate_context!())
        .expect("Erreur lors du démarrage de l'application");
}

fn main() {
    run()
}
