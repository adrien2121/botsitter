use crate::harness::{RunContext, RunPlan};
use crate::logging;
use crate::models::{AppState, ChildOutcome};
use anyhow::Result;
use std::sync::{Arc, Mutex};

pub async fn run(show_logs: bool, plan: RunPlan) -> Result<ChildOutcome> {
    let logger_paths = crate::paths::current_logger_paths();
    logging::reset_log_file(&logger_paths);
    let state = Arc::new(Mutex::new(AppState::new()));
    let status_rx = state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .live_status
        .subscribe();
    let metadata = crate::live_logs::SessionMetadata {
        pid: std::process::id(),
        provider: plan.session.provider,
        cwd: std::env::current_dir()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned(),
        model: plan.session.model.clone(),
        started_at: chrono::Local::now().to_rfc3339(),
    };
    let (logger_handle, logger_ready_rx) =
        logging::init_logging(logger_paths.clone(), metadata, status_rx);
    logging::log_to_file("System initialized. Logger active. Starting passive monitoring.");

    if show_logs {
        if logger_ready_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .is_ok()
        {
            open_logs_terminal(std::process::id());
        } else {
            println!(
                "[System] Warning: Live log viewer failed to start (logger did not become ready)."
            );
        }
    }

    let context = RunContext {
        state,
        monitor: plan.monitor,
    };
    let outcome = plan.runner.run(context).await;

    logging::log_to_file("[System] Child process exited. Shutting down.");
    logging::shutdown_logging(logger_handle, &logger_paths).await;
    outcome
}

fn open_logs_terminal(pid: u32) {
    println!("[System] Live log streaming enabled.");
    println!("[System] Launching botsitter-logs in a new terminal...");

    let logs_bin = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|dir| dir.join("botsitter-logs")))
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| "botsitter-logs".to_string());

    #[cfg(target_os = "macos")]
    {
        let script = format!(
            r#"tell application "Terminal" to do script "{} {}""#,
            logs_bin, pid
        );
        let _ = std::process::Command::new("osascript")
            .arg("-e")
            .arg(script)
            .spawn();
    }
    #[cfg(target_os = "windows")]
    {
        let _ = std::process::Command::new("cmd")
            .arg("/c")
            .arg(format!("start \"Botsitter Logs\" \"{}\" {}", logs_bin, pid))
            .spawn();
    }
    #[cfg(target_os = "linux")]
    {
        let _ = std::process::Command::new("gnome-terminal")
            .arg("--")
            .arg(&logs_bin)
            .arg(pid.to_string())
            .spawn();
    }
}
