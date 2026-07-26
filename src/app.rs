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
        .and_then(|path| path.parent().map(|dir| dir.join(logs_binary_name())))
        .unwrap_or_else(|| std::path::PathBuf::from(logs_binary_name()));

    #[cfg(target_os = "macos")]
    {
        let script = r#"on run argv
tell application "Terminal" to do script (quoted form of item 1 of argv & " " & quoted form of item 2 of argv)
end run"#;
        let _ = std::process::Command::new("osascript")
            .arg("-e")
            .arg(script)
            .arg("--")
            .arg(&logs_bin)
            .arg(pid.to_string())
            .spawn();
    }
    #[cfg(target_os = "windows")]
    {
        let _ = std::process::Command::new("cmd")
            .arg("/c")
            .arg("start")
            .arg("")
            .arg(&logs_bin)
            .arg(pid.to_string())
            .spawn();
    }
    #[cfg(target_os = "linux")]
    {
        let terminals = [
            std::env::var_os("TERMINAL").map(|terminal| (terminal, "-e")),
            Some(("x-terminal-emulator".into(), "-e")),
            Some(("gnome-terminal".into(), "--")),
            Some(("konsole".into(), "-e")),
            Some(("xfce4-terminal".into(), "-x")),
            Some(("xterm".into(), "-e")),
        ];
        let mut launched = false;
        for (terminal, separator) in terminals.into_iter().flatten() {
            let result = std::process::Command::new(&terminal)
                .arg(separator)
                .arg(&logs_bin)
                .arg(pid.to_string())
                .spawn();
            if result.is_ok() {
                launched = true;
                break;
            }
        }
        if !launched {
            println!(
                "[System] Warning: Could not spawn a terminal window automatically. Run manually: {} {}",
                logs_bin.display(), pid
            );
        }
    }
}

fn logs_binary_name() -> String {
    format!("botsitter-logs{}", std::env::consts::EXE_SUFFIX)
}

#[cfg(test)]
mod tests {
    use super::logs_binary_name;

    #[test]
    fn companion_binary_uses_platform_executable_suffix() {
        assert_eq!(
            logs_binary_name(),
            format!("botsitter-logs{}", std::env::consts::EXE_SUFFIX)
        );
    }
}
