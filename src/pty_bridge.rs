use crate::cli::CommandSpec;
use crate::logging::log_to_file;
use crate::models::{mark_output_activity, SharedAppState};
use anyhow::Result;
use portable_pty::{Child, CommandBuilder, MasterPty, NativePtySystem, PtySize, PtySystem};
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub type SharedPtyWriter = Arc<Mutex<Box<dyn Write + Send>>>;

pub struct PtySession {
    pub child: Box<dyn Child + Send + Sync>,
    pub master: Box<dyn MasterPty + Send>,
    pub reader: Box<dyn Read + Send>,
    pub writer: SharedPtyWriter,
    pub initial_size: TerminalSize,
}

pub type TerminalSize = (u16, u16);

pub fn spawn_command_in_pty(command: CommandSpec) -> Result<PtySession> {
    let initial_size = crossterm::terminal::size().unwrap_or((120, 40));
    let pty_system = NativePtySystem::default();
    let pair = pty_system.openpty(to_pty_size(initial_size))?;

    let child = pair.slave.spawn_command(build_command(command))?;
    drop(pair.slave);

    let reader = pair.master.try_clone_reader()?;
    let writer = Arc::new(Mutex::new(pair.master.take_writer()?));

    Ok(PtySession {
        child,
        master: pair.master,
        reader,
        writer,
        initial_size,
    })
}

pub fn spawn_output_reader(
    mut reader: Box<dyn Read + Send>,
    state: SharedAppState,
) -> tokio::task::JoinHandle<()> {
    let reads = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let last_read_seconds = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let started = Instant::now();

    let heartbeat_reads = Arc::clone(&reads);
    let heartbeat_bytes = Arc::clone(&bytes);
    let heartbeat_last_read = Arc::clone(&last_read_seconds);
    let heartbeat_done = Arc::clone(&done);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await;
        while !heartbeat_done.load(Ordering::Relaxed) {
            interval.tick().await;
            let silence = started
                .elapsed()
                .as_secs()
                .saturating_sub(heartbeat_last_read.load(Ordering::Relaxed));
            log_to_file(&output_telemetry_summary(
                heartbeat_reads.load(Ordering::Relaxed),
                heartbeat_bytes.load(Ordering::Relaxed),
                silence,
            ));
        }
    });

    tokio::task::spawn_blocking(move || {
        log_to_file("[PTY Output] reader started");
        let activity_tracker = state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .last_output_activity
            .clone();
        let mut buf = [0u8; 64 * 1024];
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        let mut write_error_logged = false;
        let mut flush_error_logged = false;
        loop {
            let n = match reader.read(&mut buf) {
                Ok(0) => {
                    log_to_file("[PTY Output] reader reached EOF");
                    break;
                }
                Ok(n) => n,
                Err(error) => {
                    log_to_file(&format!("[PTY Output Error] read failed: {error}"));
                    break;
                }
            };
            reads.fetch_add(1, Ordering::Relaxed);
            bytes.fetch_add(n as u64, Ordering::Relaxed);
            last_read_seconds.store(started.elapsed().as_secs(), Ordering::Relaxed);
            mark_output_activity(&activity_tracker);

            if let Err(error) = stdout.write_all(&buf[..n]) {
                if !write_error_logged {
                    log_to_file(&format!("[PTY Output Error] stdout write failed: {error}"));
                    write_error_logged = true;
                }
            }
            if let Err(error) = stdout.flush() {
                if !flush_error_logged {
                    log_to_file(&format!("[PTY Output Error] stdout flush failed: {error}"));
                    flush_error_logged = true;
                }
            }
            log_to_file(&format!(
                "[PTY Output] {}",
                String::from_utf8_lossy(&buf[..n])
            ));
        }
        done.store(true, Ordering::Relaxed);
        log_to_file(&format!(
            "[PTY Output] reader stopped; reads={} bytes={}",
            reads.load(Ordering::Relaxed),
            bytes.load(Ordering::Relaxed)
        ));
    })
}

fn output_telemetry_summary(reads: u64, bytes: u64, silence_seconds: u64) -> String {
    format!("[PTY Output] alive; reads={reads} bytes={bytes} silence={silence_seconds}s")
}

pub fn spawn_input_writer(writer: SharedPtyWriter) {
    std::thread::spawn(move || {
        let mut buf = [0u8; 1024];
        let mut stdin = io::stdin();
        while let Ok(n) = stdin.read(&mut buf) {
            if n == 0 {
                break;
            }

            let mut pty_writer = writer.lock().unwrap_or_else(|e| e.into_inner());
            if pty_writer.write_all(&buf[..n]).is_err() {
                break;
            }
            let _ = pty_writer.flush();
        }
    });
}

pub fn spawn_resize_poller(master: Box<dyn MasterPty + Send>, initial_size: TerminalSize) {
    #[cfg(unix)]
    std::thread::spawn(move || {
        use signal_hook::consts::SIGWINCH;
        use signal_hook::iterator::Signals;

        let mut current_size = initial_size;
        if let Ok(mut signals) = Signals::new([SIGWINCH]) {
            for _ in signals.forever() {
                if let Ok(new_size) = crossterm::terminal::size() {
                    if new_size != current_size {
                        current_size = new_size;
                        let _ = master.resize(to_pty_size(new_size));
                    }
                }
            }
        }
    });

    #[cfg(not(unix))]
    tokio::spawn(async move {
        let mut current_size = initial_size;
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            if let Ok(new_size) = crossterm::terminal::size() {
                if new_size != current_size {
                    current_size = new_size;
                    let _ = master.resize(to_pty_size(new_size));
                }
            }
        }
    });
}

fn build_command(command: CommandSpec) -> CommandBuilder {
    let mut cmd = CommandBuilder::new(command.program);
    if !command.args.is_empty() {
        cmd.args(command.args);
    }

    if let Ok(current_dir) = std::env::current_dir() {
        cmd.cwd(&current_dir);
        #[cfg(unix)]
        cmd.env("PWD", current_dir.to_string_lossy().as_ref());
    }

    cmd
}

fn to_pty_size((cols, rows): TerminalSize) -> PtySize {
    PtySize {
        rows,
        cols,
        pixel_width: 0,
        pixel_height: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::output_telemetry_summary;

    #[test]
    fn output_telemetry_summary_distinguishes_silence_from_forwarded_bytes() {
        assert_eq!(
            output_telemetry_summary(0, 0, 90),
            "[PTY Output] alive; reads=0 bytes=0 silence=90s"
        );
        assert_eq!(
            output_telemetry_summary(3, 42, 5),
            "[PTY Output] alive; reads=3 bytes=42 silence=5s"
        );
    }
}
