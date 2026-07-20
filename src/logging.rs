use chrono::Local;
use std::collections::VecDeque;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Receiver as StdReceiver;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, Sender};
use tokio::time::timeout;

/// A message sent to the dedicated logger thread.
enum LogMessage {
    /// A single line of text to be written to the log.
    Line(String),
    /// A signal to flush the buffer and terminate the logger thread.
    Shutdown { dropped: u64 },
}

/// Global sender for the logging channel.
/// `OnceLock` is a modern and efficient way to handle global static initialization.
static LOGGER_SENDER: OnceLock<Sender<LogMessage>> = OnceLock::new();
static DROPPED_LOG_MESSAGES: AtomicU64 = AtomicU64::new(0);
static MANIFEST_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The maximum size of the log file in bytes before it is rotated. (10 MiB)
const MAX_LOG_SIZE: u64 = 10 * 1024 * 1024;
const MAX_STARTUP_BUFFER_LINES: usize = 500;
const CLIENT_WRITE_TIMEOUT: Duration = Duration::from_millis(100);

fn try_queue_line(sender: &Sender<LogMessage>, dropped: &AtomicU64, mut line: String) {
    match sender.try_reserve() {
        Ok(permit) => {
            let count = dropped.swap(0, Ordering::AcqRel);
            if count > 0 {
                line =
                    format!("[Logger] {count} diagnostic message(s) dropped: channel full\n{line}");
            }
            permit.send(LogMessage::Line(line));
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            dropped.fetch_add(1, Ordering::Relaxed);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

fn push_startup_line(buffer: &mut VecDeque<String>, line: String, max_lines: usize) {
    if buffer.len() == max_lines {
        buffer.pop_front();
    }
    buffer.push_back(line);
}

async fn write_with_timeout<W>(writer: &mut W, bytes: &[u8], duration: Duration) -> bool
where
    W: AsyncWrite + Unpin,
{
    timeout(duration, writer.write_all(bytes))
        .await
        .is_ok_and(|result| result.is_ok())
}

async fn run_logger(
    listener: TcpListener,
    path: std::path::PathBuf,
    mut log_rx: mpsc::Receiver<LogMessage>,
    mut status_rx: tokio::sync::watch::Receiver<crate::live_logs::LiveStatus>,
    max_startup_lines: usize,
    client_write_timeout: Duration,
) {
    let mut clients: Vec<TcpStream> = Vec::new();
    let mut initial_buffer: VecDeque<String> = VecDeque::new();
    let mut has_first_client_connected = false;

    let mut writer = match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
    {
        Ok(file) => BufWriter::new(file),
        Err(_) => return,
    };
    let mut current_size = tokio::fs::metadata(&path)
        .await
        .map(|m| m.len())
        .unwrap_or(0);

    loop {
        tokio::select! {
            Ok((mut stream, _)) = listener.accept() => {
                let mut client_ok = true;
                for line in &initial_buffer {
                    if !write_with_timeout(
                        &mut stream,
                        line.as_bytes(),
                        client_write_timeout,
                    )
                    .await
                    {
                        client_ok = false;
                        break;
                    }
                }
                if client_ok {
                    let frame = crate::live_logs::encode_status_frame(&status_rx.borrow().clone())
                        .expect("LiveStatus serialization cannot fail");
                    client_ok =
                        write_with_timeout(&mut stream, &frame, client_write_timeout).await;
                }
                if client_ok {
                    has_first_client_connected = true;
                    clients.push(stream);
                }
            }
            Ok(()) = status_rx.changed() => {
                let frame = crate::live_logs::encode_status_frame(
                    &status_rx.borrow_and_update().clone(),
                )
                .expect("LiveStatus serialization cannot fail");
                let mut dead_clients = Vec::new();
                for (index, client) in clients.iter_mut().enumerate() {
                    if !write_with_timeout(client, &frame, client_write_timeout).await {
                        dead_clients.push(index);
                    }
                }
                for index in dead_clients.into_iter().rev() {
                    clients.remove(index);
                }
            }
            Some(msg) = log_rx.recv() => {
                match msg {
                    LogMessage::Line(mut line) => {
                        line.push('\n');
                        if !has_first_client_connected {
                            push_startup_line(
                                &mut initial_buffer,
                                line.clone(),
                                max_startup_lines,
                            );
                        } else {
                            let mut dead_clients = Vec::new();
                            for (i, client) in clients.iter_mut().enumerate() {
                                if !write_with_timeout(
                                    client,
                                    line.as_bytes(),
                                    client_write_timeout,
                                )
                                .await
                                {
                                    dead_clients.push(i);
                                }
                            }
                            for i in dead_clients.into_iter().rev() {
                                clients.remove(i);
                            }
                        }

                        if current_size > MAX_LOG_SIZE {
                            if writer.flush().await.is_err() { return; }
                            drop(writer);

                            let backup_path = path.with_extension("log.old");
                            if tokio::fs::rename(&path, &backup_path).await.is_err() { return; }

                            let new_file = match tokio::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&path)
                            .await {
                                Ok(f) => f,
                                Err(_) => return,
                            };
                            writer = BufWriter::new(new_file);
                            current_size = 0;

                            let rotation_msg = format!("[{}] Log file exceeded {}MB and was rotated. Previous log saved to: {}\n",
                                Local::now().format("%H:%M:%S"),
                                MAX_LOG_SIZE / (1024 * 1024),
                                backup_path.display()
                            );
                            if writer.write_all(rotation_msg.as_bytes()).await.is_ok() {
                                current_size += rotation_msg.len() as u64;
                            }
                        }

                        let bytes_to_write = line.len() as u64;
                        if writer.write_all(line.as_bytes()).await.is_err() {
                            return;
                        }
                        current_size += bytes_to_write;
                    }
                    LogMessage::Shutdown { dropped } => {
                        if dropped > 0 {
                            let summary = format!(
                                "[Logger] {dropped} diagnostic message(s) dropped: channel full\n"
                            );
                            let _ = writer.write_all(summary.as_bytes()).await;
                        }
                        break;
                    }
                }
            }
            else => {
                break;
            }
        }
    }
    let _ = writer.flush().await;
}

fn write_manifest(
    path: &std::path::Path,
    manifest: &crate::live_logs::SessionManifest,
) -> std::io::Result<()> {
    use std::io::Write;

    let bytes = serde_json::to_vec(manifest).map_err(std::io::Error::other)?;
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let (temporary, mut file) = loop {
        let nonce = MANIFEST_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let temporary = path.with_extension(format!("port.tmp-{}-{nonce}", manifest.pid));
        match options.open(&temporary) {
            Ok(file) => break (temporary, file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };
    if let Err(error) = file.write_all(&bytes).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    drop(file);
    if let Err(error) = std::fs::rename(&temporary, path) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    Ok(())
}

/// Initializes the asynchronous logger, spawning a dedicated thread for file I/O.
///
/// This function should be called once at application startup. It returns the handle
/// to the logger thread, which should be used for a graceful shutdown.
pub fn init_logging(
    paths: crate::paths::LoggerPaths,
    metadata: crate::live_logs::SessionMetadata,
    status_rx: tokio::sync::watch::Receiver<crate::live_logs::LiveStatus>,
) -> (tokio::task::JoinHandle<()>, StdReceiver<()>) {
    let _ = fs::remove_file(&paths.port);

    let (log_tx, log_rx) = mpsc::channel::<LogMessage>(100); // Use a buffered channel
                                                             // This will succeed as we call init_logging() only once.
    let _ = LOGGER_SENDER.set(log_tx);

    // Channel to signal that the TCP listener is ready.
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();

    let handle = tokio::spawn(async move {
        // --- 1. Set up network listener for live logs ---
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(l) => l,
            Err(_) => {
                /* Failed to bind, live logging will be unavailable */
                return;
            }
        };
        if let Ok(addr) = listener.local_addr() {
            let manifest = crate::live_logs::SessionManifest {
                version: 1,
                port: addr.port(),
                pid: metadata.pid,
                provider: metadata.provider,
                cwd: metadata.cwd,
                model: metadata.model,
                started_at: metadata.started_at,
            };
            if write_manifest(&paths.port, &manifest).is_ok() {
                // Successfully wrote the port file. Signal that we are ready.
                let _ = ready_tx.send(()); // Use blocking send for initial setup
            }
        }
        run_logger(
            listener,
            paths.log.clone(),
            log_rx,
            status_rx,
            MAX_STARTUP_BUFFER_LINES,
            CLIENT_WRITE_TIMEOUT,
        )
        .await;
        // Clean up the port file on graceful shutdown.
    });

    (handle, ready_rx)
}

/// Signals the logger thread to shut down and waits for it to finish.
/// This ensures all pending log messages are flushed to the file.
pub async fn shutdown_logging(
    handle: tokio::task::JoinHandle<()>,
    paths: &crate::paths::LoggerPaths,
) {
    if let Some(sender) = LOGGER_SENDER.get() {
        // The receiver might already be gone if the thread panicked, so ignore errors.
        let dropped = DROPPED_LOG_MESSAGES.swap(0, Ordering::AcqRel);
        if sender.send(LogMessage::Shutdown { dropped }).await.is_err() {
            DROPPED_LOG_MESSAGES.fetch_add(dropped, Ordering::Relaxed);
        }
    }
    // Wait for the logger thread to process all messages and exit.
    let _ = handle.await;
    let _ = fs::remove_file(&paths.port);
}

pub fn reset_log_file(paths: &crate::paths::LoggerPaths) {
    let _ = std::fs::remove_file(&paths.log);
}

pub fn log_to_file(msg: &str) {
    if let Some(sender) = LOGGER_SENDER.get() {
        let line = format!("[{}] {}", Local::now().format("%H:%M:%S"), msg);
        try_queue_line(sender, &DROPPED_LOG_MESSAGES, line);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        push_startup_line, run_logger, try_queue_line, write_manifest, write_with_timeout,
        LogMessage,
    };
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    fn unique_log_path(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "botsitter-{name}-{}-{unique}.log",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn full_channel_reports_aggregate_on_next_success() {
        let (tx, mut rx) = mpsc::channel(1);
        let dropped = AtomicU64::new(0);
        tx.try_send(LogMessage::Line("occupied".into())).unwrap();

        try_queue_line(&tx, &dropped, "lost one".into());
        try_queue_line(&tx, &dropped, "lost two".into());
        assert_eq!(dropped.load(Ordering::Relaxed), 2);
        assert!(matches!(rx.recv().await, Some(LogMessage::Line(_))));

        try_queue_line(&tx, &dropped, "next".into());
        let Some(LogMessage::Line(line)) = rx.recv().await else {
            panic!()
        };
        assert!(line.contains("2 diagnostic message(s) dropped"));
        assert!(line.contains("next"));
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn startup_buffer_keeps_newest_lines_only() {
        let mut buffer = VecDeque::new();

        for i in 0..5 {
            push_startup_line(&mut buffer, format!("line {i}\n"), 3);
        }

        assert_eq!(
            buffer.into_iter().collect::<Vec<_>>(),
            vec![
                "line 2\n".to_string(),
                "line 3\n".to_string(),
                "line 4\n".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn slow_writer_times_out() {
        let (mut writer, _reader) = tokio::io::duplex(1);
        let bytes = vec![b'x'; 1024 * 1024];

        assert!(!write_with_timeout(&mut writer, &bytes, Duration::from_millis(1)).await);
    }

    #[tokio::test]
    async fn non_reading_client_does_not_block_sentinel_work() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let non_reading_client = TcpStream::connect(address).await.unwrap();
        let mut non_reading_client = BufReader::new(non_reading_client);
        let path = unique_log_path("slow-client");
        let (_, status_rx) =
            tokio::sync::watch::channel(crate::live_logs::LiveStatus::Monitoring {
                reason: crate::live_logs::MonitoringReason::NoActiveLimit,
            });
        let (log_tx, log_rx) = mpsc::channel(1);
        let logger = tokio::spawn(run_logger(
            listener,
            path.clone(),
            log_rx,
            status_rx,
            500,
            Duration::from_millis(100),
        ));

        let mut status_frame = Vec::new();
        timeout(
            Duration::from_secs(5),
            non_reading_client.read_until(b'\n', &mut status_frame),
        )
        .await
        .expect("initial status frame timed out")
        .unwrap();
        assert!(matches!(
            crate::live_logs::decode_status_frame(&String::from_utf8(status_frame).unwrap()),
            Ok(Some(crate::live_logs::LiveStatus::Monitoring { .. }))
        ));

        let marker = "client-registered";
        log_tx
            .send(LogMessage::Line(marker.to_string()))
            .await
            .unwrap();
        let mut received_marker = vec![0; marker.len() + 1];
        timeout(
            Duration::from_secs(5),
            non_reading_client.read_exact(&mut received_marker),
        )
        .await
        .expect("client registration marker timed out")
        .unwrap();
        assert_eq!(received_marker, format!("{marker}\n").as_bytes());

        log_tx
            .send(LogMessage::Line("x".repeat(16 * 1024 * 1024)))
            .await
            .unwrap();

        let (mut sentinel_writer, mut sentinel_reader) = tokio::io::duplex(64);
        let sentinel = timeout(Duration::from_secs(5), async {
            sentinel_writer.write_all(b"sentinel").await.unwrap();
            let mut bytes = [0; 8];
            sentinel_reader.read_exact(&mut bytes).await.unwrap();
            bytes
        })
        .await
        .expect("sentinel work timed out");
        assert_eq!(&sentinel, b"sentinel");

        log_tx
            .send(LogMessage::Shutdown { dropped: 0 })
            .await
            .unwrap();
        timeout(Duration::from_secs(5), logger)
            .await
            .expect("logger did not shut down")
            .unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn late_client_receives_latest_status_without_polluting_log() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let path = unique_log_path("status-replay");
        let (status_tx, status_rx) =
            tokio::sync::watch::channel(crate::live_logs::LiveStatus::RateLimited {
                target: "2026-07-20T15:42:00-04:00".into(),
            });
        let (log_tx, log_rx) = mpsc::channel(4);
        let logger = tokio::spawn(run_logger(
            listener,
            path.clone(),
            log_rx,
            status_rx,
            500,
            Duration::from_millis(100),
        ));

        let mut first = TcpStream::connect(address).await.unwrap();
        let mut first_frame = vec![0; 512];
        timeout(Duration::from_secs(2), first.read(&mut first_frame))
            .await
            .unwrap()
            .unwrap();
        drop(first);

        status_tx.send_replace(crate::live_logs::LiveStatus::Resuming);

        let mut late = TcpStream::connect(address).await.unwrap();
        let mut received = vec![0; 512];
        let count = timeout(Duration::from_secs(2), late.read(&mut received))
            .await
            .unwrap()
            .unwrap();
        let output = String::from_utf8_lossy(&received[..count]);
        assert!(output.contains("\u{1e}"));
        assert!(output.contains("resuming"));

        log_tx
            .send(LogMessage::Line("history".into()))
            .await
            .unwrap();
        log_tx
            .send(LogMessage::Shutdown { dropped: 0 })
            .await
            .unwrap();
        logger.await.unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("history"));
        assert!(!raw.contains("rate_limited"));
        std::fs::remove_file(path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn manifest_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let path = unique_log_path("manifest-permissions").with_extension("port");
        let manifest = crate::live_logs::SessionManifest {
            version: 1,
            port: 49152,
            pid: std::process::id(),
            provider: crate::live_logs::ProviderName::Claude,
            cwd: "/private/project".into(),
            model: None,
            started_at: "2026-07-20T14:32:00-04:00".into(),
        };
        write_manifest(&path, &manifest).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        std::fs::remove_file(path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn manifest_does_not_reuse_stale_deterministic_temp_file() {
        use std::os::unix::fs::PermissionsExt;

        let path = unique_log_path("manifest-stale-temp").with_extension("port");
        let manifest = crate::live_logs::SessionManifest {
            version: 1,
            port: 49152,
            pid: std::process::id(),
            provider: crate::live_logs::ProviderName::Claude,
            cwd: "/private/project".into(),
            model: None,
            started_at: "2026-07-20T14:32:00-04:00".into(),
        };
        let stale = path.with_extension(format!("port.tmp-{}", manifest.pid));
        std::fs::write(&stale, b"sentinel").unwrap();
        std::fs::set_permissions(&stale, std::fs::Permissions::from_mode(0o666)).unwrap();

        write_manifest(&path, &manifest).unwrap();

        assert_eq!(std::fs::read(&stale).unwrap(), b"sentinel");
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(stale).unwrap();
    }
}
