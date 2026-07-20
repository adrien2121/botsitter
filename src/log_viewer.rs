use crate::live_logs::PortRecord;
use std::io::{BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};

pub struct ViewerIdentity {
    pub provider: String,
    pub pid: u32,
}

pub struct DiscoveredSession {
    pub path: PathBuf,
    pub record: PortRecord,
}

impl DiscoveredSession {
    pub fn pid(&self) -> u32 {
        self.record.pid()
    }

    pub fn port(&self) -> u16 {
        self.record.port()
    }

    fn started_key(&self) -> i128 {
        match &self.record {
            PortRecord::Manifest(value) => chrono::DateTime::parse_from_rfc3339(&value.started_at)
                .map(|time| time.timestamp_millis() as i128)
                .unwrap_or(i128::MIN),
            PortRecord::Legacy { modified, .. } => modified
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|value| value.as_millis() as i128)
                .unwrap_or(i128::MIN),
        }
    }

    fn viewer_identity(&self) -> ViewerIdentity {
        ViewerIdentity {
            provider: match &self.record {
                PortRecord::Manifest(value) => value.provider.to_string(),
                PortRecord::Legacy { .. } => "botsitter".into(),
            },
            pid: self.pid(),
        }
    }
}

fn status_text(status: &crate::live_logs::LiveStatus) -> String {
    use crate::live_logs::{LiveStatus, MonitoringReason};

    match status {
        LiveStatus::Monitoring {
            reason: MonitoringReason::NoActiveLimit,
        } => "MONITORING | no active rate limit".into(),
        LiveStatus::Monitoring {
            reason: MonitoringReason::ContinueSent,
        } => "MONITORING | continue sent".into(),
        LiveStatus::Monitoring {
            reason: MonitoringReason::ClearedCancelled,
        } => "MONITORING | rate limit cleared; continue cancelled".into(),
        LiveStatus::RateLimited { target } => {
            let time = chrono::DateTime::parse_from_rfc3339(target)
                .map(|value| value.format("%H:%M %:z").to_string())
                .unwrap_or_else(|_| target.clone());
            format!("RATE LIMITED | continue scheduled {time}")
        }
        LiveStatus::Resuming => "RESUMING | sending continue".into(),
        LiveStatus::Retrying { next_attempt } => {
            let time = chrono::DateTime::parse_from_rfc3339(next_attempt)
                .map(|value| value.format("%H:%M:%S %:z").to_string())
                .unwrap_or_else(|_| next_attempt.clone());
            format!("RETRYING | next attempt {time}")
        }
        LiveStatus::ContinueFailed => "CONTINUE FAILED | waiting for new limit event".into(),
    }
}

fn format_footer(
    identity: &ViewerIdentity,
    status: &crate::live_logs::LiveStatus,
    width: u16,
) -> String {
    format!(
        "{} PID {} | {}",
        identity.provider,
        identity.pid,
        status_text(status)
    )
    .chars()
    .take(width as usize)
    .collect()
}

fn draw_footer<W: Write>(
    writer: &mut W,
    identity: &ViewerIdentity,
    status: &crate::live_logs::LiveStatus,
    width: u16,
) -> std::io::Result<()> {
    crossterm::execute!(
        writer,
        crossterm::cursor::MoveToColumn(0),
        crossterm::terminal::Clear(crossterm::terminal::ClearType::CurrentLine)
    )?;
    writer.write_all(format_footer(identity, status, width).as_bytes())?;
    writer.flush()
}

pub fn render_stream<R, W, F>(
    mut reader: R,
    writer: &mut W,
    identity: ViewerIdentity,
    mut interactive: bool,
    mut width: F,
) -> std::io::Result<()>
where
    R: BufRead,
    W: Write,
    F: FnMut() -> u16,
{
    let mut status = crate::live_logs::LiveStatus::Monitoring {
        reason: crate::live_logs::MonitoringReason::NoActiveLimit,
    };
    if interactive && draw_footer(writer, &identity, &status, width()).is_err() {
        interactive = false;
    }

    let mut line = String::new();
    while reader.read_line(&mut line)? > 0 {
        if line.starts_with(crate::live_logs::STATUS_FRAME_PREFIX) {
            if let Ok(Some(next_status)) = crate::live_logs::decode_status_frame(&line) {
                status = next_status;
                if interactive {
                    draw_footer(writer, &identity, &status, width())?;
                }
            }
        } else if interactive {
            crossterm::execute!(
                writer,
                crossterm::cursor::MoveToColumn(0),
                crossterm::terminal::Clear(crossterm::terminal::ClearType::CurrentLine)
            )?;
            writer.write_all(line.as_bytes())?;
            draw_footer(writer, &identity, &status, width())?;
        } else {
            writer.write_all(line.as_bytes())?;
        }
        line.clear();
    }

    if interactive {
        crossterm::execute!(
            writer,
            crossterm::cursor::MoveToColumn(0),
            crossterm::terminal::Clear(crossterm::terminal::ClearType::CurrentLine)
        )?;
        writer.write_all(
            format!("{} PID {} | DISCONNECTED", identity.provider, identity.pid)
                .chars()
                .take(width() as usize)
                .collect::<String>()
                .as_bytes(),
        )?;
        writer.write_all(b"\n")?;
    }
    writer.flush()
}

pub fn stream_session(session: &DiscoveredSession, interactive: bool) -> anyhow::Result<()> {
    let address = std::net::SocketAddr::from(([127, 0, 0, 1], session.port()));
    let stream = std::net::TcpStream::connect_timeout(&address, std::time::Duration::from_secs(2))?;
    let result = render_stream(
        std::io::BufReader::new(stream),
        &mut std::io::stdout().lock(),
        session.viewer_identity(),
        interactive && std::io::stdout().is_terminal(),
        || {
            crossterm::terminal::size()
                .map(|(columns, _)| columns)
                .unwrap_or(80)
        },
    );
    match result {
        Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        result => Ok(result?),
    }
}

pub fn discover_sessions(temp_dir: &Path) -> std::io::Result<Vec<DiscoveredSession>> {
    let mut sessions = Vec::new();
    for entry in std::fs::read_dir(temp_dir)?.flatten() {
        let path = entry.path();
        let Some(pid) = crate::paths::pid_from_port_path(&path) else {
            continue;
        };
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let Ok(contents) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(record) = crate::live_logs::parse_port_record(
            &contents,
            pid,
            metadata
                .modified()
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
        ) else {
            continue;
        };
        let address = std::net::SocketAddr::from(([127, 0, 0, 1], record.port()));
        if std::net::TcpStream::connect_timeout(&address, std::time::Duration::from_millis(100))
            .is_err()
        {
            continue;
        }
        sessions.push(DiscoveredSession { path, record });
    }
    sessions.sort_by(|a, b| b.started_key().cmp(&a.started_key()));
    Ok(sessions)
}

pub fn format_menu(sessions: &[DiscoveredSession]) -> String {
    use std::fmt::Write as _;

    let mut output = String::from("Active botsitter sessions\n\n");
    for (index, session) in sessions.iter().enumerate() {
        match &session.record {
            PortRecord::Manifest(value) => {
                let started = chrono::DateTime::parse_from_rfc3339(&value.started_at)
                    .map(|time| {
                        time.with_timezone(&chrono::Local)
                            .format("%H:%M %Z")
                            .to_string()
                    })
                    .unwrap_or_else(|_| value.started_at.clone());
                let model = value.model.as_deref().unwrap_or("default");
                let _ = writeln!(
                    output,
                    "{}. {} | model {} | started {} | PID {}",
                    index + 1,
                    value.provider,
                    model,
                    started,
                    value.pid
                );
                let _ = writeln!(output, "   {}\n", value.cwd);
            }
            PortRecord::Legacy { pid, .. } => {
                let _ = writeln!(
                    output,
                    "{}. metadata unavailable | PID {}\n",
                    index + 1,
                    pid
                );
            }
        }
    }
    output.push_str("Select session: ");
    output
}

pub fn session_for_pid(temp_dir: &Path, pid: u32) -> anyhow::Result<DiscoveredSession> {
    let path = crate::paths::LoggerPaths::for_pid_in(temp_dir, pid).port;
    let metadata = std::fs::metadata(&path)?;
    let contents = std::fs::read_to_string(&path)?;
    let record = crate::live_logs::parse_port_record(
        &contents,
        pid,
        metadata
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
    )?;
    Ok(DiscoveredSession { path, record })
}

pub fn selection_tty_error(
    pid: Option<u32>,
    stdin_tty: bool,
    stdout_tty: bool,
) -> Option<&'static str> {
    (pid.is_none() && (!stdin_tty || !stdout_tty))
        .then_some("session selection requires a TTY; pass a PID")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_logs::{ProviderName, SessionManifest};
    use std::net::TcpListener;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(label: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "botsitter-log-viewer-{label}-{}-{nonce}",
                std::process::id()
            ));
            std::fs::create_dir(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn write_manifest(directory: &Path, manifest: SessionManifest) {
        let path = crate::paths::LoggerPaths::for_pid_in(directory, manifest.pid).port;
        std::fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    }

    fn unused_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[test]
    fn discovery_hides_unreachable_and_sorts_newest_first() {
        let directory = TestDir::new("discovery");
        let older = TcpListener::bind("127.0.0.1:0").unwrap();
        let newer = TcpListener::bind("127.0.0.1:0").unwrap();
        write_manifest(
            &directory.path,
            SessionManifest {
                version: 1,
                port: older.local_addr().unwrap().port(),
                pid: 41,
                provider: ProviderName::Claude,
                cwd: "/older".into(),
                model: None,
                started_at: "2026-07-20T13:00:00-04:00".into(),
            },
        );
        write_manifest(
            &directory.path,
            SessionManifest {
                version: 1,
                port: newer.local_addr().unwrap().port(),
                pid: 42,
                provider: ProviderName::Codex,
                cwd: "/newer".into(),
                model: Some("gpt-5.4".into()),
                started_at: "2026-07-20T14:00:00-04:00".into(),
            },
        );
        write_manifest(
            &directory.path,
            SessionManifest {
                version: 1,
                port: unused_port(),
                pid: 43,
                provider: ProviderName::Claude,
                cwd: "/dead".into(),
                model: None,
                started_at: "2026-07-20T15:00:00-04:00".into(),
            },
        );

        let sessions = discover_sessions(&directory.path).unwrap();
        assert_eq!(
            sessions
                .iter()
                .map(DiscoveredSession::pid)
                .collect::<Vec<_>>(),
            vec![42, 41]
        );
        let menu = format_menu(&sessions);
        assert!(menu.contains("Codex | model gpt-5.4"));
        assert!(menu.contains("/newer"));
        assert!(!menu.contains("/dead"));
    }

    #[test]
    fn bare_selection_requires_tty_but_exact_pid_does_not() {
        assert_eq!(
            selection_tty_error(None, false, true),
            Some("session selection requires a TTY; pass a PID")
        );
        assert_eq!(
            selection_tty_error(None, true, false),
            Some("session selection requires a TTY; pass a PID")
        );
        assert_eq!(selection_tty_error(Some(42), false, false), None);
    }

    #[test]
    fn interactive_stream_keeps_status_in_footer_only() {
        let status = crate::live_logs::LiveStatus::RateLimited {
            target: "2026-07-20T15:42:00-04:00".into(),
        };
        let mut input = b"[14:31:00] first\n".to_vec();
        input.extend(crate::live_logs::encode_status_frame(&status).unwrap());
        input.extend(b"[14:31:01] second\n");
        let mut output = Vec::new();

        render_stream(
            std::io::BufReader::new(input.as_slice()),
            &mut output,
            ViewerIdentity {
                provider: "Claude".into(),
                pid: 18421,
            },
            true,
            || 120,
        )
        .unwrap();

        let rendered = String::from_utf8(output).unwrap();
        assert!(rendered.contains("first"));
        assert!(rendered.contains("second"));
        assert!(
            rendered.contains("RATE LIMITED | continue scheduled 15:42 -04:00"),
            "{rendered:?}"
        );
        assert!(!rendered.contains("\"state\":\"rate_limited\""));
    }

    #[test]
    fn plain_stream_suppresses_frames_and_ansi() {
        let status = crate::live_logs::LiveStatus::Resuming;
        let mut input = b"plain\n".to_vec();
        input.extend(crate::live_logs::encode_status_frame(&status).unwrap());
        let mut output = Vec::new();
        render_stream(
            std::io::BufReader::new(input.as_slice()),
            &mut output,
            ViewerIdentity {
                provider: "Codex".into(),
                pid: 42,
            },
            false,
            || 80,
        )
        .unwrap();
        assert_eq!(output, b"plain\n");
    }

    #[test]
    fn footer_truncates_to_terminal_width() {
        let footer = format_footer(
            &ViewerIdentity {
                provider: "Claude".into(),
                pid: 18421,
            },
            &crate::live_logs::LiveStatus::Monitoring {
                reason: crate::live_logs::MonitoringReason::NoActiveLimit,
            },
            24,
        );
        assert!(footer.chars().count() <= 24);
    }
}
