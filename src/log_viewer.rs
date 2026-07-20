use crate::live_logs::PortRecord;
use std::path::{Path, PathBuf};

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
}
