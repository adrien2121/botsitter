use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::time::SystemTime;

pub const STATUS_FRAME_PREFIX: char = '\u{001e}';

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderName {
    Claude,
    Codex,
}

impl std::fmt::Display for ProviderName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Claude => "Claude",
            Self::Codex => "Codex",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionSpec {
    pub provider: ProviderName,
    pub model: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionMetadata {
    pub pid: u32,
    pub provider: ProviderName,
    pub cwd: String,
    pub model: Option<String>,
    pub started_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionManifest {
    pub version: u8,
    pub port: u16,
    pub pid: u32,
    pub provider: ProviderName,
    pub cwd: String,
    pub model: Option<String>,
    pub started_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PortRecord {
    Manifest(SessionManifest),
    Legacy {
        port: u16,
        pid: u32,
        modified: SystemTime,
    },
}

impl PortRecord {
    pub fn port(&self) -> u16 {
        match self {
            Self::Manifest(value) => value.port,
            Self::Legacy { port, .. } => *port,
        }
    }

    pub fn pid(&self) -> u32 {
        match self {
            Self::Manifest(value) => value.pid,
            Self::Legacy { pid, .. } => *pid,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitoringReason {
    NoActiveLimit,
    ContinueSent,
    ClearedCancelled,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum LiveStatus {
    Monitoring { reason: MonitoringReason },
    RateLimited { target: String },
    Resuming,
    Retrying { next_attempt: String },
    ContinueFailed,
}

#[derive(Deserialize, Serialize)]
struct StatusEnvelope {
    version: u8,
    #[serde(rename = "type")]
    kind: String,
    #[serde(flatten)]
    status: LiveStatus,
}

pub fn explicit_model(args: &[OsString]) -> Option<String> {
    let mut model = None;
    let mut index = 0;
    while index < args.len() && args[index] != "--" {
        let Some(arg) = args[index].to_str() else {
            index += 1;
            continue;
        };
        if arg == "-m" || arg == "--model" {
            let value = args.get(index + 1)?;
            if value == "--" {
                return None;
            }
            model = value.to_str().map(str::to_owned);
            index += 2;
            continue;
        }
        if let Some(value) = arg
            .strip_prefix("--model=")
            .filter(|value| !value.is_empty())
        {
            model = Some(value.to_owned());
        }
        index += 1;
    }
    model
}

pub fn encode_status_frame(status: &LiveStatus) -> std::result::Result<Vec<u8>, serde_json::Error> {
    let mut bytes = vec![STATUS_FRAME_PREFIX as u8];
    bytes.extend(serde_json::to_vec(&StatusEnvelope {
        version: 1,
        kind: "status".into(),
        status: status.clone(),
    })?);
    bytes.push(b'\n');
    Ok(bytes)
}

pub fn decode_status_frame(
    line: &str,
) -> std::result::Result<Option<LiveStatus>, serde_json::Error> {
    let Some(json) = line.strip_prefix(STATUS_FRAME_PREFIX) else {
        return Ok(None);
    };
    let frame: StatusEnvelope = serde_json::from_str(json.trim_end())?;
    if frame.version != 1 || frame.kind != "status" {
        return Ok(None);
    }
    Ok(Some(frame.status))
}

pub fn parse_port_record(contents: &str, pid: u32, modified: SystemTime) -> Result<PortRecord> {
    if let Ok(port) = contents.trim().parse::<u16>() {
        return Ok(PortRecord::Legacy {
            port,
            pid,
            modified,
        });
    }
    let manifest: SessionManifest =
        serde_json::from_str(contents).context("invalid session manifest")?;
    if manifest.version != 1 {
        bail!("unsupported session manifest version")
    }
    if manifest.pid != pid {
        bail!("session manifest PID mismatch")
    }
    Ok(PortRecord::Manifest(manifest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn explicit_model_uses_last_flag_before_child_boundary() {
        let args = vec![
            OsString::from("--model=sonnet"),
            OsString::from("-m"),
            OsString::from("opus"),
            OsString::from("--"),
            OsString::from("--model"),
            OsString::from("private-child-model"),
        ];
        assert_eq!(explicit_model(&args).as_deref(), Some("opus"));
        assert_eq!(explicit_model(&[OsString::from("--model")]), None);
        assert_eq!(explicit_model(&[]), None);
    }

    #[test]
    fn explicit_model_does_not_consume_child_boundary_as_value() {
        let args = [
            OsString::from("--model"),
            OsString::from("--"),
            OsString::from("private prompt"),
        ];
        assert_eq!(explicit_model(&args), None);
    }

    #[cfg(unix)]
    #[test]
    fn explicit_model_skips_unrelated_non_utf8_arguments() {
        use std::os::unix::ffi::OsStringExt;

        let args = [
            OsString::from_vec(vec![0xff]),
            OsString::from("--model"),
            OsString::from("opus"),
        ];
        assert_eq!(explicit_model(&args).as_deref(), Some("opus"));
    }

    #[test]
    fn manifest_round_trips() {
        let manifest = SessionManifest {
            version: 1,
            port: 49152,
            pid: 18421,
            provider: ProviderName::Claude,
            cwd: "/tmp/project".into(),
            model: Some("opus".into()),
            started_at: "2026-07-20T14:32:00-04:00".into(),
        };
        let json = serde_json::to_string(&manifest).unwrap();
        assert_eq!(
            serde_json::from_str::<SessionManifest>(&json).unwrap(),
            manifest
        );
    }

    #[test]
    fn legacy_numeric_port_remains_parseable() {
        let record =
            parse_port_record("49152\n", 18421, UNIX_EPOCH + Duration::from_secs(7)).unwrap();
        assert_eq!(record.port(), 49152);
        assert_eq!(record.pid(), 18421);
        assert!(matches!(record, PortRecord::Legacy { .. }));
    }

    #[test]
    fn status_frame_round_trips_and_plain_lines_are_ignored() {
        let status = LiveStatus::RateLimited {
            target: "2026-07-20T15:42:00-04:00".into(),
        };
        let encoded = String::from_utf8(encode_status_frame(&status).unwrap()).unwrap();
        assert_eq!(decode_status_frame(&encoded).unwrap(), Some(status));
        assert_eq!(decode_status_frame("[14:32:00] normal\n").unwrap(), None);
    }
}
