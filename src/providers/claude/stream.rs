use super::cli::stream_resume_command;
use super::transcript::limit_update_from_message;
use crate::cli::CommandSpec;
use crate::harness::{LimitUpdate, ResumeOutcome, ResumeSink, RunContext, RunFuture, Runner};
use crate::logging::log_to_file;
use crate::models::{mark_output_activity, ChildOutcome, OutputActivity, SharedAppState};
use anyhow::Result;
use chrono::{DateTime, Local};
use serde_json::Value;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;

const MAX_PARSER_LINE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
enum StreamResumeCommand {
    Continue,
}

struct StreamResumeSink {
    sender: mpsc::UnboundedSender<StreamResumeCommand>,
}

impl ResumeSink for StreamResumeSink {
    fn resume(&self) -> ResumeOutcome {
        match self.sender.send(StreamResumeCommand::Continue) {
            Ok(()) => ResumeOutcome::Sent,
            Err(_) => ResumeOutcome::DefiniteFailure(
                "stream-json runner is no longer available".to_string(),
            ),
        }
    }
}

pub(super) struct ClaudeStreamRunner {
    command: CommandSpec,
}

impl ClaudeStreamRunner {
    pub(super) fn new(command: CommandSpec) -> Self {
        Self { command }
    }
}

impl Runner for ClaudeStreamRunner {
    fn run(self: Box<Self>, context: RunContext) -> RunFuture {
        Box::pin(async move {
            let (resume_tx, resume_rx) = mpsc::unbounded_channel();
            crate::monitor::spawn_lockout_monitor(
                Arc::clone(&context.state),
                context.monitor,
                Arc::new(StreamResumeSink { sender: resume_tx }),
            );
            run_stream_json_print(self.command, context.state, resume_rx).await
        })
    }
}

#[derive(Debug)]
struct StreamJsonSignal {
    session_id: Option<String>,
    rate_limit: Option<LimitUpdate>,
}

#[derive(Debug)]
enum StreamLineResult {
    Signal(StreamJsonSignal),
    Ignored,
    InvalidJson,
}

#[derive(Debug, PartialEq, Eq)]
enum StreamProcessAction {
    Exit(ChildOutcome),
    Restart,
}

async fn pump_raw_output<R, W>(
    mut reader: R,
    mut writer: W,
    line_tx: mpsc::Sender<String>,
    activity: OutputActivity,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = [0_u8; 8192];
    let mut pending = Vec::new();
    let mut discard_until_newline = false;

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }

        writer.write_all(&buf[..n]).await?;
        writer.flush().await?;
        mark_output_activity(&activity);

        let mut cursor = 0;
        while cursor < n {
            if discard_until_newline {
                if let Some(newline) = buf[cursor..n].iter().position(|byte| *byte == b'\n') {
                    cursor += newline + 1;
                    discard_until_newline = false;
                } else {
                    break;
                }
                continue;
            }

            let bytes = &buf[cursor..n];
            if let Some(newline) = bytes.iter().position(|byte| *byte == b'\n') {
                let line_bytes = &bytes[..newline];
                let remaining_capacity = MAX_PARSER_LINE_BYTES - pending.len();
                if line_bytes.len() > remaining_capacity {
                    pending.clear();
                    log_to_file(
                        "[Stream JSON] NDJSON line exceeded parser limit; raw output was preserved.",
                    );
                } else {
                    pending.extend_from_slice(line_bytes);
                    if let Ok(line) = String::from_utf8(std::mem::take(&mut pending)) {
                        let line = line.trim_end_matches('\r').to_string();
                        let _ = line_tx.try_send(line);
                    }
                }
                cursor += newline + 1;
            } else {
                let remaining_capacity = MAX_PARSER_LINE_BYTES - pending.len();
                if bytes.len() <= remaining_capacity {
                    pending.extend_from_slice(bytes);
                    break;
                }

                pending.extend_from_slice(&bytes[..remaining_capacity]);
                pending.clear();
                log_to_file(
                    "[Stream JSON] NDJSON line exceeded parser limit; raw output was preserved.",
                );
                discard_until_newline = true;
                cursor += remaining_capacity;
            }
        }
    }
}

async fn run_stream_json_print(
    command: CommandSpec,
    state: SharedAppState,
    mut resume_rx: mpsc::UnboundedReceiver<StreamResumeCommand>,
) -> Result<ChildOutcome> {
    let latest_session_id = Arc::new(Mutex::new(None::<String>));
    let initial_program = command.program.clone();
    let mut next_command = command;

    loop {
        let action = run_one_stream_process(
            next_command.clone(),
            Arc::clone(&state),
            Arc::clone(&latest_session_id),
            &mut resume_rx,
        )
        .await?;

        match action {
            StreamProcessAction::Exit(outcome) => return Ok(outcome),
            StreamProcessAction::Restart => {}
        }

        let Some(session_id) = latest_session_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        else {
            log_to_file(
                "[Stream JSON] Cannot resume: no session id was observed in stream output.",
            );
            return Ok(ChildOutcome::Code(0));
        };

        next_command = resume_command_with_program(&initial_program, &session_id);
    }
}

async fn run_one_stream_process(
    command: CommandSpec,
    state: SharedAppState,
    latest_session_id: Arc<Mutex<Option<String>>>,
    resume_rx: &mut mpsc::UnboundedReceiver<StreamResumeCommand>,
) -> Result<StreamProcessAction> {
    let starting_lockout_revision = state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .lockout_revision;
    let mut child = Command::new(&command.program)
        .args(&command.args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    let stdout = child.stdout.take().expect("stdout was piped");
    let stderr = child.stderr.take().expect("stderr was piped");
    let activity = {
        state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .last_output_activity
            .clone()
    };

    let (line_tx, line_rx) = mpsc::channel(1024);
    let stdout_task = tokio::spawn(pump_raw_output(
        stdout,
        tokio::io::stdout(),
        line_tx,
        activity.clone(),
    ));
    let stderr_task = tokio::spawn(pump_raw_output(
        stderr,
        tokio::io::stderr(),
        mpsc::channel(1).0,
        activity,
    ));
    let parser_task = tokio::spawn(parse_stream_lines(
        line_rx,
        Arc::clone(&state),
        latest_session_id,
    ));

    let (live_restart_requested, child_status) = tokio::select! {
        status = child.wait() => {
            let status = status?;
            log_to_file(&format!(
                "[Stream JSON] Claude exited with status {status}."
            ));
            (false, Some(status))
        }
        command = resume_rx.recv() => {
            match command {
                Some(StreamResumeCommand::Continue) => {
                    log_to_file("[Stream JSON] Continue requested. Killing print-mode child for restart with --resume.");
                    restart_running_child(&mut child).await?;
                    (true, None)
                }
                None => {
                    let status = child.wait().await?;
                    log_to_file(&format!(
                        "[Stream JSON] Claude exited with status {status}."
                    ));
                    (false, Some(status))
                }
            }
        }
    };

    stdout_task.await??;
    stderr_task.await??;
    parser_task.await?;

    if live_restart_requested {
        return Ok(StreamProcessAction::Restart);
    }

    let outcome = child_status
        .map(ChildOutcome::from_std)
        .unwrap_or(ChildOutcome::Code(0));
    Ok(await_resume_after_exit(
        lockout_recorded_since(&state, starting_lockout_revision),
        outcome,
        resume_rx,
    )
    .await)
}

fn lockout_recorded_since(state: &SharedAppState, starting_revision: u64) -> bool {
    let state = state.lock().unwrap_or_else(|e| e.into_inner());
    state.lockout_revision != starting_revision && state.lockout_target_time.is_some()
}

async fn await_resume_after_exit(
    resume_pending: bool,
    original_outcome: ChildOutcome,
    resume_rx: &mut mpsc::UnboundedReceiver<StreamResumeCommand>,
) -> StreamProcessAction {
    if let Ok(StreamResumeCommand::Continue) = resume_rx.try_recv() {
        return StreamProcessAction::Restart;
    }

    if !resume_pending {
        return StreamProcessAction::Exit(original_outcome);
    }

    while let Some(command) = resume_rx.recv().await {
        if matches!(command, StreamResumeCommand::Continue) {
            return StreamProcessAction::Restart;
        }
    }

    StreamProcessAction::Exit(original_outcome)
}

async fn restart_running_child(child: &mut tokio::process::Child) -> Result<()> {
    match child.start_kill() {
        Ok(()) => {
            let _ = child.wait().await?;
            Ok(())
        }
        Err(error) => {
            if child.try_wait()?.is_some() {
                Ok(())
            } else {
                Err(error.into())
            }
        }
    }
}

fn resume_command_with_program(program: &std::ffi::OsStr, session_id: &str) -> CommandSpec {
    let mut command = stream_resume_command(session_id);
    command.program = program.to_os_string();
    command
}

async fn parse_stream_lines(
    mut line_rx: mpsc::Receiver<String>,
    state: SharedAppState,
    latest_session_id: Arc<Mutex<Option<String>>>,
) {
    while let Some(line) = line_rx.recv().await {
        match parse_stream_line(&line) {
            StreamLineResult::Signal(signal) => {
                if let Some(session_id) = signal.session_id {
                    *latest_session_id.lock().unwrap_or_else(|e| e.into_inner()) = Some(session_id);
                }
                if let Some(limit) = signal.rate_limit {
                    crate::monitor::record_limit_update(&state, limit);
                }
            }
            StreamLineResult::InvalidJson => {
                log_to_file("[Stream JSON] Invalid NDJSON line; raw output was preserved.");
            }
            StreamLineResult::Ignored => {}
        }
    }
}

fn parse_stream_line(line: &str) -> StreamLineResult {
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return StreamLineResult::InvalidJson;
    };

    let session_id = find_string_field(&value, &["session_id", "sessionId"]);
    let rate_limit = extract_rate_limit(&value);

    if session_id.is_some() || rate_limit.is_some() {
        StreamLineResult::Signal(StreamJsonSignal {
            session_id,
            rate_limit,
        })
    } else {
        StreamLineResult::Ignored
    }
}

fn extract_rate_limit(value: &Value) -> Option<LimitUpdate> {
    if value.get("error").and_then(Value::as_str) != Some("rate_limit") {
        return None;
    }

    let timestamp = find_string_field(value, &["timestamp"])?;
    let log_time = DateTime::parse_from_rfc3339(&timestamp)
        .ok()?
        .with_timezone(&Local);
    let message = find_rate_limit_text(value)?;

    limit_update_from_message(log_time, &message, Local::now())
}

fn find_rate_limit_text(value: &Value) -> Option<String> {
    find_string_value(value, &|text| {
        let lower = text.to_ascii_lowercase();
        (lower.contains("rate_limit") || lower.contains("limit")) && lower.contains("reset")
    })
}

fn find_string_field(value: &Value, names: &[&str]) -> Option<String> {
    match value {
        Value::Object(map) => {
            for name in names {
                if let Some(Value::String(text)) = map.get(*name) {
                    return Some(text.clone());
                }
            }
            map.values()
                .find_map(|child| find_string_field(child, names))
        }
        Value::Array(items) => items
            .iter()
            .find_map(|child| find_string_field(child, names)),
        _ => None,
    }
}

fn find_string_value(value: &Value, matches: &dyn Fn(&str) -> bool) -> Option<String> {
    match value {
        Value::String(text) if matches(text) => Some(text.clone()),
        Value::Array(items) => items
            .iter()
            .find_map(|child| find_string_value(child, matches)),
        Value::Object(map) => map
            .values()
            .find_map(|child| find_string_value(child, matches)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        await_resume_after_exit, lockout_recorded_since, parse_stream_line, pump_raw_output,
        resume_command_with_program, StreamLineResult, StreamProcessAction, StreamResumeCommand,
        StreamResumeSink, MAX_PARSER_LINE_BYTES,
    };
    #[cfg(unix)]
    use super::{run_one_stream_process, run_stream_json_print};
    use crate::cli::CommandSpec;
    use crate::harness::{LimitState, LimitUpdate, ResumeOutcome, ResumeSink};
    use crate::models::{output_is_hot, AppState, ChildOutcome};
    use chrono::{DateTime, Duration as ChronoDuration, Local};
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::{AsyncRead, ReadBuf};
    use tokio::sync::mpsc;

    struct FragmentedReader {
        chunks: VecDeque<Vec<u8>>,
    }

    impl FragmentedReader {
        fn new(chunks: &[&[u8]]) -> Self {
            Self {
                chunks: chunks.iter().map(|chunk| chunk.to_vec()).collect(),
            }
        }

        fn new_owned(chunks: Vec<Vec<u8>>) -> Self {
            Self {
                chunks: chunks.into(),
            }
        }
    }

    impl AsyncRead for FragmentedReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if let Some(chunk) = self.chunks.pop_front() {
                buf.put_slice(&chunk);
            }
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn stream_resume_sends_continue_command() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let sink = StreamResumeSink { sender };

        assert_eq!(sink.resume(), ResumeOutcome::Sent);
        assert_eq!(receiver.recv().await, Some(StreamResumeCommand::Continue));
    }

    #[test]
    fn closed_stream_is_a_definite_failure() {
        let (sender, receiver) = mpsc::unbounded_channel();
        drop(receiver);
        let sink = StreamResumeSink { sender };

        assert_eq!(
            sink.resume(),
            ResumeOutcome::DefiniteFailure("stream-json runner is no longer available".to_string())
        );
    }

    #[test]
    fn invalid_json_is_reported_without_failing_raw_output() {
        assert!(matches!(
            parse_stream_line("not json"),
            StreamLineResult::InvalidJson
        ));
    }

    #[test]
    fn ignores_valid_signal_free_event() {
        assert!(matches!(
            parse_stream_line(r#"{"type":"result","result":"ok"}"#),
            StreamLineResult::Ignored
        ));
    }

    #[test]
    fn extracts_session_id_from_stream_event() {
        let result = parse_stream_line(
            r#"{"type":"system","session_id":"11111111-1111-1111-1111-111111111111"}"#,
        );

        let StreamLineResult::Signal(signal) = result else {
            panic!("expected signal");
        };
        assert_eq!(
            signal.session_id.as_deref(),
            Some("11111111-1111-1111-1111-111111111111")
        );
        assert!(signal.rate_limit.is_none());
    }

    #[test]
    fn extracts_rate_limit_from_nested_stream_event() {
        let result = parse_stream_line(
            r#"{"type":"error","timestamp":"2099-07-09T10:00:00-04:00","error":"rate_limit","message":{"content":[{"type":"text","text":"Claude limit reached; resets 5:30pm"}]}}"#,
        );

        let StreamLineResult::Signal(signal) = result else {
            panic!("expected signal");
        };
        let limit = signal.rate_limit.expect("rate limit");
        assert_eq!(
            limit.event_time,
            DateTime::parse_from_rfc3339("2099-07-09T10:00:00-04:00")
                .unwrap()
                .with_timezone(&Local)
        );
        assert!(matches!(
            limit.state,
            LimitState::Locked { ref display, .. } if display == "5:30pm"
        ));
    }

    #[test]
    fn ignores_assistant_text_that_only_mentions_a_limit_reset() {
        assert!(matches!(
            parse_stream_line(
                r#"{"type":"assistant","timestamp":"2099-07-09T10:00:00-04:00","message":{"content":[{"type":"text","text":"Claude limit reached; resets 5:30pm"}]}}"#,
            ),
            StreamLineResult::Ignored
        ));
    }

    #[test]
    fn ignores_unknown_json_shape() {
        assert!(matches!(
            parse_stream_line(r#"{"type":"assistant","message":{"content":[]}}"#),
            StreamLineResult::Ignored
        ));
    }

    #[test]
    fn waits_only_for_lockouts_recorded_during_current_process() {
        let state = Arc::new(Mutex::new(AppState::new()));
        state.lock().unwrap().lockout_target_time = Some(Local::now() + ChronoDuration::hours(1));
        let starting_revision = state.lock().unwrap().lockout_revision;

        assert!(!lockout_recorded_since(&state, starting_revision));

        let event_time = Local::now();
        crate::monitor::record_limit_update(
            &state,
            LimitUpdate {
                event_time,
                state: LimitState::Locked {
                    target_time: event_time + ChronoDuration::hours(2),
                    display: "later".to_string(),
                },
            },
        );

        assert!(lockout_recorded_since(&state, starting_revision));
    }

    #[test]
    fn newer_clear_does_not_leave_resume_pending() {
        let state = Arc::new(Mutex::new(AppState::new()));
        let starting_revision = state.lock().unwrap().lockout_revision;
        let event_time = Local::now();

        crate::monitor::record_limit_update(
            &state,
            LimitUpdate {
                event_time,
                state: LimitState::Locked {
                    target_time: event_time + ChronoDuration::hours(2),
                    display: "later".to_string(),
                },
            },
        );
        crate::monitor::record_limit_update(
            &state,
            LimitUpdate {
                event_time: event_time + ChronoDuration::seconds(1),
                state: LimitState::Clear,
            },
        );

        assert!(!lockout_recorded_since(&state, starting_revision));
    }

    #[tokio::test]
    async fn pump_preserves_raw_bytes_and_emits_complete_lines() {
        let state = AppState::new();
        let (line_tx, mut line_rx) = mpsc::channel(8);
        let input = b"{\"type\":\"one\"}\n{\"type\":\"two\"}\n".as_slice();
        let mut output = Vec::new();

        pump_raw_output(
            input,
            &mut output,
            line_tx,
            state.last_output_activity.clone(),
        )
        .await
        .expect("pump succeeds");

        assert_eq!(output, b"{\"type\":\"one\"}\n{\"type\":\"two\"}\n");
        assert_eq!(line_rx.recv().await.as_deref(), Some("{\"type\":\"one\"}"));
        assert_eq!(line_rx.recv().await.as_deref(), Some("{\"type\":\"two\"}"));
        assert!(output_is_hot(
            &state.last_output_activity,
            Duration::from_secs(2)
        ));
    }

    #[tokio::test]
    async fn pump_keeps_incomplete_tail_raw_without_parsing_it() {
        let state = AppState::new();
        let (line_tx, mut line_rx) = mpsc::channel(8);
        let input = b"{\"partial\":true}".as_slice();
        let mut output = Vec::new();

        pump_raw_output(
            input,
            &mut output,
            line_tx,
            state.last_output_activity.clone(),
        )
        .await
        .expect("pump succeeds");

        assert_eq!(output, b"{\"partial\":true}");
        assert!(line_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn pump_never_blocks_when_line_channel_is_full() {
        let state = AppState::new();
        let (line_tx, _line_rx) = mpsc::channel(1);
        line_tx.try_send("{\"filled\":true}".to_string()).unwrap();
        let input = b"{\"type\":\"one\"}\n{\"type\":\"two\"}\n".as_slice();
        let mut output = Vec::new();

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            pump_raw_output(
                input,
                &mut output,
                line_tx,
                state.last_output_activity.clone(),
            ),
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(output, b"{\"type\":\"one\"}\n{\"type\":\"two\"}\n");
    }

    #[tokio::test]
    async fn oversized_line_preserves_raw_bytes_and_resumes_after_newline() {
        let oversized = vec![b'x'; MAX_PARSER_LINE_BYTES + 1];
        let valid = b"\n{\"type\":\"system\",\"session_id\":\"after-cap\"}\n";
        let input = [oversized.as_slice(), valid].concat();
        let (line_tx, mut line_rx) = mpsc::channel(4);
        let mut output = Vec::new();

        pump_raw_output(
            FragmentedReader::new_owned(input.chunks(8192).map(Vec::from).collect()),
            &mut output,
            line_tx,
            AppState::new().last_output_activity,
        )
        .await
        .unwrap();

        assert_eq!(output, input);
        assert_eq!(
            line_rx.recv().await.as_deref(),
            Some(r#"{"type":"system","session_id":"after-cap"}"#)
        );
        assert!(line_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn fragmented_overloaded_input_preserves_every_raw_byte() {
        let state = AppState::new();
        let (line_tx, mut line_rx) = mpsc::channel(1);
        line_tx.try_send("already full".to_string()).unwrap();
        let chunks: &[&[u8]] = &[
            b"{\"type\":\"ass",
            b"istant\"}\nnot json\n{\"unknown\":",
            b"true}\n{\"partial\":true}",
        ];
        let expected = chunks.concat();
        let mut output = Vec::new();

        tokio::time::timeout(
            Duration::from_secs(1),
            pump_raw_output(
                FragmentedReader::new(chunks),
                &mut output,
                line_tx,
                state.last_output_activity.clone(),
            ),
        )
        .await
        .expect("pump deadlocked")
        .expect("pump failed");

        assert_eq!(output, expected);
        assert_eq!(line_rx.try_recv().as_deref(), Ok("already full"));
    }

    #[tokio::test]
    async fn waits_for_continue_after_child_exit_when_lockout_is_active() {
        let (resume_tx, mut resume_rx) = mpsc::unbounded_channel();

        let wait_task = tokio::spawn(async move {
            await_resume_after_exit(true, ChildOutcome::Code(7), &mut resume_rx).await
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!wait_task.is_finished());

        resume_tx
            .send(StreamResumeCommand::Continue)
            .expect("send continue");

        assert!(matches!(
            wait_task.await.expect("join wait task"),
            StreamProcessAction::Restart
        ));
    }

    #[tokio::test]
    async fn consumes_queued_continue_after_child_exit_without_lockout() {
        let (resume_tx, mut resume_rx) = mpsc::unbounded_channel();
        resume_tx
            .send(StreamResumeCommand::Continue)
            .expect("queue continue");

        assert!(matches!(
            await_resume_after_exit(false, ChildOutcome::Code(0), &mut resume_rx).await,
            StreamProcessAction::Restart
        ));
    }

    #[tokio::test]
    async fn closed_resume_channel_returns_original_nonzero_outcome() {
        let (resume_tx, mut resume_rx) = mpsc::unbounded_channel();
        drop(resume_tx);

        assert_eq!(
            await_resume_after_exit(true, ChildOutcome::Code(7), &mut resume_rx).await,
            StreamProcessAction::Exit(ChildOutcome::Code(7))
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn nonzero_child_without_lockout_returns_its_code() {
        let state = Arc::new(Mutex::new(AppState::new()));
        let (_resume_tx, mut resume_rx) = mpsc::unbounded_channel();

        let action = run_one_stream_process(
            CommandSpec {
                program: "/bin/sh".into(),
                args: vec!["-c".into(), "exit 7".into()],
            },
            state,
            Arc::new(Mutex::new(None)),
            &mut resume_rx,
        )
        .await
        .unwrap();

        assert_eq!(action, StreamProcessAction::Exit(ChildOutcome::Code(7)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn restarted_child_outcome_becomes_final() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::{SystemTime, UNIX_EPOCH};

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "botsitter-stream-outcome-{}-{nonce}.sh",
            std::process::id()
        ));
        let ready_path = path.with_extension("ready");
        std::fs::write(
            &path,
            b"#!/bin/sh\ncase \" $* \" in *\" --resume \"*) exit 9;; esac\nprintf '{\"type\":\"system\",\"session_id\":\"session-123\"}\\n'\n: > \"$1\"\nexec /bin/sleep 2\n",
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let state = Arc::new(Mutex::new(AppState::new()));
        let (resume_tx, resume_rx) = mpsc::unbounded_channel();
        let command = CommandSpec {
            program: path.clone().into_os_string(),
            args: vec![ready_path.clone().into_os_string()],
        };
        let task = tokio::spawn(run_stream_json_print(command, state, resume_rx));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !ready_path.is_file() {
            assert!(tokio::time::Instant::now() < deadline, "helper not ready");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        resume_tx.send(StreamResumeCommand::Continue).unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("stream runner timed out")
            .expect("stream runner task failed")
            .expect("stream runner returned an error");

        assert_eq!(outcome, ChildOutcome::Code(9));
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(ready_path).unwrap();
    }

    #[test]
    fn resume_command_preserves_original_program() {
        assert_eq!(
            resume_command_with_program(std::ffi::OsStr::new("/opt/bin/claude"), "session-123"),
            CommandSpec {
                program: "/opt/bin/claude".into(),
                args: vec![
                    "--resume".into(),
                    "session-123".into(),
                    "-p".into(),
                    "--output-format".into(),
                    "stream-json".into(),
                    "--verbose".into(),
                    "--include-partial-messages".into(),
                    "continue".into(),
                ],
            }
        );
    }
}
