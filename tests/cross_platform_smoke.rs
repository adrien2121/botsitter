use portable_pty::{Child, CommandBuilder, NativePtySystem, PtySize, PtySystem};
use std::fs;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const WAIT: Duration = Duration::from_secs(20);
const SENTINEL: &str = "CROSS-PLATFORM-PTY-SMOKE";

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(case: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "botsitter-cross-platform-{}-{nonce}-{case}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create isolated test directory");
        Self { path }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct ChildGuard(Option<Box<dyn Child + Send + Sync>>);

impl ChildGuard {
    fn child_mut(&mut self) -> &mut (dyn Child + Send + Sync) {
        self.0.as_deref_mut().expect("child guard already consumed")
    }

    fn take(&mut self) -> Box<dyn Child + Send + Sync> {
        self.0.take().expect("child guard already consumed")
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct CaseResult {
    capture: Vec<u8>,
    log: String,
    live_log: Vec<u8>,
    output: Vec<u8>,
}

fn wait_until_with_diagnostics<T, F>(deadline: Instant, description: &str, mut predicate: F) -> T
where
    F: FnMut() -> (Option<T>, String),
{
    loop {
        let (value, diagnostics) = predicate();
        if let Some(value) = value {
            return value;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for {description}\n{diagnostics}");
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn read_live(stream: &mut TcpStream, live: &mut Vec<u8>) {
    let mut chunk = [0_u8; 1024];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(count) => live.extend_from_slice(&chunk[..count]),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                break
            }
            Err(error) if is_normal_peer_close(&error) => break,
            Err(error) => panic!("read live logger: {error}"),
        }
    }
}

fn is_normal_peer_close(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::ConnectionReset
}

#[cfg(test)]
mod tests {
    use super::is_normal_peer_close;
    use std::io;

    #[test]
    fn recognizes_connection_reset_as_normal_peer_close() {
        assert!(is_normal_peer_close(&io::Error::from(
            io::ErrorKind::ConnectionReset,
        )));
    }

    #[test]
    fn rejects_unrelated_errors_as_normal_peer_close() {
        assert!(!is_normal_peer_close(&io::Error::from(
            io::ErrorKind::Other
        )));
    }
}

fn diagnostics(output: &Arc<Mutex<Vec<u8>>>, live: &[u8], log: &Path) -> String {
    format!(
        "PTY output:\n{}\nlive log:\n{}\npersistent log:\n{}",
        String::from_utf8_lossy(&output.lock().expect("lock PTY output")),
        String::from_utf8_lossy(live),
        fs::read_to_string(log).unwrap_or_default()
    )
}

fn diagnostics_bytes(output: &[u8], live: &[u8], log: &Path) -> String {
    format!(
        "PTY output:\n{}\nlive log:\n{}\npersistent log:\n{}",
        String::from_utf8_lossy(output),
        String::from_utf8_lossy(live),
        fs::read_to_string(log).unwrap_or_default()
    )
}

fn connect_logger(path: &Path) -> Option<TcpStream> {
    let contents = fs::read_to_string(path).ok()?;
    let pid = botsitter::paths::pid_from_port_path(path)?;
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    let record = botsitter::live_logs::parse_port_record(&contents, pid, modified).ok()?;
    TcpStream::connect(("127.0.0.1", record.port())).ok()
}

fn open_pair() -> portable_pty::PtyPair {
    NativePtySystem::default()
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .expect("open outer PTY")
}

fn run_pty_case(provider: &str) -> CaseResult {
    let root = TestDir::new(provider);
    let root_path = root.path.clone();
    let tmp = root.path.join("tmp");
    let home = root.path.join("home");
    let codex_home = root.path.join("codex-home");
    let capture = root.path.join("capture");
    fs::create_dir_all(&tmp).expect("create isolated temp directory");
    fs::create_dir_all(&home).expect("create isolated home directory");
    fs::create_dir_all(&codex_home).expect("create isolated Codex home");

    let pair = open_pair();
    let mut reader = pair.master.try_clone_reader().expect("clone PTY reader");
    let output = Arc::new(Mutex::new(Vec::new()));
    let output_copy = Arc::clone(&output);
    let output_thread = thread::spawn(move || {
        let mut chunk = [0_u8; 4096];
        while let Ok(count) = reader.read(&mut chunk) {
            if count == 0 {
                break;
            }
            output_copy
                .lock()
                .expect("lock PTY output")
                .extend_from_slice(&chunk[..count]);
        }
    });

    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_botsitter"));
    command.args([provider, "--", env!("CARGO_BIN_EXE_codex-test-child")]);
    command.env("BOTSITTER_TEST_EVENT", "pty-smoke");
    command.env("BOTSITTER_TEST_SENTINEL", SENTINEL);
    command.env("BOTSITTER_CAPTURE", &capture);
    for key in ["TMPDIR", "TMP", "TEMP"] {
        command.env(key, &tmp);
    }
    command.env("HOME", &home);
    command.env("USERPROFILE", &home);
    command.env("CODEX_HOME", &codex_home);

    let child = pair.slave.spawn_command(command).expect("spawn botsitter");
    let wrapper_pid = child.process_id().expect("botsitter PID");
    let paths = botsitter::paths::LoggerPaths::for_pid_in(&tmp, wrapper_pid);
    let mut child = ChildGuard(Some(child));
    drop(pair.slave);

    let mut live_stream = wait_until_with_diagnostics(
        Instant::now() + WAIT,
        "logger port record and TCP connection",
        || {
            (
                connect_logger(&paths.port),
                diagnostics(&output, &[], &paths.log),
            )
        },
    );
    live_stream
        .set_nonblocking(true)
        .expect("make live logger nonblocking");
    let mut live_log = Vec::new();
    let mut writer = pair.master.take_writer().expect("take outer PTY writer");
    writer.write_all(b"PING\r").expect("write PTY smoke input");
    writer.flush().expect("flush PTY smoke input");
    drop(writer);

    let status = wait_until_with_diagnostics(Instant::now() + WAIT, "pty smoke child exit", || {
        read_live(&mut live_stream, &mut live_log);
        (
            child.child_mut().try_wait().expect("poll botsitter"),
            diagnostics(&output, &live_log, &paths.log),
        )
    });
    assert_eq!(
        status.exit_code(),
        7,
        "provider {provider} returned unexpected status\n{}",
        diagnostics(&output, &live_log, &paths.log)
    );
    drop(child.take());
    drop(pair.master);
    output_thread.join().expect("join PTY reader");
    read_live(&mut live_stream, &mut live_log);

    let output = output.lock().expect("lock final PTY output").clone();
    let output_text = String::from_utf8_lossy(&output);
    let log = fs::read_to_string(&paths.log).expect("read persistent log");
    let capture_bytes = fs::read(&capture).expect("read PTY smoke capture");
    assert!(
        output_text.contains(SENTINEL),
        "missing sentinel in PTY output"
    );
    assert!(
        output_text.contains("ECHO:PING"),
        "missing echo in PTY output"
    );
    assert_eq!(
        String::from_utf8_lossy(&capture_bytes).trim(),
        "PING",
        "unexpected PTY capture"
    );
    assert!(log.contains(SENTINEL), "missing sentinel in persistent log");
    assert!(log.contains("ECHO:PING"), "missing echo in persistent log");
    wait_until_with_diagnostics(Instant::now() + WAIT, "port manifest cleanup", || {
        (
            (!paths.port.exists()).then_some(()),
            diagnostics_bytes(&output, &live_log, &paths.log),
        )
    });
    drop(root);
    assert!(!root_path.exists(), "isolated root remained after PTY case");

    CaseResult {
        capture: capture_bytes,
        log,
        live_log,
        output,
    }
}

fn run_resume_case() -> CaseResult {
    let root = TestDir::new("resume");
    let root_path = root.path.clone();
    let tmp = root.path.join("tmp");
    let home = root.path.join("home");
    let codex_home = root.path.join("codex-home");
    let capture = root.path.join("capture");
    let trigger = root.path.join("trigger");
    fs::create_dir_all(&tmp).expect("create isolated temp directory");
    fs::create_dir_all(&home).expect("create isolated home directory");
    fs::create_dir_all(&codex_home).expect("create isolated Codex home");

    let pair = open_pair();
    let mut reader = pair.master.try_clone_reader().expect("clone PTY reader");
    let output = Arc::new(Mutex::new(Vec::new()));
    let output_copy = Arc::clone(&output);
    let output_thread = thread::spawn(move || {
        let mut chunk = [0_u8; 4096];
        while let Ok(count) = reader.read(&mut chunk) {
            if count == 0 {
                break;
            }
            output_copy
                .lock()
                .expect("lock PTY output")
                .extend_from_slice(&chunk[..count]);
        }
    });

    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_botsitter"));
    command.args(["codex", "--", env!("CARGO_BIN_EXE_codex-test-child")]);
    command.env("BOTSITTER_TEST_EVENT", "saturated");
    command.env("BOTSITTER_TEST_SENTINEL", SENTINEL);
    command.env("BOTSITTER_CAPTURE", &capture);
    command.env("BOTSITTER_TRIGGER", &trigger);
    for key in ["TMPDIR", "TMP", "TEMP"] {
        command.env(key, &tmp);
    }
    command.env("HOME", &home);
    command.env("USERPROFILE", &home);
    command.env("CODEX_HOME", &codex_home);

    let child = pair.slave.spawn_command(command).expect("spawn botsitter");
    let wrapper_pid = child.process_id().expect("botsitter PID");
    let paths = botsitter::paths::LoggerPaths::for_pid_in(&tmp, wrapper_pid);
    let mut child = ChildGuard(Some(child));
    drop(pair.slave);
    let mut live_stream =
        wait_until_with_diagnostics(Instant::now() + WAIT, "resume logger readiness", || {
            (
                connect_logger(&paths.port),
                diagnostics(&output, &[], &paths.log),
            )
        });
    live_stream
        .set_nonblocking(true)
        .expect("make live logger nonblocking");
    let mut live_log = Vec::new();
    wait_until_with_diagnostics(
        Instant::now() + WAIT,
        "live logger watcher readiness",
        || {
            read_live(&mut live_stream, &mut live_log);
            (
                String::from_utf8_lossy(&live_log)
                    .contains("Event-driven file watcher active")
                    .then_some(()),
                diagnostics(&output, &live_log, &paths.log),
            )
        },
    );
    fs::write(&trigger, b"go").expect("release resume fixture");

    let status = wait_until_with_diagnostics(Instant::now() + WAIT, "resume child exit", || {
        read_live(&mut live_stream, &mut live_log);
        (
            child.child_mut().try_wait().expect("poll botsitter"),
            diagnostics(&output, &live_log, &paths.log),
        )
    });
    assert_eq!(
        status.exit_code(),
        0,
        "resume returned unexpected status\n{}",
        diagnostics(&output, &live_log, &paths.log)
    );
    drop(child.take());
    drop(pair.master);
    output_thread.join().expect("join PTY reader");
    read_live(&mut live_stream, &mut live_log);

    let output = output.lock().expect("lock final PTY output").clone();
    let log = fs::read_to_string(&paths.log).expect("read persistent log");
    let capture_bytes = fs::read(&capture).expect("read resume capture");
    assert_eq!(
        String::from_utf8_lossy(&capture_bytes).trim(),
        "continue",
        "unexpected resume input"
    );
    assert_eq!(
        log.matches("[System] Resume command sent.").count(),
        1,
        "resume count mismatch\n{}",
        diagnostics_bytes(&output, &live_log, &paths.log)
    );
    wait_until_with_diagnostics(
        Instant::now() + WAIT,
        "resume port manifest cleanup",
        || {
            (
                (!paths.port.exists()).then_some(()),
                diagnostics_bytes(&output, &live_log, &paths.log),
            )
        },
    );
    drop(root);
    assert!(
        !root_path.exists(),
        "isolated root remained after resume case"
    );

    CaseResult {
        capture: capture_bytes,
        log,
        live_log,
        output,
    }
}

#[test]
#[ignore]
fn release_binaries_forward_pty_and_cleanup() {
    for provider in ["claude", "codex"] {
        let result = run_pty_case(provider);
        assert_eq!(String::from_utf8_lossy(&result.capture).trim(), "PING");
        assert!(String::from_utf8_lossy(&result.output).contains(SENTINEL));
        assert!(String::from_utf8_lossy(&result.live_log).contains(SENTINEL));
        assert!(result.log.contains(SENTINEL));
    }
}

#[test]
#[ignore]
fn release_codex_resume_signal_and_cleanup() {
    let result = run_resume_case();
    assert_eq!(String::from_utf8_lossy(&result.capture).trim(), "continue");
    assert_eq!(
        result.log.matches("[System] Resume command sent.").count(),
        1
    );
}
