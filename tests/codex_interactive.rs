#![cfg(unix)]

use portable_pty::{Child, CommandBuilder, NativePtySystem, PtySize, PtySystem};
use std::fs;
use std::io::Read;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PRIVATE_MARKER: &str = "PRIVATE-CODEX-FIXTURE-4d9c6a21";

struct TestDir(PathBuf);

impl TestDir {
    fn new(mode: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "botsitter-codex-interactive-{}-{nonce}-{mode}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create isolated test directory");
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
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

fn process_guard() -> MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}

fn wait_for_live_log(
    port_path: &Path,
    expected: &str,
    child: &mut (dyn Child + Send + Sync),
    output: &Arc<Mutex<Vec<u8>>>,
) -> (TcpStream, Vec<u8>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let port = loop {
        if let (Ok(contents), Some(pid), Ok(modified)) = (
            fs::read_to_string(port_path),
            botsitter::paths::pid_from_port_path(port_path),
            fs::metadata(port_path).and_then(|metadata| metadata.modified()),
        ) {
            if let Ok(record) = botsitter::live_logs::parse_port_record(&contents, pid, modified) {
                break record.port();
            }
        }
        assert!(
            Instant::now() < deadline,
            "logger port did not become ready"
        );
        thread::sleep(Duration::from_millis(10));
    };
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect live logger");
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .expect("bound live logger reads");
    let mut live = Vec::new();
    let mut chunk = [0_u8; 512];
    loop {
        if String::from_utf8_lossy(&live).contains(expected) {
            return (stream, live);
        }
        if let Some(status) = child.try_wait().expect("poll botsitter during startup") {
            panic!(
                "botsitter exited before {expected:?}: {status:?}; output:\n{}\nlive log:\n{}",
                String::from_utf8_lossy(&output.lock().expect("lock PTY output")),
                String::from_utf8_lossy(&live)
            );
        }
        match stream.read(&mut chunk) {
            Ok(0) => panic!("live logger closed before {expected:?}"),
            Ok(count) => live.extend_from_slice(&chunk[..count]),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => panic!("read live logger: {error}"),
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected:?}; output:\n{}\nlive log:\n{}",
            String::from_utf8_lossy(&output.lock().expect("lock PTY output")),
            String::from_utf8_lossy(&live)
        );
    }
}

struct CaseResult {
    capture: Option<Vec<u8>>,
    log: String,
    live_log: Vec<u8>,
    output: Vec<u8>,
}

fn assert_no_resume_diagnostics(log: &str, mode: &str) {
    for diagnostic in [
        "[LOCKOUT ON STARTUP]",
        "[LOCKOUT DETECTED]",
        "[Lockout Cooldown]",
        "[Trigger] Reset time reached.",
        "[Resume Error]",
        "[System] Resume command sent.",
    ] {
        assert!(
            !log.contains(diagnostic),
            "unexpected resume/expiry diagnostic {diagnostic:?} for mode {mode}; log:\n{log}"
        );
    }
}

fn drain_live_log(stream: &mut TcpStream, live: &mut Vec<u8>) {
    let mut chunk = [0_u8; 512];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(count) => live.extend_from_slice(&chunk[..count]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return,
            Err(error) => panic!("read live logger: {error}"),
        }
    }
}

fn run_case(mode: &str) -> CaseResult {
    let root = TestDir::new(mode);
    let codex_home = root.0.join("codex");
    let tmp = root.0.join("tmp");
    let capture = root.0.join("capture");
    let trigger = root.0.join("trigger");
    fs::create_dir(&codex_home).expect("create isolated Codex home");
    fs::create_dir(&tmp).expect("create isolated temp directory");

    let pair = NativePtySystem::default()
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .expect("open outer PTY");
    let mut reader = pair.master.try_clone_reader().expect("clone PTY reader");
    let output = Arc::new(Mutex::new(Vec::new()));
    let output_copy = Arc::clone(&output);
    let output_thread = thread::spawn(move || {
        let mut chunk = [0_u8; 512];
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
    command.env("CODEX_HOME", &codex_home);
    command.env("TMPDIR", &tmp);
    command.env("BOTSITTER_CAPTURE", &capture);
    command.env("BOTSITTER_TRIGGER", &trigger);
    command.env("BOTSITTER_TEST_EVENT", mode);
    command.env("BOTSITTER_TEST_SENTINEL", PRIVATE_MARKER);
    let child = pair
        .slave
        .spawn_command(command)
        .expect("spawn real botsitter binary");
    let wrapper_pid = child.process_id().expect("botsitter PID");
    let log_path = tmp.join(format!("botsitter-{wrapper_pid}.log"));
    let port_path = tmp.join(format!("botsitter-{wrapper_pid}.port"));
    let mut child = ChildGuard(Some(child));
    drop(pair.slave);

    let (mut live_stream, mut live_log) = wait_for_live_log(
        &port_path,
        "Event-driven file watcher active",
        child.child_mut(),
        &output,
    );
    live_stream
        .set_nonblocking(true)
        .expect("make live logger nonblocking");
    fs::write(&trigger, b"go").expect("release helper");

    let deadline = Instant::now() + Duration::from_secs(15);
    let status = loop {
        drain_live_log(&mut live_stream, &mut live_log);
        if let Some(status) = child.child_mut().try_wait().expect("poll botsitter") {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "botsitter did not exit for mode {mode}; output:\n{}\nlive log:\n{}\npersistent log:\n{}",
            String::from_utf8_lossy(&output.lock().expect("lock PTY output")),
            String::from_utf8_lossy(&live_log),
            fs::read_to_string(&log_path).unwrap_or_default(),
        );
        thread::sleep(Duration::from_millis(10));
    };
    drop(child.take());
    drop(pair.master);
    output_thread.join().expect("join outer PTY reader");
    live_stream
        .set_nonblocking(false)
        .expect("restore blocking live logger");
    live_stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("bound final logger read");
    live_stream
        .read_to_end(&mut live_log)
        .expect("finish reading live logger");
    let output = output.lock().expect("lock final PTY output").clone();
    assert_eq!(
        status.exit_code(),
        0,
        "mode {mode} failed; output:\n{}\nlog:\n{}",
        String::from_utf8_lossy(&output),
        fs::read_to_string(&log_path).unwrap_or_default()
    );

    CaseResult {
        capture: fs::read(&capture).ok(),
        log: fs::read_to_string(log_path).expect("read persistent log"),
        live_log,
        output,
    }
}

#[test]
fn real_codex_watch_wait_and_resume_semantics() {
    let _guard = process_guard();

    let saturated = run_case("saturated");
    assert_eq!(saturated.capture.as_deref(), Some(b"continue\n".as_slice()));
    assert_eq!(
        saturated
            .log
            .matches("[System] Resume command sent.")
            .count(),
        1
    );

    for mode in ["missing-reset", "null", "clear-after-lock"] {
        let result = run_case(mode);
        assert_eq!(result.capture, None, "unexpected resume for mode {mode}");
        assert_no_resume_diagnostics(&result.log, mode);
        assert!(
            !result
                .output
                .windows(PRIVATE_MARKER.len())
                .any(|window| window == PRIVATE_MARKER.as_bytes()),
            "private fixture marker reached PTY output for mode {mode}"
        );
        assert!(
            !result.log.contains(PRIVATE_MARKER),
            "private fixture marker reached persistent log for mode {mode}"
        );
        assert!(
            !result
                .live_log
                .windows(PRIVATE_MARKER.len())
                .any(|window| window == PRIVATE_MARKER.as_bytes()),
            "private fixture marker reached live log for mode {mode}"
        );
        match mode {
            "missing-reset" => assert!(result.log.contains(
                "[Parser] saturated rate-limit window has no valid future reset timestamp."
            )),
            "null" => assert!(!result.log.contains("[Parser]")),
            "clear-after-lock" => {
                assert!(result
                    .log
                    .contains("[LIMIT UPDATE] Transcript state cleared from file watcher."));
            }
            _ => unreachable!(),
        }
    }
    assert!(!saturated.log.contains(PRIVATE_MARKER));
    assert!(!saturated
        .live_log
        .windows(PRIVATE_MARKER.len())
        .any(|window| window == PRIVATE_MARKER.as_bytes()));
    assert!(!saturated
        .output
        .windows(PRIVATE_MARKER.len())
        .any(|window| window == PRIVATE_MARKER.as_bytes()));
}
