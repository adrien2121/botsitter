#![cfg(unix)]

use chrono::{Duration as ChronoDuration, Local, SecondsFormat};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "botsitter-streaming-stress-{}-{nanos}",
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

fn expected_output() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"{\"type\":\"assistant\"}\n");
    bytes.extend_from_slice(b"not json\n");
    bytes.extend_from_slice(b"{\"unknown\":true}\n");
    bytes.extend_from_slice(
        b"{\"type\":\"system\",\"session_id\":\"11111111-1111-1111-1111-111111111111\"}\n",
    );
    for index in 0..1_100 {
        bytes
            .extend_from_slice(format!("{{\"type\":\"overload\",\"index\":{index}}}\n").as_bytes());
    }
    bytes.extend_from_slice(b"{\"incomplete\":true}");
    bytes
}

fn first_difference(actual: &[u8], expected: &[u8]) -> Option<usize> {
    actual
        .iter()
        .zip(expected)
        .position(|(actual, expected)| actual != expected)
        .or_else(|| (actual.len() != expected.len()).then(|| actual.len().min(expected.len())))
}

fn wait_for_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.is_file() {
        let entries = path
            .parent()
            .and_then(|parent| fs::read_dir(parent).ok())
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name())
            .collect::<Vec<_>>();
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {}; entries: {entries:?}",
            path.display(),
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn read_log(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_default()
}

fn wait_for_log_text(log: &Arc<Mutex<Vec<u8>>>, text: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !String::from_utf8_lossy(&log.lock().expect("lock live log")).contains(text) {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for live log text {text:?}; received:\n{}",
            String::from_utf8_lossy(&log.lock().expect("lock live log"))
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn kill_and_reap(child: &mut Child) -> ExitStatus {
    let _ = child.kill();
    child.wait().expect("reap botsitter after kill")
}

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn child_mut(&mut self) -> &mut Child {
        self.0.as_mut().expect("child guard already consumed")
    }

    fn kill_and_reap(mut self) -> ExitStatus {
        let mut child = self.0.take().expect("child guard already consumed");
        kill_and_reap(&mut child)
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = kill_and_reap(child);
        }
    }
}

#[test]
fn real_binary_streaming_stress_delivers_one_watcher_lockout() {
    let root = TestDir::new();
    let home = root.0.join("home");
    let project = home.join(".claude/projects/test");
    let tmp = root.0.join("tmp");
    let bin = root.0.join("bin");
    fs::create_dir_all(&project).expect("create isolated project");
    fs::create_dir(&tmp).expect("create isolated tmp");
    fs::create_dir(&bin).expect("create isolated bin");
    symlink(
        env!("CARGO_BIN_EXE_stream-stress-child"),
        bin.join("claude"),
    )
    .expect("symlink stress helper as claude");

    let session = project.join("session.jsonl");
    fs::write(&session, b"{\"type\":\"baseline\"}\n").expect("seed session log");
    let child = Command::new(env!("CARGO_BIN_EXE_botsitter"))
        .args([
            "claude",
            "--",
            "claude",
            "-p",
            "--output-format",
            "stream-json",
            "no-stream-signal",
        ])
        .env("HOME", &home)
        .env("TMPDIR", &tmp)
        .env("PATH", &bin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn real botsitter binary");
    let wrapper_pid = child.id();
    let log_path = tmp.join(format!("botsitter-{wrapper_pid}.log"));
    let port_path = tmp.join(format!("botsitter-{wrapper_pid}.port"));
    let mut child = ChildGuard(Some(child));

    let mut stdout = child.child_mut().stdout.take().expect("capture stdout");
    let stdout_thread = thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).expect("read stdout");
        bytes
    });
    let mut child_stderr = child.child_mut().stderr.take().expect("capture stderr");
    let stderr = Arc::new(Mutex::new(Vec::new()));
    let stderr_copy = Arc::clone(&stderr);
    let (ready_tx, ready_rx) = mpsc::channel();
    let stderr_thread = thread::spawn(move || {
        let mut chunk = [0_u8; 256];
        let mut ready_sent = false;
        loop {
            let count = child_stderr.read(&mut chunk).expect("read stderr");
            if count == 0 {
                break;
            }
            let mut bytes = stderr_copy.lock().expect("lock stderr");
            bytes.extend_from_slice(&chunk[..count]);
            if !ready_sent && bytes.windows(b"READY\n".len()).any(|w| w == b"READY\n") {
                ready_tx.send(()).expect("signal helper readiness");
                ready_sent = true;
            }
        }
    });

    wait_for_file(&port_path);
    let contents = fs::read_to_string(&port_path).expect("read logger port");
    let modified = fs::metadata(&port_path)
        .and_then(|metadata| metadata.modified())
        .expect("read logger port timestamp");
    let port = botsitter::live_logs::parse_port_record(&contents, wrapper_pid, modified)
        .expect("parse logger port")
        .port();
    let address = ("127.0.0.1", port);
    let mut readiness_client =
        TcpStream::connect(address).expect("connect readiness logger client");
    let live_log = Arc::new(Mutex::new(Vec::new()));
    let live_log_copy = Arc::clone(&live_log);
    let log_thread = thread::spawn(move || {
        let mut chunk = [0_u8; 512];
        loop {
            match readiness_client.read(&mut chunk) {
                Ok(0) => break,
                Ok(count) => live_log_copy
                    .lock()
                    .expect("lock live log")
                    .extend_from_slice(&chunk[..count]),
                Err(error) => panic!("read live logger socket: {error}"),
            }
        }
    });
    let _non_reading_client =
        TcpStream::connect(address).expect("connect non-reading logger client");
    wait_for_log_text(&live_log, "Event-driven file watcher active");
    ready_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("wait for helper READY");

    let reset = Local::now() + ChronoDuration::hours(2);
    let row = format!(
        "{{\"timestamp\":\"{}\",\"error\":\"rate_limit\",\"message\":{{\"content\":[{{\"type\":\"text\",\"text\":\"Claude limit reached; resets {}\"}}]}}}}\n",
        Local::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        reset.format("%-I:%M%P")
    );
    let mut session_writer = OpenOptions::new()
        .append(true)
        .open(&session)
        .expect("open watched session");
    session_writer
        .write_all(row.as_bytes())
        .expect("append one rate-limit row");
    session_writer.sync_all().expect("sync watched session row");
    drop(session_writer);

    let lockout = "[LOCKOUT DETECTED] Rate limit hit from file watcher.";
    let child_exited = "[Stream JSON] Claude exited with status exit status: 0.";
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let log = live_log.lock().expect("lock live log").clone();
        let wrapper_status = child.child_mut().try_wait().expect("poll botsitter");
        if log
            .windows(lockout.len())
            .filter(|window| *window == lockout.as_bytes())
            .count()
            == 1
            && String::from_utf8_lossy(&log).contains(child_exited)
        {
            assert!(wrapper_status.is_none(), "botsitter exited instead of awaiting Continue; status={wrapper_status:?}; stderr:\n{}\nlive log:\n{}", String::from_utf8_lossy(&stderr.lock().expect("lock stderr")), String::from_utf8_lossy(&log));
            break;
        }
        if Instant::now() >= deadline {
            let status = child.kill_and_reap();
            let stdout = stdout_thread.join().expect("join stdout reader");
            stderr_thread.join().expect("join stderr reader");
            panic!("timed out waiting for one watcher lockout and child completion; forced status={status}; stdout={} bytes:\n{}\nstderr:\n{}\npersistent log:\n{}\nlive log:\n{}", stdout.len(), String::from_utf8_lossy(&stdout), String::from_utf8_lossy(&stderr.lock().expect("lock stderr")), read_log(&log_path), String::from_utf8_lossy(&log));
        }
        thread::sleep(Duration::from_millis(10));
    }

    let status = child.kill_and_reap();
    let stdout = stdout_thread.join().expect("join stdout reader");
    stderr_thread.join().expect("join stderr reader");
    log_thread.join().expect("join live log reader");
    let stderr = stderr.lock().expect("lock stderr").clone();
    let log = live_log.lock().expect("lock live log").clone();
    let expected = expected_output();
    let difference = first_difference(&stdout, &expected);
    assert!(
        !status.success() && difference.is_none(),
        "unexpected forced termination/output; first differing offset={difference:?}; actual length={}; expected length={}; status={}; stderr:\n{}\nlive log:\n{}",
        stdout.len(),
        expected.len(),
        status,
        String::from_utf8_lossy(&stderr),
        String::from_utf8_lossy(&log)
    );
    assert_eq!(
        String::from_utf8_lossy(&log).matches(lockout).count(),
        1,
        "expected one watcher lockout; status={}; stdout={} bytes; stderr:\n{}\nlive log:\n{}",
        status,
        stdout.len(),
        String::from_utf8_lossy(&stderr),
        String::from_utf8_lossy(&log)
    );
    assert!(!String::from_utf8_lossy(&log).contains("Raw Limit Message"));
    assert!(
        stderr
            .windows(b"READY\n".len())
            .any(|window| window == b"READY\n"),
        "helper readiness missing; status={status}; stderr:\n{}\nlive log:\n{}",
        String::from_utf8_lossy(&stderr),
        String::from_utf8_lossy(&log)
    );
}
