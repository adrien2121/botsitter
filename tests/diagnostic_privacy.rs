#![cfg(unix)]

use chrono::{Duration as ChronoDuration, Timelike};
use portable_pty::{CommandBuilder, NativePtySystem, PtySize, PtySystem};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PRIVATE_MARKER: &str = "PRIVATE-SESSION-CONTENT-7b61f1e9";
const PRIVATE_PATH_MARKER: &str = "PRIVATE-PATH-4db272ce";

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "botsitter-diagnostic-privacy-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn wait_for_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
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
            "missing file: {}; entries: {entries:?}",
            path.display(),
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_text(log: &Arc<Mutex<Vec<u8>>>, text: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let captured = log.lock().unwrap().clone();
        if String::from_utf8_lossy(&captured).contains(text) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "missing live diagnostic: {text}; captured:\n{}",
            String::from_utf8_lossy(&captured),
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn arbitrary_session_content_is_absent_from_live_and_persistent_logs() {
    let root = TestDir::new();
    let home = root.0.join(format!("home-{PRIVATE_PATH_MARKER}"));
    let project = home.join(".claude/projects/test");
    let tmp = root.0.join("tmp");
    let bin = root.0.join("bin");
    let done = root.0.join("done");
    fs::create_dir_all(&project).unwrap();
    fs::create_dir(&tmp).unwrap();
    fs::create_dir(&bin).unwrap();
    let session = project.join(format!("session-{PRIVATE_PATH_MARKER}.jsonl"));
    fs::write(&session, b"{\"type\":\"baseline\"}\n").unwrap();
    let claude = bin.join("claude");
    fs::write(
        &claude,
        b"#!/bin/sh\ncase \" $* \" in *\" --resume \"*) exit 0;; esac\nprintf '{\"type\":\"system\",\"session_id\":\"11111111-1111-1111-1111-111111111111\"}\\n'\ni=0\nwhile [ ! -f \"$BOTSITTER_PRIVACY_DONE\" ] && [ \"$i\" -lt 200 ]; do /bin/sleep 0.05; i=$((i + 1)); done\ntest -f \"$BOTSITTER_PRIVACY_DONE\"\n",
    )
    .unwrap();
    fs::set_permissions(&claude, fs::Permissions::from_mode(0o755)).unwrap();

    let pair = NativePtySystem::default()
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .unwrap();
    let mut outer_reader = pair.master.try_clone_reader().unwrap();
    let output_thread = thread::spawn(move || {
        let mut output = Vec::new();
        outer_reader.read_to_end(&mut output).unwrap();
        output
    });
    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_botsitter"));
    command.args([
        "claude",
        "--",
        "claude",
        "-p",
        "--output-format",
        "stream-json",
        "privacy-test",
    ]);
    command.env("HOME", &home);
    command.env("TMPDIR", &tmp);
    command.env("PATH", &bin);
    command.env("BOTSITTER_PRIVACY_DONE", &done);
    let mut child = pair.slave.spawn_command(command).unwrap();
    let pid = child.process_id().unwrap();
    drop(pair.slave);
    let port_path = tmp.join(format!("botsitter-{pid}.port"));
    let log_path = tmp.join(format!("botsitter-{pid}.log"));
    wait_for_file(&port_path);
    let contents = fs::read_to_string(&port_path).unwrap();
    let modified = fs::metadata(&port_path).unwrap().modified().unwrap();
    let port = botsitter::live_logs::parse_port_record(&contents, pid, modified)
        .unwrap()
        .port();
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let live = Arc::new(Mutex::new(Vec::new()));
    let live_copy = Arc::clone(&live);
    let reader = thread::spawn(move || {
        let mut chunk = [0_u8; 512];
        loop {
            let count = stream.read(&mut chunk).unwrap();
            if count == 0 {
                break;
            }
            live_copy.lock().unwrap().extend_from_slice(&chunk[..count]);
        }
    });
    wait_for_text(&live, "Event-driven file watcher active");
    thread::sleep(Duration::from_millis(250));

    {
        let mut file = OpenOptions::new().append(true).open(&session).unwrap();
        let event_time = chrono::Local::now()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap()
            - ChronoDuration::minutes(2);
        let reset = (chrono::Local::now() + ChronoDuration::minutes(1)).format("%-I:%M%P");
        writeln!(
            file,
            "{{\"timestamp\":\"{}\",\"error\":\"rate_limit\",\"message\":{{\"content\":[{{\"type\":\"text\",\"text\":\"{PRIVATE_MARKER} Claude limit reached; resets {reset}\"}}]}}}}",
            event_time.to_rfc3339(),
        )
        .unwrap();
        file.sync_all().unwrap();
    }
    wait_for_text(&live, "[File Event] Triggering scan. Changed transcripts:");
    wait_for_text(
        &live,
        "[LOCKOUT DETECTED] Rate limit hit from file watcher.",
    );

    {
        let mut file = OpenOptions::new().append(true).open(&session).unwrap();
        let event_time = chrono::Local::now()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap()
            - ChronoDuration::minutes(1);
        let reset = event_time.format("%b %-d at %-I:%M%P");
        writeln!(
            file,
            "{{\"timestamp\":\"{}\",\"error\":\"rate_limit\",\"message\":{{\"content\":[{{\"type\":\"text\",\"text\":\"{PRIVATE_MARKER} Claude limit reached; resets {reset}\"}}]}}}}",
            event_time.to_rfc3339(),
        )
        .unwrap();
        file.sync_all().unwrap();
    }
    wait_for_text(
        &live,
        "[LIMIT UPDATE] Transcript state cleared from file watcher.",
    );

    fs::write(&done, b"done").unwrap();
    let status = child.wait().unwrap();
    assert_eq!(status.exit_code(), 0);
    drop(pair.master);
    let output = output_thread.join().unwrap();
    reader.join().unwrap();
    let persistent = fs::read_to_string(log_path).unwrap();
    let live = String::from_utf8_lossy(&live.lock().unwrap()).into_owned();
    for private in [PRIVATE_MARKER, PRIVATE_PATH_MARKER] {
        assert!(!persistent.contains(private));
        assert!(!live.contains(private));
        assert!(!String::from_utf8_lossy(&output).contains(private));
    }
    assert!(persistent.contains("[File Event] Triggering scan. Changed transcripts:"));
    assert!(persistent.contains("[LOCKOUT DETECTED] Rate limit hit from file watcher."));
}
