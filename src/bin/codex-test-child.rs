use chrono::{SecondsFormat, Utc};
use serde_json::json;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

fn append(path: &Path, value: serde_json::Value) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open rollout fixture");
    writeln!(file, "{value}").expect("append rollout fixture");
    file.sync_all().expect("sync rollout fixture");
}

fn snapshot(used_percent: f64, resets_at: serde_json::Value, sentinel: &str) -> serde_json::Value {
    json!({
        "timestamp": Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "rate_limits": {
                "primary": {
                    "used_percent": used_percent,
                    "window_minutes": 300,
                    "resets_at": resets_at
                },
                "secondary": null,
                "rate_limit_reached_type": null
            },
            "test_fixture_marker": sentinel
        }
    })
}

fn capture_input_for(path: &Path, duration: Duration) {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut line = String::new();
        let result = io::stdin().read_line(&mut line).map(|_| line);
        let _ = sender.send(result);
    });
    match receiver.recv_timeout(duration) {
        Ok(Ok(line)) => {
            assert!(!line.is_empty(), "resume input reached unexpected EOF");
            fs::write(path, line).expect("write unexpected resume capture");
        }
        Ok(Err(error)) => panic!("read possible resume input: {error}"),
        Err(mpsc::RecvTimeoutError::Timeout) => {}
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("resume input reader disconnected"),
    }
}

fn main() {
    let mode = std::env::var("BOTSITTER_TEST_EVENT").unwrap_or_else(|_| "saturated".into());

    if mode == "pty-smoke" {
        let capture = std::env::var_os("BOTSITTER_CAPTURE").expect("BOTSITTER_CAPTURE");
        let sentinel = std::env::var("BOTSITTER_TEST_SENTINEL").expect("BOTSITTER_TEST_SENTINEL");
        println!("{sentinel}");
        let mut line = String::new();
        io::stdin()
            .read_line(&mut line)
            .expect("read PTY smoke input");
        fs::write(capture, &line).expect("write PTY smoke capture");
        print!("ECHO:{line}");
        io::stdout().flush().expect("flush PTY smoke echo");
        std::process::exit(7);
    }

    let codex_home = std::env::var_os("CODEX_HOME").expect("CODEX_HOME");
    let capture = std::env::var_os("BOTSITTER_CAPTURE").expect("BOTSITTER_CAPTURE");
    let trigger = std::env::var_os("BOTSITTER_TRIGGER").expect("BOTSITTER_TRIGGER");
    let sentinel = std::env::var("BOTSITTER_TEST_SENTINEL").expect("BOTSITTER_TEST_SENTINEL");
    let session_dir = Path::new(&codex_home).join("sessions/2026/07/15");
    fs::create_dir_all(&session_dir).expect("create rollout fixture directory");
    let rollout = session_dir.join("rollout-test.jsonl");

    let deadline = Instant::now() + Duration::from_secs(10);
    while !Path::new(&trigger).exists() {
        assert!(Instant::now() < deadline, "test trigger timed out");
        thread::sleep(Duration::from_millis(10));
    }

    match mode.as_str() {
        "saturated" => {
            append(
                &rollout,
                snapshot(100.0, json!(Utc::now().timestamp() + 8), &sentinel),
            );
            let mut line = String::new();
            io::stdin().read_line(&mut line).expect("read resume line");
            fs::write(capture, line).expect("write resume capture");
        }
        "missing-reset" => {
            append(
                &rollout,
                snapshot(100.0, serde_json::Value::Null, &sentinel),
            );
            capture_input_for(Path::new(&capture), Duration::from_secs(6));
        }
        "null" => {
            append(
                &rollout,
                json!({
                    "timestamp": Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
                    "type": "event_msg",
                    "payload": {
                        "type": "token_count",
                        "rate_limits": null,
                        "test_fixture_marker": sentinel
                    }
                }),
            );
            capture_input_for(Path::new(&capture), Duration::from_secs(6));
        }
        "clear-after-lock" => {
            let reset = json!(Utc::now().timestamp() + 8);
            append(&rollout, snapshot(100.0, reset.clone(), &sentinel));
            thread::sleep(Duration::from_millis(100));
            append(&rollout, snapshot(50.0, reset, &sentinel));
            capture_input_for(Path::new(&capture), Duration::from_secs(9));
        }
        other => panic!("unknown BOTSITTER_TEST_EVENT: {other}"),
    }
}
