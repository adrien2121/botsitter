#![cfg(unix)]

use std::fs;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "botsitter-logger-isolation-{}-{nonce}",
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

fn port_is_reachable(path: &Path) -> bool {
    let Ok(contents) = fs::read_to_string(path) else {
        return false;
    };
    let Some(pid) = botsitter::paths::pid_from_port_path(path) else {
        return false;
    };
    let modified = fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let Ok(record) = botsitter::live_logs::parse_port_record(&contents, pid, modified) else {
        return false;
    };
    TcpStream::connect_timeout(
        &SocketAddr::from(([127, 0, 0, 1], record.port())),
        Duration::from_millis(100),
    )
    .is_ok()
}

fn wait_for_reachable_port(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !port_is_reachable(path) {
        assert!(
            Instant::now() < deadline,
            "unreachable port: {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_absent(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while path.exists() {
        assert!(
            Instant::now() < deadline,
            "port remained: {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn concurrent_runs_keep_independent_logger_files() {
    use portable_pty::{CommandBuilder, NativePtySystem, PtySize, PtySystem};

    let root = TestDir::new();
    let tmp = root.0.join("tmp");
    std::fs::create_dir(&tmp).unwrap();

    let size = PtySize {
        rows: 24,
        cols: 80,
        pixel_width: 0,
        pixel_height: 0,
    };
    let first_pair = NativePtySystem::default().openpty(size).unwrap();
    let mut first_command = CommandBuilder::new(env!("CARGO_BIN_EXE_botsitter"));
    first_command.args(["claude", "--", "/bin/sleep", "2"]);
    first_command.env("TMPDIR", &tmp);
    let mut first = first_pair.slave.spawn_command(first_command).unwrap();
    let first_pid = first.process_id().unwrap();
    drop(first_pair.slave);
    let _first_master = first_pair.master;

    let second_pair = NativePtySystem::default().openpty(size).unwrap();
    let mut second_command = CommandBuilder::new(env!("CARGO_BIN_EXE_botsitter"));
    second_command.args(["codex", "--", "/bin/sleep", "10"]);
    second_command.env("TMPDIR", &tmp);
    let mut second = second_pair.slave.spawn_command(second_command).unwrap();
    let second_pid = second.process_id().unwrap();
    drop(second_pair.slave);
    let _second_master = second_pair.master;

    let first_port = tmp.join(format!("botsitter-{first_pid}.port"));
    let second_port = tmp.join(format!("botsitter-{second_pid}.port"));
    wait_for_reachable_port(&first_port);
    wait_for_reachable_port(&second_port);
    assert_ne!(first_port, second_port);

    assert!(first.wait().unwrap().success());
    wait_for_absent(&first_port);
    assert!(port_is_reachable(&second_port));
    assert!(tmp.join(format!("botsitter-{second_pid}.log")).is_file());

    let _ = second.kill();
    let _ = second.wait();
}
