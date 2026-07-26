use anyhow::{Context, Result};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::io::{IsTerminal, Write};
use std::thread;
use std::time::Duration;

fn requested_pid() -> Result<Option<u32>> {
    let mut args = std::env::args().skip(1);
    let pid = args
        .next()
        .map(|value| value.parse::<u32>().context("PID must be a decimal u32"))
        .transpose()?;
    anyhow::ensure!(args.next().is_none(), "usage: botsitter-logs [pid]");
    Ok(pid)
}

fn selection_index(input: &str, session_count: usize) -> Result<usize> {
    let selection = input
        .trim()
        .parse::<usize>()
        .context("selection must be a number")?;
    anyhow::ensure!(
        (1..=session_count).contains(&selection),
        "selection out of range"
    );
    Ok(selection - 1)
}

fn choose_session(sessions: &[botsitter::log_viewer::DiscoveredSession]) -> Result<usize> {
    print!("{}", botsitter::log_viewer::format_menu(sessions));
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    selection_index(&input, sessions.len())
}

fn is_relevant_port_file_event(event: &notify::Event, pid: Option<u32>) -> bool {
    use notify::EventKind;
    matches!(event.kind, EventKind::Create(_) | EventKind::Modify(_))
        && event.paths.iter().any(|path| match pid {
            Some(pid) => path == &botsitter::paths::LoggerPaths::for_pid(pid).port,
            None => botsitter::paths::pid_from_port_path(path).is_some(),
        })
}

fn wait_for_port_event(
    events: &std::sync::mpsc::Receiver<notify::Result<notify::Event>>,
    pid: Option<u32>,
) -> Result<()> {
    loop {
        let event = events.recv().context("filesystem watcher stopped")??;
        if is_relevant_port_file_event(&event, pid) {
            thread::sleep(Duration::from_millis(50));
            return Ok(());
        }
    }
}

fn main() -> Result<()> {
    let pid = requested_pid()?;
    if let Some(message) = botsitter::log_viewer::selection_tty_error(
        pid,
        std::io::stdin().is_terminal(),
        std::io::stdout().is_terminal(),
    ) {
        anyhow::bail!(message);
    }

    let temp_dir = std::env::temp_dir();
    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher: RecommendedWatcher =
        notify::recommended_watcher(tx).context("Failed to create filesystem watcher")?;
    watcher
        .watch(&temp_dir, RecursiveMode::NonRecursive)
        .context("Failed to start watching temp directory")?;

    if let Some(pid) = pid {
        let interactive = std::io::stdout().is_terminal();
        if interactive {
            println!("Waiting for botsitter session to start...");
        }
        loop {
            if let Ok(session) = botsitter::log_viewer::session_for_pid(&temp_dir, pid) {
                if botsitter::log_viewer::stream_session(&session, interactive).is_ok() {
                    return Ok(());
                }
            }
            wait_for_port_event(&rx, Some(pid))?;
        }
    }

    let mut waiting = false;
    loop {
        let sessions = botsitter::log_viewer::discover_sessions(&temp_dir)?;
        if sessions.is_empty() {
            if !waiting {
                println!("Waiting for an active botsitter session...");
                waiting = true;
            }
            wait_for_port_event(&rx, None)?;
            continue;
        }
        waiting = false;
        let selected = choose_session(&sessions)?;
        if botsitter::log_viewer::stream_session(&sessions[selected], true).is_err() {
            println!("Selected session is no longer available. Refreshing...");
            continue;
        }
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::{is_relevant_port_file_event, selection_index};

    #[test]
    fn menu_selection_is_one_based_and_bounded() {
        assert_eq!(selection_index("1\n", 2).unwrap(), 0);
        assert_eq!(selection_index("2", 2).unwrap(), 1);
        assert_eq!(
            selection_index("0", 2).unwrap_err().to_string(),
            "selection out of range"
        );
        assert_eq!(
            selection_index("3", 2).unwrap_err().to_string(),
            "selection out of range"
        );
        assert_eq!(
            selection_index("x", 2).unwrap_err().to_string(),
            "selection must be a number"
        );
    }

    #[test]
    fn event_filter_is_exact_for_requested_pid() {
        let event = notify::Event::new(notify::EventKind::Create(notify::event::CreateKind::File))
            .add_path(botsitter::paths::LoggerPaths::for_pid(41).port);
        assert!(is_relevant_port_file_event(&event, Some(41)));
        assert!(!is_relevant_port_file_event(&event, Some(42)));
        assert!(is_relevant_port_file_event(&event, None));
    }
}
