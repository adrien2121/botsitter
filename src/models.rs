use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

pub type OutputActivity = Arc<AtomicU64>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildOutcome {
    Code(u8),
    NoPortableCode,
}

impl ChildOutcome {
    pub fn from_std(status: std::process::ExitStatus) -> Self {
        status
            .code()
            .and_then(|code| u8::try_from(code).ok())
            .map(Self::Code)
            .unwrap_or(Self::NoPortableCode)
    }

    pub fn from_pty(status: portable_pty::ExitStatus) -> Self {
        u8::try_from(status.exit_code())
            .map(Self::Code)
            .unwrap_or(Self::NoPortableCode)
    }

    pub fn wrapper_code(self) -> u8 {
        match self {
            Self::Code(code) => code,
            Self::NoPortableCode => 1,
        }
    }
}

pub struct AppState {
    pub lockout_target_time: Option<chrono::DateTime<chrono::Local>>,
    pub latest_rate_limit_event_time: Option<chrono::DateTime<chrono::Local>>,
    /// Increments for live stream/file events; startup scan establishes baseline state.
    pub lockout_revision: u64,
    pub resume_exhausted_revision: Option<u64>,
    pub file_size_cache: HashMap<PathBuf, u64>,
    pub last_output_activity: OutputActivity,
    pub live_status: tokio::sync::watch::Sender<crate::live_logs::LiveStatus>,
}

impl AppState {
    pub fn new() -> Self {
        let (live_status, _) =
            tokio::sync::watch::channel(crate::live_logs::LiveStatus::Monitoring {
                reason: crate::live_logs::MonitoringReason::NoActiveLimit,
            });
        Self {
            lockout_target_time: None,
            latest_rate_limit_event_time: None,
            lockout_revision: 0,
            resume_exhausted_revision: None,
            file_size_cache: HashMap::new(),
            last_output_activity: Arc::new(AtomicU64::new(0)),
            live_status,
        }
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

pub type SharedAppState = Arc<Mutex<AppState>>;

pub fn mark_output_activity(activity: &AtomicU64) {
    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    activity.store(now_nanos, std::sync::atomic::Ordering::Relaxed);
}

pub fn output_is_hot(activity: &AtomicU64, threshold: std::time::Duration) -> bool {
    let last_activity_ns = activity.load(std::sync::atomic::Ordering::Relaxed);
    if last_activity_ns == 0 {
        return false;
    }

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    std::time::Duration::from_nanos(now_ns.saturating_sub(last_activity_ns)) < threshold
}

#[cfg(test)]
mod tests {
    use super::{mark_output_activity, output_is_hot, AppState, ChildOutcome};
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    #[test]
    fn new_state_has_cold_output_activity() {
        let state = AppState::new();

        assert_eq!(state.last_output_activity.load(Ordering::Relaxed), 0);
        assert!(!output_is_hot(
            &state.last_output_activity,
            Duration::from_secs(2)
        ));
    }

    #[test]
    fn marking_activity_makes_output_hot() {
        let state = AppState::new();

        mark_output_activity(&state.last_output_activity);

        assert!(output_is_hot(
            &state.last_output_activity,
            Duration::from_secs(2)
        ));
    }

    #[test]
    fn new_lockout_revision_is_not_exhausted() {
        let mut state = AppState::new();
        state.resume_exhausted_revision = Some(7);
        state.lockout_revision = 8;

        assert_ne!(
            state.resume_exhausted_revision,
            Some(state.lockout_revision)
        );
    }

    #[test]
    fn portable_child_outcomes_preserve_only_wrapper_codes() {
        assert_eq!(
            ChildOutcome::from_pty(portable_pty::ExitStatus::with_exit_code(7)),
            ChildOutcome::Code(7)
        );
        assert_eq!(
            ChildOutcome::from_pty(portable_pty::ExitStatus::with_exit_code(256)),
            ChildOutcome::NoPortableCode
        );
        assert_eq!(ChildOutcome::NoPortableCode.wrapper_code(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn signal_exit_has_no_portable_numeric_code() {
        use std::os::unix::process::ExitStatusExt;

        let status = std::process::ExitStatus::from_raw(9);

        assert_eq!(ChildOutcome::from_std(status), ChildOutcome::NoPortableCode);
    }
}
