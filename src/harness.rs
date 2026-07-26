use crate::models::{ChildOutcome, SharedAppState};
use anyhow::Result;
use chrono::{DateTime, Local};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

pub trait SessionRoot: Send + Sync {
    fn resolve(&self) -> Option<PathBuf>;
}

pub trait TranscriptParser: Send + Sync {
    fn parse_line(&self, line: &str, now: DateTime<Local>) -> ParseOutcome;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParseOutcome {
    Update(LimitUpdate),
    Ignored,
    Diagnostic(ParseDiagnostic),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LimitUpdate {
    pub event_time: DateTime<Local>,
    pub state: LimitState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LimitState {
    Locked {
        target_time: DateTime<Local>,
        display: String,
    },
    Clear,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParseDiagnostic {
    MalformedRecord,
    MissingEventTimestamp,
    SaturatedWithoutFutureReset,
}

impl ParseDiagnostic {
    pub fn message(self) -> &'static str {
        match self {
            Self::MalformedRecord => "rate-limit record has an unsupported or malformed schema",
            Self::MissingEventTimestamp => "rate-limit record has no valid event timestamp",
            Self::SaturatedWithoutFutureReset => {
                "saturated rate-limit window has no valid future reset timestamp"
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResumeOutcome {
    Sent,
    DefiniteFailure(String),
    AmbiguousFailure(String),
}

pub trait ResumeSink: Send + Sync {
    fn resume(&self) -> ResumeOutcome;
}

pub type RunFuture = Pin<Box<dyn Future<Output = Result<ChildOutcome>> + Send>>;

pub trait Runner: Send {
    fn run(self: Box<Self>, context: RunContext) -> RunFuture;
}

#[derive(Clone)]
pub struct MonitorSpec {
    pub root: Arc<dyn SessionRoot>,
    pub parser: Arc<dyn TranscriptParser>,
}

pub struct RunContext {
    pub state: SharedAppState,
    pub monitor: MonitorSpec,
}

pub struct RunPlan {
    pub session: crate::live_logs::SessionSpec,
    pub monitor: MonitorSpec,
    pub runner: Box<dyn Runner>,
}
