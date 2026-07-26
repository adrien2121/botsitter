mod cli;
mod root;
mod transcript;

use crate::harness::{MonitorSpec, RunPlan};
use crate::runners::pty::PtyRunner;
use anyhow::Result;
use std::ffi::OsString;
use std::sync::Arc;

pub fn prepare(args: Vec<OsString>) -> Result<RunPlan> {
    let model = crate::live_logs::explicit_model(&args);
    let command = cli::command(args)?;
    Ok(RunPlan {
        session: crate::live_logs::SessionSpec {
            provider: crate::live_logs::ProviderName::Codex,
            model,
        },
        monitor: MonitorSpec {
            root: Arc::new(root::CodexRoot),
            parser: Arc::new(transcript::CodexTranscriptParser),
        },
        runner: Box::new(PtyRunner::new(command)),
    })
}
