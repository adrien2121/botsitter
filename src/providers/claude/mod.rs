mod cli;
mod reset_time;
mod root;
mod stream;
mod transcript;

use crate::harness::{MonitorSpec, RunPlan};
use anyhow::Result;
use std::ffi::OsString;
use std::sync::Arc;

pub fn prepare(args: Vec<OsString>) -> Result<RunPlan> {
    let model = crate::live_logs::explicit_model(&args);
    let command = cli::command(args)?;
    Ok(RunPlan {
        session: crate::live_logs::SessionSpec {
            provider: crate::live_logs::ProviderName::Claude,
            model,
        },
        monitor: MonitorSpec {
            root: Arc::new(root::ClaudeRoot),
            parser: Arc::new(transcript::ClaudeTranscriptParser),
        },
        runner: cli::runner(command),
    })
}
