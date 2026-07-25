use anyhow::Result;
use botsitter::app;
use botsitter::cli::Cli;
use botsitter::providers;
use clap::Parser;
use keepawake::Builder;

#[tokio::main]
async fn main() -> Result<std::process::ExitCode> {
    let cli = Cli::parse();

    let _awake_guard = if cli.prevent_sleep {
        match Builder::default().idle(true).sleep(true).create() {
            Ok(guard) => Some(guard),
            Err(error) => {
                eprintln!("[botsitter] Warning: could not prevent sleep: {error}");
                None
            }
        }
    } else {
        None
    };

    let mut provider_and_args = cli.provider_and_args.into_iter();
    let provider = provider_and_args.next().expect("clap requires provider");
    let args = provider_and_args.collect();
    let plan = match provider.to_str() {
        Some("claude") => providers::claude::prepare(args)?,
        Some("codex") => providers::codex::prepare(args)?,
        Some(name) => anyhow::bail!("unsupported provider '{name}'; expected 'claude' or 'codex'"),
        None => anyhow::bail!("provider name must be valid UTF-8"),
    };
    let outcome = app::run(cli.show_logs, plan).await?;
    drop(_awake_guard);
    Ok(std::process::ExitCode::from(outcome.wrapper_code()))
}
