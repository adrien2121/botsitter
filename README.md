# botsitter

`botsitter` wraps Claude Code or Codex CLI and resumes interactive sessions after a usage limit resets.

## Install

Requires [Rust/Cargo](https://rustup.rs/) plus [Claude Code](https://code.claude.com/docs/en/setup) or [Codex CLI](https://developers.openai.com/codex/cli/).

```sh
cargo install --git https://github.com/adrien2121/botsitter.git --locked --bin botsitter --bin botsitter-logs
```

No crates.io package or GitHub release binaries are published yet.

## Usage

```sh
botsitter claude
botsitter codex --model gpt-5.4
botsitter --prevent-sleep claude --model opus
botsitter --show-logs codex
botsitter-logs [pid]
```

Wrapper options go before `claude` or `codex`; remaining arguments are forwarded. Use `botsitter claude -- <command> [args...]` to run a custom command under Claude monitoring.

`botsitter-logs` lists reachable sessions when no PID is given and connects directly when given a PID. `--show-logs` opens it in a new terminal when the platform has a supported terminal launcher.

Claude supports interactive sessions and print mode with `--output-format stream-json`. Codex support is interactive-only; `codex exec` is unsupported.

## Platform status

macOS is the currently tested development platform. Linux and Windows code paths exist but have not been validated end to end. Automatic `--show-logs` terminal launch is best effort; run `botsitter-logs [pid]` manually if it fails.

## License

[MIT](LICENSE)
