# botsitter

`botsitter` wraps Claude Code or Codex CLI and resumes interactive sessions when usage resets.

## Requirements

- [Claude Code](https://code.claude.com/docs/en/setup) or [Codex CLI](https://developers.openai.com/codex/cli/)
- [Rust/Cargo](https://rustup.rs/)

## Install

```sh
cargo install --git https://github.com/adrien2121/botsitter.git --bin botsitter --bin botsitter-logs
```

## Usage

```sh
botsitter claude
botsitter codex --model gpt-5.4
botsitter --prevent-sleep claude --model opus
botsitter claude -- caffeinate claude
botsitter --show-logs codex
botsitter-logs [pid]
```

Run `botsitter-logs` without a PID to choose from currently reachable sessions. The menu shows provider, model, start time, working directory, and PID. Run `botsitter-logs <pid>` to connect directly. Interactive viewers keep current rate-limit and scheduled `continue` state in a footer; piped output remains plain chronological logs.

Wrapper options go before `claude` or `codex`. Arguments after the provider are forwarded literally. Put `--` after the provider to run a custom command.

Claude supports interactive sessions and print mode with `--output-format stream-json`. Codex support is interactive-only; `codex exec` is not supported.
