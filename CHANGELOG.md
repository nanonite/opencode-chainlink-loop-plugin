# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Removed
- Remove the deprecated in-server child-session Chainlink engine (`/chainlink`, `run_chainlink_outer`, the `chainlink_*` plugin options, and `src/chainlink.ts`); the `chainlink-loop` CLI is the only engine, and `check:engine` now scans `src/` and `test/` in full (#10)

### Added
- Log the build identity at the start of every `chainlink-loop` run, so a detached log names the version it executed
- Add a Chainlink Engine Rule and a `check:engine` pre-commit guard: one CLI engine, no MCP, and no in-server child-session engine
- Add local checkout loading for OpenCode 2 (#1)
- Add the TUI sidebar/palette for the loop plugin (cli.json) (#2)

### Fixed
- Fix the in-server `/chainlink` path treating `provider/model#variant` as a literal model id; effort variants now reach `Model.Ref.variant`, and a failed child session reports its real error instead of "returned no report" (#9)
- Add a startup watchdog: a step that emits no output within `--startup-timeout` (default 300s) is killed and retried once, and a timeout logs the killed process group, exit code and stderr (#7)
- Retry a crashed reviewer step instead of failing the whole run; a transient reviewer exit no longer abandons the rest of the queue (#8)
- Fix chainlink-loop dying with `is in phase worker` when a reviewer step returns no session ID, and log a failed step's exit code, stderr and duration (#6)
- Fix the chainlink-loop CLI silently dropping inner-loop progress lines (#5)
- Fix extractReview discarding genuine reviewer approvals when prose with brackets precedes the JSON verdict (#4)

### Changed
- Publish the plugin under a controlled npm scope and recheck upstream install fixes (#3)
