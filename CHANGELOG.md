# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Removed
- Remove the deprecated in-server child-session Chainlink engine (`/chainlink`, `run_chainlink_outer`, the `chainlink_*` plugin options, and `src/chainlink.ts`); the `chainlink-loop` CLI is the only engine, and `check:engine` now scans `src/` and `test/` in full (#10)

### Added
- Add durable Chainlink run records with a heartbeat, atomic writes, per-task outcomes and a final `RESULT` line (#11)
- Add `chainlink-loop status`, `wait` and `start --detach`, with dead-run detection and an exact relaunch command (#12)
- Classify task outcomes from commits, escalation state and `--verify`, so reviewer approval alone is never `done` (#13)
- Add `--stop-on`, `--close-if-verified`, the end-of-run outcome table and resume from a run record (#14)
- Add the start/wait/status runbook, outcome and exit-code tables, and the run-record vocabulary (#15)
- Cross-check run-record task ids with Chainlink in `chainlink-loop status`, adding per-task `open`/`closed`/`unknown` tracker states and a `trackerResolved` note for terminal non-done runs whose tasks were later closed (#16)
- Log the build identity at the start of every `chainlink-loop` run, so a detached log names the version it executed
- Add a Chainlink Engine Rule and a `check:engine` pre-commit guard: one CLI engine, no MCP, and no in-server child-session engine
- Add local checkout loading for OpenCode 2 (#1)
- Add the TUI sidebar/palette for the loop plugin (cli.json) (#2)

### Fixed
- Fix the server plugin failing to load on OpenCode 2.3 with `Schema.optionalWith is not a function`: Effect 3 is now bundled into `dist/server.js`, so OpenCode's Effect 4 runtime no longer replaces it
- Fix scheduler idle detection on OpenCode 2.3 by handling `session.execution.started` and `session.execution.succeeded` / `failed` / `interrupted`, with `session.status` / `session.idle` kept as the legacy fallback
- Fix outcome classification so an ordinary `blocked_by` dependency is not an escalation and only an explicit blocked flag/status, blocking label, blocker/escalation comment kind, or a comment starting with `ESCALATION:` flags a task (#13)
- Fix resumed and review-first runs counting zero commits by taking the task base from the previous run record's `baseSha` (falling back to the default-branch merge-base), so prior commits reach `done` and can close under `--close-if-verified` (#13)
- Cover `--resume` over pre-existing commits and tasks with an open dependency reaching `done` and closing under `--close-if-verified` at the backlog level (#14)
- Fix the in-server `/chainlink` path treating `provider/model#variant` as a literal model id; effort variants now reach `Model.Ref.variant`, and a failed child session reports its real error instead of "returned no report" (#9)
- Add a startup watchdog: a step that emits no output within `--startup-timeout` (default 300s) is killed and retried once, and a timeout logs the killed process group, exit code and stderr (#7)
- Retry a crashed reviewer step instead of failing the whole run; a transient reviewer exit no longer abandons the rest of the queue (#8)
- Fix chainlink-loop dying with `is in phase worker` when a reviewer step returns no session ID, and log a failed step's exit code, stderr and duration (#6)
- Fix the chainlink-loop CLI silently dropping inner-loop progress lines (#5)
- Fix extractReview discarding genuine reviewer approvals when prose with brackets precedes the JSON verdict (#4)

### Changed
- Target OpenCode 2.3.2: refresh the `@opencode-ai/plugin-v2`/`@opencode-ai/theme` dev dependencies to beta-19271, load the TUI part from the server plugin entry, and document the plugin object options form
- Treat SIGHUP like SIGTERM: finish the step, write an `interrupted` record and release the lock (#12)
- Halt a multi-issue run at the first non-done outcome by default; `--stop-on never` keeps the old skip-and-continue policy (#14)
- Re-verify the outcome table against the corrected classification and extend the documented end-to-end smoke with a dependent chain and kill-then-resume (#15)
- Publish the plugin under a controlled npm scope and recheck upstream install fixes (#3)
