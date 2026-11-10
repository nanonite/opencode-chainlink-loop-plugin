# OpenCode Loop Plugin

[![npm version](https://img.shields.io/npm/v/@prevalentware/opencode-loop-plugin.svg)](https://www.npmjs.com/package/@prevalentware/opencode-loop-plugin)
[![GitHub repository](https://img.shields.io/badge/GitHub-prevalentWare%2Fopencode--loop--plugin-blue?logo=github)](https://github.com/prevalentWare/opencode-loop-plugin)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

OpenCode Loop Plugin adds Claude Code-style `/loop` recurring prompts to OpenCode and integrates with Chainlink for issue-driven coding work. The `/loop` command uses a persistent scheduler to re-inject an instruction into an idle session on an interval or at agent-chosen delays. The `chainlink-loop` CLI selects Chainlink issues and runs a worker/reviewer cycle until each issue is approved or reaches its attempt limit.

`/loop` is the complement to goal mode ([`@prevalentware/opencode-goal-plugin`](https://github.com/prevalentWare/opencode-goal-plugin)): a goal defines when a task is *done*; a loop defines when to *wake the agent up again* to look at something that changes over time.

The OpenCode Loop Plugin adds:

- `/loop <interval> <instruction>` and `/loop <instruction>` (dynamic pacing) as an OpenCode command for TUI, desktop, and web.
- A server-side scheduler with per-loop timers that injects a synthetic iteration prompt only when the session is idle, with busy backoff.
- Dynamic loops where the agent itself picks the delay before each next iteration via `schedule_next_run`, mirroring Claude Code's self-paced `/loop`.
- Agent tools: `create_loop`, `list_loops`, `stop_loop`, `pause_loop`, `resume_loop`, `run_loop`, `schedule_next_run`, and `clear_loops`.
- Persistent loop state that survives OpenCode restarts, with atomic writes and owner-only file permissions.
- A TUI sidebar with live countdowns and a `Loops` command (`Ctrl+L`) to run, pause, resume, or stop loops.
- Plan-mode safety: iterations are deferred while the session's last prompt came from a restricted agent (default: `plan`).
- Compaction context so active loops are preserved when OpenCode summarizes a long session.
- Safety rails: minimum interval, per-session loop limit, optional max runs, and automatic expiry after 7 days.
- Chainlink integration: a `chainlink-loop` CLI for unattended issue queues.

## Install

Choose the instructions that match the CLI you run:

| OpenCode version | How to identify it | Instructions |
| --- | --- | --- |
| OpenCode 1 stable | You run `opencode` and `opencode --version` prints `1.x` | [OpenCode 1](#opencode-1-stable) |
| OpenCode 2 preview | You run `opencode2` | [OpenCode 2](#opencode-2-preview) |

Do not mix the configuration formats. OpenCode 1 uses `plugin` and `tui.json`; OpenCode 2 uses `plugins` and the global `~/.config/opencode/cli.json`.

### OpenCode 1 Stable

Install for the current OpenCode project:

```bash
opencode plugin @prevalentware/opencode-loop-plugin
```

Install globally:

```bash
opencode plugin -g @prevalentware/opencode-loop-plugin
```

OpenCode detects both package entrypoints and writes the plugin into the server and TUI config targets. For manual installation, add the package to both V1 config files.

`opencode.json`:

```json
{
  "plugin": ["@prevalentware/opencode-loop-plugin"]
}
```

`tui.json`:

```json
{
  "plugin": ["@prevalentware/opencode-loop-plugin"]
}
```

### OpenCode 2 Preview

This plugin supports OpenCode 2 preview `0.0.0-next-17055` while remaining compatible with OpenCode 1. Add the package to both V2 plugin lists.

`opencode.json`:

```json
{
  "plugins": ["@prevalentware/opencode-loop-plugin"]
}
```

`~/.config/opencode/cli.json`:

```json
{
  "plugins": ["@prevalentware/opencode-loop-plugin"]
}
```

OpenCode 2 does not read the V1 `tui.json` file. The server entrypoint is loaded from `opencode.json`; the sidebar and the `Loops` command are loaded from the global `~/.config/opencode/cli.json`.

The sidebar renders the focused session's active loops. Press `Ctrl+L` to open the loop actions (refresh, run, pause, resume, stop); selecting one sends the matching loop tool call to the agent. OpenCode 2.0.16 does not surface plugin commands in the `Ctrl+P` command palette, so `Ctrl+L` is the entrypoint there.

The OpenCode 2 plugin API is still in preview. This release targets the exact preview above; later previews may require a plugin update. V2 supports the `/loop` command, all loop tools, persistent state, idle scheduling, dynamic loops, Plan-mode safety, session cleanup, and TUI sidebar/palette integration. The dedicated V1 compaction-context hook has no V2 equivalent in this preview. V2 still injects the active-loop system reminder before model dispatch, but cannot append the separate loop block directly to a compaction operation.

## Usage

Create a fixed-interval loop:

```text
/loop 10m review the current PR. If there are new comments, address them. If CI fails, diagnose the logs and fix it. If everything is green, report and stop this loop.
```

The interval can lead the instruction (`/loop 10m ...`) or trail it as an `every` clause (`/loop check the deploy every 20m`). Supported units are `s`, `m`, `h`, and `d`; the default minimum is 30 seconds.

Create a dynamic loop — the agent picks the delay between iterations based on what it observes, one iteration at a time:

```text
/loop watch the staging deploy and run smoke checks when it finishes
```

### Chainlink: the deterministic loop (recommended)

The published package includes the `chainlink-loop` executable. Install it with a package manager that puts its binaries on `PATH` (for example, `npm install -g @prevalentware/opencode-loop-plugin`), or run `bun run src/chainlink-cli.ts` from a checkout. Run it from the repository containing your Chainlink issues, with `chainlink` and `opencode` available on `PATH`. Set `CHAINLINK_DB` if the Chainlink database is outside that repository. The CLI uses the current working directory for both issue selection and worker changes.

`chainlink-loop` runs the whole outer/inner loop with **no LLM in the control path**. Code picks the issue, counts the attempts and decides when to stop. Every step is a one-shot `opencode run` process, so each one has its own `--auto` permissions, its own agent, an exit code and a killable process tree.

#### Runbook: start, wait, status

Run these from the project root. They need no ids, record paths or state paths: the CLI resolves the newest run for the current working directory and `CHAINLINK_DB`.

```bash
# Start one issue in the background.
chainlink-loop start --detach --task 67

# Block until it is done, dead or finished without a done outcome.
chainlink-loop wait

# Inspect the same run at any time.
chainlink-loop status
chainlink-loop status --json
```

`start --detach` prints the run id, log path and record path as its only stdout. `wait` prints the final `RESULT` JSON and maps its exit code from the outcome:

| Exit | Meaning |
| --- | --- |
| `0` | The run is done: every task outcome is `done`. |
| `1` | Failed, dead, exhausted, or finished with a non-done outcome. |
| `3` | `wait --timeout` expired before the run reached a verdict. |

`status` reports `running`, `done`, `not-done`, `dead`, `failed`, `exhausted` or `interrupted`. A run is `dead` when it has no final status and either its pid is gone or its heartbeat is more than three intervals stale. `status` then prints the exact relaunch command, which resumes the remaining tasks from the run record.

`status` also cross-checks every task id in the record with Chainlink (`issue show --json <id>`, honouring `CHAINLINK_DB`) and adds a per-task `tracker` state: `open`, `closed` or `unknown`. The lookup is read-only and best-effort: a missing `chainlink`, an unreachable database or an unknown id only makes that row `unknown`. When a terminal run that is not `done` has all of its tasks closed in Chainlink, `status` prints `note: all N task(s) are closed in Chainlink; this record predates their acceptance` and `status --json` sets `trackerResolved: true`. The run's own state, exit code and `wait` result do not change, so a stale record is easy to tell apart from a live failure.

#### Outcomes

Reviewer approval alone is never `done`. The CLI classifies each task from deterministic facts:

| Outcome | Meaning |
| --- | --- |
| `done` | Reviewer approved, new commits exist since the task's base sha, the issue is not escalated, and `--verify` passed when configured. |
| `approved-no-changes` | Reviewer approved but no new commits exist since the task's base sha. |
| `approved-but-escalated` | Reviewer approved but the issue is explicitly blocked (flag or status), carries a `blocked`, `blocking`, `escalated` or `escalation` label, or has a blocker/escalation comment or a comment starting with `ESCALATION:`. |
| `verify-failed` | The `--verify` command exited non-zero; its output tail became findings for the next worker attempt. |
| `exhausted` | The attempt limit was reached without a done outcome. |
| `crashed` | A worker or reviewer step died before delivering a result. |
| `interrupted` | The operator sent SIGINT, SIGTERM or SIGHUP. |

An ordinary dependency is not an escalation: `blocked_by` lists both open and closed blockers, so a task in a dependency chain can still be `done`. Only the explicit signals above count. A worker or reviewer escalates by adding a comment whose first line starts with `ESCALATION:` (or a comment created with `--kind blocker`); prose that merely mentions the word does not flag the task.

When a run is resumed, or `--review-first auto|always` reviews work already present, the task base is the `baseSha` recorded for that task by its previous run record; without one, the merge-base with the default branch is used. Commits made before the current process started therefore count, so a resumed task with prior commits can be `done` and can close under `--close-if-verified`.

The end of a run prints a task/attempts/outcome/verify table, and the same table is stored in the run record. By default the queue halts at the first task whose outcome is not `done`; use `--stop-on never` to keep the old skip-and-continue behaviour.

```bash
# drain the queue
chainlink-loop

# one issue, several in order, leave them open
chainlink-loop --task 67
chainlink-loop --task 12,71 --no-close
chainlink-loop --attempts 5 --max-tasks 3

# steer every task with a direction on top of the issue notes
chainlink-loop --prompt "reuse the existing storage layer; add no new dependencies"

# from a checkout, without installing the bin
bun run src/chainlink-cli.ts --task 67 --dry-run
```

| Flag | Meaning |
| --- | --- |
| `--task <ids>` | Comma-separated issue ids. `#67` is accepted and normalised to `67`. An explicitly named task is an operator override and runs even when a dependency in `blocked_by` is still open; queue selection still uses `issue next`/`issue ready`, which return only ready tasks. |
| `--attempts <n>` | Per-task attempt limit (default 20). |
| `--max-tasks <n>` | Stop after this many tasks. |
| `--no-close` | Leave approved issues open. Default is to close them. |
| `--close-if-verified` | Close an issue only when its outcome is `done` and `--verify` passed; otherwise leave it open. |
| `--stop-on non-done\|never` | Halt at the first non-done outcome (default), or keep skipping and continuing (`never`). |
| `--resume <runId>` | Resume the remaining tasks from a run record. Prior commits count because the task base comes from the record; `status` prints the exact command for a dead run. |
| `--verify <cmd>` | Acceptance command run in the worktree after reviewer approval. Exit 0 is required for `done`; a failure is fed back to the worker as findings. |
| `--review-first auto\|always\|never` | Review work that already exists before dispatching a fresh worker (default `auto`). |
| `--exclude <ids>` | Skip these issue subtrees when selecting ready tasks after an exhausted issue. |
| `--prompt <text>` | Operator direction layered above each task's notes. Both the worker and the reviewer follow it, so a run can be scoped without editing the issues. Also settable via `CHAINLINK_PROMPT`; `--prompt` wins. |
| `--worker-model` / `--reviewer-model` | `provider/model[#variant]` per role. The variant (for example an effort level) is passed through to both surfaces. |
| `--worker-agent` / `--reviewer-agent` | Defaults: `build` and `plan`. |
| `--worker-timeout` / `--reviewer-timeout` | Per-step wall clock limit in seconds. |
| `--startup-timeout <s>` | Kill a step that emits no output within this many seconds and retry it (default 300; 0 disables). Guards against a step whose `opencode run` never creates a session and would otherwise sit until its full per-step timeout. |
| `--startup-retries <n>` | Extra attempts for a step that never produced any output at all (default 1). The killed process group is logged. |
| `--dry-run` | Show the selected settings and, for one issue, whether existing work will be reviewed first. |
| `-v`, `--version` | Print the plugin build version and exit. |

The loop per task:

1. `opencode run --standalone --auto --agent build "<issue spec>"` — a fresh worker process.
2. `opencode run --standalone --auto --agent plan "<review prompt>"` — a fresh reviewer process that returns the verdict JSON. The `plan` agent cannot write outside its plan directory, so a reviewer cannot edit the tree even under `--auto`; the plugin also denies edit/write/patch requests raised by a reviewer session.
3. If not approved, the findings go back to the **same worker session** via `--session <id>`, so context is kept. Repeat until approval or the attempt limit.
4. Close the issue only on a `done` outcome and only when closing is enabled; a non-done outcome leaves it open for a human.

`--review-first auto` looks for a dirty tree, commits that mention the issue, a previous Chainlink workflow for the same task, or comments on the issue. If any are found, the reviewer runs **first** so a previous attempt's work is judged rather than thrown away. The task base is then the baseSha recorded by the previous run (or the merge-base with the default branch), so those existing commits count toward `done`.

**One loop per workspace.** The loop takes a lock keyed by working directory and `CHAINLINK_DB` (`$TMPDIR/chainlink-loop-*.lock`) so a second invocation fails loudly instead of dispatching two workers at the same files. A lock whose owning process is gone is taken over automatically. `SIGINT`/`SIGTERM`/`SIGHUP` stop the loop, kill the in-flight step's process group, write a terminal `interrupted` run record, and release the lock; a second signal exits immediately.

**Exhausted tasks are skipped, not fatal.** A task that burns its attempt limit is left open for a human and the loop moves on to the next ready leaf. Because `issue next` keeps returning the same still-open task, the loop falls back to `issue ready --json` and picks the first leaf it has not touched. That fallback does **not** know about a project which deliberately holds a subtree back, so use `--exclude <ids>` for those — for example `--exclude 1` to keep the queue out of a "upstream reports" epic whose children wait on a human.

### Dynamic loops

A dynamic loop mirrors Claude Code's self-paced `/loop`: at the end of each iteration the agent calls `schedule_next_run` with a delay in seconds and a one-sentence reason ("watching CI run"), or calls `stop_loop` to end the loop. If an iteration ends without doing either, the loop ends — exactly like omitting `ScheduleWakeup` in Claude Code.

### How iterations are scheduled

- Iterations only run while the session is idle. If a loop comes due while the session is busy, it is deferred with a short backoff and retried when the session goes idle.
- If several loops in one session are due at once, one iteration is injected and the rest wait for the next idle.
- Failed injections are recorded in the loop's `lastError` and retried after a backoff; they never crash OpenCode.
- Loops are stopped automatically when their session is deleted, when `max_runs` is reached, or after 7 days (configurable).

## Options

In OpenCode 1, server options use the package-and-options tuple in `opencode.json`:

```json
{
  "plugin": [
    [
      "@prevalentware/opencode-loop-plugin",
      {
        "min_interval_seconds": 30,
        "max_loops_per_session": 5,
        "busy_backoff_seconds": 60,
        "failure_backoff_seconds": 60,
        "max_loop_age_days": 7,
        "dynamic_max_delay_seconds": 86400,
        "restricted_agents": ["plan"],
        "register_command": true,
        "command_name": "loop"
      }
    ]
  ]
}
```

In OpenCode 2, use the plugin object form:

```json
{
  "plugins": [
    {
      "package": "@prevalentware/opencode-loop-plugin",
      "options": {
        "min_interval_seconds": 30,
        "max_loops_per_session": 5,
        "busy_backoff_seconds": 60,
        "failure_backoff_seconds": 60,
        "max_loop_age_days": 7,
        "dynamic_max_delay_seconds": 86400,
        "restricted_agents": ["plan"],
        "register_command": true,
        "command_name": "loop"
      }
    }
  ]
}
```

Defaults:

- `min_interval_seconds`: `30`; the smallest accepted interval and the lower clamp for dynamic delays.
- `max_loops_per_session`: `5` open (active or paused) loops per session.
- `busy_backoff_seconds`: `60`; retry delay when an iteration comes due while the session is busy.
- `failure_backoff_seconds`: `60`; retry delay when injecting the iteration prompt fails.
- `max_loop_age_days`: `7`; loops stop automatically after this age. Set `0` to disable expiry.
- `dynamic_max_delay_seconds`: `86400`; upper clamp for `schedule_next_run` delays.
- `restricted_agents`: `["plan"]`; iterations are deferred while the session's last prompt came from one of these agents.
- `register_command`: `true`
- `command_name`: `"loop"`

## State

Loop and Chainlink workflow state is stored at:

```text
$XDG_DATA_HOME/opencode-loop-plugin/loops-v2.json
```

If `XDG_DATA_HOME` is not set, the default is:

```text
~/.local/share/opencode-loop-plugin/loops-v2.json
```

The versioned filename isolates this plugin from older loop-plugin processes that use `loops.json` and do not understand Chainlink workflows.

Set `OPENCODE_LOOP_STATE_PATH` to use a custom file.

Each Chainlink CLI run also writes a durable run record next to the loop state; the CLI prints the record path at start and in its `RESULT` line, and `status --json` always includes it.

The state file is written atomically with owner-only permissions when the host filesystem supports it. Active interval loops are rehydrated and rescheduled when OpenCode restarts. Dynamic loops that were waiting on the agent to schedule their next run cannot recover on their own after a restart and are stopped with an explanatory reason. Chainlink workflows are marked `interrupted` on restart rather than blindly repeating worker actions.

## Versioning

`package.json` holds the release version. The CI publish job computes the next version from npm and rewrites it just before `npm publish`, so a checkout can sit at an older number than the latest published release.

To make a *built* artifact identify itself, `scripts/build.ts` stamps every bundle with:

- `version` — the `package.json` version at build time.
- `gitDescribe` — `git describe --tags --always`, e.g. `v0.1.8-4-g64bff2f` (the last release tag plus the commits since it).
- `gitSha` — the abbreviated commit sha.
- `gitDirty` — whether tracked *source* differed from `HEAD`. Generated paths are ignored: `dist/` and the `package.json` rewrite that CI performs.

The stamp is injected with `bun build --define` and read back by `src/version.ts`. Where to see it:

- On plugin load: `opencode-loop-plugin 0.2.0 (v0.1.8-4-g64bff2f) loaded`.
- In every loop tool result, under the `plugin` field (the TUI ignores it; the `loops` field stays the same).
- From the CLI: `chainlink-loop --version` prints `@prevalentware/opencode-loop-plugin 0.2.0 (v0.1.8-4-g64bff2f)`.

An unbundled run (`bun test`, `bun run src/chainlink-cli.ts`) reports `0.0.0-dev (dev)`, because no build stamp is present.

`bun run build` stamps both bundles; `bun run build:server` and `bun run build:cli` build one at a time.

`dist/` is committed so a checkout can load without a build step. `bun run check:dist` rebuilds with the identity recorded in `dist/build-info.json` and fails when `dist/` does not match the source; CI runs this check on every pull request. Run `bun run build` after a source change and commit `dist/` together with it.

## Credits

This plugin follows the semantics of Claude Code's `/loop` skill (interval parsing, immediate first iteration, dynamic self-pacing with an explicit schedule-or-stop contract, and 7-day auto-expiry) implemented on top of OpenCode plugin hooks. The package structure, persistence approach, and idle-continuation mechanics follow [`@prevalentware/opencode-goal-plugin`](https://github.com/prevalentWare/opencode-goal-plugin).

## Development

```bash
bun install
bun test
bun run lint
bun run typecheck
bun run build
bun run check:dist
npm pack --dry-run
```

## Publishing

This package is set up for npm Trusted Publishing from GitHub Actions. On every push to `main`, CI runs typecheck, lint, and unit tests in parallel. If they all pass, the publish job computes the next patch version from the latest version on npm, builds the package, and runs `npm publish`.

Before the first automated publish, configure the package on npm:

1. Open the package settings on npmjs.com.
2. Add a Trusted Publisher for GitHub Actions.
3. Use repository `prevalentWare/opencode-loop-plugin`.
4. Use workflow file `publish.yml`.

The repository must be public for npm provenance to be generated automatically.

## Notes

OpenCode plugin modules are target-specific. This package exports separate modules for server hooks/tools and TUI UI. Each default export includes the legacy V1 entrypoint (`server` or `tui`) and the native V2 `setup` entrypoint:

```json
{
  "exports": {
    "./server": "./dist/server.js",
    "./tui": "./src/tui.tsx"
  }
}
```

Claude Code's `/loop` has deeper runtime integration (cron scheduling, cache-aware wake-ups, event monitors). This plugin implements the same workflow with OpenCode plugin hooks: timers on the server plugin, idle detection through `session.status` / `session.idle` events, and prompt injection through `session.promptAsync`. The TUI sidebar reads loop state from the plugin's tool outputs in the session, so it works without a private channel between the TUI and the server.
