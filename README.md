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
- A TUI sidebar with live countdowns and a `Loops` command-palette entry to run, pause, resume, or stop loops.
- Plan-mode safety: iterations are deferred while the session's last prompt came from a restricted agent (default: `plan`).
- Compaction context so active loops are preserved when OpenCode summarizes a long session.
- Safety rails: minimum interval, per-session loop limit, optional max runs, and automatic expiry after 7 days.
- Chainlink integration: a `chainlink-loop` CLI for unattended issue queues, plus a `/chainlink` command and `run_chainlink_outer` tool on OpenCode 2.

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

OpenCode 2 does not read the V1 `tui.json` file. The server entrypoint is loaded from `opencode.json`; the sidebar and palette integration are loaded from the global `~/.config/opencode/cli.json`.

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
| `--task <ids>` | Comma-separated issue ids. `#67` is accepted and normalised to `67`. |
| `--attempts <n>` | Per-task attempt limit (default 20). |
| `--max-tasks <n>` | Stop after this many tasks. |
| `--no-close` | Leave approved issues open. Default is to close them. |
| `--review-first auto\|always\|never` | Review work that already exists before dispatching a fresh worker (default `auto`). |
| `--exclude <ids>` | Skip these issue subtrees when selecting ready tasks after an exhausted issue. |
| `--prompt <text>` | Operator direction layered above each task's notes. Both the worker and the reviewer follow it, so a run can be scoped without editing the issues. Also settable via `CHAINLINK_PROMPT`; `--prompt` wins. |
| `--worker-model` / `--reviewer-model` | `provider/model` per role. |
| `--worker-agent` / `--reviewer-agent` | Defaults: `build` and `plan`. |
| `--worker-timeout` / `--reviewer-timeout` | Per-step wall clock limit in seconds. |
| `--dry-run` | Show the selected settings and, for one issue, whether existing work will be reviewed first. |
| `-v`, `--version` | Print the plugin build version and exit. |

The loop per task:

1. `opencode run --standalone --auto --agent build "<issue spec>"` — a fresh worker process.
2. `opencode run --standalone --auto --agent plan "<review prompt>"` — a fresh reviewer process that returns the verdict JSON. The `plan` agent cannot write outside its plan directory, so a reviewer cannot edit the tree even under `--auto`; the plugin also denies edit/write/patch requests raised by a reviewer session.
3. If not approved, the findings go back to the **same worker session** via `--session <id>`, so context is kept. Repeat until approval or the attempt limit.
4. Close the issue only on approval and only when closing is enabled.

`--review-first auto` looks for a dirty tree, commits that mention the issue, a previous Chainlink workflow for the same task, or comments on the issue. If any are found, the reviewer runs **first** so a previous attempt's work is judged rather than thrown away.

**One loop per workspace.** The loop takes a lock keyed by working directory and `CHAINLINK_DB` (`$TMPDIR/chainlink-loop-*.lock`) so a second invocation fails loudly instead of dispatching two workers at the same files. A lock whose owning process is gone is taken over automatically. `SIGINT`/`SIGTERM` stop the loop, kill the in-flight step's process group, and release the lock; a second signal exits immediately.

**Exhausted tasks are skipped, not fatal.** A task that burns its attempt limit is left open for a human and the loop moves on to the next ready leaf. Because `issue next` keeps returning the same still-open task, the loop falls back to `issue ready --json` and picks the first leaf it has not touched. That fallback does **not** know about a project which deliberately holds a subtree back, so use `--exclude <ids>` for those — for example `--exclude 1` to keep the queue out of a "upstream reports" epic whose children wait on a human.

### `/chainlink` and the `run_chainlink_outer` tool (in-server)

The V2 plugin also registers a `/chainlink` command and a `run_chainlink_outer` tool that run the same loop using in-server child sessions.

> **Known limitation (OpenCode 2.0.16).** The command handler is registered but its `execute` callback is never invoked by this build, so the model receives the raw text `/chainlink` instead of the instruction template. Anything typed after it is then interpreted by the model, not by the adapter. Use `chainlink-loop` for unattended work; the tool path remains for calling the loop from inside a session.

What the tool path does enforce:

- Task ids are normalised, so `#67`, `67` and `issue 67` are the same task.
- A `custom_prompt` (set from `/chainlink --prompt "..."`) is layered above every task's notes. The worker follows it alongside the requirements and the reviewer holds the work to it, so you can scope a run without editing the issues.
- **Exactly one call per turn.** A second `run_chainlink_outer` in the same turn is refused without starting a workflow.
- On any failure the result carries `orchestrator_instruction: "Do not continue the task yourself…"`, because small models tend to do the task in the command turn after a tool failure.
- A task that exhausts its attempts is left open and the loop moves on. Because `issue next` keeps returning that same still-open task, the loop falls back to `issue ready --json`. Pass `exclude_task_ids` for subtrees the project deliberately holds back.
- A workflow owned by a live process is never marked `interrupted` by another instance, and if a stale `interrupted` flag appears anyway the owner reclaims it and still reaches review instead of discarding finished work.

Child sessions in this path have no interactive client, so their permission requests are answered by the plugin through the `permission.evaluate` hook:

- `chainlink_child_permissions: "allow"` (default, `inherit` is a synonym) — answer inline.
- `"deny"` — refuse everything.
- `"ask"` — legacy blocking behaviour, for debugging only.

A stalled child is interrupted and re-prompted once. If the stall was caused by an unanswered permission request, the error names it (`waiting on permission external_directory …`) and the retry prompt tells the model not to repeat the same call.

To load this local checkout instead of the npm package, add the checkout to the global `~/.config/opencode/opencode.json` (applies to every project):

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "plugins": [
    "file:///path/to/opencode-chainlink-loop-plugin"
  ]
}
```

Run `bun install` and `bun run build` first. OpenCode 2 loads a configured plugin directory through an `index.js` or `index.ts` entrypoint, and this repository ships `index.js` for that purpose. Point the entry at the directory: OpenCode 2 rejects configured file paths with `configured plugin path must be a directory`. Then run `opencode reload` or restart the service.

Git specifiers fail on OpenCode 2.0.16 for packages that declare build, prepare, or install scripts (upstream opencode issues 49704 and 46551). Until those fixes ship, load the checkout directory or use the npm package.

> **Long-lived servers and workflow state.** Ownership is tracked by pid, and a running workflow is only reclaimed when its owning process is gone. A server that has been running since *before* an upgrade still holds the old code in memory and will keep reclaiming workflows it can see. Give unattended runs their own state file to isolate them:
>
> ```bash
> OPENCODE_LOOP_STATE_PATH=~/.local/share/opencode-loop-plugin/loops-standalone.json \
> chainlink-loop --task 67
> ```
>
> Restarting the shared service (`opencode serve --service`) makes it pick up the new bundle.

Chainlink orchestration is intentionally V2-only because it uses child session creation, waiting, context collection, and interruption APIs. Ordinary `/loop` behavior and the V1 entrypoint are unchanged.

Manage loops:

```text
/loop list
/loop stop loop_7k3p9
/loop pause loop_7k3p9
/loop resume loop_7k3p9
/loop run loop_7k3p9
/loop clear
```

After creating a loop, the agent immediately performs the first iteration in the same turn — it does not wait for the first scheduled run. On each scheduled iteration the scheduler injects a synthetic prompt telling the agent to perform exactly one iteration, never to sleep or poll inside the turn, and to call `stop_loop` once the loop's purpose is achieved (or `pause_loop` if it is blocked on the user).

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
        "command_name": "loop",
        "chainlink_command_name": "chainlink",
        "chainlink_next_args": ["issue", "next", "--json"],
        "chainlink_show_args": ["issue", "show", "--json"],
        "chainlink_close_args": ["issue", "close", "--json"],
        "chainlink_max_attempts": 20,
        "chainlink_max_tasks": null,
        "chainlink_worker_agent": "build",
        "chainlink_reviewer_agent": "plan",
        "chainlink_worker_timeout_seconds": 3600,
        "chainlink_reviewer_timeout_seconds": 1800,
        "chainlink_stall_timeout_seconds": 300,
        "chainlink_child_permissions": "allow",
        "chainlink_close_completed_tasks": true,
        "chainlink_db_path": "/path/to/project/.chainlink",
        "chainlink_worker_model": "provider/worker-model",
        "chainlink_reviewer_model": "provider/reviewer-model"
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
- `chainlink_command_name`: `"chainlink"`
- `chainlink_next_args`: `["issue", "next", "--json"]`; the legacy `chainlink_ready_args` option is still accepted as an alias.
- `chainlink_show_args`: `["issue", "show", "--json"]`
- `chainlink_close_args`: `["issue", "close", "--json"]`
- `chainlink_max_attempts`: `20` worker/reviewer attempts per task.
- `chainlink_max_tasks`: `null`; an optional outer-loop task cap.
- `chainlink_worker_agent`: `"build"`
- `chainlink_reviewer_agent`: `"plan"`
- `chainlink_worker_timeout_seconds`: `3600`
- `chainlink_reviewer_timeout_seconds`: `1800`
- `chainlink_stall_timeout_seconds`: `300`; a child that produces no session activity (message parts, tool updates) for this long is interrupted and the worker is re-prompted once. Raise it if a single tool call legitimately runs longer.
- `chainlink_child_permissions`: `allow`; how the plugin answers permission requests from its own child sessions. `allow`/`inherit` answer inline, `deny` refuses, `ask` leaves them pending (the old blocking behaviour, for debugging only). A child session has no client attached, so anything left pending is indistinguishable from a hang.
- `chainlink_close_completed_tasks`: `true`; approved tasks are closed by the plugin, never by the worker. `--no-close` overrides this per run.
- `chainlink_db_path`: optional Chainlink database path; when unset, `CHAINLINK_DB` is inherited from the environment.
- `chainlink_worker_model`: optional `provider/model` reference for worker child sessions.
- `chainlink_reviewer_model`: optional `provider/model` reference for reviewer child sessions.

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
