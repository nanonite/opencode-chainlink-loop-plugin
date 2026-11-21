# Agent Notes

## Project Shape

This package is an OpenCode plugin with separate server and TUI entrypoints:

- `src/server.ts` is the server plugin. It registers the `/loop` command, the loop tools, chat/session hooks, compaction context, and the scheduler that injects loop iterations while a session is idle.
- `src/state.ts` owns loop persistence and lifecycle state. It stores JSON at `OPENCODE_LOOP_STATE_PATH` when set, otherwise under the user's OpenCode data location. It also owns interval parsing/formatting.
- `src/tui.tsx` is the Solid/OpenTUI sidebar and loop-actions UI. The `Loops` command is bound to `Ctrl+L`; keep the keybind because `Ctrl+P` palette support varies by OpenCode build. It is exported as source, so avoid adding heavy runtime dependencies here unless there is a strong reason.
- `src/prompts.ts` contains the `/loop` command template, the per-iteration synthetic prompt, the loop system reminder, and the compaction context.
- `test/` covers state, server hooks/tools/scheduler, and TUI helpers with Bun tests. Scheduler tests use 1-second intervals via the `min_interval_seconds` option.

## Scheduler Invariants

- Iterations are only injected while the session is idle; busy sessions defer with `min(intervalMs, busy_backoff_seconds)`.
- `sendingLoops` guards against concurrent iterations of the same loop; after a successful injection the session is optimistically marked busy so sibling loops defer.
- Dynamic loops live one iteration at a time: `dynamicPending` entries settle on idle, and a dynamic loop whose turn ended without `schedule_next_run` or `stop_loop` is stopped. `sawBusy` must start `false` for injected iterations (only the creating turn starts `true`) so a stale idle event cannot settle a just-injected loop.
- `runDue` must never throw: it runs from `setTimeout` callbacks, so errors are caught and logged.
- Timers are per-loop, always re-armed from persisted `nextRunAt`, and cancelled on stop/pause/dispose.

## Change Guidelines

- Preserve the public Promise-based state API (`createLoop`, `listLoops`, `stopLoop`, `recordRunSent`, etc.) because OpenCode hooks and tests call it directly.
- Keep `zod` pinned to the exact version `@opencode-ai/plugin` depends on; tool argument schemas fail typechecking across zod minor versions.
- Effect is intentionally used in the state/persistence boundary. Do not spread Effect into the TUI. The server bundle must bundle Effect: OpenCode 2.3 resolves a plugin's bare `effect` import to its own Effect 4 runtime, where the Effect 3 `Schema.optionalWith` API is missing.
- State writes should remain atomic: write to a temp file, then `rename` into place.
- Use `OPENCODE_LOOP_STATE_PATH` for tests and smoke runs so you do not touch a real user's loop state.
- All loop tools return `{ ..., loops }` for the session; the TUI sidebar parses that shape from tool outputs, so keep it stable.
- Keep the repository-root `index.js` and `tui.tsx` entrypoints and the committed `dist/` output in sync. OpenCode 2.3 loads this checkout directory through `index.js` for the server plugin and through the package's `./tui` export (`src/tui.tsx`) for the CLI/TUI plugin; the root files remain the fallback for older 2.x builds.

## Local Validation

Before treating a code change as complete, run the relevant checks. For release-level changes, run the full local gate:

```bash
bun run lint
bun run typecheck
bun test
bun run build
bun run check:dist
bun run pack:dry-run
```

`bun run build` writes `dist/server.js`. `dist/` is committed; `bun run check:dist` rebuilds with the identity recorded in `dist/build-info.json` and fails when `dist/` does not match the source. The package only publishes `dist`, `src/tui.tsx`, `LICENSE`, and `README.md`, so confirm `npm pack --dry-run` includes what runtime installation needs.

## Publishing Flow

This repo publishes from GitHub Actions on pushes to `main`. The workflow computes the next patch version from npm, builds, publishes, and creates a GitHub release.

After pushing a release change, monitor the workflow with `gh run list --branch main` and `gh run watch <run-id> --exit-status`. Verify the release and package metadata after success:

```bash
npm view @prevalentware/opencode-loop-plugin version dependencies
gh release view v<version>
```

## End-To-End Plugin Test

To test this plugin end to end, do not stop at unit tests. Run the local gates first, then load the checkout in an isolated OpenCode 2 project:

1. Point a temp config at the checkout directory: `"plugins": ["file:///path/to/opencode-chainlink-loop-plugin"]`.
2. Run `opencode run --standalone --print-logs --log-level info` and confirm a `msg="loading plugin"` line for the directory.
3. Run a smoke test with an isolated state file:

```bash
OPENCODE_LOOP_STATE_PATH="/tmp/opencode-loop-plugin-smoke/loops.json" opencode run --standalone "/loop 1m say exactly 'tick' and stop this loop after confirming it exists"
```

The smoke test should show `create_loop` (and possibly `stop_loop`) tool calls. Inspect the state file afterward to confirm JSON persistence, and clean up with `/loop stop <id>` or by deleting the smoke state file.

To test the CLI/TUI entrypoint locally, list the checkout in the project `opencode.json` and open the TUI (`opencode --standalone`). OpenCode 2.3 auto-loads the package's `./tui` export (`src/tui.tsx`) from that same entry; add the checkout to `~/.config/opencode/cli.json` only when testing against a remote server or an older 2.x build. Confirm the debug log shows `entrypoint=.../src/tui.tsx` and `stage=setup plugin=local.loop-mode.tui` for `role=cli`, the sidebar renders active loops, and `Ctrl+L` opens the loop-actions dialog.

OpenCode 2.3 uses `session.execution.started` and `session.execution.succeeded` / `failed` / `interrupted` for turn lifecycle events. The V2 handler also accepts the older `session.status` / `session.idle` names.

To exercise the scheduler, keep a standalone server alive so a loop can fire while the session is idle. Use plugin options in the object form; OpenCode 2.3 rejects the V1 tuple form:

```bash
ROOT=/tmp/opencode/loop-scheduler-smoke
mkdir -p "$ROOT/config/opencode" "$ROOT/work"
cat > "$ROOT/config/opencode/opencode.json" <<'JSON'
{
  "plugins": [
    { "package": "file:///path/to/opencode-chainlink-loop-plugin", "options": { "min_interval_seconds": 1 } }
  ]
}
JSON
cd "$ROOT/work"
XDG_CONFIG_HOME="$ROOT/config" OPENCODE_PASSWORD=smoke OPENCODE_LOOP_STATE_PATH="$ROOT/loops.json" \
  opencode serve --port 4789 --print-logs --log-level info > "$ROOT/server.log" 2>&1 &
opencode run --server http://127.0.0.1:4789 --format json \
  "Call create_loop with instruction 'say exactly tick' and interval '1s'. Do not stop the loop."
sleep 3
cat "$ROOT/loops.json"   # runCount should advance while the server stays up
```

Stop the server and delete `$ROOT` when done.

OpenCode 2 loads a configured plugin directory through an `index.js`/`index.ts` entrypoint for the server and the package's `./tui` export for the CLI, so keep the repository-root `index.js` and `tui.tsx` files in place for older 2.x builds.

### Chainlink outcome smoke (built CLI)

Run this after `bun run build`. Use the built `bin/chainlink-loop`, a throwaway `CHAINLINK_DB`, a temp `OPENCODE_LOOP_STATE_PATH`, and fake `opencode` executables so no model is called.

The fake `opencode` reads `--agent`; for `plan` it prints a `--format json` text event whose text is the approval JSON, and for `build` it commits `work.txt`. Extra modes make the other cases reproducible: `noop` reports without a commit, `escalate` adds `chainlink issue comment <id> "ESCALATION: ..."` before committing, `hang` commits and then sleeps, and `review-only` never commits. Save this as `$ROOT/bin/opencode` and `chmod +x` it:

```ts
#!/usr/bin/env bun
import { appendFileSync } from "node:fs"
import { spawnSync } from "node:child_process"

const args = process.argv.slice(2)
const agentIndex = args.indexOf("--agent")
const agent = agentIndex >= 0 ? args[agentIndex + 1] : ""
const mode = process.env.SMOKE_MODE ?? "approve"
const task = process.env.SMOKE_TASK ?? "?"
const emit = (text: string) =>
  console.log(JSON.stringify({ sessionID: "ses_step", type: "text", part: { text } }))
const commit = () => {
  appendFileSync("work.txt", `work for #${task}\n`)
  spawnSync("git", ["add", "work.txt"])
  spawnSync("git", ["commit", "-m", `work for #${task}`])
}

if (agent === "plan") {
  emit(JSON.stringify({ approved: true, summary: "lgtm", findings: [], next_action: "done" }))
  process.exit(0)
}
if (mode === "noop") {
  emit("reported without a commit")
} else if (mode === "escalate") {
  spawnSync("chainlink", ["issue", "comment", task, "ESCALATION: production credentials are unavailable"])
  commit()
  emit("committed and escalated")
} else if (mode === "hang") {
  commit()
  emit("committed then hanging")
  await Bun.sleep(300_000)
} else if (mode === "review-only") {
  emit("the resume should not dispatch a worker")
} else {
  commit()
  emit("committed the work")
}
```

Every event line has this shape:

```json
{"sessionID":"ses_step","type":"text","part":{"text":"..."}}
```

```bash
ROOT=/tmp/opencode/chainlink-outcome-smoke
rm -rf "$ROOT" && mkdir -p "$ROOT/bin" "$ROOT/state"
# Write $ROOT/bin/opencode as described above and make it executable.
export CHAINLINK_DB="$ROOT/db"
export OPENCODE_LOOP_STATE_PATH="$ROOT/state/loops.json"
export PATH="$ROOT/bin:$PATH"
CLI="$PWD/bin/chainlink-loop"

# (a) A dependent task whose blocker is still open must still end done.
mkdir -p "$ROOT/dep" && cd "$ROOT/dep"
git init -q && git config user.email smoke@example.com && git config user.name Smoke
echo base > README.md && git add README.md && git commit -qm base
A=$(chainlink issue create "Blocker stays open")
B=$(chainlink issue create "Dependent with open blocker")
AID=$(printf '%s' "$A" | sed -n 's/.*#\([0-9]*\).*/\1/p')
BID=$(printf '%s' "$B" | sed -n 's/.*#\([0-9]*\).*/\1/p')
chainlink issue block "$BID" "$AID"
SMOKE_TASK="$BID" "$CLI" --task "$BID" --review-first never --close-if-verified --verify "test -f work.txt"
"$CLI" status

# Expected RESULT: {"state":"done",...,"outcomes":{"<BID>":"done"},...}
# Expected status: "state: done" and the table row "#<BID>  1  done  passed".
# `chainlink issue show $AID --json` still reports "status": "open".

# (b) Kill a run after its worker commit, then resume it.
mkdir -p "$ROOT/resume" && cd "$ROOT/resume"
git init -q && git config user.email smoke@example.com && git config user.name Smoke
echo base > README.md && git add README.md && git commit -qm base
R=$(chainlink issue create "Killed then resumed")
RID=$(printf '%s' "$R" | sed -n 's/.*#\([0-9]*\).*/\1/p')
SMOKE_MODE=hang SMOKE_TASK="$RID" "$CLI" --task "$RID" --review-first never --no-close &
PID=$!
# Wait until the worker commit exists (git rev-list --count HEAD >= 2), then:
kill -9 "$PID"
"$CLI" status
RUN=$("$CLI" status --json | sed -n 's/.*"run":"\([^"]*\)".*/\1/p')
SMOKE_MODE=review-only SMOKE_TASK="$RID" "$CLI" --resume "$RUN" --review-first always --close-if-verified --verify "test -f work.txt"
"$CLI" status

# Expected status after the kill: "state: dead" and a "relaunch: chainlink-loop --resume ..." line.
# Expected resumed RESULT: {"state":"done",...,"outcomes":{"<RID>":"done"},...}
# Expected resumed status: "state: done"; the pre-existing commit counts because the task base
# comes from the dead run's record, not from HEAD at resume time.

# Escalation and no-change cases.
# SMOKE_MODE=escalate ... --task <id> --review-first never --no-close
#   -> RESULT "state":"not-done" with "approved-but-escalated".
# SMOKE_MODE=noop ... --task <id> --review-first never --no-close
#   -> RESULT "state":"not-done" with "approved-no-changes".
```

## Chainlink Engine Rule

This plugin supports **exactly one** Chainlink engine: the standalone `chainlink-loop` CLI (`src/chainlink-cli.ts` + `src/chainlink-process.ts`), which drives every step as a one-shot `opencode run` process. There is no LLM in the control path. Do not add a second engine, and do not add MCP support.

**MCP is not supported.** This plugin is an in-process OpenCode plugin plus a CLI. Do not add `mcp.servers` entries, `@modelcontextprotocol/*` dependencies, or an MCP transport. MCP is a separate integration channel for external servers, not a plugin entrypoint.

**The in-server child-session engine is not supported and has been removed.** Do not reintroduce `runChainlinkOuter`, `runInnerWorkflow`, `promptChild`, `ChainlinkChildRegistry`, `context.session.create(...)` orchestration, the `/chainlink` command, or the `run_chainlink_outer` tool. If an in-session entrypoint is ever needed, it must be a thin launcher for the CLI engine — spawn `chainlink-loop` or call `runProcessLoop` and stream its log — never a second orchestrator.

Enforcement: `bun run check:engine` scans `src/` and `test/` and rejects any occurrence of the forbidden identifiers. It is wired as the versioned `.githooks/pre-commit` hook; enable it once per clone with `git config core.hooksPath .githooks`. Use `git commit --no-verify` only for a deliberate refactor.
