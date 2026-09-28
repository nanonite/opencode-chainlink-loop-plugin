# Agent Notes

## Project Shape

This package is an OpenCode plugin with separate server and TUI entrypoints:

- `src/server.ts` is the server plugin. It registers the `/loop` command, the loop tools, chat/session hooks, compaction context, and the scheduler that injects loop iterations while a session is idle.
- `src/state.ts` owns loop persistence and lifecycle state. It stores JSON at `OPENCODE_LOOP_STATE_PATH` when set, otherwise under the user's OpenCode data location. It also owns interval parsing/formatting.
- `src/tui.tsx` is the Solid/OpenTUI sidebar and loop-actions UI. The `Loops` command is bound to `Ctrl+L`; OpenCode 2.0.16 does not list plugin commands in the `Ctrl+P` palette, so keep the keybind. It is exported as source, so avoid adding heavy runtime dependencies here unless there is a strong reason.
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
- Effect is intentionally used in the state/persistence boundary. Do not spread Effect into the TUI.
- State writes should remain atomic: write to a temp file, then `rename` into place.
- Use `OPENCODE_LOOP_STATE_PATH` for tests and smoke runs so you do not touch a real user's loop state.
- All loop tools return `{ ..., loops }` for the session; the TUI sidebar parses that shape from tool outputs, so keep it stable.
- Keep the repository-root `index.js` and `tui.tsx` entrypoints and the committed `dist/` output in sync; OpenCode 2 loads this checkout directory through `index.js` for the server plugin and `tui.tsx` for the CLI/TUI plugin.

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

To test the CLI/TUI entrypoint locally, add the same checkout directory to the global `~/.config/opencode/cli.json` `plugins` list and open the TUI (`opencode2 --standalone`). Confirm the debug log shows `entrypoint=.../tui.tsx` and `stage=setup plugin=local.loop-mode.tui` for `role=cli`, the sidebar renders active loops, and `Ctrl+L` opens the loop-actions dialog. OpenCode 2.0.16 does not auto-load a directory plugin's TUI component from the server plugin list, so `cli.json` must list the checkout directory explicitly.

OpenCode 2 loads a configured plugin directory through an `index.js`/`index.ts` entrypoint for the server and a `tui.tsx`/`tui.ts` entrypoint for the CLI, so keep the repository-root `index.js` and `tui.tsx` in place. Configured file paths are rejected and Git specifiers for packages with build scripts fail on OpenCode 2.0.16 (upstream opencode issues 49704 and 46551).
