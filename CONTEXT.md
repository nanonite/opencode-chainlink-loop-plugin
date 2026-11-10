# OpenCode Loop Plugin

This context defines the language for loop-mode behavior across OpenCode surfaces. It exists to keep the scheduler, the slash command, and UI expectations distinct.

Chainlink integration also provides issue-driven worker/reviewer workflows. Its standalone `chainlink-loop` CLI controls the workflow in code and runs each step through `opencode run`; this is the only supported engine and entrypoint. The former in-server `/chainlink` command and `run_chainlink_outer` tool were removed (see AGENTS.md -> Chainlink Engine Rule).

## Language

**Loop**:
A recurring instruction bound to one session, re-injected by the scheduler while the session is idle, until it is stopped, completed, or expired.
_Avoid_: cron job, watcher, goal

**Interval Loop**:
A loop with a fixed cadence (`30s`–`7d`). The scheduler owns the time between iterations.
_Avoid_: timer task

**Dynamic Loop**:
A loop without a fixed cadence. The agent ends each iteration by scheduling the next run (`schedule_next_run`) or by stopping the loop; doing neither ends the loop.
_Avoid_: self-loop, auto mode

**Iteration**:
One synthetic prompt injected into the session for a loop, performing the instruction exactly once without sleeping or polling.
_Avoid_: tick, run (in prose; `runCount` is fine in code)

**Scheduler**:
The server-plugin component that owns timers, busy/idle tracking, deferral, and injection. Only the scheduler decides when an iteration happens.
_Avoid_: daemon, worker

**Chainlink Workflow**:
Work on one Chainlink issue, including worker attempts, reviewer decisions, and optional issue closure after a `done` outcome. A workflow ends on approval, attempt exhaustion, failure, cancellation, or interruption.

**Chainlink Outer Loop**:
Selection of successive actionable Chainlink issues. It stops when the queue is empty, the configured task cap is reached, or the default stop-on policy halts at a non-done outcome.

**Chainlink Run Record**:
The durable, atomic record of one CLI run: run id, pid, workspace, build, heartbeat, current step, per-task outcomes, final status and exit code. `status` and `wait` read it; a run with no final status and a gone pid or stale heartbeat is `dead` and names its exact relaunch command.

**Tracker State**:
The per-task answer `status` gets from Chainlink with `issue show --json <id>`: `open`, `closed`, or `unknown` when the lookup cannot answer (missing binary, unreachable database, or unknown id). It is read-only, bounded and never changes the run record, its state, exit code, or `wait` result. A terminal run that is not `done` whose tasks are all `closed` is marked `trackerResolved`.
_Avoid_: verdict, accepted (the run record, not the tracker, owns the outcome)

**Task Outcome**:
The deterministic verdict for one issue: `done`, `approved-no-changes`, `approved-but-escalated`, `verify-failed`, `exhausted`, `crashed`, or `interrupted`. Reviewer approval is necessary but never sufficient for `done`; new commits since the task base sha (the prior run record's `baseSha` when resuming or reviewing existing work) and a passing `--verify` (when configured) are required. An ordinary `blocked_by` dependency is not an escalation; only an explicit blocked status/flag, a blocking/escalation label, a blocker/escalation comment kind, or a comment starting with `ESCALATION:` is.
_Avoid_: approval, complete, green

**Operator Direction**:
Free-form guidance supplied when a Chainlink run starts (`--prompt` on the CLI, `custom_prompt` for the `/chainlink` tool). It is layered above each issue's notes and applied by both the worker and the reviewer without editing the issues.
_Avoid_: system prompt, override

## Relationships

- A **Loop** is executed as a series of **Iterations** driven by the **Scheduler**.
- A **Dynamic Loop** delegates pacing to the agent one **Iteration** at a time; an **Interval Loop** never does.
- The `/loop` slash command is the user entrypoint; the loop tools are the agent entrypoint; both mutate the same persisted state owned by the server plugin.
- `chainlink-loop` is the standalone and only entrypoint for a **Chainlink Outer Loop**; the former in-server `/chainlink` command and `run_chainlink_outer` tool were removed (see AGENTS.md).
- A **Chainlink Outer Loop** runs one **Chainlink Workflow** per selected issue. Each workflow uses worker and reviewer attempts, rather than scheduler iterations.
- Each **Chainlink Outer Loop** run owns one **Chainlink Run Record**; `wait` and `status` read that record rather than the log.
- `status` adds a **Tracker State** per task without touching the **Chainlink Run Record**; a terminal non-done record whose tasks are all closed is annotated as `trackerResolved`, while its state, exit code and `wait` result stay unchanged.
- A **Task Outcome** is computed from commits since the task base (including the recorded base from a prior run when resuming), the issue's explicit escalation/blocked state, and the acceptance command, never from reviewer approval alone or from a dependency link.
- An **Operator Direction** applies to every **Chainlink Workflow** in a run, so the worker scopes to it and the reviewer verifies it.

## Flagged Ambiguities

- "loop" vs. "goal": a goal defines when a task is done; a loop defines when to wake the agent again. They are separate plugins with separate state.
- "stop" vs. "pause": stop is terminal and requires a new loop to restart; pause keeps the loop resumable and does not run iterations.
