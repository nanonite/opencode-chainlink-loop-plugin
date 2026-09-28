# OpenCode Loop Plugin

This context defines the language for loop-mode behavior across OpenCode surfaces. It exists to keep the scheduler, the slash command, and UI expectations distinct.

Chainlink integration also provides issue-driven worker/reviewer workflows. Its standalone `chainlink-loop` CLI controls the workflow in code and runs each step through `opencode run`; the OpenCode 2 `/chainlink` command and `run_chainlink_outer` tool use in-server child sessions.

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
Work on one Chainlink issue, including worker attempts, reviewer decisions, and optional issue closure after approval. A workflow ends on approval, attempt exhaustion, failure, cancellation, or interruption.

**Chainlink Outer Loop**:
Selection of successive actionable Chainlink issues. It stops when the queue is empty or the configured task cap is reached.

**Operator Direction**:
Free-form guidance supplied when a Chainlink run starts (`--prompt` on the CLI, `custom_prompt` for the `/chainlink` tool). It is layered above each issue's notes and applied by both the worker and the reviewer without editing the issues.
_Avoid_: system prompt, override

## Relationships

- A **Loop** is executed as a series of **Iterations** driven by the **Scheduler**.
- A **Dynamic Loop** delegates pacing to the agent one **Iteration** at a time; an **Interval Loop** never does.
- The `/loop` slash command is the user entrypoint; the loop tools are the agent entrypoint; both mutate the same persisted state owned by the server plugin.
- `chainlink-loop` is the standalone entrypoint for a **Chainlink Outer Loop**. On OpenCode 2, `/chainlink` and `run_chainlink_outer` provide an in-server entrypoint.
- A **Chainlink Outer Loop** runs one **Chainlink Workflow** per selected issue. Each workflow uses worker and reviewer attempts, rather than scheduler iterations.
- An **Operator Direction** applies to every **Chainlink Workflow** in a run, so the worker scopes to it and the reviewer verifies it.

## Flagged Ambiguities

- "loop" vs. "goal": a goal defines when a task is done; a loop defines when to wake the agent again. They are separate plugins with separate state.
- "stop" vs. "pause": stop is terminal and requires a new loop to restart; pause keeps the loop resumable and does not run iterations.
