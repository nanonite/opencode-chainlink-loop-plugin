#!/usr/bin/env bun
import { spawn } from "node:child_process"
import { mkdir, open } from "node:fs/promises"
import {
  collectStepOutput,
  detectExistingWork,
  opencodeRunArgs,
  runProcessLoop,
  spawnOpencodeRun,
  type ProcessLoopOptions,
  type ProcessRunner,
  type ProcessStepInput,
} from "./chainlink-process"
import { execChainlinkCommand, parseModelRef, type ChainlinkCommandRunner } from "./chainlink-core"
import { acquireLoopLock, type LoopLock } from "./chainlink-lock"
import {
  ChainlinkRunRecorder,
  deriveRunState,
  findRunRecord,
  formatRunTable,
  generateRunID,
  runExitCode,
  runLogPath,
  runRecordPath,
  runResultSummary,
  runsDirectory,
  type ChainlinkRunRecord,
  type ChainlinkRunStatus,
} from "./chainlink-runs"
import { PLUGIN_NAME, formatBuildInfo } from "./version"

function parseJSON(text: string): unknown {
  try {
    return JSON.parse(text)
  } catch {
    return {}
  }
}

/**
 * A non-LLM entry point for the Chainlink loop.
 *
 * `/chainlink` needs a model to call a tool, and that model then retypes the
 * input, retries on its own, or does the task in the command turn. This CLI has
 * none of that: code picks the issue, counts the attempts, and stops. Every step
 * is a one-shot `opencode run` process with its own `--auto` permissions.
 *
 *   bun run src/chainlink-cli.ts                      # drain the queue
 *   bun run src/chainlink-cli.ts --task 67 --no-close # one issue, leave it open
 *   bun run src/chainlink-cli.ts --task 67,71 --attempts 5
 *   bun run src/chainlink-cli.ts --prompt "keep changes minimal"
 */
export type ChainlinkCliArguments = {
  taskIds: string[] | null
  attempts: number
  maxTasks: number | null
  closeOnApproval: boolean
  /** Close only a done task whose --verify passed; otherwise leave it open. */
  closeIfVerified: boolean
  /** Halt the queue at the first task whose outcome is not done (default). */
  stopOnNonDone: boolean
  /** Pick up the remaining tasks from this run record. */
  resumeRunId: string | null
  reviewFirst: "auto" | "always" | "never"
  excludeIDs: string[]
  workerModel: string | null
  reviewerModel: string | null
  workerAgent: string
  reviewerAgent: string
  workerTimeout: number
  reviewerTimeout: number
  /** Extra reviewer processes to start after one dies before delivering a verdict. */
  reviewerRetries: number
  /** Kill a step that emits no output within this many seconds (0 disables). */
  startupTimeout: number
  /** Extra attempts for a step that never produced any output at all. */
  startupRetries: number
  /** Deterministic acceptance command run after reviewer approval. */
  verifyCommand: string | null
  customPrompt: string | null
  dryRun: boolean
  version: boolean
  help: boolean
  /** Internal: lets `start --detach` name the record before spawning the child. */
  runId: string | null
}

const USAGE = `chainlink-loop — deterministic Chainlink outer/inner loop (no LLM in the control path)

Usage:
  chainlink-loop [options]

Options:
  --task <ids>        Comma-separated issue ids, e.g. 67 or 67,71. "#67" is accepted.
  --attempts <n>      Per-task worker/reviewer attempt limit (default 20).
  --max-tasks <n>     Stop after this many tasks.
  --no-close          Leave approved issues open (default is to close them).
  --close-if-verified Close an issue only when the outcome is done and --verify
                      passed; otherwise it stays open.
  --stop-on <mode>    non-done | never. Default non-done: halt at the first task
                      whose deterministic outcome is not done. never keeps the
                      old skip-and-continue behaviour.
  --resume <runId>    Resume the remaining tasks from a run record (status shows
                      the exact command for a dead run).
  --review-first <m>  auto | always | never (default auto: review work already in
                      the tree before dispatching a fresh worker).
  --exclude <ids>     Comma-separated issue ids whose subtrees the queue must not
                      enter. Needed when a project deliberately holds a subtree
                      back: \`issue next\` honours that, but the fallback used to
                      step past an exhausted task does not.
  --worker-model <m>  provider/model[#variant] for the worker.
  --reviewer-model <m> provider/model[#variant] for the reviewer.
  --worker-agent <a>  Agent for the worker (default build).
  --reviewer-agent <a> Agent for the reviewer (default plan; plan cannot edit files).
  --worker-timeout <s>   Per-step worker timeout in seconds (default 3600).
  --reviewer-timeout <s> Per-step reviewer timeout in seconds (default 1800).
  --reviewer-retries <n> Extra reviewer processes to start when a reviewer step
                      dies before delivering a verdict (non-zero exit, spawn
                      error, timeout). A crash is retried, not treated as a
                      verdict; an operator abort is never retried. Default 1.
  --startup-timeout <s> Kill a step that emits no output within this many seconds
                      and retry it (default 300; 0 disables the watchdog). A step
                      whose opencode run never creates a session would otherwise
                      sit until its full per-step timeout.
  --startup-retries <n> Retries for a step that never produced any output at all
                      (default 1). The killed process group is logged.
  --verify <cmd>      Acceptance command run in the worktree after reviewer
                      approval. Exit 0 is required for a "done" outcome; a
                      failure becomes findings for the next worker attempt.
  --prompt <text>     Operator direction layered above each task's notes; both
                      the worker and the reviewer follow it. Quote multi-word
                      values, e.g. --prompt "prefer the existing parser".
  --dry-run           Print the plan and exit without running anything.
  -v, --version       Show the plugin build version and exit.
  -h, --help          Show this help.

Environment:
  CHAINLINK_DB        Chainlink database path (passed through to the CLI).
  CHAINLINK_PROMPT    Operator direction; overridden by --prompt.
`

function parseArgs(argv: string[]): ChainlinkCliArguments {
  const parsed: ChainlinkCliArguments = {
    taskIds: null,
    attempts: 20,
    maxTasks: null,
    closeOnApproval: true,
    closeIfVerified: false,
    stopOnNonDone: true,
    resumeRunId: null,
    reviewFirst: "auto",
    excludeIDs: (process.env.CHAINLINK_EXCLUDE ?? "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean),
    workerModel: process.env.CHAINLINK_WORKER_MODEL ?? null,
    reviewerModel: process.env.CHAINLINK_REVIEWER_MODEL ?? null,
    workerAgent: process.env.CHAINLINK_WORKER_AGENT ?? "build",
    reviewerAgent: process.env.CHAINLINK_REVIEWER_AGENT ?? "plan",
    workerTimeout: Number(process.env.CHAINLINK_WORKER_TIMEOUT ?? 3600),
    reviewerTimeout: Number(process.env.CHAINLINK_REVIEWER_TIMEOUT ?? 1800),
    reviewerRetries: Number(process.env.CHAINLINK_REVIEWER_RETRIES ?? 1),
    startupTimeout: Number(process.env.CHAINLINK_STARTUP_TIMEOUT ?? 300),
    startupRetries: Number(process.env.CHAINLINK_STARTUP_RETRIES ?? 1),
    verifyCommand: null,
    customPrompt: process.env.CHAINLINK_PROMPT ?? null,
    dryRun: false,
    version: false,
    help: false,
    runId: null,
  }
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]!
    const next = () => {
      const value = argv[i + 1]
      if (value == null) throw new Error(`${arg} needs a value`)
      i += 1
      return value
    }
    switch (arg) {
      case "-h":
      case "--help":
        parsed.help = true
        break
      case "-v":
      case "--version":
        parsed.version = true
        break
      case "--task":
      case "--tasks":
        parsed.taskIds = next()
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean)
        break
      case "--attempts":
        parsed.attempts = Number(next())
        break
      case "--max-tasks":
        parsed.maxTasks = Number(next())
        break
      case "--no-close":
        parsed.closeOnApproval = false
        break
      case "--close":
        parsed.closeOnApproval = true
        break
      case "--close-if-verified":
        parsed.closeIfVerified = true
        break
      case "--stop-on": {
        const value = next()
        if (value === "non-done") parsed.stopOnNonDone = true
        else if (value === "never") parsed.stopOnNonDone = false
        else throw new Error("--stop-on must be non-done or never")
        break
      }
      case "--resume":
        parsed.resumeRunId = next()
        break
      case "--review-first":
        parsed.reviewFirst = next() as ChainlinkCliArguments["reviewFirst"]
        break
      case "--exclude":
        parsed.excludeIDs.push(
          ...next()
            .split(",")
            .map((value) => value.trim())
            .filter(Boolean),
        )
        break
      case "--worker-model":
        parsed.workerModel = next()
        break
      case "--reviewer-model":
        parsed.reviewerModel = next()
        break
      case "--worker-agent":
        parsed.workerAgent = next()
        break
      case "--reviewer-agent":
        parsed.reviewerAgent = next()
        break
      case "--worker-timeout":
        parsed.workerTimeout = Number(next())
        break
      case "--reviewer-timeout":
        parsed.reviewerTimeout = Number(next())
        break
      case "--reviewer-retries":
        parsed.reviewerRetries = Number(next())
        break
      case "--startup-timeout":
        parsed.startupTimeout = Number(next())
        break
      case "--startup-retries":
        parsed.startupRetries = Number(next())
        break
      case "--verify":
        parsed.verifyCommand = next()
        break
      case "--prompt":
        parsed.customPrompt = next()
        break
      case "--dry-run":
        parsed.dryRun = true
        break
      case "--run-id":
        parsed.runId = next()
        break
      default:
        throw new Error(`unknown option ${arg}`)
    }
  }
  if (!Number.isFinite(parsed.attempts) || parsed.attempts < 1) throw new Error("--attempts must be a positive number")
  if (!Number.isFinite(parsed.workerTimeout) || parsed.workerTimeout < 1) throw new Error("--worker-timeout must be positive")
  if (!Number.isFinite(parsed.reviewerTimeout) || parsed.reviewerTimeout < 1) {
    throw new Error("--reviewer-timeout must be positive")
  }
  if (!Number.isInteger(parsed.reviewerRetries) || parsed.reviewerRetries < 0) {
    throw new Error("--reviewer-retries must be a non-negative whole number")
  }
  if (!Number.isFinite(parsed.startupTimeout) || parsed.startupTimeout < 0) {
    throw new Error("--startup-timeout must be a non-negative number")
  }
  if (!Number.isInteger(parsed.startupRetries) || parsed.startupRetries < 0) {
    throw new Error("--startup-retries must be a non-negative whole number")
  }
  if (!["auto", "always", "never"].includes(parsed.reviewFirst)) {
    throw new Error("--review-first must be auto, always or never")
  }
  return parsed
}

export type BuildProcessLoopOptionsInput = {
  ownerSessionID: string
  cwd: string
  parsed: ChainlinkCliArguments
  step: ProcessRunner
  runner: ChainlinkCommandRunner
  log: (line: string) => void
  dbPath: string | null
}

/**
 * Builds the loop options. Kept separate from `main` so the wiring is testable
 * without spawning processes: `log` was once omitted here, which silently
 * swallowed every line runProcessLoop/runProcessInnerLoop emitted.
 */
export function buildProcessLoopOptions(input: BuildProcessLoopOptionsInput): ProcessLoopOptions {
  return {
    ownerSessionID: input.ownerSessionID,
    cwd: input.cwd,
    taskIds: input.parsed.taskIds,
    maxAttempts: input.parsed.attempts,
    maxTasks: input.parsed.maxTasks,
    workerAgent: input.parsed.workerAgent,
    reviewerAgent: input.parsed.reviewerAgent,
    workerModel: parseModelRef(input.parsed.workerModel),
    reviewerModel: parseModelRef(input.parsed.reviewerModel),
    workerTimeoutSeconds: input.parsed.workerTimeout,
    reviewerTimeoutSeconds: input.parsed.reviewerTimeout,
    reviewerStepRetries: input.parsed.reviewerRetries,
    startupTimeoutSeconds: input.parsed.startupTimeout,
    startupRetries: input.parsed.startupRetries,
    verifyCommand: input.parsed.verifyCommand,
    verifyTimeoutSeconds: 600,
    closeOnApproval: input.parsed.closeOnApproval,
    closeIfVerified: input.parsed.closeIfVerified,
    stopOnNonDone: input.parsed.stopOnNonDone,
    reviewFirst: input.parsed.reviewFirst,
    excludeIDs: input.parsed.excludeIDs,
    customPrompt: input.parsed.customPrompt,
    runner: input.runner,
    step: input.step,
    selectionArgs: ["issue", "next", "--json"],
    showArgs: ["issue", "show", "--json"],
    closeArgs: ["issue", "close", "--json"],
    dbPath: input.dbPath,
    log: input.log,
  }
}

export type CliCommandContext = {
  cwd?: string
  dbPath?: string | null
  stdout?: (line: string) => void
  stderr?: (line: string) => void
  pollMs?: number
  isProcessAlive?: (pid: number) => boolean
}

function commandContext(context: CliCommandContext = {}) {
  return {
    cwd: context.cwd ?? process.cwd(),
    dbPath: context.dbPath === undefined ? (process.env.CHAINLINK_DB ?? null) : context.dbPath,
    stdout: context.stdout ?? ((line: string) => process.stdout.write(`${line}\n`)),
    stderr: context.stderr ?? ((line: string) => process.stderr.write(`${line}\n`)),
    pollMs: context.pollMs ?? 1000,
    isProcessAlive: context.isProcessAlive,
  }
}

function shellQuote(value: string) {
  if (/^[A-Za-z0-9_./:=@%+,-]+$/.test(value)) return value
  return `'${value.replaceAll("'", `'\\''`)}'`
}

/** The exact command that resumes a dead run without guessing a path or id. */
export function relaunchCommand(record: ChainlinkRunRecord) {
  const args: string[] = []
  for (let index = 0; index < record.argv.length; index += 1) {
    const arg = record.argv[index]!
    if (arg === "--run-id" || arg === "--review-first" || arg === "--resume") {
      index += 1
      continue
    }
    args.push(arg)
  }
  args.push("--review-first", "always")
  return ["chainlink-loop", "--resume", record.runId, ...args].map(shellQuote).join(" ")
}

/**
 * What a resumed run should pick up. An explicit task list keeps its order and
 * drops already-done tasks; queue mode keeps selecting new tasks but never
 * revisits a task this run already finished.
 */
export function planResume(
  record: ChainlinkRunRecord,
  explicitTasks: boolean,
): { taskIds: string[] | null; skipTaskIDs: string[] } {
  const remaining = record.tasks.filter((task) => task.outcome !== "done").map((task) => task.id)
  if (explicitTasks) return { taskIds: remaining, skipTaskIDs: [] }
  return { taskIds: null, skipTaskIDs: record.tasks.filter((task) => task.outcome === "done").map((task) => task.id) }
}

function statusExitCode(state: string) {
  return state === "running" || state === "done" || state === "idle" ? 0 : 1
}

export async function statusCommand(argv: string[], context: CliCommandContext = {}): Promise<number> {
  const ctx = commandContext(context)
  let runId: string | null = null
  let json = false
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]!
    if (arg === "--json") {
      json = true
    } else if (arg === "--run") {
      const value = argv[index + 1]
      if (value == null) {
        ctx.stderr("--run needs a value")
        return 2
      }
      runId = value
      index += 1
    } else {
      ctx.stderr(`unknown option ${arg}`)
      return 2
    }
  }

  const lookup = await findRunRecord({ runId, cwd: ctx.cwd, dbPath: ctx.dbPath })
  if (!lookup) {
    ctx.stderr(
      runId
        ? `no Chainlink run record found with id "${runId}"`
        : `no Chainlink run record found for ${ctx.cwd} (CHAINLINK_DB=${ctx.dbPath ?? ""})`,
    )
    return 1
  }
  const derived = deriveRunState(lookup.record, { isProcessAlive: ctx.isProcessAlive })
  const info = {
    ...runResultSummary(lookup.record, lookup.path),
    pid: lookup.record.pid,
    heartbeatAt: lookup.record.heartbeatAt,
    heartbeatAgeMs: derived.heartbeatAgeMs,
    stale: derived.stale,
    alive: derived.alive,
    selection: lookup.selection,
    candidates: lookup.candidates,
    ...(derived.state === "dead" ? { relaunch: relaunchCommand(lookup.record) } : {}),
  }
  if (json) {
    ctx.stdout(JSON.stringify(info))
  } else {
    ctx.stdout(`run: ${lookup.record.runId}`)
    ctx.stdout(`record: ${lookup.path}`)
    ctx.stdout(`state: ${derived.state}`)
    if (lookup.selection === "newest") ctx.stdout(`selection: newest of ${lookup.candidates} run(s) for this workspace`)
    ctx.stdout(`pid: ${lookup.record.pid} (${derived.alive ? "alive" : "not alive"})`)
    ctx.stdout(
      `heartbeat: ${lookup.record.heartbeatAt} (age ${Math.round(derived.heartbeatAgeMs / 1000)}s${derived.stale ? ", stale" : ""})`,
    )
    if (lookup.record.current) {
      ctx.stdout(
        `current: task ${lookup.record.current.taskID ?? "?"} attempt ${lookup.record.current.attempt ?? "?"} ${lookup.record.current.step ?? ""}`.trimEnd(),
      )
    }
    for (const line of formatRunTable(lookup.record)) ctx.stdout(line)
    if (derived.state === "dead") ctx.stdout(`relaunch: ${relaunchCommand(lookup.record)}`)
  }
  return statusExitCode(derived.state)
}

export async function waitCommand(argv: string[], context: CliCommandContext = {}): Promise<number> {
  const ctx = commandContext(context)
  let runId: string | null = null
  let timeoutSeconds = 0
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]!
    if (arg === "--run") {
      const value = argv[index + 1]
      if (value == null) {
        ctx.stderr("--run needs a value")
        return 2
      }
      runId = value
      index += 1
    } else if (arg === "--timeout") {
      const value = argv[index + 1]
      if (value == null) {
        ctx.stderr("--timeout needs a value")
        return 2
      }
      timeoutSeconds = Number(value)
      index += 1
    } else {
      ctx.stderr(`unknown option ${arg}`)
      return 2
    }
  }
  if (!Number.isFinite(timeoutSeconds) || timeoutSeconds < 0) {
    ctx.stderr("--timeout must be a non-negative number of seconds")
    return 2
  }
  const deadline = timeoutSeconds > 0 ? Date.now() + timeoutSeconds * 1000 : Number.POSITIVE_INFINITY
  while (true) {
    const lookup = await findRunRecord({ runId, cwd: ctx.cwd, dbPath: ctx.dbPath })
    if (lookup) {
      const derived = deriveRunState(lookup.record, { isProcessAlive: ctx.isProcessAlive })
      if (derived.state !== "running") {
        const result = { ...runResultSummary(lookup.record, lookup.path), wait: "finished" }
        ctx.stdout(`[chainlink] RESULT ${JSON.stringify(result)}`)
        return runExitCode(lookup.record)
      }
      if (Date.now() >= deadline) {
        const result = { ...runResultSummary(lookup.record, lookup.path), wait: "timeout" }
        ctx.stdout(`[chainlink] RESULT ${JSON.stringify(result)}`)
        return 3
      }
    } else if (Date.now() >= deadline) {
      ctx.stdout(`[chainlink] RESULT ${JSON.stringify({ wait: "timeout", error: "no run record found" })}`)
      return 3
    }
    const remaining = deadline - Date.now()
    const sleep = Number.isFinite(remaining) ? Math.max(1, Math.min(ctx.pollMs, remaining)) : ctx.pollMs
    await new Promise((resolve) => setTimeout(resolve, sleep))
  }
}

export async function startCommand(argv: string[], context: CliCommandContext = {}): Promise<number> {
  const ctx = commandContext(context)
  let detach = false
  const childArgs: string[] = []
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]!
    if (arg === "--detach") {
      detach = true
      continue
    }
    if (arg === "--run-id") {
      index += 1
      continue
    }
    childArgs.push(arg)
  }
  if (!detach) {
    ctx.stderr("start requires --detach")
    return 2
  }
  const script = process.argv[1]
  if (!script) {
    ctx.stderr("cannot locate the chainlink-loop entrypoint for --detach")
    return 2
  }
  const runId = generateRunID()
  const logPath = runLogPath(runId)
  const recordPath = runRecordPath(runId)
  await mkdir(runsDirectory(), { recursive: true, mode: 0o700 })
  const handle = await open(logPath, "a", 0o600)
  let child
  try {
    child = spawn(process.execPath, [script, ...childArgs, "--run-id", runId], {
      cwd: ctx.cwd,
      env: process.env,
      detached: true,
      stdio: ["ignore", handle.fd, handle.fd],
    })
  } finally {
    await handle.close()
  }
  child.unref()
  ctx.stdout(`run: ${runId}`)
  ctx.stdout(`log: ${logPath}`)
  ctx.stdout(`record: ${recordPath}`)
  return 0
}

export async function main(argv: string[]) {
  if (argv[0] === "status") return statusCommand(argv.slice(1))
  if (argv[0] === "wait") return waitCommand(argv.slice(1))
  if (argv[0] === "start") return startCommand(argv.slice(1))

  let parsed: ChainlinkCliArguments
  try {
    parsed = parseArgs(argv)
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n\n${USAGE}`)
    return 2
  }
  if (parsed.help) {
    process.stdout.write(USAGE)
    return 0
  }
  if (parsed.version) {
    process.stdout.write(`${PLUGIN_NAME} ${formatBuildInfo()}\n`)
    return 0
  }

  const cwd = process.cwd()
  const dbPath = process.env.CHAINLINK_DB ?? null
  const ownerSessionID = `chainlink-cli-${process.pid}`
  const log = (line: string) => process.stdout.write(`[chainlink] ${line}\n`)
  // Every run names the build it is executing, so a detached log answers
  // "which version is this?" without inspecting the process.
  log(`build: ${formatBuildInfo()}`)

  const step: ProcessRunner = async (input: ProcessStepInput) => {
    const label = input.sessionID ? `resume ${input.sessionID}` : input.title ?? "step"
    log(`$ opencode run ${input.agent ?? ""} ${label}`)
    return spawnOpencodeRun({
      ...input,
      onStart: (command, args) => log(`spawn: ${command} ${args.slice(0, -1).join(" ")} <prompt len ${input.prompt.length}>`),
    })
  }

  const options = buildProcessLoopOptions({
    ownerSessionID,
    cwd,
    parsed,
    step,
    runner: execChainlinkCommand,
    log,
    dbPath,
  })

  if (parsed.resumeRunId) {
    const lookup = await findRunRecord({ runId: parsed.resumeRunId, cwd, dbPath })
    if (!lookup) {
      process.stderr.write(`no Chainlink run record found with id "${parsed.resumeRunId}"\n`)
      return 2
    }
    const explicitTasks =
      parsed.taskIds != null || lookup.record.argv.some((arg) => arg === "--task" || arg === "--tasks")
    const plan = planResume(lookup.record, explicitTasks)
    options.taskIds = plan.taskIds
    options.skipTaskIDs = plan.skipTaskIDs
    log(`resume: ${lookup.record.runId} from ${lookup.path}`)
  }

  if (parsed.dryRun) {
    log(`cwd: ${cwd}`)
    log(`tasks: ${options.taskIds?.join(", ") ?? "(queue)"}`)
    log(`attempts: ${parsed.attempts}, close on approval: ${parsed.closeOnApproval}, review-first: ${parsed.reviewFirst}`)
    if (parsed.excludeIDs.length) log(`excluded subtrees: ${parsed.excludeIDs.join(", ")}`)
    if (parsed.customPrompt) log(`direction: ${parsed.customPrompt}`)
    if (options.taskIds?.length === 1) {
      const show = await execChainlinkCommand(["issue", "show", "--json", options.taskIds[0]!.replace(/^#/, "")], cwd, options.dbPath)
      const task = parseJSON(show.stdout) as Record<string, unknown>
      const detection = await detectExistingWork(cwd, { id: options.taskIds[0]!.replace(/^#/, ""), title: "", ...task })
      log(`existing work: ${detection.existing ? `yes — ${detection.reason} (will review first)` : `no — ${detection.reason}`}`)
    }
    return 0
  }

  const recorder = await ChainlinkRunRecorder.start({
    runId: parsed.runId ?? generateRunID(),
    cwd,
    dbPath,
    argv,
    plannedTaskIds: options.taskIds ?? undefined,
  })
  log(`run: ${recorder.runId}`)
  log(`record: ${recorder.path}`)
  options.onRunEvent = async (event) => {
    if (event.type === "task-started") {
      await recorder.taskStarted(event.taskID, event.title, event.baseSha, event.maxAttempts)
    } else if (event.type === "step") {
      await recorder.step(event.taskID, event.attempt, event.step)
    } else {
      await recorder.taskOutcome(event.taskID, event.outcome, {
        attempts: event.attempts,
        verify: event.verify,
        stopReason: event.stopReason,
      })
    }
  }

  // SIGINT/SIGTERM/SIGHUP must actually stop the loop. A handler that only logs
  // leaves the process running, so a "killed" loop keeps dispatching workers.
  const controller = new AbortController()
  let signalled = false
  let lock: LoopLock | null = null
  const onSignal = (signal: string) => {
    if (signalled) {
      log(`${signal} again, exiting now`)
      void (async () => {
        try {
          await recorder.finish("interrupted", 130)
        } catch {
          // the record is best-effort on a forced exit
        }
        try {
          await lock?.release()
        } catch {
          // the next run takes over a stale lock
        }
        process.exit(130)
      })()
      return
    }
    signalled = true
    log(`${signal} received, finishing the current step and stopping`)
    controller.abort()
  }
  process.on("SIGINT", () => onSignal("SIGINT"))
  process.on("SIGTERM", () => onSignal("SIGTERM"))
  process.on("SIGHUP", () => onSignal("SIGHUP"))
  options.signal = controller.signal

  try {
    lock = await acquireLoopLock(cwd, options.dbPath)
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`)
    await recorder.finish("failed", 2)
    return 2
  }
  log(`lock: ${lock.path}`)

  let report: Awaited<ReturnType<typeof runProcessLoop>>
  try {
    report = await runProcessLoop(options)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    const exitCode = 1
    await lock.release()
    lock = null
    await recorder.finish("failed", exitCode)
    log(`error: ${message}`)
    log(`RESULT ${JSON.stringify(recorder.snapshot().result)}`)
    return exitCode
  } finally {
    if (lock) await lock.release()
  }
  log(`status: ${report.status} after ${report.taskCount} task(s)`)
  for (const workflow of report.workflows) {
    log(`  #${workflow.taskID} ${workflow.status} (${workflow.attemptsUsed} attempt(s)) ${workflow.stopReason ?? ""}`.trimEnd())
  }
  log("summary:")
  for (const line of formatRunTable(recorder.snapshot())) log(line)
  if (report.error) log(`error: ${report.error}`)

  const status = report.status as ChainlinkRunStatus
  const allDone = Object.values(report.outcomes).every((outcome) => outcome === "done")
  const exitCode =
    report.status === "failed" || report.status === "interrupted" || report.status === "exhausted"
      ? 1
      : report.status === "idle" || allDone
        ? 0
        : 1
  await recorder.finish(status, exitCode)
  const final = runResultSummary(recorder.snapshot(), recorder.path)
  log(`RESULT ${JSON.stringify(final)}`)
  return final.exitCode
}

export { collectStepOutput, opencodeRunArgs }

const invokedDirectly = process.argv[1]?.includes("chainlink-cli")
if (invokedDirectly) {
  const code = await main(process.argv.slice(2))
  process.exit(code)
}
