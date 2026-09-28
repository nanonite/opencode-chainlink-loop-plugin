#!/usr/bin/env bun
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
import { execChainlinkCommand, parseModelRef } from "./chainlink"
import { acquireLoopLock, type LoopLock } from "./chainlink-lock"
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
 */
type Parsed = {
  taskIds: string[] | null
  attempts: number
  maxTasks: number | null
  closeOnApproval: boolean
  reviewFirst: "auto" | "always" | "never"
  excludeIDs: string[]
  workerModel: string | null
  reviewerModel: string | null
  workerAgent: string
  reviewerAgent: string
  workerTimeout: number
  reviewerTimeout: number
  dryRun: boolean
  version: boolean
  help: boolean
}

const USAGE = `chainlink-loop — deterministic Chainlink outer/inner loop (no LLM in the control path)

Usage:
  chainlink-loop [options]

Options:
  --task <ids>        Comma-separated issue ids, e.g. 67 or 67,71. "#67" is accepted.
  --attempts <n>      Per-task worker/reviewer attempt limit (default 20).
  --max-tasks <n>     Stop after this many tasks.
  --no-close          Leave approved issues open (default is to close them).
  --review-first <m>  auto | always | never (default auto: review work already in
                      the tree before dispatching a fresh worker).
  --exclude <ids>     Comma-separated issue ids whose subtrees the queue must not
                      enter. Needed when a project deliberately holds a subtree
                      back: \`issue next\` honours that, but the fallback used to
                      step past an exhausted task does not.
  --worker-model <m>  provider/model for the worker.
  --reviewer-model <m> provider/model for the reviewer.
  --worker-agent <a>  Agent for the worker (default build).
  --reviewer-agent <a> Agent for the reviewer (default plan; plan cannot edit files).
  --worker-timeout <s>   Per-step worker timeout in seconds (default 3600).
  --reviewer-timeout <s> Per-step reviewer timeout in seconds (default 1800).
  --dry-run           Print the plan and exit without running anything.
  -v, --version       Show the plugin build version and exit.
  -h, --help          Show this help.

Environment:
  CHAINLINK_DB        Chainlink database path (passed through to the CLI).
`

function parseArgs(argv: string[]): Parsed {
  const parsed: Parsed = {
    taskIds: null,
    attempts: 20,
    maxTasks: null,
    closeOnApproval: true,
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
    dryRun: false,
    version: false,
    help: false,
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
      case "--review-first":
        parsed.reviewFirst = next() as Parsed["reviewFirst"]
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
      case "--dry-run":
        parsed.dryRun = true
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
  if (!["auto", "always", "never"].includes(parsed.reviewFirst)) {
    throw new Error("--review-first must be auto, always or never")
  }
  return parsed
}

export async function main(argv: string[]) {
  let parsed: Parsed
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
  const ownerSessionID = `chainlink-cli-${process.pid}`
  const log = (line: string) => process.stdout.write(`[chainlink] ${line}\n`)

  const step: ProcessRunner = async (input: ProcessStepInput) => {
    const label = input.sessionID ? `resume ${input.sessionID}` : input.title ?? "step"
    log(`$ opencode run ${input.agent ?? ""} ${label}`)
    return spawnOpencodeRun({
      ...input,
      onStart: (command, args) => log(`spawn: ${command} ${args.slice(0, -1).join(" ")} <prompt len ${input.prompt.length}>`),
    })
  }

  const options: ProcessLoopOptions = {
    ownerSessionID,
    cwd,
    taskIds: parsed.taskIds,
    maxAttempts: parsed.attempts,
    maxTasks: parsed.maxTasks,
    workerAgent: parsed.workerAgent,
    reviewerAgent: parsed.reviewerAgent,
    workerModel: parseModelRef(parsed.workerModel),
    reviewerModel: parseModelRef(parsed.reviewerModel),
    workerTimeoutSeconds: parsed.workerTimeout,
    reviewerTimeoutSeconds: parsed.reviewerTimeout,
    closeOnApproval: parsed.closeOnApproval,
    reviewFirst: parsed.reviewFirst,
    excludeIDs: parsed.excludeIDs,
    runner: execChainlinkCommand,
    step,
    selectionArgs: ["issue", "next", "--json"],
    showArgs: ["issue", "show", "--json"],
    closeArgs: ["issue", "close", "--json"],
    dbPath: process.env.CHAINLINK_DB ?? null,
  }

  if (parsed.dryRun) {
    log(`build: ${formatBuildInfo()}`)
    log(`cwd: ${cwd}`)
    log(`tasks: ${parsed.taskIds?.join(", ") ?? "(queue)"}`)
    log(`attempts: ${parsed.attempts}, close on approval: ${parsed.closeOnApproval}, review-first: ${parsed.reviewFirst}`)
    if (parsed.excludeIDs.length) log(`excluded subtrees: ${parsed.excludeIDs.join(", ")}`)
    if (parsed.taskIds?.length === 1) {
      const show = await execChainlinkCommand(["issue", "show", "--json", parsed.taskIds[0]!.replace(/^#/, "")], cwd, options.dbPath)
      const task = parseJSON(show.stdout) as Record<string, unknown>
      const detection = await detectExistingWork(cwd, { id: parsed.taskIds[0]!.replace(/^#/, ""), title: "", ...task })
      log(`existing work: ${detection.existing ? `yes — ${detection.reason} (will review first)` : `no — ${detection.reason}`}`)
    }
    return 0
  }

  // SIGINT/SIGTERM must actually stop the loop. A handler that only logs leaves
  // the process running, so a "killed" loop keeps dispatching workers.
  const controller = new AbortController()
  let signalled = false
  const onSignal = (signal: string) => {
    if (signalled) {
      log(`${signal} again, exiting now`)
      process.exit(130)
    }
    signalled = true
    log(`${signal} received, finishing the current step and stopping`)
    controller.abort()
  }
  process.on("SIGINT", () => onSignal("SIGINT"))
  process.on("SIGTERM", () => onSignal("SIGTERM"))
  options.signal = controller.signal

  let lock: LoopLock
  try {
    lock = await acquireLoopLock(cwd, options.dbPath)
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`)
    return 2
  }
  log(`lock: ${lock.path}`)

  let report: Awaited<ReturnType<typeof runProcessLoop>>
  try {
    report = await runProcessLoop(options)
  } finally {
    await lock.release()
  }
  log(`status: ${report.status} after ${report.taskCount} task(s)`)
  for (const workflow of report.workflows) {
    log(`  #${workflow.taskID} ${workflow.status} (${workflow.attemptsUsed} attempt(s)) ${workflow.stopReason ?? ""}`.trimEnd())
  }
  if (report.error) log(`error: ${report.error}`)
  return report.status === "failed" || report.status === "interrupted" ? 1 : 0
}

export { collectStepOutput, opencodeRunArgs }

const invokedDirectly = process.argv[1]?.includes("chainlink-cli")
if (invokedDirectly) {
  const code = await main(process.argv.slice(2))
  process.exit(code)
}
