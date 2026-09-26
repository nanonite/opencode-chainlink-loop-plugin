import { chmod, mkdir, readFile, rename, writeFile } from "node:fs/promises"
import { homedir } from "node:os"
import { dirname, join } from "node:path"
import { Data, Effect, Schema } from "effect"

export type LoopStatus = "active" | "paused" | "stopped" | "completed"
export type LoopMode = "interval" | "dynamic"
export type LoopRunResult = "sent" | "skipped_busy" | "skipped_plan" | "failed"

export type CreateLoopOptions = {
  prompt: string
  intervalMs?: number | null
  mode?: LoopMode
  maxRuns?: number | null
  agent?: string | null
  maxLoopsPerSession?: number | null
}

export type Loop = {
  id: string
  sessionID: string
  prompt: string
  mode: LoopMode
  intervalMs: number | null
  status: LoopStatus
  createdAt: number
  updatedAt: number
  nextRunAt: number | null
  lastRunAt: number | null
  lastResult: LoopRunResult | null
  lastError: string | null
  lastReason: string | null
  runCount: number
  maxRuns: number | null
  agent: string | null
  stopReason: string | null
}

export type ChainlinkWorkflowStatus = "running" | "completed" | "exhausted" | "failed" | "cancelled" | "interrupted"
export type ChainlinkWorkflowPhase = "worker" | "reviewer" | "closing" | "done"

export type ChainlinkWorkflow = {
  id: string
  ownerSessionID: string
  taskID: string
  taskTitle: string
  taskJSON: string
  ownerPid: number | null
  status: ChainlinkWorkflowStatus
  phase: ChainlinkWorkflowPhase
  closed: boolean
  attemptsUsed: number
  maxAttempts: number
  workerSessionID: string | null
  reviewerSessionID: string | null
  lastReviewJSON: string | null
  lastError: string | null
  createdAt: number
  updatedAt: number
  finishedAt: number | null
  stopReason: string | null
}

export type ChainlinkWorkflowSnapshot = ChainlinkWorkflow & {
  sampledAt: number
}

export type CreateChainlinkWorkflowOptions = {
  ownerSessionID: string
  taskID: string
  taskTitle: string
  taskJSON: string
  maxAttempts: number
  ownerPid?: number | null
}

type State = {
  version: 1
  loops: Record<string, Loop>
  workflows: Record<string, ChainlinkWorkflow>
}

class StateReadError extends Data.TaggedError("StateReadError")<{
  readonly cause: unknown
}> {}

class StateDecodeError extends Data.TaggedError("StateDecodeError")<{
  readonly cause: unknown
}> {}

class StateWriteError extends Data.TaggedError("StateWriteError")<{
  readonly cause: unknown
}> {}

export const DEFAULT_MIN_INTERVAL_SECONDS = 30
export const DEFAULT_MAX_LOOPS_PER_SESSION = 5
export const MAX_PROMPT_CHARS = 4000
export const MAX_CHAINLINK_TASK_JSON_CHARS = 100_000
const MAX_INTERVAL_MS = 7 * 24 * 60 * 60 * 1000

const NullableString = Schema.NullOr(Schema.String)
const NullableNumber = Schema.NullOr(Schema.Number)
const LoopSchema = Schema.Struct({
  id: Schema.String,
  sessionID: Schema.String,
  prompt: Schema.String,
  mode: Schema.optionalWith(Schema.Literal("interval", "dynamic"), { default: () => "interval" as const }),
  intervalMs: NullableNumber,
  status: Schema.Literal("active", "paused", "stopped", "completed"),
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
  nextRunAt: NullableNumber,
  lastRunAt: Schema.optionalWith(NullableNumber, { default: () => null }),
  lastResult: Schema.optionalWith(Schema.NullOr(Schema.Literal("sent", "skipped_busy", "skipped_plan", "failed")), {
    default: () => null,
  }),
  lastError: Schema.optionalWith(NullableString, { default: () => null }),
  lastReason: Schema.optionalWith(NullableString, { default: () => null }),
  runCount: Schema.optionalWith(Schema.Number, { default: () => 0 }),
  maxRuns: Schema.optionalWith(NullableNumber, { default: () => null }),
  agent: Schema.optionalWith(NullableString, { default: () => null }),
  stopReason: Schema.optionalWith(NullableString, { default: () => null }),
})
const ChainlinkWorkflowSchema = Schema.Struct({
  id: Schema.String,
  ownerSessionID: Schema.String,
  taskID: Schema.String,
  taskTitle: Schema.String,
  taskJSON: Schema.String,
  ownerPid: Schema.optionalWith(NullableNumber, { default: () => null }),
  status: Schema.Literal("running", "completed", "exhausted", "failed", "cancelled", "interrupted"),
  phase: Schema.Literal("worker", "reviewer", "closing", "done"),
  closed: Schema.optionalWith(Schema.Boolean, { default: () => false }),
  attemptsUsed: Schema.Number,
  maxAttempts: Schema.Number,
  workerSessionID: NullableString,
  reviewerSessionID: NullableString,
  lastReviewJSON: NullableString,
  lastError: NullableString,
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
  finishedAt: NullableNumber,
  stopReason: NullableString,
})
const StateSchema = Schema.Struct({
  version: Schema.Literal(1),
  loops: Schema.Record({ key: Schema.String, value: LoopSchema }),
  workflows: Schema.optionalWith(Schema.Record({ key: Schema.String, value: ChainlinkWorkflowSchema }), {
    default: () => ({}),
  }),
})

export type LoopSnapshot = Loop & {
  sampledAt: number
}

function defaultStateFile() {
  const dataHome =
    process.env.XDG_DATA_HOME ||
    (process.platform === "win32" && process.env.APPDATA ? process.env.APPDATA : join(homedir(), ".local", "share"))
  // Keep the workflow-aware state separate from legacy loop-plugin state.
  // Older plugin processes decode and rewrite only `loops`, which would erase
  // active Chainlink workflows if they shared the same file.
  return join(dataHome, "opencode-loop-plugin", "loops-v2.json")
}

export function statePath() {
  return process.env.OPENCODE_LOOP_STATE_PATH || defaultStateFile()
}

function now() {
  return Date.now()
}

function emptyState(): State {
  return { version: 1, loops: {}, workflows: {} }
}

function isMissingStateFile(error: unknown) {
  return typeof error === "object" && error !== null && (error as NodeJS.ErrnoException).code === "ENOENT"
}

function mutableState(state: Schema.Schema.Type<typeof StateSchema>): State {
  return JSON.parse(JSON.stringify(state)) as State
}

function decodeState(value: unknown) {
  return Schema.decodeUnknown(StateSchema)(value).pipe(
    Effect.map(mutableState),
    Effect.mapError((cause) => new StateDecodeError({ cause })),
  )
}

function readStateEffect() {
  return Effect.tryPromise({
    try: () => readFile(statePath(), "utf8"),
    catch: (cause) => new StateReadError({ cause }),
  }).pipe(
    Effect.flatMap((raw) =>
      Effect.try({
        try: () => JSON.parse(raw) as unknown,
        catch: (cause) => new StateDecodeError({ cause }),
      }),
    ),
    Effect.flatMap(decodeState),
    Effect.catchAll((error) =>
      error._tag === "StateReadError" && isMissingStateFile(error.cause) ? Effect.succeed(emptyState()) : Effect.fail(error),
    ),
  )
}

function writeStateEffect(state: State) {
  return Effect.tryPromise({
    try: async () => {
      const file = statePath()
      await mkdir(dirname(file), { recursive: true, mode: 0o700 })
      const tmp = `${file}.${process.pid}.${Date.now()}.tmp`
      await writeFile(tmp, JSON.stringify(state, null, 2) + "\n", { mode: 0o600 })
      await rename(tmp, file)
      await chmod(file, 0o600).catch(() => undefined)
    },
    catch: (cause) => new StateWriteError({ cause }),
  })
}

async function readState(): Promise<State> {
  return Effect.runPromise(readStateEffect())
}

let mutationQueue: Promise<void> = Promise.resolve()

function enqueueMutation<T>(operation: () => Promise<T>) {
  const current = mutationQueue.then(operation, operation)
  mutationQueue = current.then(
    () => undefined,
    () => undefined,
  )
  return current
}

const MAX_MUTATION_ATTEMPTS = 5

async function readRawState() {
  try {
    return await readFile(statePath(), "utf8")
  } catch (error) {
    if (isMissingStateFile(error)) return null
    throw error
  }
}

async function mutate<T>(fn: (state: State) => T | Promise<T>) {
  // The promise queue serializes mutations within this process; the raw-content
  // compare before writing detects concurrent writers in other OpenCode
  // processes sharing the state file and retries on top of their changes.
  return enqueueMutation(async () => {
    let lastError: unknown
    for (let attempt = 0; attempt < MAX_MUTATION_ATTEMPTS; attempt += 1) {
      const before = await readRawState()
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const state =
            before == null
              ? emptyState()
              : yield* Effect.try({
                  try: () => JSON.parse(before) as unknown,
                  catch: (cause) => new StateDecodeError({ cause }),
                }).pipe(Effect.flatMap(decodeState))
          const value = yield* Effect.tryPromise({
            try: () => Promise.resolve(fn(state)),
            catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
          })
          return { state, value }
        }),
      )
      const current = await readRawState()
      if (current !== before) {
        lastError = new Error("state file changed by a concurrent writer")
        continue
      }
      await Effect.runPromise(writeStateEffect(result.state))
      return result.value
    }
    throw lastError instanceof Error ? lastError : new Error("state mutation failed after concurrent-writer retries")
  })
}

const INTERVAL_PATTERN = /^(\d+(?:\.\d+)?)\s*(s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)$/i

const UNIT_MS: Record<string, number> = {
  s: 1000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
}

export function parseInterval(text: string, minSeconds = DEFAULT_MIN_INTERVAL_SECONDS) {
  const match = INTERVAL_PATTERN.exec(text.trim())
  if (!match) {
    throw new Error(`invalid interval "${text}"; use a number followed by s, m, h, or d (for example "30s", "10m", "1h", "1d")`)
  }
  const amount = Number(match[1])
  const unit = match[2]!.charAt(0).toLowerCase()
  const ms = Math.round(amount * UNIT_MS[unit]!)
  const minMs = Math.max(0, minSeconds) * 1000
  if (!Number.isFinite(ms) || ms <= 0) throw new Error(`invalid interval "${text}"; the amount must be greater than zero`)
  if (ms < minMs) throw new Error(`interval "${text}" is below the minimum of ${minSeconds} seconds`)
  if (ms > MAX_INTERVAL_MS) throw new Error(`interval "${text}" is above the maximum of 7 days`)
  return ms
}

export function formatInterval(ms: number | null) {
  if (ms == null) return "dynamic"
  const units: [number, string][] = [
    [86_400_000, "d"],
    [3_600_000, "h"],
    [60_000, "m"],
    [1000, "s"],
  ]
  for (const [size, suffix] of units) {
    if (ms >= size && ms % size === 0) return `${ms / size}${suffix}`
  }
  return `${Math.round(ms / 1000)}s`
}

export function generateLoopID() {
  const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
  let suffix = ""
  for (let index = 0; index < 5; index += 1) {
    suffix += alphabet[Math.floor(Math.random() * alphabet.length)]
  }
  return `loop_${suffix}`
}

export function validatePrompt(prompt: string) {
  const value = prompt.trim()
  if (!value) throw new Error("loop instruction must not be empty")
  if ([...value].length > MAX_PROMPT_CHARS) throw new Error(`loop instruction must be at most ${MAX_PROMPT_CHARS} characters`)
  return value
}

function validateTaskID(value: string) {
  const taskID = value.trim()
  if (!taskID) throw new Error("Chainlink task id must not be empty")
  if (taskID.length > 200) throw new Error("Chainlink task id is too long")
  return taskID
}

function validateTaskJSON(value: string) {
  const taskJSON = value.trim()
  if (!taskJSON) throw new Error("Chainlink task JSON must not be empty")
  if (taskJSON.length > MAX_CHAINLINK_TASK_JSON_CHARS) {
    throw new Error(`Chainlink task JSON must be at most ${MAX_CHAINLINK_TASK_JSON_CHARS} characters`)
  }
  try {
    JSON.parse(taskJSON)
  } catch (error) {
    throw new Error(`Chainlink task JSON is invalid: ${error instanceof Error ? error.message : String(error)}`, { cause: error })
  }
  return taskJSON
}

function positiveIntegerOrNull(value: unknown) {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0 ? value : null
}

function isOpen(status: LoopStatus) {
  return status === "active" || status === "paused"
}

export function snapshot(loop: Loop): LoopSnapshot {
  return { ...loop, sampledAt: now() }
}

export function workflowSnapshot(workflow: ChainlinkWorkflow): ChainlinkWorkflowSnapshot {
  return { ...workflow, sampledAt: now() }
}

function requireLoop(state: State, loopID: string) {
  const loop = state.loops[loopID]
  if (!loop) throw new Error(`no loop found with id "${loopID}"`)
  return loop
}

/**
 * Returns a workflow this process owns, taking it back first if another
 * instance left a stale `interrupted` flag on it. Without this, a workflow that
 * another process wrongly reclaimed would refuse every subsequent transition
 * and the owner would throw away work that is already done.
 */
function requireOwnedWorkflow(state: State, workflowID: string): ChainlinkWorkflow {
  const workflow: ChainlinkWorkflow | undefined = state.workflows[workflowID]
  if (!workflow) throw new Error(`no Chainlink workflow found with id "${workflowID}"`)
  if (workflow.status === "interrupted" && workflow.ownerPid === process.pid) {
    workflow.status = "running"
    workflow.phase = workflow.workerSessionID ? "reviewer" : "worker"
    workflow.finishedAt = null
    workflow.stopReason = null
    workflow.updatedAt = now()
  }
  return workflow
}
export async function createLoop(sessionID: string, options: CreateLoopOptions) {
  const prompt = validatePrompt(options.prompt)
  const mode: LoopMode = options.mode === "dynamic" ? "dynamic" : "interval"
  const intervalMs = mode === "interval" ? positiveIntegerOrNull(options.intervalMs) : null
  if (mode === "interval" && intervalMs == null) throw new Error("interval loops require a positive interval")
  const maxRuns = positiveIntegerOrNull(options.maxRuns)
  const maxLoops = positiveIntegerOrNull(options.maxLoopsPerSession) ?? DEFAULT_MAX_LOOPS_PER_SESSION
  const agent = typeof options.agent === "string" && options.agent.trim() ? options.agent.trim() : null
  return mutate((state) => {
    const open = Object.values(state.loops).filter((loop) => loop.sessionID === sessionID && isOpen(loop.status))
    if (open.length >= maxLoops) {
      throw new Error(`this session already has ${open.length} open loop(s); stop one before creating another (limit ${maxLoops})`)
    }
    let id = generateLoopID()
    while (state.loops[id]) id = generateLoopID()
    const timestamp = now()
    const loop: Loop = {
      id,
      sessionID,
      prompt,
      mode,
      intervalMs,
      status: "active",
      createdAt: timestamp,
      updatedAt: timestamp,
      nextRunAt: mode === "interval" ? timestamp + intervalMs! : null,
      lastRunAt: null,
      lastResult: null,
      lastError: null,
      lastReason: null,
      runCount: 0,
      maxRuns,
      agent,
      stopReason: null,
    }
    state.loops[id] = loop
    return snapshot(loop)
  })
}

export async function getLoop(loopID: string) {
  const state = await readState()
  const loop = state.loops[loopID]
  return loop ? snapshot(loop) : null
}

export async function createChainlinkWorkflow(options: CreateChainlinkWorkflowOptions) {
  const taskID = validateTaskID(options.taskID)
  const taskTitle = options.taskTitle.trim() || taskID
  const taskJSON = validateTaskJSON(options.taskJSON)
  const maxAttempts = positiveIntegerOrNull(options.maxAttempts)
  if (maxAttempts == null) throw new Error("Chainlink workflow requires a positive maxAttempts")
  return mutate((state) => {
    const duplicate = Object.values(state.workflows).find(
      (workflow) => workflow.taskID === taskID && workflow.ownerSessionID === options.ownerSessionID && workflow.status === "running",
    )
    if (duplicate) throw new Error(`Chainlink task ${taskID} already has an active workflow (${duplicate.id})`)
    let id = `chainlink_${generateLoopID().slice("loop_".length)}`
    while (state.workflows[id]) id = `chainlink_${generateLoopID().slice("loop_".length)}`
    const timestamp = now()
    const workflow: ChainlinkWorkflow = {
      id,
      ownerSessionID: options.ownerSessionID,
      taskID,
      taskTitle,
      taskJSON,
      ownerPid: options.ownerPid ?? process.pid,
      status: "running",
      phase: "worker",
      closed: false,
      attemptsUsed: 0,
      maxAttempts,
      workerSessionID: null,
      reviewerSessionID: null,
      lastReviewJSON: null,
      lastError: null,
      createdAt: timestamp,
      updatedAt: timestamp,
      finishedAt: null,
      stopReason: null,
    }
    state.workflows[id] = workflow
    return workflowSnapshot(workflow)
  })
}

export async function getChainlinkWorkflow(workflowID: string) {
  const state = await readState()
  const workflow = state.workflows[workflowID]
  return workflow ? workflowSnapshot(workflow) : null
}

export async function listChainlinkWorkflows(ownerSessionID?: string) {
  const state = await readState()
  return Object.values(state.workflows)
    .filter((workflow) => ownerSessionID == null || workflow.ownerSessionID === ownerSessionID)
    .sort((a, b) => a.createdAt - b.createdAt)
    .map(workflowSnapshot)
}

export async function listActiveChainlinkWorkflows(ownerSessionID?: string) {
  return (await listChainlinkWorkflows(ownerSessionID)).filter((workflow) => workflow.status === "running")
}

export async function recordChainlinkWorkerStarted(workflowID: string, workerSessionID: string) {
  return mutate((state) => {
    const workflow = requireOwnedWorkflow(state, workflowID)
    if (workflow.status !== "running") throw new Error(`Chainlink workflow "${workflowID}" is ${workflow.status}`)
    if (workflow.phase !== "worker") throw new Error(`Chainlink workflow "${workflowID}" is in phase ${workflow.phase}`)
    workflow.workerSessionID = workerSessionID
    workflow.attemptsUsed = Math.max(1, workflow.attemptsUsed + (workflow.workerSessionID ? 0 : 1))
    workflow.updatedAt = now()
    return workflowSnapshot(workflow)
  })
}

export async function recordChainlinkWorkerStall(workflowID: string) {
  return mutate((state) => {
    const workflow = requireOwnedWorkflow(state, workflowID)
    if (workflow.status !== "running") return workflowSnapshot(workflow)
    if (workflow.phase !== "worker") throw new Error(`Chainlink workflow "${workflowID}" is in phase ${workflow.phase}`)
    workflow.attemptsUsed += 1
    workflow.updatedAt = now()
    return workflowSnapshot(workflow)
  })
}

export async function recordChainlinkReviewerStarted(workflowID: string, reviewerSessionID: string) {
  return mutate((state) => {
    const workflow = requireOwnedWorkflow(state, workflowID)
    if (workflow.status !== "running") throw new Error(`Chainlink workflow "${workflowID}" is ${workflow.status}`)
    workflow.phase = "reviewer"
    workflow.reviewerSessionID = reviewerSessionID
    workflow.updatedAt = now()
    return workflowSnapshot(workflow)
  })
}

export async function recordChainlinkReview(
  workflowID: string,
  reviewerSessionID: string,
  reviewJSON: string,
  attemptsUsed: number,
) {
  return mutate((state) => {
    const workflow = requireOwnedWorkflow(state, workflowID)
    if (workflow.status !== "running") throw new Error(`Chainlink workflow "${workflowID}" is ${workflow.status}`)
    if (workflow.phase !== "reviewer") throw new Error(`Chainlink workflow "${workflowID}" is in phase ${workflow.phase}`)
    workflow.reviewerSessionID = reviewerSessionID
    workflow.lastReviewJSON = reviewJSON.slice(0, MAX_CHAINLINK_TASK_JSON_CHARS)
    workflow.attemptsUsed = Math.max(workflow.attemptsUsed, attemptsUsed)
    workflow.phase = "worker"
    workflow.updatedAt = now()
    return workflowSnapshot(workflow)
  })
}

export async function recordChainlinkClosing(workflowID: string) {
  return mutate((state) => {
    const workflow = requireOwnedWorkflow(state, workflowID)
    if (workflow.status !== "running") throw new Error(`Chainlink workflow "${workflowID}" is ${workflow.status}`)
    workflow.phase = "closing"
    workflow.updatedAt = now()
    return workflowSnapshot(workflow)
  })
}

export async function finishChainlinkWorkflow(
  workflowID: string,
  status: Exclude<ChainlinkWorkflowStatus, "running">,
  reason?: string | null,
  closed = false,
) {
  return mutate((state) => {
    const workflow = requireOwnedWorkflow(state, workflowID)
    if (workflow.status !== "running") return workflowSnapshot(workflow)
    const timestamp = now()
    workflow.status = status
    workflow.phase = "done"
    workflow.closed = closed
    workflow.finishedAt = timestamp
    workflow.updatedAt = timestamp
    workflow.stopReason = typeof reason === "string" && reason.trim() ? reason.trim().slice(0, 400) : null
    return workflowSnapshot(workflow)
  })
}

export async function failChainlinkWorkflow(workflowID: string, error: string) {
  return mutate((state) => {
    const workflow = requireOwnedWorkflow(state, workflowID)
    if (workflow.status !== "running") return workflowSnapshot(workflow)
    const timestamp = now()
    workflow.status = "failed"
    workflow.phase = "done"
    workflow.lastError = error.slice(0, 400)
    workflow.finishedAt = timestamp
    workflow.updatedAt = timestamp
    workflow.stopReason = error.slice(0, 400)
    return workflowSnapshot(workflow)
  })
}

export async function stopChainlinkWorkflowsForSession(sessionID: string, reason: string) {
  return mutate((state) => {
    const stopped: ChainlinkWorkflowSnapshot[] = []
    for (const workflow of Object.values(state.workflows)) {
      if (workflow.status !== "running") continue
      if (
        workflow.ownerSessionID !== sessionID &&
        workflow.workerSessionID !== sessionID &&
        workflow.reviewerSessionID !== sessionID
      ) {
        continue
      }
      const timestamp = now()
      workflow.status = sessionID === workflow.ownerSessionID ? "cancelled" : "interrupted"
      workflow.phase = "done"
      workflow.finishedAt = timestamp
      workflow.updatedAt = timestamp
      workflow.stopReason = reason.slice(0, 400)
      stopped.push(workflowSnapshot(workflow))
    }
    return stopped
  })
}

function defaultIsProcessAlive(pid: number) {
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}

/**
 * Marks running workflows as interrupted when their owning process is gone.
 *
 * Ownership is decided purely by liveness, never by pid equality: OpenCode can
 * run plugin `setup` more than once per process, so an "own pid" rule would let a
 * later setup interrupt a workflow that is still running in this very process.
 * Workflows without an `ownerPid` predate ownership tracking and cannot be
 * verified, so they are treated as abandoned.
 */
export async function interruptActiveChainlinkWorkflows(
  reason: string,
  options: { isProcessAlive?: (pid: number) => boolean } = {},
) {
  const isProcessAlive = options.isProcessAlive ?? defaultIsProcessAlive
  return mutate((state) => {
    const interrupted: ChainlinkWorkflowSnapshot[] = []
    for (const workflow of Object.values(state.workflows)) {
      if (workflow.status !== "running") continue
      if (workflow.ownerPid != null && isProcessAlive(workflow.ownerPid)) continue
      const timestamp = now()
      workflow.status = "interrupted"
      workflow.phase = "done"
      workflow.finishedAt = timestamp
      workflow.updatedAt = timestamp
      workflow.stopReason = reason.slice(0, 400)
      interrupted.push(workflowSnapshot(workflow))
    }
    return interrupted
  })
}

/**
 * Takes a workflow back from a stale `interrupted` flag when this process still
 * owns it. Another instance can mark a live workflow interrupted; the worker
 * that is mid-flight should keep going to review instead of discarding its work.
 */
export async function reclaimChainlinkWorkflow(workflowID: string, ownerPid = process.pid) {
  return mutate((state) => {
    const workflow = state.workflows[workflowID]
    if (!workflow) return null
    if (workflow.status !== "interrupted" || workflow.ownerPid !== ownerPid) return workflowSnapshot(workflow)
    workflow.status = "running"
    workflow.phase = workflow.workerSessionID ? "reviewer" : "worker"
    workflow.finishedAt = null
    workflow.stopReason = null
    workflow.updatedAt = now()
    return workflowSnapshot(workflow)
  })
}

/**
 * Marks only this process's running workflows as interrupted. Called when the
 * plugin is torn down, so a reload or shutdown records the outcome instead of
 * leaving a workflow that nothing will ever finish.
 */export async function interruptChainlinkWorkflowsOwnedBy(ownerPid: number, reason: string) {
  return mutate((state) => {
    const interrupted: ChainlinkWorkflowSnapshot[] = []
    for (const workflow of Object.values(state.workflows)) {
      if (workflow.status !== "running") continue
      if (workflow.ownerPid !== ownerPid) continue
      const timestamp = now()
      workflow.status = "interrupted"
      workflow.phase = "done"
      workflow.finishedAt = timestamp
      workflow.updatedAt = timestamp
      workflow.stopReason = reason.slice(0, 400)
      interrupted.push(workflowSnapshot(workflow))
    }
    return interrupted
  })
}

export async function claimDueRun(loopID: string, leaseMs: number) {
  const lease = positiveIntegerOrNull(Math.round(leaseMs))
  if (lease == null) throw new Error("run claim lease must be a positive number of milliseconds")
  return mutate((state) => {
    const loop = requireLoop(state, loopID)
    const timestamp = now()
    if (loop.status !== "active" || loop.nextRunAt == null || loop.nextRunAt > timestamp) return null
    loop.nextRunAt = timestamp + lease
    loop.updatedAt = timestamp
    return snapshot(loop)
  })
}

export async function listLoops(sessionID?: string) {
  const state = await readState()
  return Object.values(state.loops)
    .filter((loop) => sessionID == null || loop.sessionID === sessionID)
    .sort((a, b) => a.createdAt - b.createdAt)
    .map(snapshot)
}

export async function openLoops(sessionID?: string) {
  const loops = await listLoops(sessionID)
  return loops.filter((loop) => isOpen(loop.status))
}

export async function activeLoops(sessionID?: string) {
  const loops = await listLoops(sessionID)
  return loops.filter((loop) => loop.status === "active")
}

export async function pauseLoop(loopID: string) {
  return mutate((state) => {
    const loop = requireLoop(state, loopID)
    if (loop.status !== "active") throw new Error(`loop "${loopID}" is ${loop.status}; only active loops can be paused`)
    loop.status = "paused"
    loop.nextRunAt = null
    loop.stopReason = "paused"
    loop.updatedAt = now()
    return snapshot(loop)
  })
}

export async function resumeLoop(loopID: string) {
  return mutate((state) => {
    const loop = requireLoop(state, loopID)
    if (loop.status !== "paused") throw new Error(`loop "${loopID}" is ${loop.status}; only paused loops can be resumed`)
    const timestamp = now()
    loop.status = "active"
    loop.stopReason = null
    loop.nextRunAt = loop.mode === "interval" ? timestamp + loop.intervalMs! : timestamp
    loop.updatedAt = timestamp
    return snapshot(loop)
  })
}

export async function stopLoop(loopID: string, reason?: string | null) {
  return mutate((state) => {
    const loop = requireLoop(state, loopID)
    if (!isOpen(loop.status)) throw new Error(`loop "${loopID}" is already ${loop.status}`)
    loop.status = "stopped"
    loop.nextRunAt = null
    loop.stopReason = typeof reason === "string" && reason.trim() ? reason.trim().slice(0, 400) : "stopped"
    loop.updatedAt = now()
    return snapshot(loop)
  })
}

export async function stopLoopsForSession(sessionID: string, reason: string) {
  return mutate((state) => {
    const stopped: LoopSnapshot[] = []
    for (const loop of Object.values(state.loops)) {
      if (loop.sessionID !== sessionID || !isOpen(loop.status)) continue
      loop.status = "stopped"
      loop.nextRunAt = null
      loop.stopReason = reason
      loop.updatedAt = now()
      stopped.push(snapshot(loop))
    }
    return stopped
  })
}

export async function clearClosedLoops(sessionID: string) {
  return mutate((state) => {
    let cleared = 0
    for (const [id, loop] of Object.entries(state.loops)) {
      if (loop.sessionID !== sessionID || isOpen(loop.status)) continue
      delete state.loops[id]
      cleared += 1
    }
    return cleared
  })
}

export async function scheduleNextRun(loopID: string, delayMs: number, reason?: string | null) {
  const delay = positiveIntegerOrNull(Math.round(delayMs))
  if (delay == null) throw new Error("delay must be a positive number of milliseconds")
  return mutate((state) => {
    const loop = requireLoop(state, loopID)
    if (loop.status !== "active") throw new Error(`loop "${loopID}" is ${loop.status}; only active loops can be scheduled`)
    const timestamp = now()
    loop.nextRunAt = timestamp + delay
    loop.lastReason = typeof reason === "string" && reason.trim() ? reason.trim().slice(0, 400) : loop.lastReason
    loop.updatedAt = timestamp
    return snapshot(loop)
  })
}

export async function recordRunSent(loopID: string) {
  return mutate((state) => {
    const loop = requireLoop(state, loopID)
    if (loop.status !== "active") return snapshot(loop)
    const timestamp = now()
    loop.runCount += 1
    loop.lastRunAt = timestamp
    loop.lastResult = "sent"
    loop.lastError = null
    loop.updatedAt = timestamp
    if (loop.maxRuns != null && loop.runCount >= loop.maxRuns) {
      loop.status = "completed"
      loop.nextRunAt = null
      loop.stopReason = `max runs reached (${loop.maxRuns})`
    } else if (loop.mode === "interval") {
      loop.nextRunAt = timestamp + loop.intervalMs!
    } else {
      loop.nextRunAt = null
    }
    return snapshot(loop)
  })
}

export async function recordRunDeferred(loopID: string, result: "skipped_busy" | "skipped_plan", retryDelayMs: number) {
  return mutate((state) => {
    const loop = requireLoop(state, loopID)
    if (loop.status !== "active") return snapshot(loop)
    const timestamp = now()
    loop.lastResult = result
    loop.nextRunAt = timestamp + Math.max(0, Math.round(retryDelayMs))
    loop.updatedAt = timestamp
    return snapshot(loop)
  })
}

export async function recordRunFailed(loopID: string, error: string, retryDelayMs: number) {
  return mutate((state) => {
    const loop = requireLoop(state, loopID)
    const timestamp = now()
    loop.lastResult = "failed"
    loop.lastError = error.slice(0, 400)
    loop.updatedAt = timestamp
    if (loop.status === "active") loop.nextRunAt = timestamp + Math.max(0, Math.round(retryDelayMs))
    return snapshot(loop)
  })
}

export function formatLoop(loop: LoopSnapshot) {
  const parts = [
    `${loop.id} [${loop.status}]`,
    loop.mode === "interval" ? `every ${formatInterval(loop.intervalMs)}` : "dynamic pacing",
    `runs ${loop.runCount}${loop.maxRuns == null ? "" : `/${loop.maxRuns}`}`,
  ]
  if (loop.nextRunAt != null) parts.push(`next ${new Date(loop.nextRunAt).toISOString()}`)
  else if (loop.status === "active" && loop.mode === "dynamic") parts.push("next run not scheduled yet")
  if (loop.lastResult) parts.push(`last ${loop.lastResult}`)
  if (loop.stopReason && loop.status !== "active") parts.push(`reason: ${loop.stopReason}`)
  const summary = loop.prompt.replace(/\s+/g, " ").slice(0, 120)
  return `${parts.join(", ")} - ${summary}`
}

export function formatLoops(loops: LoopSnapshot[]) {
  if (loops.length === 0) return "No loops exist for this session."
  return loops.map(formatLoop).join("\n")
}
