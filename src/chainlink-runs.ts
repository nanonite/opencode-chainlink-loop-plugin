import { mkdir, readdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises"
import { dirname, join } from "node:path"
import { statePath } from "./state"
import { workspaceKey } from "./chainlink-lock"
import { BUILD_INFO, type PluginBuildInfo } from "./version"

/**
 * Durable run records for the chainlink-loop CLI.
 *
 * The record is the CLI's answer to "is this run still alive, and did it
 * actually finish the work?" — not a transcript and not a verdict from a model.
 * It lives next to the loop state so a caller that already knows how to run the
 * CLI never has to guess a path.
 */

export const RUN_HEARTBEAT_MS = 15_000
export const RUN_HEARTBEAT_STALE_MS = RUN_HEARTBEAT_MS * 3
export const MAX_RUN_RECORDS = 50

export type ChainlinkTaskOutcome =
  | "done"
  | "approved-no-changes"
  | "approved-but-escalated"
  | "verify-failed"
  | "exhausted"
  | "crashed"
  | "interrupted"

export type ChainlinkRunStatus = "running" | "completed" | "exhausted" | "failed" | "idle" | "capped" | "interrupted"

export type ChainlinkVerifyResult = {
  command: string
  exitCode: number | null
  passed: boolean
  timedOut: boolean
  durationMs: number
  outputTail: string
}

export type ChainlinkRunTask = {
  id: string
  title: string
  status: "pending" | "running" | "finished"
  outcome: ChainlinkTaskOutcome | null
  attempts: number
  maxAttempts: number
  baseSha: string | null
  verify: ChainlinkVerifyResult | null
  startedAt: string | null
  endedAt: string | null
  stopReason: string | null
}

export type ChainlinkRunCurrent = {
  taskID: string | null
  attempt: number | null
  step: "selecting" | "worker" | "reviewer" | "verify" | "closing" | null
  since: string
}

export type ChainlinkRunRecord = {
  version: 1
  runId: string
  pid: number
  cwd: string
  chainlinkDb: string | null
  argv: string[]
  build: PluginBuildInfo
  startedAt: string
  heartbeatAt: string
  updatedAt: string
  current: ChainlinkRunCurrent | null
  tasks: ChainlinkRunTask[]
  status: ChainlinkRunStatus
  exitCode: number | null
  endedAt: string | null
  result: Record<string, unknown> | null
}

export type RunRecordFile = {
  path: string
  record: ChainlinkRunRecord
  mtimeMs: number
}

export type RunRecordLookup = {
  path: string
  record: ChainlinkRunRecord
  /** "explicit" for --run, otherwise the newest record for the workspace key. */
  selection: "explicit" | "newest"
  candidates: number
}

export function runsDirectory() {
  return join(dirname(statePath()), "chainlink-runs")
}

export function runRecordPath(runId: string) {
  return join(runsDirectory(), `${runId}.json`)
}

export function runLogPath(runId: string) {
  return join(runsDirectory(), `${runId}.log`)
}

export function generateRunID(now = Date.now()) {
  const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
  let suffix = ""
  for (let index = 0; index < 8; index += 1) {
    suffix += alphabet[Math.floor(Math.random() * alphabet.length)]
  }
  return `run_${now.toString(36)}_${suffix}`
}

async function atomicWrite(file: string, data: string) {
  await mkdir(dirname(file), { recursive: true, mode: 0o700 })
  const tmp = `${file}.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}.tmp`
  await writeFile(tmp, data, { mode: 0o600 })
  await rename(tmp, file)
}

function iso(timestamp: number) {
  return new Date(timestamp).toISOString()
}

function taskFor(record: ChainlinkRunRecord, taskID: string): ChainlinkRunTask {
  let task = record.tasks.find((candidate) => candidate.id === taskID)
  if (!task) {
    task = {
      id: taskID,
      title: taskID,
      status: "pending",
      outcome: null,
      attempts: 0,
      maxAttempts: 0,
      baseSha: null,
      verify: null,
      startedAt: null,
      endedAt: null,
      stopReason: null,
    }
    record.tasks.push(task)
  }
  return task
}

export type StartRunRecorderOptions = {
  runId?: string
  pid?: number
  cwd: string
  dbPath: string | null
  argv: string[]
  build?: PluginBuildInfo
  plannedTaskIds?: readonly string[]
  heartbeatIntervalMs?: number
  now?: () => number
}

/**
 * Owns one record file and its heartbeat timer. All transitions are serialized
 * through one write queue, so a heartbeat cannot overwrite a newer transition
 * with an older snapshot.
 */
export class ChainlinkRunRecorder {
  readonly runId: string
  readonly path: string
  readonly heartbeatIntervalMs: number
  private readonly clock: () => number
  private readonly recordValue: ChainlinkRunRecord
  private timer: ReturnType<typeof setInterval> | undefined
  private writeQueue: Promise<void> = Promise.resolve()
  private finished = false

  private constructor(options: StartRunRecorderOptions, runId: string, path: string, record: ChainlinkRunRecord) {
    this.runId = runId
    this.path = path
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? RUN_HEARTBEAT_MS
    this.clock = options.now ?? Date.now
    this.recordValue = record
  }

  static async start(options: StartRunRecorderOptions): Promise<ChainlinkRunRecorder> {
    const clock = options.now ?? Date.now
    const timestamp = clock()
    const runId = options.runId ?? generateRunID(timestamp)
    const path = runRecordPath(runId)
    const record: ChainlinkRunRecord = {
      version: 1,
      runId,
      pid: options.pid ?? process.pid,
      cwd: options.cwd,
      chainlinkDb: options.dbPath,
      argv: [...options.argv],
      build: options.build ?? BUILD_INFO,
      startedAt: iso(timestamp),
      heartbeatAt: iso(timestamp),
      updatedAt: iso(timestamp),
      current: { taskID: null, attempt: null, step: "selecting", since: iso(timestamp) },
      tasks: (options.plannedTaskIds ?? []).map((id) => ({
        id,
        title: id,
        status: "pending",
        outcome: null,
        attempts: 0,
        maxAttempts: 0,
        baseSha: null,
        verify: null,
        startedAt: null,
        endedAt: null,
        stopReason: null,
      })),
      status: "running",
      exitCode: null,
      endedAt: null,
      result: null,
    }
    const recorder = new ChainlinkRunRecorder(options, runId, path, record)
    await recorder.persist()
    recorder.startHeartbeat()
    await pruneRunRecords()
    return recorder
  }

  snapshot(): ChainlinkRunRecord {
    return JSON.parse(JSON.stringify(this.recordValue)) as ChainlinkRunRecord
  }

  private startHeartbeat() {
    if (this.heartbeatIntervalMs <= 0) return
    this.timer = setInterval(() => {
      void this.heartbeat().catch(() => undefined)
    }, this.heartbeatIntervalMs)
    // A heartbeat must never keep a finished CLI process alive.
    this.timer.unref?.()
  }

  private persist() {
    const data = `${JSON.stringify(this.recordValue, null, 2)}\n`
    const file = this.path
    const write = () => atomicWrite(file, data)
    this.writeQueue = this.writeQueue.then(write, write)
    return this.writeQueue
  }

  async update(mutator: (record: ChainlinkRunRecord) => void): Promise<ChainlinkRunRecord> {
    if (this.finished) return this.snapshot()
    mutator(this.recordValue)
    this.recordValue.updatedAt = iso(this.clock())
    await this.persist()
    return this.snapshot()
  }

  async heartbeat(): Promise<ChainlinkRunRecord> {
    if (this.finished) return this.snapshot()
    const timestamp = this.clock()
    this.recordValue.heartbeatAt = iso(timestamp)
    this.recordValue.updatedAt = iso(timestamp)
    await this.persist()
    return this.snapshot()
  }

  async setCurrent(current: Omit<ChainlinkRunCurrent, "since"> | null) {
    return this.update((record) => {
      record.current = current ? { ...current, since: iso(this.clock()) } : null
    })
  }

  async taskStarted(taskID: string, title: string, baseSha: string | null, maxAttempts: number) {
    return this.update((record) => {
      const task = taskFor(record, taskID)
      task.title = title
      task.status = "running"
      task.baseSha = baseSha
      task.maxAttempts = maxAttempts
      task.startedAt = iso(this.clock())
      task.endedAt = null
      task.stopReason = null
      record.current = { taskID, attempt: 0, step: "worker", since: iso(this.clock()) }
    })
  }

  async step(taskID: string, attempt: number, step: NonNullable<ChainlinkRunCurrent["step"]>) {
    return this.update((record) => {
      const task = taskFor(record, taskID)
      task.status = "running"
      task.attempts = Math.max(task.attempts, attempt)
      record.current = { taskID, attempt, step, since: iso(this.clock()) }
    })
  }

  async taskOutcome(
    taskID: string,
    outcome: ChainlinkTaskOutcome,
    details: {
      attempts: number
      verify?: ChainlinkVerifyResult | null
      stopReason?: string | null
    },
  ) {
    return this.update((record) => {
      const task = taskFor(record, taskID)
      task.status = "finished"
      task.outcome = outcome
      task.attempts = Math.max(task.attempts, details.attempts)
      task.verify = details.verify ?? task.verify
      task.endedAt = iso(this.clock())
      task.stopReason = details.stopReason ?? null
      if (record.current?.taskID === taskID) record.current = null
    })
  }

  async finish(status: ChainlinkRunStatus, exitCode: number, result: Record<string, unknown> | null = null) {
    if (this.finished) return this.snapshot()
    this.finished = true
    if (this.timer) clearInterval(this.timer)
    this.timer = undefined
    const timestamp = this.clock()
    this.recordValue.status = status
    this.recordValue.exitCode = exitCode
    this.recordValue.endedAt = iso(timestamp)
    this.recordValue.heartbeatAt = iso(timestamp)
    this.recordValue.updatedAt = iso(timestamp)
    this.recordValue.current = null
    this.recordValue.result = result ?? runResultSummary(this.recordValue, this.path)
    await this.persist()
    await pruneRunRecords()
    return this.snapshot()
  }

  dispose() {
    this.finished = true
    if (this.timer) clearInterval(this.timer)
    this.timer = undefined
  }
}

export async function listRunRecords(): Promise<RunRecordFile[]> {
  let names: string[]
  try {
    names = await readdir(runsDirectory())
  } catch {
    return []
  }
  const records: RunRecordFile[] = []
  for (const name of names) {
    if (!name.endsWith(".json")) continue
    const path = join(runsDirectory(), name)
    try {
      const [raw, info] = await Promise.all([readFile(path, "utf8"), stat(path)])
      const record = JSON.parse(raw) as ChainlinkRunRecord
      if (!record || record.version !== 1 || typeof record.runId !== "string") continue
      records.push({ path, record, mtimeMs: info.mtimeMs })
    } catch {
      // A half-written or foreign file is not a run record.
    }
  }
  return records.sort((a, b) => {
    const started = Date.parse(b.record.startedAt) - Date.parse(a.record.startedAt)
    return started !== 0 ? started : b.mtimeMs - a.mtimeMs
  })
}

export async function findRunRecord(options: {
  runId?: string | null
  cwd: string
  dbPath: string | null
}): Promise<RunRecordLookup | null> {
  const records = await listRunRecords()
  if (options.runId) {
    const match = records.find(
      (candidate) =>
        candidate.record.runId === options.runId || candidate.path.endsWith(`/${options.runId}.json`),
    )
    return match ? { ...match, selection: "explicit", candidates: records.length } : null
  }
  const key = workspaceKey(options.cwd, options.dbPath)
  const matching = records.filter(
    (candidate) => workspaceKey(candidate.record.cwd, candidate.record.chainlinkDb) === key,
  )
  const newest = matching[0]
  return newest ? { ...newest, selection: "newest", candidates: matching.length } : null
}

export async function pruneRunRecords(keep = MAX_RUN_RECORDS) {
  if (keep < 1) return
  let names: string[]
  try {
    names = await readdir(runsDirectory())
  } catch {
    return
  }
  const files = await Promise.all(
    names.map(async (name) => {
      const path = join(runsDirectory(), name)
      try {
        const info = await stat(path)
        return { name, path, mtimeMs: info.mtimeMs }
      } catch {
        return null
      }
    }),
  )
  const present = files.filter((file): file is { name: string; path: string; mtimeMs: number } => file !== null)
  const recordNames = new Set(present.filter((file) => file.name.endsWith(".json")).map((file) => file.name))
  const ordered = present
    .filter((file) => file.name.endsWith(".json"))
    .sort((a, b) => b.mtimeMs - a.mtimeMs)
  const remove = ordered.slice(keep)
  for (const file of remove) {
    await rm(file.path, { force: true })
    const logName = `${file.name.slice(0, -".json".length)}.log`
    if (!recordNames.has(logName)) await rm(join(runsDirectory(), logName), { force: true })
  }
}

export type RunState =
  | "running"
  | "done"
  | "not-done"
  | "dead"
  | "failed"
  | "exhausted"
  | "interrupted"
  | "idle"
  | "capped"

export type RunStateInfo = {
  state: RunState
  alive: boolean
  heartbeatAgeMs: number
  stale: boolean
}

function defaultProcessAlive(pid: number) {
  if (!Number.isInteger(pid) || pid <= 0) return false
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}

export function runIsDone(record: ChainlinkRunRecord) {
  return record.tasks.every((task) => task.outcome === "done")
}

export function deriveRunState(
  record: ChainlinkRunRecord,
  options: {
    now?: number
    isProcessAlive?: (pid: number) => boolean
    staleMs?: number
  } = {},
): RunStateInfo {
  const now = options.now ?? Date.now()
  const isProcessAlive = options.isProcessAlive ?? defaultProcessAlive
  const heartbeatAgeMs = Math.max(0, now - Date.parse(record.heartbeatAt))
  const stale = heartbeatAgeMs > (options.staleMs ?? RUN_HEARTBEAT_STALE_MS)
  const alive = isProcessAlive(record.pid)

  if (record.status === "running") {
    return { state: !alive || stale ? "dead" : "running", alive, heartbeatAgeMs, stale }
  }
  if (record.status === "completed" || record.status === "capped") {
    return {
      state: runIsDone(record) ? "done" : "not-done",
      alive,
      heartbeatAgeMs,
      stale,
    }
  }
  if (record.status === "idle") {
    return {
      state: record.tasks.length === 0 ? "idle" : runIsDone(record) ? "done" : "not-done",
      alive,
      heartbeatAgeMs,
      stale,
    }
  }
  return { state: record.status, alive, heartbeatAgeMs, stale }
}

export function runExitCode(record: ChainlinkRunRecord): number {
  const { state } = deriveRunState(record)
  return state === "done" || state === "idle" ? 0 : 1
}

export function runResultSummary(record: ChainlinkRunRecord, path: string) {
  const { state } = deriveRunState(record)
  return {
    run: record.runId,
    record: path,
    status: record.status,
    state,
    exitCode: record.exitCode ?? runExitCode(record),
    cwd: record.cwd,
    chainlinkDb: record.chainlinkDb,
    startedAt: record.startedAt,
    endedAt: record.endedAt,
    current: record.current,
    tasks: record.tasks.map((task) => ({
      id: task.id,
      title: task.title,
      attempts: task.attempts,
      outcome: task.outcome,
      verify: task.verify
        ? { command: task.verify.command, exitCode: task.verify.exitCode, passed: task.verify.passed }
        : null,
    })),
    outcomes: Object.fromEntries(record.tasks.map((task) => [task.id, task.outcome])),
    table: formatRunTable(record),
  }
}

export function formatRunTable(record: ChainlinkRunRecord): string[] {
  const lines = ["task  attempts  outcome  verify"]
  for (const task of record.tasks) {
    const verify = task.verify
      ? task.verify.passed
        ? "passed"
        : `failed (${task.verify.exitCode ?? "timeout"})`
      : "not run"
    lines.push(`#${task.id}  ${task.attempts}  ${task.outcome ?? task.status}  ${verify}`)
  }
  return lines
}
