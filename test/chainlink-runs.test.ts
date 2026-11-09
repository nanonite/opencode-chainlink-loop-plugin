import { afterEach, beforeEach, expect, test } from "bun:test"
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises"
import { join } from "node:path"
import { tmpdir } from "node:os"
import {
  ChainlinkRunRecorder,
  deriveRunState,
  listRunRecords,
  pruneRunRecords,
  runRecordPath,
  runsDirectory,
} from "../src/chainlink-runs"
import { runProcessLoop, type ProcessLoopOptions, type ProcessStepResult } from "../src/chainlink-process"
import type { ChainlinkTask } from "../src/chainlink-core"

let dir = ""

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "opencode-loop-plugin-runs-"))
  process.env.OPENCODE_LOOP_STATE_PATH = join(dir, "loops.json")
})

afterEach(async () => {
  delete process.env.OPENCODE_LOOP_STATE_PATH
  await rm(dir, { recursive: true, force: true })
})

const approval = JSON.stringify({ approved: true, summary: "lgtm", findings: [], next_action: "done" })

const stepResult = (overrides: Partial<ProcessStepResult>): ProcessStepResult => ({
  sessionID: null,
  text: "",
  exitCode: 0,
  timedOut: false,
  startupTimedOut: false,
  aborted: false,
  durationMs: 1,
  stderr: "",
  pid: null,
  ...overrides,
})

const task: ChainlinkTask = { id: "86", title: "Ingest", comments: [], blocked_by: [], status: "open" }

test("run records live under dirname(statePath())/chainlink-runs and carry the run identity", async () => {
  const recorder = await ChainlinkRunRecorder.start({
    runId: "run_identity",
    cwd: dir,
    dbPath: "/tmp/chainlink.db",
    argv: ["--task", "86"],
    plannedTaskIds: ["86"],
  })
  try {
    const path = runRecordPath("run_identity")
    expect(path).toBe(join(dir, "chainlink-runs", "run_identity.json"))
    const raw = JSON.parse(await readFile(path, "utf8"))
    expect(raw.runId).toBe("run_identity")
    expect(raw.pid).toBe(process.pid)
    expect(raw.cwd).toBe(dir)
    expect(raw.chainlinkDb).toBe("/tmp/chainlink.db")
    expect(raw.argv).toEqual(["--task", "86"])
    expect(raw.build.version).toBeDefined()
    expect(raw.startedAt).toBeTruthy()
    expect(raw.heartbeatAt).toBeTruthy()
    expect(raw.current).toEqual(expect.objectContaining({ step: "selecting" }))
    expect(raw.tasks).toEqual([expect.objectContaining({ id: "86", status: "pending" })])
    expect(raw.status).toBe("running")
    expect(raw.exitCode).toBeNull()
    expect(raw.endedAt).toBeNull()

    await recorder.taskStarted("86", "Ingest", "abc123", 2)
    await recorder.step("86", 1, "worker")
    const mid = JSON.parse(await readFile(path, "utf8"))
    expect(mid.current).toEqual(expect.objectContaining({ taskID: "86", attempt: 1, step: "worker" }))
    expect(mid.tasks[0]).toEqual(expect.objectContaining({ status: "running", baseSha: "abc123" }))

    await recorder.taskOutcome("86", "done", { attempts: 1 })
    await recorder.finish("completed", 0)
    const done = JSON.parse(await readFile(path, "utf8"))
    expect(done.status).toBe("completed")
    expect(done.exitCode).toBe(0)
    expect(done.endedAt).toBeTruthy()
    expect(done.current).toBeNull()
    expect(done.tasks[0]).toEqual(expect.objectContaining({ outcome: "done", attempts: 1 }))
    expect(done.result).toEqual(expect.objectContaining({ run: "run_identity", record: path, state: "done" }))
  } finally {
    recorder.dispose()
  }
})

test("the heartbeat updates the record and the timer is cleared on finish", async () => {
  const recorder = await ChainlinkRunRecorder.start({
    runId: "run_heartbeat",
    cwd: dir,
    dbPath: null,
    argv: [],
    heartbeatIntervalMs: 20,
  })
  try {
    const before = JSON.parse(await readFile(recorder.path, "utf8")).heartbeatAt
    await Bun.sleep(80)
    const after = JSON.parse(await readFile(recorder.path, "utf8")).heartbeatAt
    expect(Date.parse(after)).toBeGreaterThan(Date.parse(before))
  } finally {
    await recorder.finish("completed", 0)
  }
  const finishedAt = JSON.parse(await readFile(recorder.path, "utf8")).heartbeatAt
  await Bun.sleep(80)
  const still = JSON.parse(await readFile(recorder.path, "utf8")).heartbeatAt
  expect(still).toBe(finishedAt)
})

test("a run record left without finish stays non-terminal with its last heartbeat", async () => {
  const recorder = await ChainlinkRunRecorder.start({
    runId: "run_killed",
    cwd: dir,
    dbPath: null,
    argv: [],
  })
  const old = new Date(Date.now() - 120_000).toISOString()
  await recorder.update((record) => {
    record.heartbeatAt = old
  })
  recorder.dispose()

  const record = JSON.parse(await readFile(recorder.path, "utf8"))
  expect(record.status).toBe("running")
  expect(record.endedAt).toBeNull()
  expect(record.heartbeatAt).toBe(old)
  expect(deriveRunState(record, { isProcessAlive: () => false }).state).toBe("dead")
  expect(deriveRunState(record, { isProcessAlive: () => true }).state).toBe("dead")
})

test("pruning keeps the newest N records", async () => {
  const recorders: ChainlinkRunRecorder[] = []
  for (const id of ["run_old", "run_mid", "run_new"]) {
    recorders.push(
      await ChainlinkRunRecorder.start({
        runId: id,
        cwd: dir,
        dbPath: null,
        argv: [],
      }),
    )
    await Bun.sleep(5)
  }
  for (const recorder of recorders) recorder.dispose()
  await pruneRunRecords(2)
  const names = (await readdir(runsDirectory())).filter((name) => name.endsWith(".json")).sort()
  expect(names.length).toBe(2)
  expect(names).not.toContain("run_old.json")
})

test("runProcessLoop records the current step mid-run and a terminal record afterwards", async () => {
  const recorder = await ChainlinkRunRecorder.start({
    runId: "run_loop",
    cwd: dir,
    dbPath: null,
    argv: ["--task", "86"],
    plannedTaskIds: ["86"],
  })
  const seenSteps: string[] = []
  const step: ProcessLoopOptions["step"] = async (input) => {
    const mid = JSON.parse(await readFile(recorder.path, "utf8"))
    seenSteps.push(mid.current?.step)
    return input.agent === "plan"
      ? stepResult({ text: approval })
      : stepResult({ text: "did the work", sessionID: "ses_worker" })
  }
  const runner = async (args: readonly string[]) =>
    args[0] === "issue" && args[1] === "show"
      ? { stdout: JSON.stringify(task), stderr: "" }
      : { stdout: "", stderr: "" }
  const options: ProcessLoopOptions = {
    ownerSessionID: "ses_test",
    cwd: dir,
    taskIds: ["86"],
    maxAttempts: 2,
    maxTasks: null,
    workerAgent: "build",
    reviewerAgent: "plan",
    workerModel: null,
    reviewerModel: null,
    workerTimeoutSeconds: 1,
    reviewerTimeoutSeconds: 1,
    reviewerStepRetries: 0,
    startupTimeoutSeconds: 300,
    startupRetries: 0,
    closeOnApproval: false,
    reviewFirst: "never",
    runner,
    step,
    selectionArgs: [],
    showArgs: ["issue", "show", "--json"],
    closeArgs: [],
    dbPath: null,
    onRunEvent: async (event) => {
      if (event.type === "task-started") await recorder.taskStarted(event.taskID, event.title, event.baseSha, event.maxAttempts)
      else if (event.type === "step") await recorder.step(event.taskID, event.attempt, event.step)
      else await recorder.taskOutcome(event.taskID, event.outcome, { attempts: event.attempts, verify: event.verify })
    },
  }

  const report = await runProcessLoop(options)
  await recorder.finish(report.status, 0)
  expect(seenSteps).toContain("worker")
  expect(seenSteps).toContain("reviewer")
  const record = JSON.parse(await readFile(recorder.path, "utf8"))
  expect(record.status).toBe("completed")
  expect(record.tasks).toEqual([expect.objectContaining({ id: "86", status: "finished" })])
  expect(record.tasks[0].outcome).toBeTruthy()
  expect(record.endedAt).toBeTruthy()
  expect((await listRunRecords()).length).toBe(1)
})
