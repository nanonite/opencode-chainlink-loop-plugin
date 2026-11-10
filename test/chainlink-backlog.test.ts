import { afterEach, beforeEach, expect, test } from "bun:test"
import { execFileSync } from "node:child_process"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { join } from "node:path"
import { tmpdir } from "node:os"
import {
  runProcessInnerLoop,
  runProcessLoop,
  type ProcessLoopOptions,
  type ProcessStepResult,
} from "../src/chainlink-process"
import { ChainlinkRunRecorder, formatRunTable, runResultSummary } from "../src/chainlink-runs"
import { createChainlinkWorkflow } from "../src/state"
import { main, planResume } from "../src/chainlink-cli"
import type { ChainlinkTask } from "../src/chainlink-core"

let dir = ""

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "opencode-loop-plugin-backlog-"))
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

const task = (id: string): ChainlinkTask => ({ id, title: `Task ${id}`, comments: [], blocked_by: [], status: "open" })

function loopOptions(
  step: ProcessLoopOptions["step"],
  overrides: Partial<ProcessLoopOptions> = {},
): ProcessLoopOptions {
  return {
    ownerSessionID: "ses_backlog",
    cwd: dir,
    taskIds: ["1", "2"],
    maxAttempts: 1,
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
    runner: async (args) => {
      const id = String(args[args.length - 1] ?? "1")
      return { stdout: JSON.stringify(task(id)), stderr: "" }
    },
    step,
    selectionArgs: [],
    showArgs: ["issue", "show", "--json"],
    closeArgs: ["issue", "close", "--json"],
    dbPath: null,
    log: () => undefined,
    ...overrides,
  }
}

test("--stop-on non-done is the default and halts at the first non-done task", async () => {
  const seen: string[] = []
  const step: ProcessLoopOptions["step"] = async (input) => {
    seen.push(input.title ?? "")
    return input.agent === "plan" ? stepResult({ text: approval }) : stepResult({ text: "report only" })
  }

  const report = await runProcessLoop(loopOptions(step))

  expect(report.taskCount).toBe(1)
  expect(report.outcomes).toEqual({ "1": "approved-no-changes" })
  expect(seen.some((title) => title.includes("Chainlink 2"))).toBe(false)
})

test("--stop-on never keeps the old skip-and-continue behaviour", async () => {
  const seen: string[] = []
  const step: ProcessLoopOptions["step"] = async (input) => {
    seen.push(input.title ?? "")
    return input.agent === "plan" ? stepResult({ text: approval }) : stepResult({ text: "report only" })
  }

  const report = await runProcessLoop(loopOptions(step, { stopOnNonDone: false }))

  expect(report.taskCount).toBe(2)
  expect(Object.keys(report.outcomes).sort()).toEqual(["1", "2"])
  expect(seen.some((title) => title.includes("Chainlink 2"))).toBe(true)
})

test("planResume resumes explicit task lists and skips done tasks in queue mode", () => {
  const record = {
    tasks: [
      { id: "1", outcome: "done" },
      { id: "2", outcome: "interrupted" },
      { id: "3", outcome: null },
    ],
  } as never

  expect(planResume(record, true)).toEqual({ taskIds: ["2", "3"], skipTaskIDs: [] })
  expect(planResume(record, false)).toEqual({ taskIds: null, skipTaskIDs: ["1"] })
})

test("--resume picks up the record's remaining task list", async () => {
  const recorder = await ChainlinkRunRecorder.start({
    runId: "run_resume",
    cwd: dir,
    dbPath: null,
    argv: ["--task", "1,2,3"],
    plannedTaskIds: ["1", "2", "3"],
  })
  await recorder.taskOutcome("1", "done", { attempts: 1 })
  await recorder.taskOutcome("2", "interrupted", { attempts: 1 })
  recorder.dispose()

  const writes: string[] = []
  const original = process.stdout.write
  process.stdout.write = ((chunk: string | Uint8Array) => {
    writes.push(String(chunk))
    return true
  }) as typeof process.stdout.write
  try {
    expect(await main(["--resume", "run_resume", "--dry-run"])).toBe(0)
  } finally {
    process.stdout.write = original
  }
  const output = writes.join("")
  expect(output).toContain("resume: run_resume")
  expect(output).toContain("tasks: 2, 3")
})

test("the run record and RESULT mirror the task/attempt/outcome/verify table", async () => {
  const recorder = await ChainlinkRunRecorder.start({
    runId: "run_table",
    cwd: dir,
    dbPath: null,
    argv: [],
    plannedTaskIds: ["86"],
  })
  await recorder.taskStarted("86", "Ingest", null, 2)
  await recorder.step("86", 1, "worker")
  await recorder.taskOutcome("86", "done", {
    attempts: 1,
    verify: { command: "true", exitCode: 0, passed: true, timedOut: false, durationMs: 1, outputTail: "" },
  })
  await recorder.finish("completed", 0)
  const record = recorder.snapshot()

  const table = formatRunTable(record)
  expect(table[0]).toContain("task")
  expect(table[0]).toContain("outcome")
  expect(table[0]).toContain("verify")
  expect(table[1]).toContain("#86")
  expect(table[1]).toContain("done")
  expect(table[1]).toContain("passed")
  expect((record.result as { table?: string[] }).table).toEqual(table)
  expect(runResultSummary(record, recorder.path).table).toEqual(table)
})

test("--close-if-verified closes only a done task whose verify passed", async () => {
  execFileSync("git", ["init"], { cwd: dir, stdio: "ignore" })
  execFileSync("git", ["config", "user.email", "test@example.com"], { cwd: dir })
  execFileSync("git", ["config", "user.name", "Test"], { cwd: dir })
  await writeFile(join(dir, "README.md"), "base\n")
  execFileSync("git", ["add", "README.md"], { cwd: dir })
  execFileSync("git", ["commit", "-m", "base"], { cwd: dir, stdio: "ignore" })

  const closes: string[][] = []
  const runner = async (args: readonly string[]) => {
    if (args[0] === "issue" && args[1] === "close") closes.push([...args])
    return { stdout: JSON.stringify(task("86")), stderr: "" }
  }
  const step: ProcessLoopOptions["step"] = async (input) => {
    if (input.agent === "plan") return stepResult({ text: approval })
    execFileSync("git", ["commit", "--allow-empty", "-m", "work"], { cwd: dir, stdio: "ignore" })
    return stepResult({ text: "committed" })
  }
  const workflow = await createChainlinkWorkflow({
    ownerSessionID: "ses_backlog",
    taskID: "86",
    taskTitle: "Task 86",
    taskJSON: JSON.stringify(task("86")),
    maxAttempts: 1,
    ownerPid: process.pid,
  })
  const options = loopOptions(step, {
    taskIds: ["86"],
    closeOnApproval: true,
    closeIfVerified: true,
    verifyCommand: "exit 0",
    verifyTimeoutSeconds: 5,
    runner,
  })

  const result = await runProcessInnerLoop(options, workflow, task("86"))

  expect(result.outcome).toBe("done")
  expect(closes).toHaveLength(1)
})

async function initGitRepo() {
  execFileSync("git", ["init"], { cwd: dir, stdio: "ignore" })
  execFileSync("git", ["config", "user.email", "test@example.com"], { cwd: dir })
  execFileSync("git", ["config", "user.name", "Test"], { cwd: dir })
  await writeFile(join(dir, "README.md"), "base\n")
  execFileSync("git", ["add", "README.md"], { cwd: dir })
  execFileSync("git", ["commit", "-m", "base"], { cwd: dir, stdio: "ignore" })
}

test("--resume over a task with pre-existing commits is done and closes under --close-if-verified", async () => {
  await initGitRepo()
  const baseSha = execFileSync("git", ["rev-parse", "HEAD"], { cwd: dir, encoding: "utf8" }).trim()
  const recorder = await ChainlinkRunRecorder.start({
    runId: "run_resume_work",
    cwd: dir,
    dbPath: null,
    argv: ["--task", "86"],
    plannedTaskIds: ["86"],
    heartbeatIntervalMs: 0,
  })
  await recorder.taskStarted("86", "Task 86", baseSha, 1)
  await recorder.taskOutcome("86", "interrupted", { attempts: 1 })
  await recorder.finish("interrupted", 1)
  const prior = recorder.snapshot()
  recorder.dispose()
  execFileSync("git", ["commit", "--allow-empty", "-m", "work for #86"], { cwd: dir, stdio: "ignore" })

  const plan = planResume(prior, true)
  expect(plan.taskIds).toEqual(["86"])

  const closes: string[][] = []
  const runner = async (args: readonly string[]) => {
    if (args[0] === "issue" && args[1] === "close") closes.push([...args])
    return { stdout: JSON.stringify(task("86")), stderr: "" }
  }
  const step: ProcessLoopOptions["step"] = async (input) => {
    if (input.agent === "plan") return stepResult({ text: approval })
    throw new Error("the resumed task should review the pre-existing commits, not dispatch a worker")
  }

  const report = await runProcessLoop(
    loopOptions(step, {
      taskIds: plan.taskIds,
      resumeRecord: prior,
      reviewFirst: "always",
      closeOnApproval: true,
      closeIfVerified: true,
      verifyCommand: "exit 0",
      verifyTimeoutSeconds: 5,
      runner,
    }),
  )

  expect(report.outcomes["86"]).toBe("done")
  expect(closes).toHaveLength(1)
  expect(closes[0]).toContain("86")
})

test("an open blocked_by dependency is done and closes under --close-if-verified", async () => {
  await initGitRepo()
  const dependent: ChainlinkTask = { ...task("87"), blocked_by: [99] }
  const closes: string[][] = []
  const runner = async (args: readonly string[]) => {
    if (args[0] === "issue" && args[1] === "close") closes.push([...args])
    return { stdout: JSON.stringify(dependent), stderr: "" }
  }
  const step: ProcessLoopOptions["step"] = async (input) => {
    if (input.agent === "plan") return stepResult({ text: approval })
    execFileSync("git", ["commit", "--allow-empty", "-m", "work for #87"], { cwd: dir, stdio: "ignore" })
    return stepResult({ text: "committed" })
  }

  const report = await runProcessLoop(
    loopOptions(step, {
      taskIds: ["87"],
      reviewFirst: "never",
      closeOnApproval: true,
      closeIfVerified: true,
      verifyCommand: "exit 0",
      verifyTimeoutSeconds: 5,
      runner,
    }),
  )

  expect(report.outcomes["87"]).toBe("done")
  expect(closes).toHaveLength(1)
  expect(closes[0]).toContain("87")
})
