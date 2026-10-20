import { expect, test, beforeEach, afterEach } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { join } from "node:path"
import { tmpdir } from "node:os"
import { buildProcessLoopOptions, main, type ChainlinkCliArguments } from "../src/chainlink-cli"
import { reviewLogLine, runProcessInnerLoop, runStepProcess, type ProcessLoopOptions, type ProcessStepResult } from "../src/chainlink-process"
import { extractReview, REVIEW_PARSE_FAILURE, type ChainlinkReview, type ChainlinkTask } from "../src/chainlink-core"
import { createChainlinkWorkflow, getChainlinkWorkflow } from "../src/state"

function review(overrides: Partial<ChainlinkReview> = {}): ChainlinkReview {
  return { approved: false, summary: "s", findings: [], nextAction: "n", ...overrides }
}

test("reviewLogLine reports an approval", () => {
  expect(reviewLogLine("67", 2, review({ approved: true }))).toBe("task 67 attempt 2: approved")
})

test("reviewLogLine counts real findings", () => {
  expect(reviewLogLine("67", 3, review({ findings: ["a"] }))).toBe("task 67 attempt 3: changes requested (1)")
  expect(reviewLogLine("67", 3, review({ findings: ["a", "b"] }))).toBe("task 67 attempt 3: changes requested (2)")
})

test("reviewLogLine labels a parse failure distinctly from a one-finding rejection", () => {
  expect(reviewLogLine("67", 1, review({ findings: [REVIEW_PARSE_FAILURE] }))).toBe(
    "task 67 attempt 1: review parse failed (reviewer output was not JSON)",
  )
})

test("extractReview marks an unparseable reply with REVIEW_PARSE_FAILURE", () => {
  expect(extractReview("no JSON here, sorry").findings).toEqual([REVIEW_PARSE_FAILURE])
})

test("buildProcessLoopOptions wires the logger through to the loop", () => {
  const lines: string[] = []
  const log = (line: string) => lines.push(line)
  const parsed: ChainlinkCliArguments = {
    taskIds: ["67"],
    attempts: 5,
    maxTasks: null,
    closeOnApproval: false,
    reviewFirst: "auto",
    excludeIDs: [],
    workerModel: null,
    reviewerModel: null,
    workerAgent: "build",
    reviewerAgent: "plan",
    workerTimeout: 600,
    reviewerTimeout: 600,
    reviewerRetries: 1,
    startupTimeout: 300,
    startupRetries: 1,
    customPrompt: null,
    dryRun: false,
    version: false,
    help: false,
  }
  const step = async () => ({ sessionID: null, text: "", exitCode: 0, timedOut: false, startupTimedOut: false, aborted: false, durationMs: 0, stderr: "", pid: null })
  const runner = async () => ({ stdout: "", stderr: "" })

  const options = buildProcessLoopOptions({ ownerSessionID: "ses_x", cwd: "/tmp", parsed, step, runner, log, dbPath: null })

  expect(options.log).toBe(log)
  expect(options.taskIds).toEqual(["67"])
  expect(options.maxAttempts).toBe(5)
  expect(options.closeOnApproval).toBe(false)
})

test("every CLI run logs its build identity, including real runs (#10 follow-up)", async () => {
  const writes: string[] = []
  const original = process.stdout.write
  process.stdout.write = ((chunk: string | Uint8Array) => {
    writes.push(String(chunk))
    return true
  }) as typeof process.stdout.write
  try {
    // --dry-run returns before touching the lock or spawns anything, but it uses
    // the same log() the real run does, which is where the stamp now lives.
    expect(await main(["--dry-run"])).toBe(0)
  } finally {
    process.stdout.write = original
  }
  expect(writes.join("")).toContain("[chainlink] build: ")
})

let dir = ""

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "opencode-loop-plugin-"))
  process.env.OPENCODE_LOOP_STATE_PATH = join(dir, "loops.json")
})

afterEach(async () => {
  delete process.env.OPENCODE_LOOP_STATE_PATH
  await rm(dir, { recursive: true, force: true })
})

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

const innerOptions = (step: ProcessLoopOptions["step"], logLines: string[]): ProcessLoopOptions =>
  ({
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
    // Retries are opt-in per test; the crash tests below set their own budget.
    reviewerStepRetries: 0,
    startupTimeoutSeconds: 300,
    startupRetries: 0,
    closeOnApproval: false,
    reviewFirst: "always",
    runner: async () => ({ stdout: "", stderr: "" }),
    step,
    selectionArgs: [],
    showArgs: [],
    closeArgs: [],
    dbPath: null,
    log: (line) => logLines.push(line),
  }) as ProcessLoopOptions

test("a reviewer step without sessionID still records the review (review-first path)", async () => {
  const workflow = await createChainlinkWorkflow({
    ownerSessionID: "ses_test",
    taskID: task.id,
    taskTitle: task.title,
    taskJSON: JSON.stringify(task),
    maxAttempts: 2,
    ownerPid: process.pid,
  })
  const logLines: string[] = []
  const step: ProcessLoopOptions["step"] = async (input) =>
    input.agent === "plan"
      ? stepResult({ text: "" }) // reviewer: no session, no report
      : stepResult({ text: "did the work" }) // worker: no session either

  // Previously this threw `Chainlink workflow "..." is in phase worker`.
  const result = await runProcessInnerLoop(innerOptions(step, logLines), workflow, task)
  expect(result.status).toBe("exhausted")
  const persisted = await getChainlinkWorkflow(workflow.id)
  expect(persisted?.status).toBe("exhausted")
  expect(persisted?.phase).toBe("done")
})

test("a reviewer step that fails to start fails the workflow with exit code, stderr and duration", async () => {
  const workflow = await createChainlinkWorkflow({
    ownerSessionID: "ses_test",
    taskID: task.id,
    taskTitle: task.title,
    taskJSON: JSON.stringify(task),
    maxAttempts: 2,
    ownerPid: process.pid,
  })
  const logLines: string[] = []
  const step: ProcessLoopOptions["step"] = async (input) =>
    input.agent === "plan"
      ? stepResult({ exitCode: 1, stderr: "model not found", durationMs: 42 })
      : stepResult({ text: "did the work" })

  await expect(runProcessInnerLoop(innerOptions(step, logLines), workflow, task)).rejects.toThrow(
    /exited with code 1.*model not found/,
  )
  const persisted = await getChainlinkWorkflow(workflow.id)
  expect(persisted?.status).toBe("failed")
  expect(persisted?.lastError).toContain("exited with code 1")
  expect(persisted?.lastError).toContain("model not found")
  expect(logLines.some((line) => line.includes("model not found") && line.includes("42ms"))).toBe(true)
})

const approval = JSON.stringify({ approved: true, summary: "lgtm", findings: [], next_action: "close it" })

async function workflowFor(maxAttempts = 2) {
  return createChainlinkWorkflow({
    ownerSessionID: "ses_test",
    taskID: task.id,
    taskTitle: task.title,
    taskJSON: JSON.stringify(task),
    maxAttempts,
    ownerPid: process.pid,
  })
}

test("a transient reviewer crash is retried and the review still lands (#8)", async () => {
  const workflow = await workflowFor()
  const logLines: string[] = []
  let reviewerRuns = 0
  const step: ProcessLoopOptions["step"] = async (input) => {
    if (input.agent !== "plan") return stepResult({ text: "did the work" })
    reviewerRuns += 1
    return reviewerRuns === 1
      ? stepResult({ exitCode: 1, stderr: "reviewer died", durationMs: 210_000 })
      : stepResult({ text: approval })
  }
  const options = { ...innerOptions(step, logLines), reviewFirst: "never" as const, reviewerStepRetries: 1 }

  const result = await runProcessInnerLoop(options, workflow, task)

  expect(result.status).toBe("completed")
  expect(reviewerRuns).toBe(2)
  expect((await getChainlinkWorkflow(workflow.id))?.status).toBe("completed")
  expect(logLines.some((line) => line.includes("retrying reviewer (1/1)"))).toBe(true)
})

test("a reviewer that keeps crashing fails the task only after its retry budget (#8)", async () => {
  const workflow = await workflowFor()
  const logLines: string[] = []
  let reviewerRuns = 0
  const step: ProcessLoopOptions["step"] = async (input) => {
    if (input.agent !== "plan") return stepResult({ text: "did the work" })
    reviewerRuns += 1
    return stepResult({ exitCode: 1, stderr: "still dead", durationMs: 5 })
  }
  const options = { ...innerOptions(step, logLines), reviewFirst: "never" as const, reviewerStepRetries: 2 }

  await expect(runProcessInnerLoop(options, workflow, task)).rejects.toThrow(/exited with code 1.*still dead/)

  expect(reviewerRuns).toBe(3)
  expect((await getChainlinkWorkflow(workflow.id))?.status).toBe("failed")
  expect(logLines.filter((line) => line.includes("retrying reviewer")).length).toBe(2)
})

test("a reviewer cancelled by the operator is never retried (#8)", async () => {
  const workflow = await workflowFor()
  const logLines: string[] = []
  let reviewerRuns = 0
  const step: ProcessLoopOptions["step"] = async (input) => {
    if (input.agent !== "plan") return stepResult({ text: "did the work" })
    reviewerRuns += 1
    return stepResult({ aborted: true, exitCode: null, durationMs: 12 })
  }
  const options = { ...innerOptions(step, logLines), reviewFirst: "never" as const, reviewerStepRetries: 3 }

  await expect(runProcessInnerLoop(options, workflow, task)).rejects.toThrow(/was cancelled/)

  expect(reviewerRuns).toBe(1)
  expect(logLines.some((line) => line.includes("retrying reviewer"))).toBe(false)
})

test("the startup watchdog kills a step that produces no output (#7)", async () => {
  const started = Date.now()
  const result = await runStepProcess(
    { cwd: dir, prompt: "", timeoutSeconds: 30, startupTimeoutSeconds: 0.3 },
    "bash",
    ["-c", "sleep 30"],
  )

  expect(result.startupTimedOut).toBe(true)
  expect(result.timedOut).toBe(false)
  expect(result.pid).toBeGreaterThan(0)
  expect(result.text).toContain("did not start")
  expect(Date.now() - started).toBeLessThan(5000)
})

test("the startup watchdog leaves a step that emits events alone (#7)", async () => {
  const result = await runStepProcess(
    { cwd: dir, prompt: "", timeoutSeconds: 10, startupTimeoutSeconds: 0.3 },
    "bash",
    ["-c", `echo '{"sessionID":"ses_x","type":"text","part":{"text":"hi"}}'; sleep 0.6`],
  )

  expect(result.startupTimedOut).toBe(false)
  expect(result.sessionID).toBe("ses_x")
  expect(result.exitCode).toBe(0)
})

test("a worker that never starts is retried once and then fails clearly (#7)", async () => {
  const workflow = await workflowFor()
  const logLines: string[] = []
  let workerRuns = 0
  const step: ProcessLoopOptions["step"] = async (input) => {
    if (input.agent !== "build") return stepResult({ text: approval })
    workerRuns += 1
    return stepResult({ startupTimedOut: true, exitCode: null, durationMs: 300_000, pid: 4242 })
  }
  const options = {
    ...innerOptions(step, logLines),
    reviewFirst: "never" as const,
    startupTimeoutSeconds: 300,
    startupRetries: 1,
  }

  const result = await runProcessInnerLoop(options, workflow, task)

  expect(result.status).toBe("failed")
  expect(result.error).toMatch(/did not start \(no output after 300s/)
  expect(workerRuns).toBe(2)
  expect(logLines.some((line) => line.includes("killed process group 4242"))).toBe(true)
  expect(logLines.some((line) => line.includes("retrying step (1/1)"))).toBe(true)
  expect((await getChainlinkWorkflow(workflow.id))?.status).toBe("failed")
})
