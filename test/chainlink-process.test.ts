import { expect, test, beforeEach, afterEach } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { join } from "node:path"
import { tmpdir } from "node:os"
import { buildProcessLoopOptions, type ChainlinkCliArguments } from "../src/chainlink-cli"
import { reviewLogLine, runProcessInnerLoop, type ProcessLoopOptions, type ProcessStepResult } from "../src/chainlink-process"
import { extractReview, REVIEW_PARSE_FAILURE, type ChainlinkReview, type ChainlinkTask } from "../src/chainlink"
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
    customPrompt: null,
    dryRun: false,
    version: false,
    help: false,
  }
  const step = async () => ({ sessionID: null, text: "", exitCode: 0, timedOut: false, aborted: false, durationMs: 0, stderr: "" })
  const runner = async () => ({ stdout: "", stderr: "" })

  const options = buildProcessLoopOptions({ ownerSessionID: "ses_x", cwd: "/tmp", parsed, step, runner, log, dbPath: null })

  expect(options.log).toBe(log)
  expect(options.taskIds).toEqual(["67"])
  expect(options.maxAttempts).toBe(5)
  expect(options.closeOnApproval).toBe(false)
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
  aborted: false,
  durationMs: 1,
  stderr: "",
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
