import { afterEach, beforeEach, expect, test } from "bun:test"
import { execFileSync } from "node:child_process"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { join } from "node:path"
import { tmpdir } from "node:os"
import {
  runProcessInnerLoop,
  taskIsEscalated,
  type ProcessLoopOptions,
  type ProcessStepResult,
} from "../src/chainlink-process"
import { ChainlinkRunRecorder, type ChainlinkRunRecord } from "../src/chainlink-runs"
import { createChainlinkWorkflow } from "../src/state"
import { feedbackPrompt, reviewerPrompt, workerPrompt, type ChainlinkTask } from "../src/chainlink-core"

let dir = ""

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "opencode-loop-plugin-outcomes-"))
  process.env.OPENCODE_LOOP_STATE_PATH = join(dir, "loops.json")
  execFileSync("git", ["init"], { cwd: dir, stdio: "ignore" })
  execFileSync("git", ["config", "user.email", "test@example.com"], { cwd: dir })
  execFileSync("git", ["config", "user.name", "Test"], { cwd: dir })
  await writeFile(join(dir, "README.md"), "base\n")
  execFileSync("git", ["add", "README.md"], { cwd: dir })
  execFileSync("git", ["commit", "-m", "base"], { cwd: dir, stdio: "ignore" })
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

function innerOptions(
  step: ProcessLoopOptions["step"],
  overrides: Partial<ProcessLoopOptions> = {},
): ProcessLoopOptions {
  return {
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
    runner: async () => ({ stdout: JSON.stringify(task), stderr: "" }),
    step,
    selectionArgs: [],
    showArgs: ["issue", "show", "--json"],
    closeArgs: ["issue", "close", "--json"],
    dbPath: null,
    log: () => undefined,
    ...overrides,
  }
}

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

function commitWork(message = "work") {
  execFileSync("git", ["commit", "--allow-empty", "-m", message], { cwd: dir, stdio: "ignore" })
}

function headSha() {
  return execFileSync("git", ["rev-parse", "HEAD"], { cwd: dir, encoding: "utf8" }).trim()
}

async function priorRunRecord(baseSha: string): Promise<ChainlinkRunRecord> {
  const recorder = await ChainlinkRunRecorder.start({
    runId: "run_prior",
    cwd: dir,
    dbPath: null,
    argv: ["--task", task.id],
    plannedTaskIds: [task.id],
    heartbeatIntervalMs: 0,
  })
  await recorder.taskStarted(task.id, task.title, baseSha, 2)
  await recorder.taskOutcome(task.id, "interrupted", { attempts: 1 })
  await recorder.finish("interrupted", 1)
  const snapshot = recorder.snapshot()
  recorder.dispose()
  return snapshot
}

async function approveWith(
  returnedTask: ChainlinkTask,
  overrides: Partial<ProcessLoopOptions> = {},
) {
  const workflow = await workflowFor()
  const runner = async () => ({ stdout: JSON.stringify(returnedTask), stderr: "" })
  const step: ProcessLoopOptions["step"] = async (input) => {
    if (input.agent === "plan") return stepResult({ text: approval })
    commitWork()
    return stepResult({ text: "committed the work" })
  }
  return runProcessInnerLoop(innerOptions(step, { runner, ...overrides }), workflow, task)
}

test("reviewer approval with no new commits is approved-no-changes, never done", async () => {
  const workflow = await workflowFor()
  const closes: string[][] = []
  const runner = async (args: readonly string[]) => {
    if (args[0] === "issue" && args[1] === "close") closes.push([...args])
    return { stdout: JSON.stringify(task), stderr: "" }
  }
  const step: ProcessLoopOptions["step"] = async (input) =>
    input.agent === "plan" ? stepResult({ text: approval }) : stepResult({ text: "reported without a commit" })

  const result = await runProcessInnerLoop(
    innerOptions(step, { closeOnApproval: true, runner }),
    workflow,
    task,
  )

  expect(result.outcome).toBe("approved-no-changes")
  expect(result.status).toBe("completed")
  expect(closes).toEqual([])
})

test("reviewer approval with a new commit is done and closes when configured", async () => {
  const workflow = await workflowFor()
  const closes: string[][] = []
  const runner = async (args: readonly string[]) => {
    if (args[0] === "issue" && args[1] === "close") closes.push([...args])
    return { stdout: JSON.stringify(task), stderr: "" }
  }
  const step: ProcessLoopOptions["step"] = async (input) => {
    if (input.agent === "plan") return stepResult({ text: approval })
    commitWork()
    return stepResult({ text: "committed the work" })
  }

  const result = await runProcessInnerLoop(
    innerOptions(step, { closeOnApproval: true, runner }),
    workflow,
    task,
  )

  expect(result.outcome).toBe("done")
  expect(result.status).toBe("completed")
  expect(closes).toHaveLength(1)
  expect(closes[0]).toContain("86")
})

test("an open blocked_by dependency is not an escalation and the task is done", async () => {
  const dependent: ChainlinkTask = { ...task, blocked_by: [99] }
  const result = await approveWith(dependent)
  expect(result.outcome).toBe("done")
})

test("a closed blocked_by dependency is still listed on the issue but is not an escalation", async () => {
  // `chainlink issue show` keeps closed dependencies in blocked_by too; neither
  // an open nor a closed dependency is an escalation signal.
  const dependent: ChainlinkTask = { ...task, blocked_by: [11, 12] }
  const result = await approveWith(dependent)
  expect(result.outcome).toBe("done")
})

test("a comment beginning with the ESCALATION: marker is approved-but-escalated", async () => {
  const escalated: ChainlinkTask = {
    ...task,
    comments: [{ kind: "note", content: "ESCALATION: production credentials are unavailable" }],
  }
  const result = await approveWith(escalated)
  expect(result.outcome).toBe("approved-but-escalated")
})

test("a blocker-kind comment is approved-but-escalated", async () => {
  const escalated: ChainlinkTask = {
    ...task,
    comments: [{ kind: "blocker", content: "waiting for a human decision" }],
  }
  const result = await approveWith(escalated)
  expect(result.outcome).toBe("approved-but-escalated")
})

test("a blocking label is approved-but-escalated", async () => {
  const escalated: ChainlinkTask = { ...task, labels: ["blocked"] }
  const result = await approveWith(escalated)
  expect(result.outcome).toBe("approved-but-escalated")
})

test("a blocked status is approved-but-escalated", async () => {
  const escalated: ChainlinkTask = { ...task, status: "blocked" }
  const result = await approveWith(escalated)
  expect(result.outcome).toBe("approved-but-escalated")
})

test("a note that merely mentions escalation without the marker is not an escalation", async () => {
  const noteOnly: ChainlinkTask = {
    ...task,
    comments: [{ kind: "note", content: "no escalation needed for this change" }],
  }
  expect(taskIsEscalated(noteOnly)).toBe(false)
  expect(
    taskIsEscalated({
      ...task,
      comments: [{ kind: "note", content: "see the ESCALATION: note in the design doc" }],
    }),
  ).toBe(false)
  const result = await approveWith(noteOnly)
  expect(result.outcome).toBe("done")
})

test("worker and reviewer prompts use the same explicit escalation marker", async () => {
  const workflow = await workflowFor()
  const review = { approved: false, summary: "needs work", findings: ["fix it"], nextAction: "fix it" }

  expect(workerPrompt(workflow, task, 1)).toContain("ESCALATION:")
  expect(reviewerPrompt(workflow, task, 1, "worker report")).toContain("ESCALATION:")
  expect(feedbackPrompt(workflow, review)).toContain("ESCALATION:")
})

test("--resume with prior commits and reviewer approval is done and closes under --close-if-verified", async () => {
  const baseSha = headSha()
  const prior = await priorRunRecord(baseSha)
  commitWork("work for #86 before the run died")

  const workflow = await workflowFor()
  const closes: string[][] = []
  const runner = async (args: readonly string[]) => {
    if (args[0] === "issue" && args[1] === "close") closes.push([...args])
    return { stdout: JSON.stringify(task), stderr: "" }
  }
  const step: ProcessLoopOptions["step"] = async (input) => {
    if (input.agent === "plan") return stepResult({ text: approval })
    throw new Error("the resume should review the existing commits before dispatching a worker")
  }

  const result = await runProcessInnerLoop(
    innerOptions(step, {
      runner,
      resumeRecord: prior,
      reviewFirst: "always",
      closeOnApproval: true,
      closeIfVerified: true,
      verifyCommand: "exit 0",
      verifyTimeoutSeconds: 5,
    }),
    workflow,
    task,
  )

  expect(result.outcome).toBe("done")
  expect(result.verify?.passed).toBe(true)
  expect(closes).toHaveLength(1)
  expect(closes[0]).toContain("86")
})

test("--resume where nothing changed anywhere is approved-no-changes", async () => {
  const baseSha = headSha()
  const prior = await priorRunRecord(baseSha)

  const workflow = await workflowFor()
  const step: ProcessLoopOptions["step"] = async (input) => {
    if (input.agent === "plan") return stepResult({ text: approval })
    throw new Error("the resume should review first")
  }

  const result = await runProcessInnerLoop(
    innerOptions(step, {
      resumeRecord: prior,
      reviewFirst: "always",
    }),
    workflow,
    task,
  )

  expect(result.outcome).toBe("approved-no-changes")
})

test("review-first without a prior record counts commits since the default-branch merge-base", async () => {
  const defaultBranch = execFileSync("git", ["symbolic-ref", "--short", "HEAD"], {
    cwd: dir,
    encoding: "utf8",
  }).trim()
  execFileSync("git", ["checkout", "-b", "feature"], { cwd: dir, stdio: "ignore" })
  commitWork("work for #86 on a branch")

  const workflow = await workflowFor()
  const runner = async () => ({ stdout: JSON.stringify(task), stderr: "" })
  const step: ProcessLoopOptions["step"] = async (input) => {
    if (input.agent === "plan") return stepResult({ text: approval })
    throw new Error("review-first should not dispatch a worker")
  }

  const result = await runProcessInnerLoop(
    innerOptions(step, { runner, reviewFirst: "always" }),
    workflow,
    task,
  )

  expect(defaultBranch).toBeTruthy()
  expect(result.outcome).toBe("done")
})

test("a non-zero --verify is verify-failed and its output becomes the next worker's findings", async () => {
  const workflow = await workflowFor(2)
  const workerPrompts: string[] = []
  const step: ProcessLoopOptions["step"] = async (input) => {
    if (input.agent === "plan") return stepResult({ text: approval })
    workerPrompts.push(input.prompt)
    return stepResult({ text: "reported a fix" })
  }

  const result = await runProcessInnerLoop(
    innerOptions(step, {
      verifyCommand: "printf 'verify says broken'; exit 7",
      verifyTimeoutSeconds: 5,
    }),
    workflow,
    task,
  )

  expect(result.outcome).toBe("verify-failed")
  expect(result.status).toBe("exhausted")
  expect(result.verify?.exitCode).toBe(7)
  expect(result.verify?.passed).toBe(false)
  expect(result.verify?.outputTail).toContain("verify says broken")
  expect(workerPrompts).toHaveLength(2)
  expect(workerPrompts[1]).toContain("verify says broken")
  expect(workerPrompts[1]).toContain("Verification failed")
})

test("a passing --verify with a new commit is done", async () => {
  const workflow = await workflowFor()
  const step: ProcessLoopOptions["step"] = async (input) => {
    if (input.agent === "plan") return stepResult({ text: approval })
    commitWork()
    return stepResult({ text: "committed the work" })
  }

  const result = await runProcessInnerLoop(
    innerOptions(step, { verifyCommand: "exit 0", verifyTimeoutSeconds: 5 }),
    workflow,
    task,
  )

  expect(result.outcome).toBe("done")
  expect(result.verify?.passed).toBe(true)
  expect(result.verify?.exitCode).toBe(0)
})

test("--verify failure can recover on the next attempt when the command then passes", async () => {
  const workflow = await workflowFor(3)
  const step: ProcessLoopOptions["step"] = async (input) => {
    if (input.agent === "plan") return stepResult({ text: approval })
    commitWork()
    return stepResult({ text: "committed the work" })
  }
  const command = `${process.execPath} -e "const fs=require('fs');const p='${join(dir, "verify-count")}';const n=fs.existsSync(p)?Number(fs.readFileSync(p,'utf8')):0;fs.writeFileSync(p,String(n+1));process.exit(n===0?1:0)"`
  const result = await runProcessInnerLoop(
    innerOptions(step, { verifyCommand: command, verifyTimeoutSeconds: 5 }),
    workflow,
    task,
  )
  expect(result.outcome).toBe("done")
  expect(result.verify?.passed).toBe(true)
})
