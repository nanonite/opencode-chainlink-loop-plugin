import { spawn } from "node:child_process"
import {
  boundedTaskJSON,
  closeTask,
  ESCALATION_COMMENT_MARKER,
  escapeUntrustedText,
  extractReview,
  feedbackPrompt,
  fetchNextTask,
  fetchReadyTasks,
  fetchTaskById,
  normalizeTaskIds,
  REVIEW_PARSE_FAILURE,
  reviewerPrompt,
  taskFromShowOutput,
  truncate,
  workerPrompt,
  type ChainlinkCommandRunner,
  type ChainlinkModelRef,
  type ChainlinkReview,
  type ChainlinkTask,
  type ChainlinkWorkflowOptions,
} from "./chainlink-core"
import {
  listRunRecords,
  type ChainlinkRunRecord,
  type ChainlinkTaskOutcome,
  type ChainlinkVerifyResult,
} from "./chainlink-runs"
import { workspaceKey } from "./chainlink-lock"
import {
  createChainlinkWorkflow,
  failChainlinkWorkflow,
  finishChainlinkWorkflow,
  recordChainlinkClosing,
  recordChainlinkReview,
  recordChainlinkReviewerStarted,
  recordChainlinkWorkerStarted,
  listChainlinkWorkflows,
  type ChainlinkWorkflowSnapshot,
} from "./state"

/**
 * One deterministic step of the loop, executed as a short-lived `opencode run`
 * process instead of a long-lived in-server child session.
 *
 * Why a process: every step then has its own `--auto` permission mode, its own
 * agent, an exit code and a killable process tree. A child session inside a
 * server has none of those — its permission requests have nobody to answer them
 * and a stuck turn can only be waited out.
 */
export type ProcessStepInput = {
  cwd: string
  prompt: string
  timeoutSeconds: number
  /**
   * Kill the step if it has produced no output at all within this many seconds.
   * A step whose `opencode run` never creates a session hangs silently until the
   * total timeout, so the watchdog is the only thing that recovers it. 0 (or
   * omitted) disables the watchdog.
   */
  startupTimeoutSeconds?: number
  agent?: string
  model?: ChainlinkModelRef | null
  sessionID?: string | null
  title?: string
  auto?: boolean
  signal?: AbortSignal
  onStart?: (command: string, args: string[]) => void
}

export type ProcessStepResult = {
  sessionID: string | null
  text: string
  exitCode: number | null
  /** The step hit its total timeout and was killed. */
  timedOut: boolean
  /** The step produced no output inside the startup window and was killed. */
  startupTimedOut: boolean
  aborted: boolean
  durationMs: number
  stderr: string
  /** Process-group leader pid, so a timeout can report the tree it killed. */
  pid: number | null
}

export type ProcessRunner = (input: ProcessStepInput) => Promise<ProcessStepResult>

export function opencodeRunArgs(input: ProcessStepInput): string[] {
  const args = ["run", "--standalone"]
  if (input.auto !== false) args.push("--auto")
  args.push("--format", "json")
  if (input.agent) args.push("--agent", input.agent)
  if (input.model) {
    const variant = input.model.variant ? `#${input.model.variant}` : ""
    args.push("--model", `${input.model.providerID}/${input.model.id}${variant}`)
  }
  if (input.title) args.push("--title", input.title)
  if (input.sessionID) args.push("--session", input.sessionID)
  args.push(input.prompt)
  return args
}

/** Parses the `--format json` event stream into the assistant's text. */
export function collectStepOutput(stream: string): { sessionID: string | null; text: string } {
  let sessionID: string | null = null
  const parts: string[] = []
  for (const line of stream.split("\n")) {
    const trimmed = line.trim()
    if (!trimmed.startsWith("{")) continue
    let event: unknown
    try {
      event = JSON.parse(trimmed)
    } catch {
      continue
    }
    if (typeof event !== "object" || event === null) continue
    const record = event as Record<string, unknown>
    if (typeof record.sessionID === "string" && !sessionID) sessionID = record.sessionID
    if (record.type !== "text") continue
    const part = record.part
    if (typeof part !== "object" || part === null) continue
    const text = (part as Record<string, unknown>).text
    if (typeof text === "string" && text.trim()) parts.push(text.trim())
  }
  return { sessionID, text: parts.join("\n\n").trim() }
}

/**
 * Runs one step as a short-lived process in its own process group.
 *
 * Exported (rather than only reached through `spawnOpencodeRun`) so the startup
 * watchdog can be exercised with a command that really does hang.
 */
export function runStepProcess(input: ProcessStepInput, command: string, args: string[]): Promise<ProcessStepResult> {
  return new Promise<ProcessStepResult>((resolve) => {
    const started = Date.now()
    input.onStart?.(command, args)
    const child = spawn(command, args, {
      cwd: input.cwd,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
      // Own process group, so a timeout can take the server down with the CLI.
      detached: true,
    })
    const pid = child.pid ?? null
    let stdout = ""
    let stderr = ""
    let timedOut = false
    let startupTimedOut = false
    let aborted = false
    let settled = false
    let sawOutput = false

    const kill = () => {
      try {
        if (child.pid) process.kill(-child.pid, "SIGKILL")
      } catch {
        try {
          child.kill("SIGKILL")
        } catch {
          // already gone
        }
      }
    }

    const startupSeconds = input.startupTimeoutSeconds ?? 0
    const startupTimer =
      startupSeconds > 0
        ? setTimeout(() => {
            // Nothing at all came back, so this is not a slow step; waiting for
            // the full timeout would only burn more wall time.
            if (sawOutput) return
            startupTimedOut = true
            kill()
          }, startupSeconds * 1000)
        : undefined
    const timer =
      input.timeoutSeconds > 0
        ? setTimeout(() => {
            timedOut = true
            kill()
          }, input.timeoutSeconds * 1000)
        : undefined
    const onAbort = () => {
      aborted = true
      kill()
    }
    input.signal?.addEventListener("abort", onAbort, { once: true })

    const settle = (exitCode: number | null) => {
      if (settled) return
      settled = true
      if (timer) clearTimeout(timer)
      if (startupTimer) clearTimeout(startupTimer)
      input.signal?.removeEventListener("abort", onAbort)
      const parsed = collectStepOutput(stdout)
      // A timeout or abort truncates the stream; say so instead of relaying a
      // half-written report as if it were complete.
      const note = startupTimedOut ? "did not start" : timedOut ? "timed out" : aborted ? "aborted" : null
      resolve({
        sessionID: input.sessionID ?? parsed.sessionID,
        text: note ? `${parsed.text}\n\n[process ${note}]`.trim() : parsed.text,
        exitCode,
        timedOut,
        startupTimedOut,
        aborted,
        durationMs: Date.now() - started,
        stderr: stderr.trim(),
        pid,
      })
    }

    child.stdout?.on("data", (chunk: Buffer) => {
      const text = chunk.toString("utf8")
      // Only real event traffic cancels the watchdog: a byte-level banner does
      // not mean the run is working.
      if (!sawOutput && text.includes("{")) {
        sawOutput = true
        if (startupTimer) clearTimeout(startupTimer)
      }
      stdout += text
    })
    child.stderr?.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf8")
      if (stderr.length > 200_000) stderr = stderr.slice(-100_000)
    })
    child.on("error", (error) => {
      stderr += `\n[failed to start ${command}: ${error.message}]`
      settle(null)
    })
    child.on("close", (code) => settle(code))
  })
}

export const spawnOpencodeRun: ProcessRunner = (input) =>
  runStepProcess(input, "opencode", opencodeRunArgs(input))

export type ChainlinkRunEvent =
  | { type: "task-started"; taskID: string; title: string; baseSha: string | null; maxAttempts: number }
  | { type: "step"; taskID: string; attempt: number; step: "worker" | "reviewer" | "verify" | "closing" }
  | {
      type: "task-outcome"
      taskID: string
      outcome: ChainlinkTaskOutcome
      attempts: number
      verify?: ChainlinkVerifyResult | null
      stopReason?: string | null
    }

export type ProcessLoopOptions = {
  ownerSessionID: string
  cwd: string
  taskIds: string[] | null
  maxAttempts: number
  maxTasks: number | null
  workerAgent: string
  reviewerAgent: string
  workerModel: ChainlinkModelRef | null
  reviewerModel: ChainlinkModelRef | null
  workerTimeoutSeconds: number
  reviewerTimeoutSeconds: number
  /**
   * How many extra reviewer processes to start when a reviewer step dies before
   * it can deliver a verdict (non-zero exit, spawn error, timeout). A reviewer
   * crash says nothing about the task's work, so the first one must not fail the
   * task — let alone abandon the rest of the queue. An operator abort is never
   * retried.
   */
  reviewerStepRetries: number
  /**
   * Kill a step that emits no output within this many seconds (see
   * `ProcessStepInput.startupTimeoutSeconds`). 0 disables the watchdog, which
   * means a step that never starts waits out the whole per-step timeout.
   */
  startupTimeoutSeconds: number
  /**
   * Extra attempts for a step that never started (no output at all before the
   * startup watchdog fired). A step that hangs at startup is stuck, not slow, so
   * a fresh process is the only recovery.
   */
  startupRetries: number
  closeOnApproval: boolean
  /**
   * When set, an issue is closed only after a reviewer approval that also
   * produced new commits and passed `--verify`. Otherwise it stays open.
   */
  closeIfVerified?: boolean
  /** Deterministic acceptance command run after reviewer approval. */
  verifyCommand?: string | null
  verifyTimeoutSeconds?: number
  /**
   * Default true: halt the queue at the first task whose deterministic outcome
   * is not "done". `--stop-on never` restores the old skip-and-continue policy.
   */
  stopOnNonDone?: boolean
  /** Tasks already finished by a resumed run; never select them again. */
  skipTaskIDs?: readonly string[]
  onRunEvent?: (event: ChainlinkRunEvent) => void | Promise<void>
  /** "auto" reviews first when the tree already holds work for this task. */
  reviewFirst: "auto" | "always" | "never"
  /**
   * The run record being resumed (`--resume`). It supplies the task's prior
   * baseSha, so commits made before the dead run stopped still count as work.
   */
  resumeRecord?: ChainlinkRunRecord | null
  /**
   * Issue ids whose subtrees the queue fallback must not enter. `issue next`
   * already honours a workspace's intended chain, but the `issue ready`
   * fallback used to step past an exhausted task does not, so subtrees the
   * project deliberately holds back need naming explicitly.
   */
  excludeIDs?: readonly string[]
  /**
   * Operator-supplied direction for the whole run, layered above each task's
   * notes. It steers how the tasks are done (or reviewed) without editing the
   * issues themselves.
   */
  customPrompt?: string | null
  runner: ChainlinkCommandRunner
  step: ProcessRunner
  selectionArgs: string[]
  showArgs: string[]
  closeArgs: string[]
  dbPath: string | null
  signal?: AbortSignal
  log?: (line: string) => void
}

export type ProcessLoopReport = {
  status: "completed" | "exhausted" | "failed" | "idle" | "capped" | "interrupted"
  taskCount: number
  workflows: ChainlinkWorkflowSnapshot[]
  outcomes: Record<string, ChainlinkTaskOutcome>
  error?: string
}

function gitRunner(args: readonly string[], cwd: string) {
  return new Promise<string>((resolve) => {
    const child = spawn("git", [...args], { cwd, stdio: ["ignore", "pipe", "ignore"] })
    let out = ""
    child.stdout?.on("data", (chunk: Buffer) => {
      out += chunk.toString("utf8")
    })
    child.on("error", () => resolve(""))
    child.on("close", () => resolve(out))
  })
}

/** The task's base commit, or null when the workspace is not a git repository. */
export async function gitHead(cwd: string): Promise<string | null> {
  const value = (await gitRunner(["rev-parse", "HEAD"], cwd)).trim()
  return value || null
}

/**
 * Counts commits made since the task started. A null result means git could not
 * answer, which is treated as "no proven change" by the classifier.
 */
export async function gitCommitCountSince(cwd: string, baseSha: string | null): Promise<number | null> {
  if (!baseSha) return null
  const raw = (await gitRunner(["rev-list", "--count", `${baseSha}..HEAD`], cwd)).trim()
  if (!/^\d+$/.test(raw)) return null
  return Number(raw)
}

/** The common ancestor of HEAD and `ref`, or null when git cannot answer. */
export async function gitMergeBase(cwd: string, ref: string): Promise<string | null> {
  const value = (await gitRunner(["merge-base", "HEAD", ref], cwd)).trim()
  return /^[0-9a-f]{7,64}$/i.test(value) ? value : null
}

/**
 * The merge-base with the repository's default branch. This is the fallback
 * base for a task whose prior run record has no baseSha: commits already on a
 * feature branch then still count as work done for the task.
 */
export async function gitDefaultBranchBase(cwd: string): Promise<string | null> {
  for (const ref of ["refs/remotes/origin/HEAD", "origin/main", "origin/master", "main", "master", "trunk"]) {
    const base = await gitMergeBase(cwd, ref)
    if (base) return base
  }
  return null
}

/**
 * Deterministic escalation signal on the issue itself: a blocked flag or status,
 * a blocking/escalation label, a blocker or escalation comment kind, or a comment
 * that starts with the explicit `ESCALATION:` marker.
 *
 * A dependency (`blocked_by`) is deliberately not a signal: Chainlink lists both
 * open and closed blockers there, and an ordinary dependency is not an
 * escalation. Authored prose is not a signal either, so "no escalation needed"
 * cannot flag a task; only the marker at the start of a comment does.
 */
export function taskIsEscalated(task: ChainlinkTask): boolean {
  if (task.blocked === true || task.status === "blocked") return true
  const labels = Array.isArray(task.labels) ? task.labels.map((label) => String(label)) : []
  if (labels.some((label) => /^(blocked|blocking|escalated|escalation)$/i.test(label))) return true
  const comments = Array.isArray(task.comments) ? task.comments : []
  return comments.some((comment) => {
    if (typeof comment !== "object" || comment === null) return false
    const record = comment as Record<string, unknown>
    if (typeof record.kind === "string" && /^(blocker|escalation)$/i.test(record.kind)) return true
    return (
      typeof record.content === "string" &&
      record.content.trimStart().toLowerCase().startsWith(ESCALATION_COMMENT_MARKER.toLowerCase())
    )
  })
}

async function refreshTask(options: ProcessLoopOptions, task: ChainlinkTask): Promise<ChainlinkTask> {
  try {
    const show = await options.runner([...options.showArgs, task.id], options.cwd, options.dbPath)
    return taskFromShowOutput(show.stdout, task)
  } catch {
    return task
  }
}

/**
 * Runs the operator's acceptance command in the worktree. It is a process, not a
 * model: exit 0 is the only pass, and the output tail is the finding handed back
 * to the worker when it fails.
 */
export function runVerifyCommand(
  command: string,
  cwd: string,
  timeoutSeconds: number,
  signal?: AbortSignal,
): Promise<ChainlinkVerifyResult> {
  return new Promise<ChainlinkVerifyResult>((resolve) => {
    const started = Date.now()
    const child = spawn(command, {
      cwd,
      shell: true,
      stdio: ["ignore", "pipe", "pipe"],
      detached: true,
    })
    let stdout = ""
    let stderr = ""
    let timedOut = false
    let aborted = false
    let settled = false

    const kill = () => {
      try {
        if (child.pid) process.kill(-child.pid, "SIGKILL")
      } catch {
        try {
          child.kill("SIGKILL")
        } catch {
          // already gone
        }
      }
    }
    const timer =
      timeoutSeconds > 0
        ? setTimeout(() => {
            timedOut = true
            kill()
          }, timeoutSeconds * 1000)
        : undefined
    const onAbort = () => {
      aborted = true
      kill()
    }
    signal?.addEventListener("abort", onAbort, { once: true })

    const settle = (exitCode: number | null) => {
      if (settled) return
      settled = true
      if (timer) clearTimeout(timer)
      signal?.removeEventListener("abort", onAbort)
      const output = `${stdout}${stderr}`.trim()
      const outputTail = output.length > 8000 ? output.slice(-8000) : output
      resolve({
        command,
        exitCode,
        passed: !timedOut && !aborted && exitCode === 0,
        timedOut,
        durationMs: Date.now() - started,
        outputTail,
      })
    }

    child.stdout?.on("data", (chunk: Buffer) => {
      stdout += chunk.toString("utf8")
      if (stdout.length > 200_000) stdout = stdout.slice(-100_000)
    })
    child.stderr?.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf8")
      if (stderr.length > 200_000) stderr = stderr.slice(-100_000)
    })
    child.on("error", () => settle(null))
    child.on("close", (code) => settle(code))
  })
}

/**
 * Reviewer approval is necessary but never sufficient. "done" needs a commit
 * made since the task's base sha, no escalation on the issue, and (when
 * configured) a passing --verify command.
 */
export async function classifyApprovedOutcome(
  options: ProcessLoopOptions,
  task: ChainlinkTask,
  baseSha: string | null,
  verify: ChainlinkVerifyResult | null,
): Promise<ChainlinkTaskOutcome> {
  if (verify && !verify.passed) return "verify-failed"
  const fresh = await refreshTask(options, task)
  if (taskIsEscalated(fresh)) return "approved-but-escalated"
  const commits = await gitCommitCountSince(options.cwd, baseSha)
  if (commits == null || commits === 0) return "approved-no-changes"
  return "done"
}

/**
 * Detects work already done for this task, so the loop reviews it instead of
 * dispatching a fresh worker that would throw it away.
 *
 * Git is only one signal, and a workspace may not be a repository at all, so two
 * more are used: a previous workflow for the same task, and comments on the
 * issue (which is where a blocked or escalated attempt records its outcome).
 */
export async function detectExistingWork(cwd: string, task: ChainlinkTask, priorWorkflows: readonly ChainlinkWorkflowSnapshot[] = []): Promise<{ existing: boolean; reason: string }> {
  const status = (await gitRunner(["status", "--porcelain"], cwd)).trim()
  if (status) return { existing: true, reason: "the working tree has uncommitted changes" }
  const log = (await gitRunner(["log", "--oneline", "-30"], cwd)).trim()
  if (log) {
    const patterns = [new RegExp(`(^|\\s)#${task.id}(\\s|$)`), new RegExp(`\\b${task.id}\\b`), new RegExp(`chainlink #${task.id}`, "i")]
    if (patterns.some((pattern) => pattern.test(log))) {
      return { existing: true, reason: "recent commits mention this issue" }
    }
  }
  const prior = priorWorkflows.filter((workflow) => workflow.taskID === task.id && workflow.workerSessionID)
  if (prior.length) {
    return { existing: true, reason: `a previous Chainlink run (${prior[0]!.id}) already dispatched a worker for this issue` }
  }
  const comments = Array.isArray(task.comments) ? task.comments : []
  if (comments.length) {
    return { existing: true, reason: `the issue already has ${comments.length} comment(s) recording earlier work` }
  }
  return { existing: false, reason: "no prior work detected" }
}

/** The baseSha recorded for this task by a prior run, if it has one. */
export function recordedBaseSha(record: ChainlinkRunRecord | null | undefined, taskID: string): string | null {
  const task = record?.tasks.find((candidate) => candidate.id === taskID)
  return task?.baseSha ?? null
}

/**
 * Picks the sha that proves new work for this task.
 *
 * A fresh run starts from HEAD. A resumed or review-first run must not: the work
 * that already exists was committed before this process started. In that case
 * the task's recorded baseSha from its previous run record is the only sha that
 * makes those commits countable; when no record has it, the merge-base with the
 * default branch keeps commits on a feature branch visible. HEAD is the last
 * resort so a repository with no default branch still behaves like before.
 */
export async function resolveTaskBaseSha(
  options: ProcessLoopOptions,
  task: ChainlinkTask,
  usePriorWork: boolean,
): Promise<string | null> {
  if (!usePriorWork) return gitHead(options.cwd)
  const prior = recordedBaseSha(options.resumeRecord, task.id)
  if (prior) return prior
  const key = workspaceKey(options.cwd, options.dbPath)
  for (const { record } of await listRunRecords()) {
    if (workspaceKey(record.cwd, record.chainlinkDb) !== key) continue
    const base = recordedBaseSha(record, task.id)
    if (base) return base
  }
  return (await gitDefaultBranchBase(options.cwd)) ?? (await gitHead(options.cwd))
}

/**
 * One line describing a review outcome. A parse failure is called out explicitly
 * so an operator can tell "the reviewer replied with prose we could not read"
 * from "the reviewer raised one real finding" without opening the session
 * database. Both look like `changes requested (1)` otherwise.
 */
export function reviewLogLine(taskID: string, attempt: number, review: ChainlinkReview): string {
  const outcome = review.approved
    ? "approved"
    : review.findings.length === 1 && review.findings[0] === REVIEW_PARSE_FAILURE
      ? "review parse failed (reviewer output was not JSON)"
      : `changes requested (${review.findings.length})`
  return `task ${taskID} attempt ${attempt}: ${outcome}`
}

export type ProcessInnerResult = {
  status: "completed" | "exhausted" | "failed" | "interrupted"
  outcome: ChainlinkTaskOutcome
  error?: string
  review?: ChainlinkReview
  verify?: ChainlinkVerifyResult | null
  baseSha: string | null
}

/**
 * Runs one task to approval, exhaustion or failure using one-shot processes.
 * There is no model in the control path: this function picks the task, counts
 * the attempts and decides when to stop.
 */
export async function runProcessInnerLoop(
  options: ProcessLoopOptions,
  workflow: ChainlinkWorkflowSnapshot,
  task: ChainlinkTask,
): Promise<ProcessInnerResult> {
  const log = options.log ?? (() => undefined)
  const promptContext = { ...workflow, taskID: task.id, taskTitle: task.title }
  let workerSessionID: string | null = null
  let workerOutput = ""
  let existingWork = false
  let pendingReview: ChainlinkReview | null = null

  const detection =
    options.reviewFirst === "never"
      ? { existing: false, reason: "review-first is off" }
      : await detectExistingWork(options.cwd, task, await listChainlinkWorkflows(options.ownerSessionID))
  const reviewFirst = options.reviewFirst === "always" || (options.reviewFirst === "auto" && detection.existing)
  const baseSha = await resolveTaskBaseSha(
    options,
    task,
    detection.existing || reviewFirst || options.resumeRecord != null,
  )
  const emit = async (event: ChainlinkRunEvent) => {
    try {
      await options.onRunEvent?.(event)
    } catch (error) {
      // A record write must never turn a working loop into a failed one.
      log(`run record update failed: ${error instanceof Error ? error.message : String(error)}`)
    }
  }
  await emit({ type: "task-started", taskID: task.id, title: task.title, baseSha, maxAttempts: workflow.maxAttempts })

  const reviewerStepRetries = Math.max(0, options.reviewerStepRetries ?? 0)
  const startupRetries = Math.max(0, options.startupRetries ?? 0)

  /**
   * Runs a step, restarting a process that never produced any output. The
   * startup watchdog has already killed the hung process; a step that never
   * starts is stuck rather than slow, so a fresh process is the only recovery.
   * An operator abort is never retried.
   */
  const runStep = async (input: Omit<ProcessStepInput, "startupTimeoutSeconds">): Promise<ProcessStepResult> => {
    const request: ProcessStepInput = { ...input, startupTimeoutSeconds: options.startupTimeoutSeconds }
    let result = await options.step(request)
    for (let retry = 1; result.startupTimedOut && !input.signal?.aborted && retry <= startupRetries; retry += 1) {
      const stderr = result.stderr.trim()
      const killed = result.pid ? `process group ${result.pid}` : "the step"
      log(
        `step did not start (no output after ${options.startupTimeoutSeconds}s) after ${result.durationMs}ms; killed ${killed}` +
          `${stderr ? `: ${truncate(stderr, 300)}` : ""}; retrying step (${retry}/${startupRetries})`,
      )
      result = await options.step(request)
    }
    return result
  }

  /**
   * Runs the reviewer, retrying a step that dies before delivering a verdict.
   * A crash (non-zero exit, spawn error, timeout) is an infrastructure failure,
   * not a verdict on the task: it says nothing about the work under review, so
   * failing the whole queue on the first one is both surprising and expensive.
   * An operator abort is the one failure that is never retried.
   */
  const runReviewer = async (attempt: number): Promise<ChainlinkReview> => {
    let lastFailure: string | null = null
    for (let retry = 0; retry <= reviewerStepRetries; retry += 1) {
      await emit({ type: "step", taskID: task.id, attempt, step: "reviewer" })
      const result = await runStep({
        cwd: options.cwd,
        prompt: reviewerPrompt(promptContext, task, attempt, workerOutput, options.customPrompt),
        timeoutSeconds: options.reviewerTimeoutSeconds,
        agent: options.reviewerAgent,
        model: options.reviewerModel,
        title: `Chainlink ${task.id} reviewer ${attempt}`,
        signal: options.signal,
      })
      // Record the phase move even when the step yielded no session ID; a
      // reviewer that failed to start has none, and without "reviewer" the
      // recordChainlinkReview below would die on a phase mismatch.
      await recordChainlinkReviewerStarted(workflow.id, result.sessionID)
      if (!result.timedOut && !result.startupTimedOut && !result.aborted && result.exitCode === 0) {
        return extractReview(result.text || "The reviewer returned no report.")
      }
      const reason = result.timedOut
        ? `timed out after ${options.reviewerTimeoutSeconds}s`
        : result.startupTimedOut
          ? `did not start (no output after ${options.startupTimeoutSeconds}s)`
          : result.aborted
            ? "was cancelled"
            : result.exitCode == null
              ? "failed to start"
              : `exited with code ${result.exitCode}`
      const stderr = result.stderr.trim()
      const message = `Chainlink reviewer ${attempt} for task ${task.id} ${reason} after ${result.durationMs}ms${stderr ? `: ${truncate(stderr, 300)}` : ""}`
      lastFailure = message
      // A startup hang was already retried by runStep; only other crashes use
      // the reviewer budget, so the two retries do not compound.
      const willRetry = !result.aborted && !result.startupTimedOut && retry < reviewerStepRetries
      log(willRetry ? `${message}; retrying reviewer (${retry + 1}/${reviewerStepRetries})` : message)
      if (!willRetry) break
    }
    const message = lastFailure ?? `Chainlink reviewer ${attempt} for task ${task.id} failed to start`
    await failChainlinkWorkflow(workflow.id, message).catch(() => null)
    throw new Error(message)
  }

  /**
   * Handles a reviewer approval: run the acceptance gate, classify the outcome
   * from deterministic facts, and close only a genuinely done task. A failed
   * verification returns null after setting pendingReview, so the same worker
   * session gets the command output as findings on the next attempt.
   */
  const finishApproved = async (attempt: number, review: ChainlinkReview): Promise<ProcessInnerResult | null> => {
    let verify: ChainlinkVerifyResult | null = null
    if (options.verifyCommand?.trim()) {
      await emit({ type: "step", taskID: task.id, attempt, step: "verify" })
      log(`task ${task.id} attempt ${attempt}: verify ${options.verifyCommand}`)
      verify = await runVerifyCommand(
        options.verifyCommand,
        options.cwd,
        options.verifyTimeoutSeconds ?? 600,
        options.signal,
      )
      log(
        `task ${task.id} attempt ${attempt}: verify ${
          verify.passed ? "passed" : `failed (exit ${verify.exitCode ?? "timeout"})`
        }`,
      )
    }
    const outcome = await classifyApprovedOutcome(options, task, baseSha, verify)
    if (outcome === "verify-failed") {
      const tail = verify?.outputTail.trim() || "The verification command failed without output."
      pendingReview = {
        approved: false,
        summary: `Verification failed (exit ${verify?.exitCode ?? "timeout"}).`,
        findings: [truncate(tail, 8000)],
        nextAction: "Fix the verification failure, rerun the command, and report the result.",
      }
      log(`task ${task.id} attempt ${attempt}: verification failed; feeding output back as findings`)
      if (attempt >= workflow.maxAttempts) {
        await finishChainlinkWorkflow(workflow.id, "exhausted", `verification failed after ${attempt} attempt(s)`)
        return { status: "exhausted", outcome, review, verify, baseSha }
      }
      return null
    }

    const shouldClose =
      outcome === "done" && options.closeOnApproval && (!options.closeIfVerified || verify?.passed === true)
    if (shouldClose) {
      await emit({ type: "step", taskID: task.id, attempt, step: "closing" })
      await recordChainlinkClosing(workflow.id)
      await closeTask(options as unknown as ChainlinkWorkflowOptions, task.id)
    }
    const reason =
      outcome === "done"
        ? shouldClose
          ? "reviewer approved and issue closed"
          : "reviewer approved; issue left open"
        : outcome === "approved-no-changes"
          ? `reviewer approved but no new commits since ${baseSha ?? "the task base"}`
          : "reviewer approved but the issue is escalated or blocked"
    await finishChainlinkWorkflow(workflow.id, "completed", reason, shouldClose)
    return { status: "completed", outcome, review, verify, baseSha }
  }

  // Someone may already have done this task. Review before adding to it.
  if (reviewFirst) {
    existingWork = true
    workerOutput = "(no worker report: reviewing work already present in the working tree)"
    log(`task ${task.id}: existing work detected (${detection.reason}); reviewing before dispatching a worker`)
    const review = await runReviewer(1)
    await recordChainlinkReview(workflow.id, "", JSON.stringify(review), 1)
    if (review.approved) {
      const approved = await finishApproved(1, review)
      if (approved) return approved
      // Verification failed and findings are pending: continue into the worker loop.
    } else if (workflow.maxAttempts <= 1) {
      await finishChainlinkWorkflow(workflow.id, "exhausted", "attempt limit reached without approval")
      return { status: "exhausted", outcome: "exhausted", review, baseSha }
    }
  }

  for (let attempt = 1; attempt <= workflow.maxAttempts; attempt += 1) {
    if (options.signal?.aborted) {
      await failChainlinkWorkflow(workflow.id, "Chainlink loop cancelled")
      return { status: "interrupted", outcome: "interrupted", error: "Chainlink loop cancelled", baseSha }
    }

    const first = attempt === 1 && !existingWork && !pendingReview
    // After the first rejection the next worker turn carries the review
    // findings, so the same session keeps its context instead of restarting.
    const prompt = pendingReview ? feedbackPrompt(promptContext, pendingReview, options.customPrompt) : workerPrompt(promptContext, task, attempt, undefined, options.customPrompt)
    await emit({ type: "step", taskID: task.id, attempt, step: "worker" })
    log(`task ${task.id} attempt ${attempt}/${workflow.maxAttempts}: worker ${first ? "start" : "resume"}`)
    const worker = await runStep({
      cwd: options.cwd,
      prompt,
      timeoutSeconds: options.workerTimeoutSeconds,
      agent: options.workerAgent,
      model: options.workerModel,
      sessionID: first ? null : workerSessionID,
      title: `Chainlink ${task.id} worker`,
      signal: options.signal,
    })
    if (worker.sessionID) {
      if (first) {
        workerSessionID = worker.sessionID
        await recordChainlinkWorkerStarted(workflow.id, worker.sessionID)
      } else {
        workerSessionID = worker.sessionID
      }
    }
    workerOutput = worker.text
    if (worker.startupTimedOut) {
      const stderr = worker.stderr.trim()
      const error = `Chainlink ${task.id} worker did not start (no output after ${options.startupTimeoutSeconds}s after ${worker.durationMs}ms, exit ${worker.exitCode ?? "killed"})${stderr ? `: ${truncate(stderr, 300)}` : ""}`
      log(`task ${task.id} attempt ${attempt}: worker did not start after ${worker.durationMs}ms${stderr ? `: ${truncate(stderr, 300)}` : ""}`)
      await failChainlinkWorkflow(workflow.id, error)
      return { status: "failed", outcome: "crashed", error, baseSha }
    }
    if (worker.timedOut || worker.aborted) {
      const reason = worker.timedOut
        ? `timed out after ${options.workerTimeoutSeconds}s`
        : "process was cancelled"
      const stderr = worker.stderr.trim()
      const killed = worker.pid ? `, process group ${worker.pid}` : ""
      log(
        `task ${task.id} attempt ${attempt}: worker ${reason} after ${worker.durationMs}ms (exit ${worker.exitCode ?? "killed"}${killed})` +
          `${stderr ? `: ${truncate(stderr, 300)}` : ""}`,
      )
      const error = `Chainlink ${task.id} worker ${reason}`
      await failChainlinkWorkflow(workflow.id, error)
      return {
        status: worker.aborted ? "interrupted" : "failed",
        outcome: worker.aborted ? "interrupted" : "crashed",
        error,
        baseSha,
      }
    }
    if (!workerOutput.trim()) {
      const stderr = worker.stderr.trim()
      const error = `Chainlink worker ${task.id} produced no report (exit ${worker.exitCode ?? "spawn error"}, ${worker.durationMs}ms)${stderr ? `: ${truncate(stderr, 300)}` : ""}`
      log(`task ${task.id} attempt ${attempt}: worker produced no report after ${worker.durationMs}ms${stderr ? `: ${truncate(stderr, 300)}` : ""}`)
      await failChainlinkWorkflow(workflow.id, error)
      return { status: "failed", outcome: "crashed", error, baseSha }
    }

    const review = await runReviewer(attempt)
    await recordChainlinkReview(workflow.id, "", JSON.stringify(review), attempt)
    log(reviewLogLine(task.id, attempt, review))

    if (review.approved) {
      const approved = await finishApproved(attempt, review)
      if (approved) return approved
      continue
    }
    if (attempt === workflow.maxAttempts) {
      await finishChainlinkWorkflow(workflow.id, "exhausted", `attempt limit ${workflow.maxAttempts} reached`)
      return { status: "exhausted", outcome: "exhausted", review, baseSha }
    }
    pendingReview = review
  }

  await finishChainlinkWorkflow(workflow.id, "exhausted", "attempt limit reached")
  return { status: "exhausted", outcome: "exhausted", baseSha }
}

/**
 * The outer loop: take the next actionable task (or the explicit list) and drive
 * it to completion. Stops when the queue is empty.
 *
 * A task that exhausts its attempts is *skipped*, not fatal: it stays open for a
 * human, and the loop moves on to the next ready leaf. `issue next` keeps
 * returning the same exhausted task, so the loop falls back to `issue ready` and
 * picks the first leaf it has not touched yet.
 */
export async function runProcessLoop(options: ProcessLoopOptions): Promise<ProcessLoopReport> {
  const log = options.log ?? (() => undefined)
  const workflows: ChainlinkWorkflowSnapshot[] = []
  const outcomes: Record<string, ChainlinkTaskOutcome> = {}
  const seen = new Set<string>()
  const exhausted = new Set<string>()
  const skip = new Set((options.skipTaskIDs ?? []).map((id) => id.replace(/^#/, "")))
  const requested = options.taskIds != null ? [...new Set(normalizeTaskIds(options.taskIds))] : null
  let index = 0
  let taskCount = 0
  let sawExhausted = false
  const stopOnNonDone = options.stopOnNonDone !== false

  const emit = async (event: ChainlinkRunEvent) => {
    try {
      await options.onRunEvent?.(event)
    } catch (error) {
      log(`run record update failed: ${error instanceof Error ? error.message : String(error)}`)
    }
  }

  const chainlinkOptions = {
    ...options,
    closeOnApproval: options.closeOnApproval,
  } as unknown as ChainlinkWorkflowOptions

  const pickNext = async (): Promise<ChainlinkTask | undefined> => {
    const first = await fetchNextTask(chainlinkOptions)
    if (!first || (!seen.has(first.id) && !skip.has(first.id))) return first
    // `next` is stuck on a task we already handled: scan the ready set instead.
    const excluded = new Set((options.excludeIDs ?? []).map((id) => id.replace(/^#/, "")))
    const ready = await fetchReadyTasks(chainlinkOptions)
    return ready.find(
      (candidate) =>
        !seen.has(candidate.id) &&
        !skip.has(candidate.id) &&
        !excluded.has(candidate.id) &&
        (typeof candidate.parent_id !== "string" && typeof candidate.parent_id !== "number"
          ? true
          : !excluded.has(String(candidate.parent_id))),
    )
  }

  while (true) {
    const done = (): ProcessLoopReport =>
      sawExhausted
        ? { status: "exhausted", taskCount, workflows, outcomes }
        : { status: "completed", taskCount, workflows, outcomes }
    if (options.signal?.aborted) return { status: "interrupted", taskCount, workflows, outcomes, error: "cancelled" }
    if (options.maxTasks != null && taskCount >= options.maxTasks) return { status: "capped", taskCount, workflows, outcomes }
    if (requested && index >= requested.length) return done()

    let task: ChainlinkTask | undefined
    try {
      task = requested ? await fetchTaskById(chainlinkOptions, requested[index++]!) : await pickNext()
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      // A task this loop has already handled cannot be selected again; in queue
      // mode that just means there is nothing new, not that the run is broken.
      if (!requested && seen.has(message.match(/#?(\d+)/)?.[1] ? `#${message.match(/#?(\d+)/)![1]}` : "")) {
        return done()
      }
      return { status: "failed", taskCount, workflows, outcomes, error: message }
    }
    if (!task) return sawExhausted ? { status: "exhausted", taskCount, workflows, outcomes } : { status: "idle", taskCount, workflows, outcomes }
    if (seen.has(task.id)) {
      if (requested) return { status: "failed", taskCount, workflows, outcomes, error: `Chainlink returned duplicate task ${task.id}` }
      return done()
    }
    if (skip.has(task.id)) continue
    seen.add(task.id)

    const workflow = await createChainlinkWorkflow({
      ownerSessionID: options.ownerSessionID,
      taskID: task.id,
      taskTitle: task.title,
      taskJSON: boundedTaskJSON(task),
      maxAttempts: options.maxAttempts,
      ownerPid: process.pid,
    })
    workflows.push(workflow)
    taskCount += 1
    log(`task ${task.id}: ${task.title}`)

    try {
      const result = await runProcessInnerLoop(options, workflow, task)
      outcomes[task.id] = result.outcome
      if (result.status === "exhausted") {
        sawExhausted = true
        exhausted.add(task.id)
        log(`task ${task.id}: attempt limit reached, leaving it open and moving on`)
      }
      // Keep the recorded terminal state in the report.
      workflows[workflows.length - 1] = (await listChainlinkWorkflows(options.ownerSessionID)).find(
        (candidate) => candidate.id === workflow.id,
      ) ?? workflow
      const attempts = workflows[workflows.length - 1]!.attemptsUsed
      await emit({
        type: "task-outcome",
        taskID: task.id,
        outcome: result.outcome,
        attempts,
        verify: result.verify ?? null,
        stopReason: result.error ?? result.review?.summary ?? null,
      })
      if (result.status === "failed" || result.status === "interrupted") {
        return {
          status: result.status === "interrupted" ? "interrupted" : "failed",
          taskCount,
          workflows,
          outcomes,
          error: result.error,
        }
      }
      if (stopOnNonDone && result.outcome !== "done") {
        log(`task ${task.id}: outcome ${result.outcome}; stopping because --stop-on is non-done`)
        return {
          status: result.status,
          taskCount,
          workflows,
          outcomes,
          error: result.error,
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      const failed = await failChainlinkWorkflow(workflow.id, message).catch(() => null)
      if (failed) workflows[workflows.length - 1] = failed
      const outcome: ChainlinkTaskOutcome = options.signal?.aborted ? "interrupted" : "crashed"
      outcomes[task.id] = outcome
      await emit({
        type: "task-outcome",
        taskID: task.id,
        outcome,
        attempts: workflows[workflows.length - 1]!.attemptsUsed,
        stopReason: message,
      })
      return {
        status: options.signal?.aborted ? "interrupted" : "failed",
        taskCount,
        workflows,
        outcomes,
        error: message,
      }
    }
  }
}

export { truncate, escapeUntrustedText }
