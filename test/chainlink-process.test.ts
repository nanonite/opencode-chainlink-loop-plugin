import { expect, test } from "bun:test"
import { buildProcessLoopOptions, type ChainlinkCliArguments } from "../src/chainlink-cli"
import { reviewLogLine } from "../src/chainlink-process"
import { extractReview, REVIEW_PARSE_FAILURE, type ChainlinkReview } from "../src/chainlink"

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
  const step = async () => ({ sessionID: null, text: "", exitCode: 0, timedOut: false, aborted: false, durationMs: 0 })
  const runner = async () => ({ stdout: "", stderr: "" })

  const options = buildProcessLoopOptions({ ownerSessionID: "ses_x", cwd: "/tmp", parsed, step, runner, log, dbPath: null })

  expect(options.log).toBe(log)
  expect(options.taskIds).toEqual(["67"])
  expect(options.maxAttempts).toBe(5)
  expect(options.closeOnApproval).toBe(false)
})
