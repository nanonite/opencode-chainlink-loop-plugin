import { expect, test } from "bun:test"
import { extractReview, feedbackPrompt, reviewerPrompt, workerPrompt, type ChainlinkTask } from "../src/chainlink"
import { parseChainlinkArguments } from "../src/prompts"
import type { ChainlinkWorkflowSnapshot } from "../src/state"

function workflow(overrides: Partial<ChainlinkWorkflowSnapshot> = {}): ChainlinkWorkflowSnapshot {
  return {
    id: "chainlink_abc",
    ownerSessionID: "ses_owner",
    taskID: "67",
    taskTitle: "Add parser",
    taskJSON: "{}",
    ownerPid: 1,
    status: "running",
    phase: "worker",
    closed: false,
    attemptsUsed: 0,
    maxAttempts: 3,
    workerSessionID: null,
    reviewerSessionID: null,
    lastReviewJSON: null,
    lastError: null,
    createdAt: 0,
    updatedAt: 0,
    finishedAt: null,
    stopReason: null,
    sampledAt: 0,
    ...overrides,
  }
}

const task: ChainlinkTask = { id: "67", title: "Add parser" }

test("parseChainlinkArguments reads a quoted --prompt alongside task ids", () => {
  const parsed = parseChainlinkArguments('#67 --attempts 5 --prompt "reuse the existing parser"', 20)
  expect(parsed.task_ids).toEqual(["67"])
  expect(parsed.max_attempts).toBe(5)
  expect(parsed.custom_prompt).toBe("reuse the existing parser")
})

test("parseChainlinkArguments accepts single quotes and a prompt before the task", () => {
  const parsed = parseChainlinkArguments("--prompt 'prefer tests first' 67", 20)
  expect(parsed.task_ids).toEqual(["67"])
  expect(parsed.custom_prompt).toBe("prefer tests first")
})

test("parseChainlinkArguments defaults custom_prompt to null", () => {
  const parsed = parseChainlinkArguments("#1", 20)
  expect(parsed.custom_prompt).toBeNull()
  expect(parsed.close_on_approval).toBe(true)
})

test("parseChainlinkArguments rejects a missing or empty --prompt", () => {
  expect(() => parseChainlinkArguments("#1 --prompt", 20)).toThrow("--prompt requires a direction string")
  expect(() => parseChainlinkArguments('#1 --prompt ""', 20)).toThrow("--prompt requires a direction string")
})

test("parseChainlinkArguments names --prompt in the unrecognized-argument error", () => {
  expect(() => parseChainlinkArguments("--bogus", 20)).toThrow("--prompt")
})

test("worker prompt layers the operator direction above the task notes", () => {
  const prompt = workerPrompt(workflow(), task, 1, undefined, "keep changes minimal")
  expect(prompt).toContain("<chainlink_direction>\nkeep changes minimal")
  expect(prompt.indexOf("chainlink_direction")).toBeLessThan(prompt.indexOf("chainlink_task"))
})

test("reviewer prompt receives the direction and is told to hold the work to it", () => {
  const prompt = reviewerPrompt(workflow(), task, 1, "worker report", "keep changes minimal")
  expect(prompt).toContain("<chainlink_direction>\nkeep changes minimal")
  expect(prompt).toContain("hold the work to it")
})

test("feedback prompt keeps the direction on later attempts", () => {
  const prompt = feedbackPrompt(workflow(), { approved: false, summary: "s", findings: ["f"], nextAction: "n" }, "keep changes minimal")
  expect(prompt).toContain("<chainlink_direction>\nkeep changes minimal")
})

test("prompts omit the direction block when none is set", () => {
  expect(workerPrompt(workflow(), task, 1)).not.toContain("chainlink_direction")
  expect(reviewerPrompt(workflow(), task, 1, "report")).not.toContain("chainlink_direction")
  expect(feedbackPrompt(workflow(), { approved: true, summary: "s", findings: [], nextAction: "n" })).not.toContain(
    "chainlink_direction",
  )
})

const approval = JSON.stringify({
  approved: true,
  summary: "looks good",
  findings: [],
  next_action: "finalize",
})

test("extractReview accepts a fenced approval after prose containing a Markdown link", () => {
  const output = [
    "Reviewed against the changelog:",
    "",
    "The `[Unreleased]` section is empty, which matches the task.",
    "",
    "```json",
    approval,
    "```",
  ].join("\n")
  const review = extractReview(output)
  expect(review.approved).toBe(true)
  expect(review.findings).toEqual([])
})

test("extractReview accepts a fenced approval after prose containing an empty object", () => {
  const output = [
    'The `{}` case is handled, as is the "work_package"-only case.',
    "",
    "```json",
    approval,
    "```",
  ].join("\n")
  expect(extractReview(output).approved).toBe(true)
})

test("extractReview accepts a bare verdict after prose containing a bracketed call", () => {
  const output = [
    'The tests cover `pipeline.main([... "report", "feature-ledger"])`.',
    "",
    approval,
  ].join("\n")
  expect(extractReview(output).approved).toBe(true)
})

test("extractReview accepts a fenced approval with trailing prose after the fence", () => {
  const output = [
    "Narrative before the verdict with a stray [bracket].",
    "",
    "```json",
    approval,
    "```",
    "",
    "Let me know if you want a deeper pass.",
  ].join("\n")
  expect(extractReview(output).approved).toBe(true)
})

test("extractReview is not fooled by a parseable object in prose after the verdict", () => {
  const output = [approval, "", 'See the note `{"note": 1}` for details.'].join("\n")
  expect(extractReview(output).approved).toBe(true)
})

test("extractReview prefers the last fenced block when several are present", () => {
  const output = [
    "```json",
    '{"approved": false, "summary": "stale", "findings": ["stale"], "next_action": "n"}',
    "```",
    "",
    "```json",
    approval,
    "```",
  ].join("\n")
  expect(extractReview(output).approved).toBe(true)
})

test("extractReview still rejects a genuinely malformed reply (fail closed)", () => {
  const review = extractReview("I could not review this, sorry. No JSON here.")
  expect(review.approved).toBe(false)
  expect(review.findings).toEqual(["Reviewer output was not valid JSON."])
})

test("extractReview does not approve when findings are present", () => {
  const output = JSON.stringify({
    approved: true,
    summary: "mostly fine",
    findings: ["missing regression test"],
    next_action: "add a test",
  })
  expect(extractReview(output).approved).toBe(false)
})
