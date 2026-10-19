// Enforces the workspace rule in AGENTS.md: this plugin supports exactly one
// Chainlink engine, the standalone `chainlink-loop` CLI over one-shot
// `opencode run` processes. A commit may not add another engine (in-server
// child sessions) or MCP support.
//
// The check is diff-based: it inspects only lines added to `src/` or `test/`, so
// the grandfathered in-server implementation that predates the rule does not
// block unrelated commits. Once that implementation is removed, this can become
// a full-tree scan.
//
// Wired as `.githooks/pre-commit` via `git config core.hooksPath .githooks`.
// Bypass with `git commit --no-verify` only for a deliberate removal/refactor.

import { execFileSync } from "node:child_process"

type Forbidden = {
  pattern: RegExp
  why: string
}

const FORBIDDEN: Forbidden[] = [
  { pattern: /\brunChainlinkOuter\b/, why: "in-server child-session engine (one CLI engine only)" },
  { pattern: /\brunInnerWorkflow\b/, why: "in-server child-session engine" },
  { pattern: /\bpromptChild\b/, why: "in-server child-session prompt helper" },
  { pattern: /\bChainlinkChildRegistry\b/, why: "in-server child-session permissions" },
  { pattern: /context\.session\.create\s*\(/, why: "in-server child-session creation" },
  { pattern: /\brun_chainlink_outer\b/, why: "in-server Chainlink tool" },
  { pattern: /\bchainlinkCommandTemplate\b/, why: "the /chainlink in-server command" },
  { pattern: /@modelcontextprotocol\//, why: "MCP support" },
  { pattern: /\bmcpServers\b/, why: "MCP support" },
  { pattern: /\bmcp\.servers\b/, why: "MCP support" },
]

const SCANNED = /^(src|test)\//
const IGNORED = /^dist\//

function stagedDiff(): string {
  try {
    return execFileSync("git", ["diff", "--cached", "--unified=0", "--no-color"], { encoding: "utf8" })
  } catch {
    return ""
  }
}

const violations: Array<{ file: string; line: string; why: string }> = []
let file = ""

for (const raw of stagedDiff().split("\n")) {
  if (raw.startsWith("+++ ")) {
    file = raw.slice(4).replace(/^b\//, "")
    continue
  }
  if (!raw.startsWith("+")) continue
  if (!SCANNED.test(file) || IGNORED.test(file)) continue
  const added = raw.slice(1)
  for (const { pattern, why } of FORBIDDEN) {
    if (pattern.test(added)) violations.push({ file, line: added.trim(), why })
  }
}

if (violations.length) {
  console.error("[check:engine] This repository supports exactly one Chainlink engine: the CLI.")
  console.error("[check:engine] See AGENTS.md -> Chainlink Engine Rule.\n")
  for (const violation of violations) {
    console.error(`  ${violation.file}: ${violation.line}`)
    console.error(`    -> ${violation.why}`)
  }
  console.error("\nUse `git commit --no-verify` only for a deliberate removal/refactor of the deprecated engine.")
  process.exit(1)
}

console.log("[check:engine] ok")
