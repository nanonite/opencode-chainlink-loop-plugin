// Enforces the workspace rule in AGENTS.md: this plugin supports exactly one
// Chainlink engine, the standalone `chainlink-loop` CLI over one-shot
// `opencode run` processes. A second engine (in-server child sessions) and MCP
// support are not allowed anywhere in `src/` or `test/`.
//
// Wired as `.githooks/pre-commit` via `git config core.hooksPath .githooks`.
// Bypass with `git commit --no-verify` only for a deliberate refactor that
// temporarily needs one of the forbidden identifiers.

import { execFileSync } from "node:child_process"
import { readFileSync } from "node:fs"

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

const ROOTS = ["src", "test"]

const files = execFileSync("git", ["ls-files", "--", ...ROOTS], { encoding: "utf8" })
  .split("\n")
  .filter(Boolean)

const violations: Array<{ file: string; number: number; line: string; why: string }> = []
for (const file of files) {
  const lines = readFileSync(file, "utf8").split("\n")
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index]!
    for (const { pattern, why } of FORBIDDEN) {
      if (pattern.test(line)) violations.push({ file, number: index + 1, line: line.trim(), why })
    }
  }
}

if (violations.length) {
  console.error("[check:engine] This repository supports exactly one Chainlink engine: the CLI.")
  console.error("[check:engine] See AGENTS.md -> Chainlink Engine Rule.\n")
  for (const violation of violations) {
    console.error(`  ${violation.file}:${violation.number}: ${violation.line}`)
    console.error(`    -> ${violation.why}`)
  }
  console.error("\nUse `git commit --no-verify` only for a deliberate refactor.")
  process.exit(1)
}

console.log("[check:engine] ok")
