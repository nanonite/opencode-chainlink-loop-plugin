// Verifies that the committed `dist/` is a faithful build of the working tree.
//
// A plain `bun run build && git diff --exit-code dist` cannot pass: every build
// embeds its own git identity (`git describe`, sha, dirty flag), so a rebuild
// at a later commit never matches the committed bundle. This script rebuilds
// with the identity recorded in the committed `dist/build-info.json`; any
// remaining difference means `dist/` is stale.
//
// Usage: `bun run check:dist` (CI runs this on every pull request).

import { execFileSync } from "node:child_process"
import { readFileSync } from "node:fs"
import { fileURLToPath } from "node:url"

type BuildInfo = {
  version: string
  gitDescribe: string | null
  gitSha: string | null
  gitDirty: boolean
}

const root = fileURLToPath(new URL("..", import.meta.url))

let committed: BuildInfo
try {
  committed = JSON.parse(
    readFileSync(new URL("../dist/build-info.json", import.meta.url), "utf8"),
  ) as BuildInfo
} catch {
  console.error("[check-dist] dist/build-info.json is missing; run `bun run build` and commit dist/")
  process.exit(1)
}

const env = {
  ...process.env,
  PLUGIN_BUILD_VERSION: committed.version,
  PLUGIN_BUILD_DESCRIBE: committed.gitDescribe ?? "",
  PLUGIN_BUILD_SHA: committed.gitSha ?? "",
  PLUGIN_BUILD_DIRTY: committed.gitDirty ? "true" : "false",
}

execFileSync(process.execPath, [fileURLToPath(new URL("./build.ts", import.meta.url))], {
  cwd: root,
  env,
  stdio: "inherit",
})

const status = execFileSync("git", ["status", "--porcelain", "--", "dist"], {
  cwd: root,
  encoding: "utf8",
})
if (status.trim().length > 0) {
  console.error("[check-dist] committed dist/ does not match a fresh build:")
  console.error(status.trimEnd())
  console.error("[check-dist] run `bun run build` and commit dist/ together with the source change")
  process.exit(1)
}

console.log("[check-dist] committed dist/ matches the working tree")
