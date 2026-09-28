import { expect, test } from "bun:test"
import { BUILD_INFO, PLUGIN_NAME, formatBuildInfo, type PluginBuildInfo } from "../src/version"

// These tests run against the unbundled module, so no `--define` stamp is
// present and the module must fall back to a dev identity instead of throwing.
test("reports a dev identity when the module is not build-stamped", () => {
  expect(BUILD_INFO.name).toBe(PLUGIN_NAME)
  expect(BUILD_INFO.source).toBe("dev")
  expect(BUILD_INFO.version).toBe("0.0.0-dev")
  expect(BUILD_INFO.gitDescribe).toBeNull()
  expect(BUILD_INFO.gitSha).toBeNull()
  expect(BUILD_INFO.gitDirty).toBe(false)
})

function info(overrides: Partial<PluginBuildInfo> = {}): PluginBuildInfo {
  return {
    name: PLUGIN_NAME,
    version: "0.2.0",
    gitDescribe: "v0.1.8-4-g64bff2f",
    gitSha: "64bff2f",
    gitDirty: false,
    source: "build",
    ...overrides,
  }
}

test("formats a stamped identity", () => {
  expect(formatBuildInfo(info())).toBe("0.2.0 (v0.1.8-4-g64bff2f)")
})

test("marks a dirty build", () => {
  expect(formatBuildInfo(info({ gitDirty: true }))).toBe("0.2.0 (v0.1.8-4-g64bff2f, dirty)")
})

test("falls back to the sha when git describe is unavailable", () => {
  expect(formatBuildInfo(info({ gitDescribe: null }))).toBe("0.2.0 (64bff2f)")
})

test("labels an unstamped dev build", () => {
  expect(formatBuildInfo(info({ gitDescribe: null, gitSha: null, source: "dev" }))).toBe("0.2.0 (dev)")
})
