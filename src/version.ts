// Build identity for the compiled plugin.
//
// `scripts/build.ts` replaces the identifiers below with literals through
// `bun build --define`, so a published bundle can say exactly which source it
// came from: package version, `git describe`, short sha, and whether the
// working tree was dirty at build time.
//
// When this module runs unbundled — unit tests, the TUI entrypoint (shipped as
// source, not built), or `bun run src/...` — the identifiers are undeclared.
// `typeof` on an undeclared identifier is safe and yields "undefined", so the
// guards below report a dev identity instead of throwing.

declare const __PLUGIN_VERSION__: unknown
declare const __PLUGIN_GIT_DESCRIBE__: unknown
declare const __PLUGIN_GIT_SHA__: unknown
declare const __PLUGIN_GIT_DIRTY__: unknown

export const PLUGIN_NAME = "@prevalentware/opencode-loop-plugin"

export type PluginBuildInfo = {
  name: string
  /** Version from package.json (the release version when CI bumped it). */
  version: string
  /** `git describe --tags --always` at build time, or null outside a build. */
  gitDescribe: string | null
  /** Abbreviated commit sha at build time, or null outside a build. */
  gitSha: string | null
  /** True when tracked source (excluding package.json) differed from HEAD. */
  gitDirty: boolean
  /** "build" for a stamped bundle, "dev" for an unbundled/untagged run. */
  source: "build" | "dev"
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null
}

const injectedVersion = readString(
  typeof __PLUGIN_VERSION__ === "string" ? __PLUGIN_VERSION__ : undefined,
)

export const BUILD_INFO: PluginBuildInfo = {
  name: PLUGIN_NAME,
  version: injectedVersion ?? "0.0.0-dev",
  gitDescribe: readString(typeof __PLUGIN_GIT_DESCRIBE__ === "string" ? __PLUGIN_GIT_DESCRIBE__ : undefined),
  gitSha: readString(typeof __PLUGIN_GIT_SHA__ === "string" ? __PLUGIN_GIT_SHA__ : undefined),
  gitDirty: typeof __PLUGIN_GIT_DIRTY__ === "boolean" ? __PLUGIN_GIT_DIRTY__ : false,
  source: injectedVersion ? "build" : "dev",
}

/**
 * Short one-line identity, e.g. `0.2.0 (v0.1.8-4-g64bff2f)`,
 * `0.2.0 (v0.1.8-4-g64bff2f, dirty)`, or `0.0.0-dev (dev)`.
 */
export function formatBuildInfo(info: PluginBuildInfo = BUILD_INFO): string {
  const parts: string[] = [info.version]
  const revision = info.gitDescribe ?? info.gitSha
  if (revision) parts.push(`(${revision}${info.gitDirty ? ", dirty" : ""})`)
  else if (info.source === "dev") parts.push("(dev)")
  return parts.join(" ")
}
