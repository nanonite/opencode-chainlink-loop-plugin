import { mkdir, open, readFile, rm } from "node:fs/promises"
import { dirname, join } from "node:path"
import { tmpdir } from "node:os"

/**
 * A single-writer lock for the loop.
 *
 * Two loops over the same workspace dispatch two workers to the same files, so
 * a second invocation has to fail loudly rather than quietly interleave. The
 * lock holds the owning pid; a lock whose pid is gone is stale and is taken over.
 */
export type LoopLock = {
  path: string
  release: () => Promise<void>
}

function defaultProcessAlive(pid: number) {
  // pid 0 addresses the whole process group and negative pids address groups:
  // signalling either from a liveness probe would be a bug, not a check.
  if (!Number.isInteger(pid) || pid <= 0) return false
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}

export function lockPathFor(cwd: string, dbPath: string | null): string {
  const key = `${cwd}::${dbPath ?? ""}`.replace(/[^a-zA-Z0-9._-]+/g, "_").slice(-120)
  return join(tmpdir(), `chainlink-loop-${key}.lock`)
}

export async function acquireLoopLock(
  cwd: string,
  dbPath: string | null,
  pid = process.pid,
  isProcessAlive: (pid: number) => boolean = defaultProcessAlive,
): Promise<LoopLock> {
  const path = lockPathFor(cwd, dbPath)
  await mkdir(dirname(path), { recursive: true })

  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const handle = await open(path, "wx")
      await handle.writeFile(JSON.stringify({ pid, cwd, dbPath, startedAt: new Date().toISOString() }))
      await handle.close()
      return {
        path,
        release: async () => {
          await rm(path, { force: true })
        },
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error
      const raw = await readFile(path, "utf8").catch(() => "")
      let owner: number | null = null
      try {
        owner = (JSON.parse(raw) as { pid?: number }).pid ?? null
      } catch {
        // Unreadable lock: treat it as abandoned rather than trusting it.
      }
      if (owner != null && owner !== pid && isProcessAlive(owner)) {
        throw new Error(
          `another Chainlink loop is already running for this workspace (pid ${owner}, lock ${path}). ` +
            "Stop it first, or run with a different CHAINLINK_DB or working directory.",
          { cause: error },
        )
      }
      // Stale lock from a process that is gone: take it over.
      await rm(path, { force: true })
    }
  }
  throw new Error(`could not acquire the Chainlink loop lock at ${path}`)
}
