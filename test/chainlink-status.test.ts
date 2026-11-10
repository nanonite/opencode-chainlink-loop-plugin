import { afterEach, beforeEach, expect, test } from "bun:test"
import { access, mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises"
import { join } from "node:path"
import { tmpdir } from "node:os"
import { spawn } from "node:child_process"
import { ChainlinkRunRecorder, runRecordPath, runsDirectory } from "../src/chainlink-runs"
import { lockPathFor } from "../src/chainlink-lock"
import { statusCommand, waitCommand } from "../src/chainlink-cli"

let dir = ""

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "opencode-loop-plugin-status-"))
  process.env.OPENCODE_LOOP_STATE_PATH = join(dir, "loops.json")
})

afterEach(async () => {
  delete process.env.OPENCODE_LOOP_STATE_PATH
  await rm(dir, { recursive: true, force: true })
})

function capture() {
  const lines: string[] = []
  return {
    lines,
    stdout: (line: string) => lines.push(line),
    stderr: (line: string) => lines.push(line),
  }
}

async function startRecord(
  runId: string,
  options: { pid?: number; argv?: string[]; dbPath?: string | null } = {},
) {
  return ChainlinkRunRecorder.start({
    runId,
    cwd: dir,
    dbPath: options.dbPath ?? null,
    argv: options.argv ?? ["--task", "86", "--no-close"],
    pid: options.pid ?? process.pid,
  })
}

test("status reports a dead run when its pid is gone and gives the exact relaunch command", async () => {
  const recorder = await startRecord("run_dead_pid", { pid: 2_147_483_647 })
  recorder.dispose()
  const output = capture()

  const code = await statusCommand(["--json"], { cwd: dir, dbPath: null, stdout: output.stdout, stderr: output.stderr })

  expect(code).toBe(1)
  const info = JSON.parse(output.lines.join("\n"))
  expect(info.state).toBe("dead")
  expect(info.record).toBe(runRecordPath("run_dead_pid"))
  expect(info.relaunch).toContain("chainlink-loop")
  expect(info.relaunch).toContain("--resume run_dead_pid")
  expect(info.relaunch).toContain("--task 86")
  expect(info.relaunch).toContain("--review-first always")
})

test("status reports a stale-heartbeat run as dead even while the pid exists", async () => {
  const recorder = await startRecord("run_stale")
  await recorder.update((record) => {
    record.heartbeatAt = new Date(Date.now() - 120_000).toISOString()
  })
  recorder.dispose()
  const output = capture()

  const code = await statusCommand(["--json"], { cwd: dir, dbPath: null, stdout: output.stdout, stderr: output.stderr })

  expect(code).toBe(1)
  const info = JSON.parse(output.lines.join("\n"))
  expect(info.state).toBe("dead")
  expect(info.stale).toBe(true)
  expect(info.pid).toBe(process.pid)
})

test("status reports a live run as running", async () => {
  const recorder = await startRecord("run_live")
  recorder.dispose()
  const output = capture()

  const code = await statusCommand(["--json"], { cwd: dir, dbPath: null, stdout: output.stdout, stderr: output.stderr })

  expect(code).toBe(0)
  const info = JSON.parse(output.lines.join("\n"))
  expect(info.state).toBe("running")
  expect(info.selection).toBe("newest")
})

test("status with no --run picks the newest record for the cwd+db key and says so", async () => {
  const older = await startRecord("run_older")
  await older.taskOutcome("86", "done", { attempts: 1 })
  await older.finish("completed", 0)
  await Bun.sleep(5)
  const newer = await startRecord("run_newer")
  newer.dispose()
  const output = capture()

  await statusCommand(["--json"], { cwd: dir, dbPath: null, stdout: output.stdout, stderr: output.stderr })

  const info = JSON.parse(output.lines.join("\n"))
  expect(info.run).toBe("run_newer")
  expect(info.selection).toBe("newest")
  expect(info.candidates).toBe(2)
  expect(info.record).toBe(runRecordPath("run_newer"))
})

test("wait returns the RESULT and exit 0 for a done run", async () => {
  const recorder = await startRecord("run_wait_done")
  await recorder.taskOutcome("86", "done", { attempts: 1 })
  await recorder.finish("completed", 0)
  const output = capture()

  const code = await waitCommand(["--timeout", "1"], {
    cwd: dir,
    dbPath: null,
    stdout: output.stdout,
    stderr: output.stderr,
    pollMs: 20,
  })

  expect(code).toBe(0)
  const line = output.lines.find((candidate) => candidate.includes("RESULT "))
  expect(line).toBeTruthy()
  const info = JSON.parse(line!.slice(line!.indexOf("RESULT ") + "RESULT ".length))
  expect(info.state).toBe("done")
  expect(info.run).toBe("run_wait_done")
  expect(info.record).toBe(runRecordPath("run_wait_done"))
})

test("wait exits 1 when the run finished but its outcome is not done", async () => {
  const recorder = await startRecord("run_wait_not_done")
  await recorder.taskOutcome("86", "approved-no-changes", { attempts: 1 })
  await recorder.finish("completed", 0)
  const output = capture()

  const code = await waitCommand(["--timeout", "1"], {
    cwd: dir,
    dbPath: null,
    stdout: output.stdout,
    stderr: output.stderr,
    pollMs: 20,
  })

  expect(code).toBe(1)
  const line = output.lines.find((candidate) => candidate.includes("RESULT "))
  expect(line).toContain('"state":"not-done"')
})

test("an idle queue that finished with a non-done task still exits 1 and reports not-done", async () => {
  const recorder = await startRecord("run_idle_not_done")
  await recorder.taskOutcome("86", "approved-no-changes", { attempts: 1 })
  await recorder.finish("idle", 0)
  const output = capture()

  const waitCode = await waitCommand(["--timeout", "1"], {
    cwd: dir,
    dbPath: null,
    stdout: output.stdout,
    stderr: output.stderr,
    pollMs: 20,
  })
  const statusCode = await statusCommand(["--json"], {
    cwd: dir,
    dbPath: null,
    stdout: output.stdout,
    stderr: output.stderr,
  })

  expect(waitCode).toBe(1)
  expect(statusCode).toBe(1)
  const info = JSON.parse(output.lines.at(-1)!)
  expect(info.state).toBe("not-done")
})

test("wait exits 3 on timeout and leaves the run running", async () => {
  const recorder = await startRecord("run_wait_timeout")
  recorder.dispose()
  const output = capture()

  const code = await waitCommand(["--timeout", "0.15"], {
    cwd: dir,
    dbPath: null,
    stdout: output.stdout,
    stderr: output.stderr,
    pollMs: 20,
  })

  expect(code).toBe(3)
  const line = output.lines.find((candidate) => candidate.includes("RESULT "))
  expect(line).toContain('"wait":"timeout"')
  const record = JSON.parse(await readFile(runRecordPath("run_wait_timeout"), "utf8"))
  expect(record.status).toBe("running")
})

async function writeFakeBin(binDir: string) {
  await mkdir(binDir, { recursive: true })
  const chainlink = `#!/usr/bin/env bash
if [ "$1" = "issue" ] && [ "$2" = "show" ]; then
  printf '{"id":1,"title":"Signal task","status":"open","comments":[],"blocked_by":[]}\\n'
  exit 0
fi
printf '{}\\n'
exit 0
`
  const opencode = `#!/usr/bin/env bash
printf '{"sessionID":"ses_signal","type":"text","part":{"text":"working"}}\\n'
sleep 30
`
  await writeFile(join(binDir, "chainlink"), chainlink, { mode: 0o755 })
  await writeFile(join(binDir, "opencode"), opencode, { mode: 0o755 })
}

type RunRecordShape = {
  status?: string
  current?: { step?: string } | null
  [key: string]: unknown
}

async function waitForRecord(): Promise<RunRecordShape> {
  let lastRecord: RunRecordShape | null = null
  for (let attempt = 0; attempt < 200; attempt += 1) {
    try {
      const names = await readdir(runsDirectory())
      const name = names.find((candidate) => candidate.endsWith(".json"))
      if (name) {
        const record = JSON.parse(await readFile(join(runsDirectory(), name), "utf8")) as RunRecordShape
        lastRecord = record
        // A run now legitimately sits in "selecting" while it decides whether
        // prior work exists; the signal tests need the record after the worker
        // step has started.
        if (record.current?.step === "worker") return record
      }
    } catch {
      // directory not created yet
    }
    await Bun.sleep(20)
  }
  throw new Error(`run record did not reach the worker step: ${JSON.stringify(lastRecord)}`)
}

async function runSignalCase(signal: NodeJS.Signals) {
  const binDir = join(dir, "bin")
  const work = join(dir, "work")
  await mkdir(work, { recursive: true })
  await writeFakeBin(binDir)
  const statePath = process.env.OPENCODE_LOOP_STATE_PATH!
  const dbPath = join(dir, "db")
  const child = spawn(process.execPath, [join(import.meta.dir, "..", "src", "chainlink-cli.ts"), "--task", "1", "--review-first", "never"], {
    cwd: work,
    env: {
      ...process.env,
      PATH: `${binDir}:${process.env.PATH ?? ""}`,
      CHAINLINK_DB: dbPath,
      OPENCODE_LOOP_STATE_PATH: statePath,
    },
    stdio: ["ignore", "pipe", "pipe"],
  })
  let output = ""
  child.stdout?.on("data", (chunk: Buffer) => {
    output += chunk.toString("utf8")
  })
  child.stderr?.on("data", (chunk: Buffer) => {
    output += chunk.toString("utf8")
  })
  const record = await waitForRecord()
  expect(record.status).toBe("running")
  expect(record.current?.step).toBe("worker")
  child.kill(signal)
  const exitCode = await new Promise<number | null>((resolve) => child.on("close", (code) => resolve(code)))
  expect(exitCode).toBe(1)
  expect(output).toContain(`${signal} received`)
  const names = (await readdir(runsDirectory())).filter((name) => name.endsWith(".json"))
  const saved = JSON.parse(await readFile(join(runsDirectory(), names[0]!), "utf8"))
  expect(saved.status).toBe("interrupted")
  expect(saved.endedAt).toBeTruthy()
  expect(saved.current).toBeNull()
  await expect(access(lockPathFor(work, dbPath))).rejects.toThrow()
}

test("SIGTERM writes a terminal interrupted record and releases the lock", async () => {
  await runSignalCase("SIGTERM")
})

test("SIGHUP writes a terminal interrupted record and releases the lock", async () => {
  await runSignalCase("SIGHUP")
})
