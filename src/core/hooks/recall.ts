// The hook's unit recall search under a deadline. node:sqlite is synchronous, so a timer on the thread that runs the
// query cannot fire until the query returns, and a worker thread blocked in a native read cannot be stopped (the
// process waits for it at exit). The query runs in a child process on its own read-only connection; this process
// owns the timer and kills the child at the deadline. Past RECALL_DEADLINE_MS recall is empty and the hook succeeds.
// D-CI-PERF-7: node:fs through getBuiltinModule; a static ESM import of node:fs also loads fs/promises, readline and streams.
const { appendFileSync, statSync } = process.getBuiltinModule("node:fs") as typeof import("node:fs");
import { join } from "node:path";
import { RECALL_BUSY_TIMEOUT_MS, RECALL_DEADLINE_MS, RECALL_TIMEOUT_LOG } from "../contracts/constants.ts";
import type { ProjectContext, UnitCandidate } from "../contracts/types.ts";
import { unitQuery, type UnitQuery } from "../store/fts.ts";

const LOG_MAX_BYTES = 65536;

// The child's program: unitQuery's own source, run on the connection described by argv[1] (JSON).
const CHILD_SOURCE =
  "const { DatabaseSync } = require('node:sqlite');" +
  `const unitQuery = ${unitQuery.toString()};` +
  "const a = JSON.parse(process.argv[1]);" +
  "const raw = new DatabaseSync(a.dbPath, { readOnly: true, timeout: a.busyMs });" +
  "process.stdout.write(JSON.stringify(unitQuery(raw, a.query)));" +
  "raw.close();";

function logTimeout(project: ProjectContext, reason: string, ms: number): void {
  try {
    const path = join(project.projectDir, RECALL_TIMEOUT_LOG);
    try {
      if (statSync(path).size > LOG_MAX_BYTES) return;
    } catch { /* no log yet */ }
    appendFileSync(path, new Date().toISOString() + " " + reason + " " + String(ms) + "ms\n");
  } catch { /* the log is a signal, never a failure */ }
}

/** Ranked units for a prompt, or [] when the search errors or passes the deadline. Never throws. */
export async function recallUnits(project: ProjectContext, query: UnitQuery, deadlineMs: number = RECALL_DEADLINE_MS): Promise<UnitCandidate[]> {
  if (query.match === "" || query.sessionId === "") return [];
  const started = Date.now();
  // Loaded here, not at the top: every hook event loads this module, and only a recall needs a child process.
  const { spawn } = await import("node:child_process");
  return new Promise((resolve) => {
    let done = false;
    let out = "";
    const finish = (rows: UnitCandidate[], failure?: string): void => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      if (failure !== undefined) logTimeout(project, failure, Date.now() - started);
      try {
        child.kill("SIGKILL");
        child.stdout.destroy();
        child.unref();
      } catch { /* already gone */ }
      resolve(rows);
    };
    const timer = setTimeout(() => finish([], "deadline"), deadlineMs);
    const child = spawn(process.execPath, ["-e", CHILD_SOURCE, JSON.stringify({ dbPath: project.dbPath, busyMs: RECALL_BUSY_TIMEOUT_MS, query })], { stdio: ["ignore", "pipe", "ignore"] });
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (d: string) => { out += d; });
    child.once("error", (e) => finish([], "error:" + e.message));
    child.once("close", (code) => {
      if (code !== 0) return finish([], "error:exit " + String(code));
      try {
        finish(JSON.parse(out) as UnitCandidate[]);
      } catch {
        finish([], "error:bad output");
      }
    });
  });
}
