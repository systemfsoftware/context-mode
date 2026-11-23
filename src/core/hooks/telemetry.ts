// Crash-safe hook telemetry. A START record goes to a tmpfs ndjson file the moment a UserPromptSubmit hook has its input, an END
// record when the process exits. A hook the host kills at 5 s leaves a START with no END; the shipper (a user timer) turns that
// into a "killed_or_over_5s" run and sends everything to OTLP. Nothing here touches the store or blocks: one appendFileSync to
// RAM-backed storage per record, and every failure is swallowed.
// D-CI-PERF-7: node:fs through getBuiltinModule; a static ESM import of node:fs also loads fs/promises, readline and streams.
const { appendFileSync, readFileSync, statSync } = process.getBuiltinModule("node:fs") as typeof import("node:fs");
import { join } from "node:path";
import { TELEMETRY_FILE } from "../contracts/constants.ts";

interface Run {
  id: string;
  event: string;
  startupMs: number;
  phases: Record<string, number>;
  recall: { skip?: string; t1Ms?: number; t2Ms?: number; interrupted?: boolean; hits?: number };
  dbPath?: string;
}

let run: Run | null = null;

function file(): string {
  return join(process.env["XDG_RUNTIME_DIR"] ?? "/tmp", TELEMETRY_FILE);
}

function line(o: object): void {
  try {
    appendFileSync(file(), JSON.stringify(o) + "\n");
  } catch { /* telemetry is a signal, never a failure */ }
}

function loadAvg(): number | null {
  try {
    return Number(readFileSync("/proc/loadavg", "utf8").split(" ")[0]);
  } catch {
    return null;
  }
}

function ioPressure(): number | null {
  try {
    const m = /some avg10=([\d.]+)/.exec(readFileSync("/proc/pressure/io", "utf8"));
    return m === null ? null : Number(m[1]);
  } catch {
    return null;
  }
}

function size(path: string): number | null {
  try {
    return statSync(path).size;
  } catch {
    return null;
  }
}

/** Mark the run's start (once). startup_ms is the time from process start to this call: runtime start and module load. */
export function telemetryStart(event: string, sessionId: string): void {
  if (run !== null || process.env["CONTEXT_MODE_TELEMETRY"] === "0" || event !== "UserPromptSubmit") return;
  const id = String(process.pid) + "-" + String(Date.now());
  run = { id, event, startupMs: performance.now(), phases: {}, recall: {} };
  line({ k: "S", id, pid: process.pid, ts: Date.now(), session: sessionId, startup_ms: Math.round(run.startupMs), load1: loadAvg(), io_psi: ioPressure() });
  process.once("exit", () => end());
}

export function telemetryDb(dbPath: string): void {
  if (run !== null) run.dbPath = dbPath;
}

/** Record a phase's duration in ms (db_open, recall_t1, ...). */
export function telemetryPhase(name: string, ms: number): void {
  if (run !== null) run.phases[name] = Math.round(ms * 10) / 10;
}

export function telemetryRecall(info: Run["recall"]): void {
  if (run !== null) Object.assign(run.recall, info);
}

function end(): void {
  if (run === null) return;
  const r = run;
  run = null;
  const total = performance.now();
  const db = r.dbPath === undefined ? null : size(r.dbPath);
  const wal = r.dbPath === undefined ? null : size(r.dbPath + "-wal");
  line({
    k: "E", id: r.id, ts: Date.now(), total_ms: Math.round(total), startup_ms: Math.round(r.startupMs), phases: r.phases, recall: r.recall,
    status: process.exitCode === undefined || process.exitCode === 0 ? "ok" : "error", db_bytes: db, wal_bytes: wal, load1: loadAvg(), io_psi: ioPressure(),
  });
}
