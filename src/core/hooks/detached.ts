// impl/09 §3 (appendDetached), A21; 00 rule "Journal writes from hooks are detached" (GP3, A32);
// DEVIATIONS D-M3-H1-3.
import { openDb } from "../store/db.ts";
import { appendEvent } from "../journal/append.ts";
import type { CanonicalHookEvent, Chunk, NewJournalRow, ProjectContext, ToolCall } from "../contracts/types.ts";

export type DetachedJob =
  | { kind: "append"; project: ProjectContext; row: NewJournalRow }
  | { kind: "index"; project: ProjectContext; sessionId: string; sourceId: number; label: string; text: string; chunks: Chunk[] }
  | { kind: "echo"; ev: CanonicalHookEvent; project: ProjectContext; prompt: string }
  | { kind: "ask"; ev: CanonicalHookEvent; project: ProjectContext; tool: ToolCall; text: string };

let sink: ((job: DetachedJob) => void) | null = null;

export function deferDetachedTo(fn: (job: DetachedJob) => void): void {
  sink = fn;
}

export function detach(job: DetachedJob, inProcess: () => Promise<void>): void {
  if (sink !== null) sink(job);
  else void inProcess();
}

export const DETACHED_JOBS_ARG = "--detached-jobs";
const JOB_FILE_PREFIX = "detached-";
const JOB_FILE_SUFFIX = ".json";
const SETTLE_POLL_MS = 50;
const SETTLE_MAX_MS = 15000;
const JOB_FILE_STALE_MS = 120000;

export async function detachedSettled(project: ProjectContext): Promise<void> {
  const { readdirSync, statSync } = process.getBuiltinModule("node:fs") as typeof import("node:fs");
  const { join } = process.getBuiltinModule("node:path") as typeof import("node:path");
  const end = Date.now() + SETTLE_MAX_MS;
  for (;;) {
    let pending = false;
    try {
      for (const n of readdirSync(project.projectDir)) {
        if (!n.startsWith(JOB_FILE_PREFIX) || !n.endsWith(JOB_FILE_SUFFIX)) continue;
        try {
          if (Date.now() - statSync(join(project.projectDir, n)).mtimeMs < JOB_FILE_STALE_MS) pending = true;
        } catch {
          continue;
        }
      }
    } catch {
      return;
    }
    if (!pending || Date.now() > end) return;
    await new Promise((r) => setTimeout(r, SETTLE_POLL_MS));
  }
}

export function spawnDetached(batch: DetachedJob[], entry: string): void {
  const { spawn } = process.getBuiltinModule("node:child_process") as typeof import("node:child_process");
  const { writeFileSync } = process.getBuiltinModule("node:fs") as typeof import("node:fs");
  const { randomBytes } = process.getBuiltinModule("node:crypto") as typeof import("node:crypto");
  const { join } = process.getBuiltinModule("node:path") as typeof import("node:path");
  try {
    const file = join(batch[0]!.project.projectDir, JOB_FILE_PREFIX + process.pid + "-" + randomBytes(6).toString("hex") + JOB_FILE_SUFFIX);
    writeFileSync(file, JSON.stringify(batch), { mode: 0o600 });
    spawn(process.execPath, [entry, DETACHED_JOBS_ARG, file], { detached: true, stdio: "ignore", windowsHide: true }).unref();
  } catch (err) {
    process.stderr.write("hooks: detached write failed: " + (err instanceof Error ? err.message : String(err)) + "\n");
  }
}

// §3.2 steps 1-2: open and append; every error ends here as one stderr line. No closeDb (A21):
// the hook process exits right after encode, and a checkpoint would make this write blocking.
export async function writeJournal(project: ProjectContext, row: NewJournalRow): Promise<void> {
  try {
    const db = await openDb(project);
    appendEvent(db, row);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    process.stderr.write("hooks: journal write failed: " + message + "\n");
  }
}

/**
 * Fire-and-forget journal append. Returns before the row is written; never throws, never rejects.
 * `sessionId` is the session the row belongs to; the row itself carries it in `session_id`.
 */
export function appendDetached(project: ProjectContext, sessionId: string, row: NewJournalRow): void {
  void sessionId;
  detach({ kind: "append", project, row }, () => writeJournal(project, row));
}
