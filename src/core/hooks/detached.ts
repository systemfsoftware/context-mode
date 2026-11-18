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
