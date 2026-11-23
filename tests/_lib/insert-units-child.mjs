// A separate process that writes one session's units into a shared project store (the concurrency test's writer).
import { closeDb, openDb } from "../../src/core/store/db.ts";
import { insertUnits } from "../../src/core/store/fts.ts";

const [dbPath, projectDir, session, sources, perSource] = process.argv.slice(2);
const project = { projectKey: "c".repeat(16), projectDir, dbPath, worktree: projectDir, containmentRoot: projectDir, agent: "claude-code", source: "env:CLAUDE_PROJECT_DIR" };
const db = await openDb(project);
for (let n = 0; n < Number(sources); n++) {
  const id = Number(db.raw.prepare(
    "INSERT INTO sources(session_id, worktree, agent, kind, label, bytes_total, chunk_count, created_ts, created_at, accessed_ts) VALUES (?, '/w', 'claude-code', 'content', ?, 1, 1, 1, '', 1) RETURNING id",
  ).get(session, "l-" + session + "-" + n).id);
  insertUnits(db, id, session, Array.from({ length: Number(perSource) }, (_, ord) => ({ ord, body: "zebra " + session + " " + n + " " + ord })));
}
closeDb(db);
