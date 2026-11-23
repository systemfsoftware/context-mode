// Line units live in one FTS5 table per session. Recall reads only its own session's table, under a deadline, and
// retention deletes whole sessions. These tests write through real processes and real worker threads.
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { UNIT_MAX_ROWS_PER_SESSION, RECALL_TIMEOUT_LOG } from "../src/core/contracts/constants.ts";
import { closeDb, openDb } from "../src/core/store/db.ts";
import { deleteChunkRows, insertUnits, queryUnits, unitMatch, unitTableName } from "../src/core/store/fts.ts";
import { pruneStore } from "../src/core/store/cleanup.ts";
import { recallUnits } from "../src/core/hooks/recall.ts";

const run = promisify(execFile);

function projectIn(root) {
  const key = "c".repeat(16);
  return { projectKey: key, projectDir: join(root, "projects", key), dbPath: join(root, "projects", key, "context.db"), worktree: root, containmentRoot: root, agent: "claude-code", source: "env:CLAUDE_PROJECT_DIR" };
}

function addSource(db, session, label = "l") {
  return Number(db.raw.prepare(
    "INSERT INTO sources(session_id, worktree, agent, kind, label, bytes_total, chunk_count, created_ts, created_at, accessed_ts) VALUES (?, '/w', 'claude-code', 'content', ?, 1, 1, 1, '', ?) RETURNING id",
  ).get(session, label, Date.now()).id);
}

const units = (session, n) => Array.from({ length: n }, (_, ord) => ({ ord, body: "zebra " + session + " " + ord }));
const tableCount = (db) => db.raw.prepare("SELECT count(*) AS n FROM sqlite_master WHERE type='table' AND name GLOB 'lu_*' AND name NOT GLOB '*_*_*'").get().n;

async function withStore(fn) {
  const root = mkdtempSync(join(tmpdir(), "cm-session-recall-"));
  const project = projectIn(root);
  const db = await openDb(project);
  try { await fn({ root, project, db }); } finally { closeDb(db); rmSync(root, { recursive: true, force: true }); }
}

test("the store has no shared line_units table and a sources(session_id, id) index", async () => {
  await withStore(({ db }) => {
    assert.equal(db.raw.prepare("SELECT count(*) AS n FROM sqlite_master WHERE name = 'line_units'").get().n, 0);
    assert.equal(db.raw.prepare("SELECT count(*) AS n FROM sqlite_master WHERE name = 'idx_sources_session_id'").get().n, 1);
  });
});

test("each session reads only its own table; a session with none reads nothing", async () => {
  await withStore(({ db }) => {
    insertUnits(db, addSource(db, "A"), "A", units("A", 30));
    insertUnits(db, addSource(db, "B"), "B", units("B", 20));
    assert.equal(tableCount(db), 2);
    const a = queryUnits(db, "A", "zebra", 1000);
    const b = queryUnits(db, "B", "zebra", 1000);
    assert.equal(a.length, 30);
    assert.equal(b.length, 20);
    assert.ok(a.every((u) => u.body.includes(" A ")) && b.every((u) => u.body.includes(" B ")));
    assert.deepEqual(queryUnits(db, "nobody", "zebra", 10), []);
  });
});

test("concurrent processes writing different sessions into one store lose and leak nothing", async () => {
  const root = mkdtempSync(join(tmpdir(), "cm-session-recall-conc-"));
  const project = projectIn(root);
  const db = await openDb(project);
  try {
    const sessions = ["s1", "s2", "s3", "s4", "s5", "s6"];
    const child = join(import.meta.dirname, "_lib", "insert-units-child.mjs");
    await Promise.all(sessions.map((s) => run(process.execPath, [child, project.dbPath, project.projectDir, s, "6", "200"])));
    for (const s of sessions) {
      assert.equal(db.raw.prepare(`SELECT count(*) AS n FROM "${unitTableName(s)}"`).get().n, 6 * 200, "every unit of " + s + " is stored");
      const rows = queryUnits(db, s, "zebra", 5000);
      assert.ok(rows.length > 0 && rows.every((u) => u.body.includes(" " + s + " ")), s + " sees only its own units");
    }
    assert.equal(tableCount(db), sessions.length);
  } finally { closeDb(db); rmSync(root, { recursive: true, force: true }); }
}, { timeout: 120000 });

test("a session's table is capped at its newest rows", async () => {
  await withStore(({ db }) => {
    const id = addSource(db, "A");
    insertUnits(db, id, "A", Array.from({ length: UNIT_MAX_ROWS_PER_SESSION + 100 }, (_, ord) => ({ ord, body: "zebra " + ord })));
    const t = unitTableName("A");
    assert.equal(db.raw.prepare(`SELECT count(*) AS n FROM "${t}"`).get().n, UNIT_MAX_ROWS_PER_SESSION);
    assert.equal(db.raw.prepare(`SELECT min(ord) AS m FROM "${t}"`).get().m, 100, "the oldest units left");
  });
});

test("deleting a source or a session removes its units and nothing else", async () => {
  await withStore(({ db }) => {
    const a1 = addSource(db, "A", "one"); const a2 = addSource(db, "A", "two"); const b1 = addSource(db, "B", "one");
    insertUnits(db, a1, "A", units("A1", 5)); insertUnits(db, a2, "A", units("A2", 7)); insertUnits(db, b1, "B", units("B1", 9));
    deleteChunkRows(db.raw, { sourceId: a1 });
    assert.equal(queryUnits(db, "A", "zebra", 100).length, 7);
    deleteChunkRows(db.raw, { label: "one" });
    assert.equal(queryUnits(db, "B", "zebra", 100).length, 0);
    assert.equal(queryUnits(db, "A", "zebra", 100).length, 7);
    deleteChunkRows(db.raw, { session: "A" });
    assert.equal(tableCount(db), 1 /* B's emptied table remains */);
    assert.equal(queryUnits(db, "A", "zebra", 100).length, 0);
  });
});

test("recall runs in a child process and returns the session's ranked units", async () => {
  await withStore(async ({ project, db }) => {
    insertUnits(db, addSource(db, "A"), "A", units("A", 25));
    closeDb(db);
    const rows = await recallUnits(project, { sessionId: "A", match: unitMatch("zebra marker"), maxSourceId: 1e9, limit: 8, prelimit: 500 });
    assert.equal(rows.length, 8);
    assert.ok(rows.every((u) => u.body.includes(" A ")));
    Object.assign(db, { raw: (await openDb(project)).raw }); // the store's own close() in withStore needs an open handle
  });
});

test("past the deadline recall is empty, the call still resolves, and the timeout is logged", async () => {
  await withStore(async ({ project, db }) => {
    insertUnits(db, addSource(db, "A"), "A", units("A", 25));
    const rows = await recallUnits(project, { sessionId: "A", match: unitMatch("zebra"), maxSourceId: 1e9, limit: 8, prelimit: 500 }, 1);
    assert.deepEqual(rows, []);
    const log = readFileSync(join(project.projectDir, RECALL_TIMEOUT_LOG), "utf8");
    assert.match(log, /deadline \d+ms\n$/);
  });
});

test("a store the worker cannot open fails open to empty recall", async () => {
  await withStore(async ({ root, project }) => {
    const rows = await recallUnits({ ...project, dbPath: join(root, "missing", "context.db") }, { sessionId: "A", match: "zebra", maxSourceId: 1e9, limit: 8, prelimit: 500 });
    assert.deepEqual(rows, []);
  });
});

test("retention deletes sessions idle past the TTL, keeps active ones, and ages legacy files", async () => {
  await withStore(({ root, db }) => {
    const old = Date.now() - 10 * 86400000;
    const oldId = addSource(db, "old"); insertUnits(db, oldId, "old", units("old", 5));
    db.raw.prepare("UPDATE sources SET accessed_ts = ? WHERE session_id = 'old'").run(old);
    insertUnits(db, addSource(db, "live"), "live", units("live", 5));
    const legacy = join(root, "sessions"); mkdirSync(legacy);
    writeFileSync(join(legacy, "stale.db"), "x"); writeFileSync(join(legacy, "fresh.db"), "x");
    utimesSync(join(legacy, "stale.db"), new Date(old), new Date(old));
    const result = pruneStore(db, { legacyDirs: [legacy] });
    assert.equal(result.sessionsDeleted, 1);
    assert.equal(result.legacyFilesDeleted, 1);
    assert.equal(queryUnits(db, "old", "zebra", 10).length, 0);
    assert.equal(queryUnits(db, "live", "zebra", 10).length, 5);
    assert.ok(!existsSync(join(legacy, "stale.db")) && existsSync(join(legacy, "fresh.db")));
    assert.equal(db.raw.prepare("PRAGMA auto_vacuum").get().auto_vacuum, 2);
  });
});

test("over the size cap the least recently active session goes first, the newest stays", async () => {
  await withStore(({ db }) => {
    const now = Date.now();
    for (const [s, age] of [["oldest", 3], ["middle", 2], ["newest", 0]]) {
      insertUnits(db, addSource(db, s), s, units(s, 200));
      db.raw.prepare("UPDATE sources SET accessed_ts = ? WHERE session_id = ?").run(now - age * 86400000, s);
    }
    const result = pruneStore(db, { capBytes: 1, now });
    assert.equal(result.sessionsDeleted, 2);
    assert.equal(queryUnits(db, "newest", "zebra", 10).length, 10, "the session active within the grace window stays");
    assert.equal(queryUnits(db, "oldest", "zebra", 10).length, 0);
  });
});
