// Line units live in one FTS5 table per session. Recall reads only its own session's table, under a deadline, and
// retention deletes whole sessions. These tests write through real processes and real worker threads.
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { UNIT_MAX_ROWS_PER_SESSION } from "../src/core/contracts/constants.ts";
import { closeDb, openDb } from "../src/core/store/db.ts";
import { deleteChunkRows, insertUnits, queryUnits, unitMatch, unitQuery, unitTableName } from "../src/core/store/fts.ts";
import { pruneStore } from "../src/core/store/cleanup.ts";
import { recallBudgetMs, recallSkipReason, recallTierOne } from "../src/core/hooks/recall.ts";

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

test("units go to a per-session table and index on sources(session_id, id); the legacy shared table stays untouched", async () => {
  await withStore(({ db }) => {
    insertUnits(db, addSource(db, "A"), "A", units("A", 3));
    assert.equal(db.raw.prepare("SELECT count(*) AS n FROM sqlite_master WHERE name = 'idx_sources_session_id'").get().n, 1);
    assert.equal(db.raw.prepare("SELECT count(*) AS n FROM line_units").get().n, 0, "an older build's table is neither read nor written");
    assert.equal(db.raw.prepare("PRAGMA user_version").get().user_version, 6);
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

test("tier 1 returns the session's ranked units in process", async () => {
  await withStore(async ({ project, db }) => {
    insertUnits(db, addSource(db, "A"), "A", units("A", 25));
    const ro = await openDb(project, { readOnly: true });
    const t1 = recallTierOne(ro, { sessionId: "A", match: unitMatch("zebra marker"), maxSourceId: 1e9, limit: 8, prelimit: 500 }, 900);
    assert.equal(t1.units.length, 8);
    assert.equal(t1.interrupted, false);
    assert.ok(t1.units.every((u) => u.body.includes(" A ")));
    closeDb(ro);
  });
});

test("past its deadline a query aborts mid-statement and tier 1 reports the interrupt with no units", async () => {
  await withStore(async ({ project, db }) => {
    insertUnits(db, addSource(db, "A"), "A", Array.from({ length: 3000 }, (_, ord) => ({ ord, body: "zebra row " + ord })));
    const ro = await openDb(project, { readOnly: true });
    assert.throws(() => unitQuery(ro.raw, { sessionId: "A", match: "zebra", maxSourceId: 1e9, limit: 8, prelimit: 500, deadline: performance.now() - 1 }), /recall deadline/);
    const t1 = recallTierOne(ro, { sessionId: "A", match: "zebra", maxSourceId: 1e9, limit: 8, prelimit: 500 }, 0);
    assert.deepEqual(t1.units, []);
    closeDb(ro);
  });
});

test("the recall budget is min(900, 3500 - elapsed - 500) and prompts without signal skip recall", () => {
  assert.equal(recallBudgetMs(100), 900);
  assert.equal(recallBudgetMs(2500), 500);
  assert.ok(recallBudgetMs(3200) <= 0);
  assert.equal(recallSkipReason("continue"), "confirmation");
  assert.equal(recallSkipReason("ok."), "confirmation");
  assert.equal(recallSkipReason("/compact"), "slash");
  assert.equal(recallSkipReason("why now"), "short");
  assert.equal(recallSkipReason("why did the deploy roll back on canary-3"), null);
});

test("the read-only prompt path cannot write: a write on the recall connection fails", async () => {
  await withStore(async ({ project }) => {
    const ro = await openDb(project, { readOnly: true });
    assert.throws(() => ro.raw.exec("INSERT INTO meta(key, value) VALUES ('x', 'y')"));
    closeDb(ro);
  });
});

test("a session over its byte cap evicts its oldest units down to the low mark, tracked in session_bytes", async () => {
  await withStore(({ db }) => {
    const id = addSource(db, "A");
    const caps = { cap: 100000, low: 60000 };
    for (let round = 0; round < 4; round++) insertUnits(db, id, "A", Array.from({ length: 100 }, (_, i) => ({ ord: round * 100 + i, body: "w".repeat(300) + " " + (round * 100 + i) })), caps);
    const tracked = db.raw.prepare("SELECT total_bytes AS n FROM session_bytes WHERE session_id = 'A'").get().n;
    const actual = db.raw.prepare(`SELECT SUM(LENGTH(CAST(c0 AS BLOB))) AS n FROM "${unitTableName("A")}_content"`).get().n;
    assert.equal(tracked, actual, "the tracked total matches the stored text");
    assert.ok(tracked <= caps.cap && tracked > 0);
    assert.equal(db.raw.prepare(`SELECT max(ord) AS m FROM "${unitTableName("A")}"`).get().m, 399, "the newest unit stays");
    assert.ok(db.raw.prepare(`SELECT min(ord) AS m FROM "${unitTableName("A")}"`).get().m > 0, "the oldest went first");
    deleteChunkRows(db.raw, { session: "A" });
    assert.equal(db.raw.prepare("SELECT count(*) AS n FROM session_bytes WHERE session_id = 'A'").get().n, 0);
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
    db.raw.prepare("INSERT INTO line_units(body, source_id, session_id, ord) VALUES ('zebra', 1, 'live', 0)").run();
    const result = pruneStore(db, { legacyDirs: [legacy] });
    assert.equal(db.raw.prepare("SELECT count(*) AS n FROM line_units").get().n, 0, "the shared table an older build wrote is emptied");
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
    const result = pruneStore(db, { highBytes: 1, lowBytes: 1, now });
    assert.equal(result.sessionsDeleted, 2);
    assert.equal(queryUnits(db, "newest", "zebra", 10).length, 10, "the session active within the grace window stays");
    assert.equal(queryUnits(db, "oldest", "zebra", 10).length, 0);
  });
});

test("the hot window searches only the session's newest rows and loses none inside it", async () => {
  await withStore(async ({ project, db }) => {
    insertUnits(db, addSource(db, "A"), "A", Array.from({ length: 300 }, (_, ord) => ({ ord, body: "zebra unit " + ord })));
    const ro = await openDb(project, { readOnly: true });
    const q = { sessionId: "A", match: "zebra", maxSourceId: 1e9, limit: 1000, prelimit: 500, deadline: performance.now() + 5000 };
    const ords = (rows) => rows.map((u) => u.ord).sort((a, b) => a - b);
    assert.equal(unitQuery(ro.raw, { ...q, window: 100 }).length, 100);
    assert.deepEqual(ords(unitQuery(ro.raw, { ...q, window: 100 })), Array.from({ length: 100 }, (_, i) => 200 + i), "exactly the newest 100");
    assert.equal(unitQuery(ro.raw, { ...q, window: 8000 }).length, 300, "a window wider than the table loses nothing");
    closeDb(ro);
  });
});

test("a prompt on a project with no store yet is allowed: recall fails open instead of failing the hook", async () => {
  const { handleHook } = await import("../src/core/hooks/dispatch.ts");
  const root = mkdtempSync(join(tmpdir(), "cm-failopen-"));
  const old = process.env.HOME;
  try {
    const out = await handleHook("claude-code", { hook_event_name: "UserPromptSubmit", session_id: "s-new", cwd: root, prompt: "what did the deploy script print earlier" }, { HOME: root, USERPROFILE: root, CLAUDE_PROJECT_DIR: root, PATH: process.env.PATH });
    assert.deepEqual(out, { stdout: "", exitCode: 0 });
  } finally {
    process.env.HOME = old;
    rmSync(root, { recursive: true, force: true });
  }
});

test("a store created without INCREMENTAL auto_vacuum is rebuilt once, then reclaim returns freed pages to the filesystem", async () => {
  const { migrateAutoVacuum, reclaim } = await import("../src/core/store/cleanup.ts");
  const { DatabaseSync } = await import("node:sqlite");
  const root = mkdtempSync(join(tmpdir(), "cm-av-"));
  try {
    const path = join(root, "context.db");
    const raw = new DatabaseSync(path);
    raw.exec("PRAGMA journal_mode = WAL; CREATE TABLE t(x BLOB)");
    const ins = raw.prepare("INSERT INTO t VALUES (?)");
    for (let i = 0; i < 400; i++) ins.run(Buffer.alloc(8192, i));
    raw.exec("DELETE FROM t; PRAGMA wal_checkpoint(TRUNCATE)");
    const before = statSync(path).size;
    const db = { raw, path, projectKey: "k" };
    assert.equal(raw.prepare("PRAGMA auto_vacuum").get().auto_vacuum, 0);
    assert.ok(raw.prepare("PRAGMA freelist_count").get().freelist_count > 0);
    // At auto_vacuum 0 reclaim cannot shrink the file: it leaves the free pages (the P0 a pruning loop depended on).
    reclaim(db);
    assert.equal(statSync(path).size, before);
    assert.equal(migrateAutoVacuum(db, () => 95), false, "a busy disk defers the rebuild");
    assert.equal(raw.prepare("PRAGMA auto_vacuum").get().auto_vacuum, 0);
    assert.equal(migrateAutoVacuum(db, () => 0), true);
    assert.equal(raw.prepare("PRAGMA auto_vacuum").get().auto_vacuum, 2);
    assert.ok(statSync(path).size < before / 4, "the rebuild returned the free pages");
    // Now deletes free pages that reclaim returns.
    for (let i = 0; i < 400; i++) ins.run(Buffer.alloc(8192, i));
    raw.exec("PRAGMA wal_checkpoint(TRUNCATE)");
    const full = statSync(path).size;
    raw.exec("DELETE FROM t; PRAGMA wal_checkpoint(TRUNCATE)");
    reclaim(db, 1000, () => 0);
    assert.ok(statSync(path).size < full / 4, "reclaim shrank the file");
    raw.close();
  } finally { rmSync(root, { recursive: true, force: true }); }
});
