// impl/02 §8, A-S1, A-S2, A-S7, ADR-0002 decision 4, DECISIONS-r9 V22, DEVIATIONS D-M1-11, D-M1-T7-1.
// pruneStore: session retention + size cap, run out of band against the open project's own DB.
// sweepDeadProjects: whole-project delete; the live-PID check and the delete decision run in one
// BEGIN IMMEDIATE (#880 race class). Never on a timer: the caller runs both once (D-M1-11).
import { existsSync, readdirSync, rmSync, statSync } from "node:fs";
import { join } from "node:path";
import type { DatabaseSync } from "node:sqlite";
import {
  DB_FILE_NAME,
  DB_SHM_SUFFIX,
  DB_TIMEOUT_MS,
  DB_WAL_SUFFIX,
  INCREMENTAL_VACUUM_PAGES,
  MS_PER_DAY,
  PROJECT_DEAD_IDLE_DAYS,
  PROJECTS_DIR_NAME,
  RETENTION_IDLE_DAYS,
  RETENTION_SIZE_CAP_BYTES,
} from "../contracts/constants.ts";
import { CtxError } from "../contracts/errors.ts";
import type { DbHandle } from "../contracts/types.ts";
import { closeDb, withWrite } from "./db.ts";
import { deleteChunkRows } from "./fts.ts";
import { listLiveOwners } from "./owners.ts";
import { applyMigrations } from "./schema.ts";

export interface PruneOptions {
  now?: number;
  ttlDays?: number;
  capBytes?: number;
  /** Directories of old per-session database files (v1 sessions/); aged out and counted against the cap. */
  legacyDirs?: readonly string[];
}

export interface PruneResult { sessionsDeleted: number; legacyFilesDeleted: number; usedBytes: number }

const ACTIVE_GRACE_MS = 3600000; // a session active within the last hour is never evicted for the cap

function pragmaNumber(raw: DatabaseSync, name: string): number {
  const row = raw.prepare("PRAGMA " + name).get() as Record<string, number>;
  return Number(Object.values(row)[0]);
}

function usedDbBytes(raw: DatabaseSync): number {
  return (pragmaNumber(raw, "page_count") - pragmaNumber(raw, "freelist_count")) * pragmaNumber(raw, "page_size");
}

function legacyFiles(dirs: readonly string[]): Array<{ path: string; size: number; mtime: number }> {
  const out: Array<{ path: string; size: number; mtime: number }> = [];
  for (const dir of dirs) {
    let names: string[];
    try {
      names = readdirSync(dir);
    } catch {
      continue;
    }
    for (const name of names) {
      try {
        const st = statSync(join(dir, name));
        if (st.isFile()) out.push({ path: join(dir, name), size: st.size, mtime: st.mtimeMs });
      } catch { /* gone between the listing and the stat */ }
    }
  }
  return out;
}

function lastActiveBySession(raw: DatabaseSync): Array<{ session: string; at: number }> {
  const rows = raw.prepare(
    "SELECT session_id AS s, MAX(t) AS t FROM (" +
    "SELECT session_id, MAX(accessed_ts) AS t FROM sources GROUP BY session_id UNION ALL " +
    "SELECT session_id, MAX(created_at) AS t FROM events GROUP BY session_id) GROUP BY session_id ORDER BY t ASC",
  ).all() as Array<{ s: string; t: number }>;
  return rows.map((r) => ({ session: r.s, at: Number(r.t) }));
}

// One session leaves the store whole: chunks, trigram rows, its unit table, sources and journal rows.
function deleteSession(db: DbHandle, session: string): void {
  withWrite(db, (h) => {
    deleteChunkRows(h.raw, { session });
    h.raw.prepare("DELETE FROM sources WHERE session_id = ?").run(session);
    h.raw.prepare("DELETE FROM events WHERE session_id = ?").run(session);
  });
}

/**
 * Retention, run out of band (the server, at most hourly; never a hook). A session with no activity for ttlDays is
 * deleted whole; legacy per-session files older than that go too; then, while the store (context.db's live pages
 * plus the legacy files) is over capBytes, the least recently active session or file goes first. Freed pages are
 * returned to the filesystem with an incremental vacuum.
 */
export function pruneStore(db: DbHandle, opts: PruneOptions = {}): PruneResult {
  const now = opts.now ?? Date.now();
  const cutoff = now - (opts.ttlDays ?? RETENTION_IDLE_DAYS) * MS_PER_DAY;
  const cap = opts.capBytes ?? RETENTION_SIZE_CAP_BYTES;
  const legacyDirs = opts.legacyDirs ?? [];
  let sessionsDeleted = 0;
  let legacyFilesDeleted = 0;

  // INCREMENTAL auto_vacuum only applies to a database built with it or rebuilt once by VACUUM.
  if (pragmaNumber(db.raw, "auto_vacuum") !== 2) {
    db.raw.exec("PRAGMA auto_vacuum = INCREMENTAL");
    db.raw.exec("VACUUM");
  }

  // The shared table an older build wrote is dead weight here (nothing reads it); an older build still running may refill it.
  if (db.raw.prepare("SELECT 1 AS x FROM sqlite_master WHERE type = 'table' AND name = 'line_units'").get() !== undefined) {
    withWrite(db, (h) => { h.raw.exec("DELETE FROM line_units"); });
  }

  for (const s of lastActiveBySession(db.raw)) {
    if (s.at >= cutoff) break;
    deleteSession(db, s.session);
    sessionsDeleted += 1;
  }

  let files = legacyFiles(legacyDirs).sort((a, b) => a.mtime - b.mtime);
  for (const f of files) {
    if (f.mtime >= cutoff) break;
    try {
      rmSync(f.path, { force: true });
      legacyFilesDeleted += 1;
    } catch { /* kept; counted below */ }
  }
  files = legacyFiles(legacyDirs).sort((a, b) => a.mtime - b.mtime);

  let legacyBytes = files.reduce((n, f) => n + f.size, 0);
  for (const f of files) {
    if (usedDbBytes(db.raw) + legacyBytes <= cap) break;
    rmSync(f.path, { force: true });
    legacyBytes -= f.size;
    legacyFilesDeleted += 1;
  }
  for (const s of lastActiveBySession(db.raw)) {
    if (usedDbBytes(db.raw) + legacyBytes <= cap) break;
    if (s.at >= now - ACTIVE_GRACE_MS) break;
    deleteSession(db, s.session);
    db.raw.exec("PRAGMA incremental_vacuum(" + String(INCREMENTAL_VACUUM_PAGES) + ")");
    sessionsDeleted += 1;
  }
  db.raw.exec("PRAGMA incremental_vacuum(" + String(INCREMENTAL_VACUUM_PAGES) + ")");
  db.raw.exec("PRAGMA wal_checkpoint(PASSIVE)");
  return { sessionsDeleted, legacyFilesDeleted, usedBytes: usedDbBytes(db.raw) + legacyBytes };
}

function userVersion(raw: DatabaseSync): number {
  return (raw.prepare("PRAGMA user_version").get() as { user_version: number }).user_version;
}

// §8.2 step 2: openDb-equivalent access, synchronous (D-M1-T7-1). Same checks as impl/02 §6.1
// steps 4-7; the caller already passed openDb's Node gate for its own project.
function openForSweep(dbPath: string, projectKey: string): DbHandle {
  if (!existsSync(dbPath) && (existsSync(dbPath + DB_WAL_SUFFIX) || existsSync(dbPath + DB_SHM_SUFFIX))) {
    throw new CtxError("DB_GONE");
  }
  const sqlite = process.getBuiltinModule("node:sqlite") as typeof import("node:sqlite") | undefined;
  if (sqlite === undefined) throw new CtxError("NO_SQLITE");
  const raw = new sqlite.DatabaseSync(dbPath, { timeout: DB_TIMEOUT_MS });
  try {
    applyMigrations(raw, userVersion(raw));
  } catch (err) {
    raw.close();
    throw err;
  }
  return { raw, projectKey, path: dbPath };
}

// D-M1-11: last access = MAX(sources.accessed_ts, events.created_at); an empty project = 0.
function lastAccess(raw: DatabaseSync): number {
  const row = raw
    .prepare(
      "SELECT MAX(COALESCE((SELECT MAX(accessed_ts) FROM sources), 0), COALESCE((SELECT MAX(created_at) FROM events), 0)) AS t",
    )
    .get() as { t: number };
  return row.t;
}

// §8.2 steps 2-4 for one project. The live-PID check and the age check run inside one
// BEGIN IMMEDIATE; a live owner, any age, means keep (V22). The directory goes after closeDb
// (step 4 order: Windows cannot delete an open file).
function sweepOne(configDir: string, projectKey: string, cutoff: number): boolean {
  const dir = join(configDir, PROJECTS_DIR_NAME, projectKey);
  const db = openForSweep(join(dir, DB_FILE_NAME), projectKey);
  let dead: boolean;
  try {
    dead = withWrite(db, (h) => listLiveOwners(configDir, projectKey).length === 0 && lastAccess(h.raw) < cutoff);
  } finally {
    closeDb(db);
  }
  if (dead) rmSync(dir, { recursive: true, force: true });
  return dead;
}

/** impl/02 §8.2: delete whole projects with no live owner and no access for PROJECT_DEAD_IDLE_DAYS. */
export function sweepDeadProjects(configDir: string): string[] {
  let entries;
  try {
    entries = readdirSync(join(configDir, PROJECTS_DIR_NAME), { withFileTypes: true });
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw err;
  }
  const cutoff = Date.now() - PROJECT_DEAD_IDLE_DAYS * MS_PER_DAY;
  const deleted: string[] = [];
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    try {
      if (sweepOne(configDir, entry.name, cutoff)) deleted.push(entry.name);
    } catch (err) {
      // §8.2 errors: a corrupt or busy project is skipped, never deleted; the loop goes on.
      console.error("context-mode: cleanup skipped project " + entry.name + ": " + (err as Error).message);
    }
  }
  return deleted;
}
