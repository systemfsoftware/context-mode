// The hook's unit recall under a wall-clock governor. node:sqlite has no progress handler, so a query is bounded
// cooperatively: unitQuery registers a SQL function that throws once the deadline has passed and calls it per candidate row,
// which aborts the statement. Time is measured from process start (performance.now), so runtime startup counts against the
// hook's ceiling. Recall runs in the hook process on a read-only connection; it writes nothing.
import { HOOK_CEILING_MS, RECALL_BUDGET_MS, RECALL_HOT_WINDOW, RECALL_MIN_PROMPT_CHARS, RECALL_RESERVE_MS, RECALL_TIER1_MS } from "../contracts/constants.ts";
import type { DbHandle, UnitCandidate } from "../contracts/types.ts";
import { unitQuery, type UnitQuery } from "../store/fts.ts";

// The governor's clock. A hook process measures from process start (runtime startup counts against the ceiling); a host that
// runs hooks in its own long-lived process (OpenCode, Pi) measures from each hook's entry.
let processClock = false;
let base = 0;

/** The hook CLI entry: time runs from process start. */
export function useProcessClock(): void {
  processClock = true;
}

/** A hook begins: with the process clock, base stays 0 (process start); otherwise it is now. */
export function beginHook(): void {
  base = processClock ? 0 : performance.now();
}

export function hookElapsedMs(): number {
  return performance.now() - base;
}

/** ms left for recall: min(RECALL_BUDGET_MS, ceiling - elapsed - reserve); 0 or less means none. */
export function recallBudgetMs(elapsedMs: number = hookElapsedMs()): number {
  return Math.min(RECALL_BUDGET_MS, HOOK_CEILING_MS - elapsedMs - RECALL_RESERVE_MS);
}

const CONFIRMATIONS = new Set(["continue", "yes", "no", "ok", "okay", "y", "n", "go", "go on", "proceed", "thanks", "thank you", "sure", "done", "next", "lgtm", "yep", "nope"]);

/** A prompt with too little signal to search for: null when recall should run, else the skip reason. */
export function recallSkipReason(prompt: string): string | null {
  const p = prompt.trim();
  if (p.startsWith("/")) return "slash";
  if (CONFIRMATIONS.has(p.toLowerCase().replace(/[.!]+$/, ""))) return "confirmation";
  if (p.length < RECALL_MIN_PROMPT_CHARS) return "short";
  return null;
}

export interface TierOne { units: UnitCandidate[]; interrupted: boolean; ms: number }

/** Tier 1: the current session's units within min(RECALL_TIER1_MS, budget). An interrupt keeps nothing (a ranked query returns at the end). */
export function recallTierOne(db: DbHandle, query: UnitQuery, budgetMs: number): TierOne {
  const started = performance.now();
  const limit = Math.min(RECALL_TIER1_MS, budgetMs);
  if (limit <= 0 || query.match === "" || query.sessionId === "") return { units: [], interrupted: false, ms: 0 };
  try {
    const units = unitQuery(db.raw, { ...query, deadline: started + limit, window: RECALL_HOT_WINDOW });
    return { units, interrupted: false, ms: performance.now() - started };
  } catch (err) {
    const interrupted = /recall deadline/.test(String((err as Error).message));
    if (!interrupted) process.stderr.write("hooks: recall failed: " + String((err as Error).message) + "\n");
    return { units: [], interrupted, ms: performance.now() - started };
  }
}
