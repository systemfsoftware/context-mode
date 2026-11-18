// impl/09 §5.2, impl/00 §5.3: read stdin, parse, handleHook, write the native output, exit.
// D-HNE-GUARD-1 (owner decision 2026-10-29): a hook never errors. Any throw ends in failureOutput: exit 0, this host's shape,
// ctx_* allowed, flooding tools denied with the redirect. The build allows static imports only (one bundle, no module can go
// missing at run time); node:sqlite is the one lazy import and lives inside handleHook's guarded store stage.
// Agent-specific output rules (Kiro prompt stdout) live in the adapter's encode (A28), not here.
import type { HookOutput } from "../core/contracts/types.ts";
import { AGENT_IDS, type AgentId } from "../core/contracts/agents.ts";
import { AGENT_ARG } from "../core/contracts/constants.ts";
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { deferDetachedTo, type DetachedJob, failureOutput, handleHook, logHookError, readStdin, runDetachedJob, writeOut } from "../core/hooks/dispatch.ts";

const DETACHED_JOBS_ARG = "--detached-jobs";
const jobs: DetachedJob[] = [];

const EMPTY: HookOutput = { stdout: "", exitCode: 0 };

function isAgentId(s: string): s is AgentId {
  return (AGENT_IDS as readonly string[]).includes(s);
}

// D-M3-H2-8: no process.exit. The answer goes out first; the process then ends when the event loop is
// empty, so a detached journal or index write (dispatch §4.4, §4.6) lands instead of being cut off.
function finish(out: HookOutput): void {
  process.exitCode = out.exitCode;
  if (out.stdout !== "") writeOut(out.stdout);
  if (out.stderr !== undefined && out.stderr !== "") process.stderr.write(out.stderr + "\n"); // kiro deny (D-HF-KIRO-SHELL-1)
  if (jobs.length > 0) spawnJobs(jobs.splice(0));
}

function spawnJobs(batch: DetachedJob[]): void {
  try {
    const file = join(batch[0]!.project.projectDir, "detached-" + process.pid + "-" + randomBytes(6).toString("hex") + ".json");
    writeFileSync(file, JSON.stringify(batch), { mode: 0o600 });
    spawn(process.execPath, [process.argv[1]!, DETACHED_JOBS_ARG, file], { detached: true, stdio: "ignore" }).unref();
  } catch (err) {
    process.stderr.write("hooks: detached write failed: " + (err instanceof Error ? err.message : String(err)) + "\n");
  }
}

async function runJobs(file: string): Promise<void> {
  const batch = JSON.parse(readFileSync(file, "utf8")) as DetachedJob[];
  unlinkSync(file);
  for (const job of batch) await runDetachedJob(job);
}

/** The failure path: log with the stack, then the host-shaped decision. If even this cannot load, allow (never an exit != 0). */
function failed(agent: AgentId, raw: unknown, stage: string, err: unknown, env: NodeJS.ProcessEnv): HookOutput {
  process.stderr.write("hooks: " + stage + " failed: " + (err instanceof Error ? err.message : String(err)) + "\n");
  try {
    logHookError(env, agent, stage, err);
    return failureOutput(agent, raw, err);
  } catch {
    return EMPTY;
  }
}

export async function main(argv: string[], env: NodeJS.ProcessEnv): Promise<void> {
  if (argv[0] === DETACHED_JOBS_ARG) return runJobs(argv[1] ?? "");
  deferDetachedTo((job) => jobs.push(job));
  // Step 1: --agent <id>; unknown -> allow (no host shape to answer in).
  const i = argv.indexOf(AGENT_ARG);
  const agent = i >= 0 ? argv[i + 1] ?? "" : "";
  if (!isAgentId(agent)) {
    console.error("hooks: unknown " + AGENT_ARG + " " + JSON.stringify(agent) + ". Fix: pass " + AGENT_ARG + " with one of " + AGENT_IDS.join(", ") + ".");
    return finish(EMPTY);
  }
  // Steps 2-3: stdin, JSON. Unreadable input names no tool, so the answer is this host's allow shape, and the error is logged.
  let raw: unknown;
  try {
    raw = JSON.parse(await readStdin());
  } catch (err) {
    return finish(failed(agent, undefined, "stdin", err, env));
  }
  // Steps 4-5: handleHook guards each stage itself; this catch covers a throw while dispatch loads and anything unforeseen.
  try {
    return finish(await handleHook(agent, raw, env));
  } catch (err) {
    return finish(failed(agent, raw, "load", err, env));
  }
}

void main(process.argv.slice(2), process.env);
