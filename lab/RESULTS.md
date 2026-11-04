# Results

Tool calls and minutes per run, as reported by the agent harness. Checks are from `grade.py` (9 total); every report was also read by hand.

| Version | Change | Runs (tool calls) | Median calls | Median minutes | Checks | Kept? |
|---|---|---|---|---|---|---|
| i0 | baseline (skill as on #144) | 49, 64, 50 | 50 | 33 (27, 34, 33) | 9/9 ×3 | — |
| i1 | skill: "Running Aquifer locally" (build/run, where every setting is, how `rps` and `max_concurrent` combine) | 38, 41, 41 | 41 (−18%) | 29 (29, 29, 18) | 9/9 ×3 (hand-graded; see notes) | yes |
| ref2 | i1 skill, new prompt (reference) | 37, 39, 36 | 37 | — | 9/9 ×3 (ref2-r2 hand-graded: mentions flush=0 only to flag a docs inconsistency) | — |
| i2 | version check on source builds, laptop notes (bind, `.l8-key`), API.md section map, limit details | 38, 33, 43 | 38 (+3%) | — | 9/9 ×3 | no (failed try 1/5); reverted |

## i0 notes

All three reports were accurate and honest (read by hand). Shared friction: each agent spent 8–11 calls reading Aquifer's Go source (`cmd/aquifer/main.go`, `internal/dispatch/*`) to learn how to run Aquifer locally (env vars, YAML) and how `rps` / `max_concurrent` combine across tenant queues. The env var table exists in `docs/pages/configuration.md`, but the skill never points there and the root `CONFIGURATION.md` is only rationale. Run 2 also spent ~15 calls iterating its own header-splitting scheme (v1–v6) before settling.

## i1 notes

Median tool calls fell from 50 to 41 (−18%); reads of Aquifer's Go source fell from about 5 per run to 1–2. All three reports were correct by hand.

- `grade.py` flagged i1-r1 for flush=0 advice, but the report warns *against* it. Fixed the check to ignore warnings.
- i1-r2 did not write `REPORT.md`: the harness blocks subagents from writing report files, and it returned the report in its reply instead. The other runs (i0 and i1) hit the same block on the Write tool and wrote the file through Bash. Graded by hand from the reply. Not archived pending a decision on the protocol.
- New gaps found by i1-r2: a source build prints `aquifer dev`, so the skill's version check fails; no documented way to bind to loopback only; `aquifer_flow_rate_rps` appears to over-report (126–210 vs ~70 real); redistribution of unused `rps` share isn't documented.

## Protocol change after i1

From i2 on, agents return the report as their final reply instead of writing `REPORT.md` (the harness blocks subagents from writing report files), and I save the reply as `REPORT.md`. Because that changes the task, each try is now run side by side with 3 runs of the current best skill (`ref`) under the same prompt, and compared against those.

## Counting

Tool calls are counted from each run's transcript (every tool_use, including ToolSearch). The harness's own `tool_uses` resets when a run is resumed, so after the usage-limit interruption (five i2/ref2 runs were cut off and resumed) it can't be used. Recounted this way, i0 = 49, 65, 50 (median 50) and i1 = 38, 47, 45 (median 45, −10%, so i1 only just clears the bar on this count; the harness counted 38, 41, 41).

## i2 notes

No gain. Its accuracy fixes are worth keeping outside the speed measurement (recommended doc fixes, not part of the skill on this branch):
- a source build prints `aquifer dev`; the skill's version check should say to use `git describe --tags`;
- Aquifer listens on all interfaces and has no bind setting; say so;
- it writes `.l8-key`/`l8-trust` into the working directory (`L8_KEY_PATH`); `L8_KEY_PATH` isn't in docs/pages/configuration.md.

Every i2 and ref2 run built backend pacing headers and spent 2–4 extra passes on them, and in almost every run they did worse than the static ceiling. Two skill errors fed that: the table calls `X-Aqueduct-Queue-Depth` "this queue's jobs" (it's node-wide: `store.Counts().QueueDepth`), and the "divide by active queues" advice starves a big tenant when tenants are uneven. ref2-r2 also noticed the README and configuration.md still recommend `AQUIFER_PEBBLE_FLUSH_INTERVAL_MS=0` for zero loss while the skill says not to.
