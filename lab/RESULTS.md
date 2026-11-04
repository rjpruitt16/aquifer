# Results

Tool calls and minutes per run, as reported by the agent harness. Checks are from `grade.py` (9 total); every report was also read by hand.

| Version | Change | Runs (tool calls) | Median calls | Median minutes | Checks | Kept? |
|---|---|---|---|---|---|---|
| i0 | baseline (skill as on #144) | 49, 64, 50 | 50 | 33 (27, 34, 33) | 9/9 ×3 | — |
| i1 | skill: "Running Aquifer locally" (build/run, where every setting is, how `rps` and `max_concurrent` combine) | 38, 41, 41 | 41 (−18%) | 29 (29, 29, 18) | 9/9 ×3 (hand-graded; see notes) | yes |

## i0 notes

All three reports were accurate and honest (read by hand). Shared friction: each agent spent 8–11 calls reading Aquifer's Go source (`cmd/aquifer/main.go`, `internal/dispatch/*`) to learn how to run Aquifer locally (env vars, YAML) and how `rps` / `max_concurrent` combine across tenant queues. The env var table exists in `docs/pages/configuration.md`, but the skill never points there and the root `CONFIGURATION.md` is only rationale. Run 2 also spent ~15 calls iterating its own header-splitting scheme (v1–v6) before settling.

## i1 notes

Median tool calls fell from 50 to 41 (−18%); reads of Aquifer's Go source fell from about 5 per run to 1–2. All three reports were correct by hand.

- `grade.py` flagged i1-r1 for flush=0 advice, but the report warns *against* it. Fixed the check to ignore warnings.
- i1-r2 did not write `REPORT.md`: the harness blocks subagents from writing report files, and it returned the report in its reply instead. The other runs (i0 and i1) hit the same block on the Write tool and wrote the file through Bash. Graded by hand from the reply. Not archived pending a decision on the protocol.
- New gaps found by i1-r2: a source build prints `aquifer dev`, so the skill's version check fails; no documented way to bind to loopback only; `aquifer_flow_rate_rps` appears to over-report (126–210 vs ~70 real); redistribution of unused `rps` share isn't documented.

## Protocol change after i1

From i2 on, agents return the report as their final reply instead of writing `REPORT.md` (the harness blocks subagents from writing report files), and I save the reply as `REPORT.md`. Because that changes the task, each try is now run side by side with 3 runs of the current best skill (`ref`) under the same prompt, and compared against those.
