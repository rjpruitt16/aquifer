# Results

Tool calls and minutes per run, as reported by the agent harness. Checks are from `grade.py` (9 total); every report was also read by hand.

| Version | Change | Runs (tool calls) | Median calls | Median minutes | Checks | Kept? |
|---|---|---|---|---|---|---|
| i0 | baseline (skill as on #144) | 49, 64, 50 | 50 | 33 (27, 34, 33) | 9/9 ×3 | — |

## i0 notes

All three reports were accurate and honest (read by hand). Shared friction: each agent spent 8–11 calls reading Aquifer's Go source (`cmd/aquifer/main.go`, `internal/dispatch/*`) to learn how to run Aquifer locally (env vars, YAML) and how `rps` / `max_concurrent` combine across tenant queues. The env var table exists in `docs/pages/configuration.md`, but the skill never points there and the root `CONFIGURATION.md` is only rationale. Run 2 also spent ~15 calls iterating its own header-splitting scheme (v1–v6) before settling.
