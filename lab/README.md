# Onboarding lab (experiment branch, not for merge)

Measures how smoothly a fresh coding agent pilots Aquifer with `skills/aquifer-pilot`, and iterates on docs and the skill (no code or architecture changes) to make it smoother.

- `sample-app/`: orders-api, a service whose real bottleneck is a 4-connection database pool (about 80 writes/s). `partners.py` replays a 170 orders/s burst with immediate client retries; one big partner, three small ones.
- `PROMPT.md`: the exact prompt each fresh agent gets.
- `grade.py`: the automatic checks. Every report is also read by hand.
- `RESULTS.md`: every iteration, its change, tool calls, time and checks.
- `runs/`: each run's report and the files it changed.

Rule: 3 runs per version, compare medians. A change is kept if median tool calls drop at least 10% and no check regresses. Stop after 5 consecutive tries that fail that bar.
