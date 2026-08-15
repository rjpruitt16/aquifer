# Configuration rationale

Details behind the admission-control defaults and backoff behavior in the README's Configuration table.

## Why body-size and DB-size admission are on by default

Aquifer protects itself from the traffic it's meant to be absorbing without needing to be told to. The defaults are sized off the infrastructure this project is actually benchmarked against (a single 512MB Fly.io instance with a 1GB volume — see [benchmark.md](benchmark.md)); set an explicit `0` to disable a check entirely, or raise it for a bigger deployment.

Memory is the exception: there's no safe one-size-fits-all default since it depends on your own deployment's memory budget, not Aquifer's disk usage, so it stays disabled until you set it — Aquifer logs a warning on startup if you haven't (benchmarked safe at 400MB on a 512MB instance, as a starting point).

See [benchmark.md](benchmark.md) for real numbers, including what happens under sustained load, a 10x burst, a memory ceiling, a mid-flight crash, multi-tenant fairness, and capacity/drain time by machine size.

## Retry-After backoff

Retry-After backs off exponentially under sustained pressure. A single rejection returns your configured base value (default 5s). Each additional *consecutive* rejection — with no allowed request in between — doubles it: 5s → 10s → 20s → 40s → capped at 60s. The moment a request is allowed again, it resets to the base.

This exists so that clients retrying into a sustained overload spread out over time instead of all hammering the same fixed 5-second ceiling forever, which is exactly the pattern that keeps an overloaded instance from ever catching up.
