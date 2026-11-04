I run a small service, orders-api, checked out at {WORKDIR}. During sales our partner stores flood it, and the small shops' orders fail even though they barely send anything. Someone suggested Aquifer.

The Aquifer repository is checked out at {AQUIFER}. Read it from there, not from GitHub or the docs site, and use the pilot skill it ships for coding agents. I'm hands-off: run the pilot yourself, end to end, and write your findings to {WORKDIR}/REPORT.md.

Rules:
- Everything runs locally on this laptop. Use only ports {BASE} to {BASE_END}.
- Keep load at or under 300 requests a second, and each load test under 2 minutes.
- Don't modify anything in {AQUIFER}. You may change files in {WORKDIR}.
- Stop every process you started before you finish.

When you're done, reply with one line: the path to the report.
