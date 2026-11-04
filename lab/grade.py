#!/usr/bin/env python3
"""Grade one onboarding run.  grade.py WORKDIR AQUIFER_CHECKOUT PORT_BASE

Automatic checks only; each report is also read by hand (see RESULTS.md)."""
import json, pathlib, re, socket, subprocess, sys

work, aq, base = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), int(sys.argv[3])
orig = pathlib.Path(__file__).parent / "sample-app"
report = (work / "REPORT.md").read_text() if (work / "REPORT.md").exists() else ""
changed = {p.name: p.read_text() for p in work.glob("*.py")
           if not (orig / p.name).exists() or (orig / p.name).read_text() != p.read_text()}
code = "\n".join(changed.values())

checks = {
    "report_written": bool(report),
    "bottleneck_is_db_pool": bool(re.search(r"(connection )?pool|POOL_SIZE|connections?", report, re.I)),
    # Either the backend paces from the pool, or the report shows the static ceiling alone was enough.
    # (Changed after i2: for a fixed pool the static ceiling measured better, and the skill now says so.)
    "pacing_choice_measured": bool(re.search(r"X-(Aqueduct|Aquifer)-(Rps|Max-Concurrent|Max-Backlog)", code, re.I))
        or bool(re.search(r"static", report, re.I) and re.search(r"\b0\b[^\n]{0,40}503|503[^\n]{0,40}\b0\b|no 503", report, re.I)),
    # Only applies when the backend sends pacing headers.
    "headers_come_from_pool_signal": not re.search(r"X-(Aqueduct|Aquifer)-(Rps|Max-Concurrent)", code, re.I)
        or bool(re.search(r"pool_stats|pool_in_use|_in_use|_waiting|POOL_SIZE|wait", code)),
    "before_and_after_numbers": bool(re.search(r"(direct|baseline|before)", report, re.I))
        and bool(re.search(r"aquifer", report, re.I)) and len(re.findall(r"\d", report)) > 20,
    "small_partners_reported": bool(re.search(r"corner-shop|kiosk|deli|small", report, re.I)),
    # Mentioning flush=0 to warn against it is fine; recommending it isn't.
    "no_flush_zero_advice": not any(
        not re.search(r"\b(don't|do not|never|avoid|not)\b", report[max(0, m.start() - 80):m.start()], re.I)
        for m in re.finditer(r"FLUSH_INTERVAL_MS\W{0,3}=\W{0,3}0", report)),
    "aquifer_checkout_untouched": subprocess.run(
        ["git", "-C", str(aq), "status", "--porcelain", "--", ".", ":!lab/runs"],
        capture_output=True, text=True).stdout.strip() == "",
}
def listening(port):
    with socket.socket() as s:
        s.settimeout(0.2)
        return s.connect_ex(("127.0.0.1", port)) == 0
checks["processes_stopped"] = not any(listening(p) for p in range(base, base + 10))
print(json.dumps({"passed": sum(checks.values()), "of": len(checks), "checks": checks,
                  "changed_files": sorted(changed)}, indent=2))
