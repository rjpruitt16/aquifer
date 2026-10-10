"""Creates the soak monitor on Claude Managed Agents: a locked-down
environment, a vault holding the two secrets, the agent, and a scheduled
deployment (created paused, so a manual dry run comes first).

    ANTHROPIC_API_KEY=... SOAK_REPORT_TOKEN_FILE=... GITHUB_TOKEN_FILE=... \
        python3 benchmark/soak/monitor/setup.py

Secrets are read from files and sent only to the vault; nothing is printed
except resource IDs. IDs are written to monitor/ids.json.
"""
import json
import os
import pathlib
import urllib.error
import urllib.request

API = "https://api.anthropic.com/v1"
HERE = pathlib.Path(__file__).parent
STATUS_HOST = "aquifer-soak-driver.fly.dev"
MODEL = os.environ.get("SOAK_MONITOR_MODEL", "claude-haiku-5-5")
BUDGET_CENTS = os.environ.get("SOAK_MONITOR_BUDGET_CENTS", "100")  # per run


def call(method, path, body=None):
    req = urllib.request.Request(
        f"{API}{path}{'&' if '?' in path else '?'}beta=true",
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "x-api-key": os.environ["ANTHROPIC_API_KEY"],
            "anthropic-version": "2023-06-01",
            "anthropic-beta": "managed-agents-2026-04-01",
            "content-type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as err:
        raise SystemExit(f"{method} {path}: {err.code} {err.read().decode()[:800]}")


def secret(env):
    return pathlib.Path(os.environ[env]).read_text().strip()


ids_path = HERE / "ids.json"
ids = json.loads(ids_path.read_text()) if ids_path.exists() else {}

if "environment" not in ids:
    env = call("POST", "/environments", {
        "name": "aquifer-soak-monitor",
        "config": {"type": "cloud", "networking": {
            "type": "limited",
            "allowed_hosts": [STATUS_HOST, "api.github.com"],
        }},
    })
    ids["environment"] = env["id"]

if "vault" not in ids:
    vault = call("POST", "/vaults", {"display_name": "aquifer-soak-monitor"})
    ids["vault"] = vault["id"]
    for name, file_env, host in [
        ("SOAK_REPORT_TOKEN", "SOAK_REPORT_TOKEN_FILE", STATUS_HOST),
        ("GITHUB_TOKEN", "GITHUB_TOKEN_FILE", "api.github.com"),
    ]:
        call("POST", f"/vaults/{vault['id']}/credentials", {
            "display_name": name,
            "auth": {
                "type": "environment_variable",
                "secret_name": name,
                "secret_value": secret(file_env),
                "networking": {"type": "limited", "allowed_hosts": [host]},
                "injection_location": {"header": True},
            },
        })

agent_body = {
    "name": "Aquifer soak monitor",
    "model": MODEL,
    "system": (HERE / "system.md").read_text(),
    "tools": [{"type": "agent_toolset_20260401"}],
}
if "agent" not in ids:
    ids["agent"] = call("POST", "/agents", agent_body)["id"]
else:
    call("POST", f"/agents/{ids['agent']}", agent_body)

if "deployment" not in ids:
    dep = call("POST", "/deployments", {
        "name": "Aquifer soak monitor, every 6h",
        "agent": ids["agent"],
        "environment_id": ids["environment"],
        "vault_ids": [ids["vault"]],
        "initial_events": [{"type": "user.message", "content": [{"type": "text", "text":
            "Run your soak-test check now: read your earlier reports, review the status pages, "
            "post one report, and raise a soak-alert issue only if something warrants it."}]}],
        "schedule": {"type": "cron", "expression": "15 */6 * * *", "timezone": "UTC"},
        "budget": {"type": "limit", "max_list_cost": {"amount": BUDGET_CENTS, "currency": "USD"}},
    })
    ids["deployment"] = dep["id"]
    call("POST", f"/deployments/{dep['id']}/pause")

ids_path.write_text(json.dumps(ids, indent=2) + "\n")
print(json.dumps(ids, indent=2))
