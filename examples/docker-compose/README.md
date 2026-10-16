# Aquifer With Docker Compose

The root [`compose.yaml`](../../compose.yaml) runs the latest checksum-verified Aquifer release with a persistent Docker volume. It binds to `127.0.0.1:8080` by default so Aquifer is not exposed directly to untrusted callers.

```bash
docker compose build --pull --no-cache
docker compose up --detach
curl http://127.0.0.1:8080/ready
```

The uncached build resolves the latest GitHub release and verifies its published checksum. Use the same two commands to upgrade to a newer latest release. Pin a version for repeatable, cacheable deployments:

```bash
AQUIFER_VERSION=v0.14.0 docker compose up --detach --build
```

Set `AQUIFER_ALLOWED_URL_DOMAINS` when using URL-routed jobs, especially before placing an authenticated reverse proxy in front:

```bash
AQUIFER_ALLOWED_URL_DOMAINS=hooks.example.com,api.example.com \
  docker compose up --detach --build
```

To use another host port while keeping the loopback-only default:

```bash
AQUIFER_HOST_PORT=18080 docker compose up --detach
```

Queued jobs, terminal results, and the L8 key persist in the `aquifer_data` volume across container replacement. `docker compose down` keeps that volume; deleting the volume deletes Aquifer's local state.

Compose is a single-node deployment. Put authentication and destination policy in front of Aquifer before exposing it outside the host, and use platform-specific storage, networking, health checks, and redundancy for production.
