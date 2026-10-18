# Canalis + Aquifer Quick Start

This example puts [Canalis](https://github.com/rjpruitt16/canalis-rs) in front of two independently persisted Aquifer nodes. Canalis keeps each tenant sticky to one node, sends new tenants only to nodes with room, and holds work in its durable line when both nodes are full. Each Aquifer still owns the target-facing queue, fairness, and dynamic pacing.

```text
caller -> Canalis -> Aquifer A -> target API
                  -> Aquifer B -> target API
             |          |
             +-- Valkey-+  shared membership and assignment state
```

## Local Docker Compose

From the Aquifer repository:

```bash
docker compose -f examples/canalis/compose.yaml up --detach --build
curl http://127.0.0.1:8080/health
```

Wait until the health response reports `"instances":{"total":2`. Then send a request through Canalis:

```bash
curl -N http://127.0.0.1:8080/proxy \
  -H 'content-type: application/json' \
  -d '{
    "user_id":"quickstart-user",
    "idempotent_key":"quickstart-1",
    "url":"https://postman-echo.com/get",
    "method":"GET",
    "webhook_url":"https://postman-echo.com/post"
  }'
```

The example binds Canalis to loopback, publishes neither Aquifer node, and exposes no Valkey port. Each service has its own Docker volume. Stop it without deleting queued state using `docker compose -f examples/canalis/compose.yaml down`; add `--volumes` only when you intend to delete all quick-start data.

Useful overrides:

```bash
AQUIFER_VERSION=v0.14.0 \
CANALIS_VERSION=0.2 \
AQUIFER_ALLOWED_URL_DOMAINS=api.example.com \
docker compose -f examples/canalis/compose.yaml up --detach --build
```

## AWS: Two EC2 Hosts

The [AWS quick start](aws/README.md) creates two small EC2 hosts in one VPC. The first runs Valkey, Canalis, and Aquifer A with Docker Compose. The second runs Aquifer B with Docker Compose. Only Canalis is reachable from the operator CIDR; Aquifer B accepts traffic only from the first host. Both hosts use encrypted EBS and Session Manager instead of SSH.

## Fly.io: Private Four-App Evaluation

The [Fly.io quick start](fly/README.md) creates private Valkey and Canalis apps plus two private Aquifer apps, each with its own volume. Canalis is reached locally through `fly proxy`; no service receives a public IP.

## Production Boundary

These are evaluation topologies, not production reference architectures. They use one Canalis node and one Valkey node, so either can interrupt new routing decisions. Production deployments need redundant Canalis and Valkey nodes, TLS at the ingress boundary, explicit authentication, backups, monitoring, and capacity tests against the real target API.
