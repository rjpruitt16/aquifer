# Quick start

**Binary**
```bash
go install github.com/rjpruitt16/aquifer/cmd/aquifer@latest
aquifer
```

Then send it a job and watch it go through the line:
```bash
curl -s localhost:8080/jobs -H 'Content-Type: application/json' -d '{
  "user_id": "me", "idempotent_key": "first-job",
  "url": "https://httpbin.org/post", "method": "POST", "body": "{\"hello\":\"aquifer\"}"
}'
# {"job_id":"c22c473d…","status":"queued"}

curl -N localhost:8080/jobs/<job_id>/stream
# event: queued
# event: completed   (data carries the target API's response_status and body)
```

**Docker Compose**
```bash
docker compose build --pull --no-cache
docker compose up --detach
curl http://127.0.0.1:8080/ready
```

Compose installs the latest checksum-verified release, persists `/data`, and listens only on loopback by default. See the [Docker Compose guide](../../examples/docker-compose/README.md) for version pinning and destination allowlisting.

**Fly.io**
```bash
./examples/fly/deploy-quickstart.sh
```

The [Fly.io quick start](../../examples/fly/README.md) deploys the latest checksum-verified release to a private one-CPU Machine with an encrypted persistent volume. Use `fly proxy` for local evaluation; Aquifer is not exposed publicly by default.

**AWS test node**
```bash
export AWS_REGION=us-east-2
./examples/aws/deploy-quickstart.sh
```

The [AWS quick start](../../examples/aws/README.md) deploys the latest release to a CIDR-restricted one-vCPU EC2 instance with encrypted local storage. It is intended for evaluation, not production.

**Canalis fleet**
```bash
docker compose -f examples/canalis/compose.yaml up --detach --build
```

The [Canalis + Aquifer quick start](../../examples/canalis/README.md) runs two independently persisted Aquifer nodes behind Canalis. The same guide includes tested two-host AWS and private Fly.io deployment paths.

Cross-platform release binaries (linux/darwin, amd64/arm64) are attached to every [GitHub Release](https://github.com/rjpruitt16/aquifer/releases); no Go toolchain required if you'd rather grab one directly.
