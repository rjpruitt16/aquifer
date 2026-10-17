# Aquifer Fly.io Quick Start

This quick start deploys the latest checksum-verified Aquifer release to one `shared-cpu-1x` Fly Machine with 512 MB of memory and a 1 GB encrypted Fly Volume. It uses the repository's root [`fly.toml`](../../fly.toml).

The app is private by default. Aquifer does not authenticate callers, so the script allocates Flycast networking without allocating public IP addresses. Evaluate it through `fly proxy`, or let a trusted service in the same Fly private network call it through Fly Proxy at `http://APP_NAME.flycast`. The private service keeps HTTP enabled because Flycast terminates this route inside Fly's private network; do not add public IPs to this configuration.

## Deploy

Install `flyctl`, sign in, clone Aquifer, and run:

```bash
./examples/fly/deploy-quickstart.sh
```

The script creates a globally unique app name, creates the persistent volume, resolves the latest Aquifer GitHub release, and verifies the release checksum during the image build. Save the printed app name for upgrades and cleanup.

Useful overrides:

```bash
APP_NAME=my-aquifer \
FLY_REGION=ord \
AQUIFER_VERSION=v0.14.0 \
VOLUME_SIZE_GB=2 \
VM_MEMORY_MB=512 \
./examples/fly/deploy-quickstart.sh
```

Set `FLY_ORG` when the app should be created outside your personal organization. `VM_MEMORY_MB` defaults to `512` and must be at least `256`; the script derives `AQUIFER_MEMORY_LIMIT_MB` from it unless that limit is set explicitly. Treat 256 MB as a small evaluation size and benchmark before using it for real traffic.

Contributors can set `AQUIFER_VERSION=source` to build the current checkout instead of a published release. The default remains the latest checksum-verified release.

## Connect

Start a local authenticated Fly proxy:

```bash
flyctl proxy 8080:8080 --app APP_NAME
curl http://127.0.0.1:8080/ready
```

The Machine remains running even when inbound traffic is quiet because Aquifer may still be draining queued work or delivering completion webhooks. This costs more than scaling to zero, but stopping based only on HTTP idleness can strand accepted jobs.

## Limits

- One Machine and one volume are for evaluation, not high availability.
- Fly Volumes are local to one host and are not automatically replicated.
- The private deployment is not directly reachable from an Inlet Worker or Lambda in another cloud. Use an authenticated gateway or private cross-cloud network; do not expose Aquifer publicly without caller authentication.
- Rerun the script with the same `APP_NAME` to deploy a newer release while retaining the volume.

Inspect and remove the deployment with `flyctl status`, `flyctl volumes list`, and `flyctl apps destroy`. Confirm volume cleanup in the Fly dashboard when the test is finished.
