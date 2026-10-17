# Canalis + Aquifer on Fly.io

This evaluation deploys four private Fly apps in one organization and region:

- one Valkey Machine with an encrypted volume;
- one Canalis Machine with an encrypted volume;
- two Aquifer Machines in separate apps, each with its own encrypted volume.

The apps communicate over Fly's private `.internal` network. Canalis receives a private Flycast address but no public IP. Connect from your workstation with `fly proxy`.

## Deploy

Install `flyctl`, sign in, choose an organization slug from `flyctl orgs list`, and run from the Aquifer repository:

```bash
FLY_ORG=my-org ./examples/canalis/fly/deploy-quickstart.sh
```

The script creates a unique prefix, deploys all four apps, and waits for Canalis to report both Aquifer nodes. Keep the printed `APP_PREFIX`; cleanup uses it. `FLY_ORG` is required so a noninteractive deployment never guesses which organization should own billable resources.

Useful overrides:

```bash
APP_PREFIX=my-canalis-demo \
FLY_ORG=my-org \
FLY_REGION=ord \
AQUIFER_VERSION=v0.14.0 \
CANALIS_VERSION=0.2 \
AQUIFER_ALLOWED_URL_DOMAINS=api.example.com \
./examples/canalis/fly/deploy-quickstart.sh
```

## Verify

Open a private local tunnel in one terminal:

```bash
flyctl proxy 8080:8080 --app APP_PREFIX-canalis
```

Then call Canalis from another terminal:

```bash
curl http://127.0.0.1:8080/health
curl -N http://127.0.0.1:8080/proxy \
  -H 'content-type: application/json' \
  -d '{
    "user_id":"fly-quickstart",
    "idempotent_key":"fly-quickstart-1",
    "url":"https://postman-echo.com/get",
    "method":"GET",
    "webhook_url":"https://postman-echo.com/post"
  }'
```

The health response should report `"instances":{"total":2`. Inspect individual services with `flyctl logs --app APP_NAME` and `flyctl status --app APP_NAME`.

## Limits And Cleanup

- This is one Canalis node and one Valkey node, not a highly available control plane.
- All four Machines stay running because each may hold durable state or accepted work. They can incur compute and volume charges while idle.
- Valkey has no password because it is reachable only inside the Fly organization network. Use authenticated, encrypted Valkey and explicit network policy for production.
- The deployment is private and has no end-user authentication layer. `fly proxy` supplies the trusted operator boundary for this evaluation.
- Destroying the apps also destroys their attached volumes and queued test data.

Delete all four apps and their resources:

```bash
APP_PREFIX=the-prefix-printed-at-deploy \
  ./examples/canalis/fly/destroy-quickstart.sh
```
