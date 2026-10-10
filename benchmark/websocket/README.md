# WebSocket benchmark lab

Fly apps for [benchmark.md §13](../../benchmark.md#13-websocket-sessions-per-machine-flyio-2026-10-10). Create them in one organization so they share a private network, deploy from the repository root, then drive the benchmark from the client machine.

```sh
for a in ws-lab-valkey ws-lab-backend ws-lab-aquifer ws-lab-client; do fly apps create $a --org <org>; done
fly deploy -c benchmark/websocket/fly.valkey.toml --ha=false
fly deploy -c benchmark/websocket/fly.backend.toml --dockerfile benchmark/websocket/Dockerfile.bench --ha=false
fly deploy -c benchmark/websocket/fly.aquifer.toml --dockerfile benchmark/websocket/Dockerfile.aquifer --ha=false
fly deploy -c benchmark/websocket/fly.client.toml --dockerfile benchmark/websocket/Dockerfile.bench --ha=false

# Hold 10,000 idle sessions:
fly ssh console -a ws-lab-client -C "/bench -target ws://ws-lab-aquifer.internal:8080/websocket \
  -upstream ws://ws-lab-backend.internal:6060/socket -connections 10000 -ramp 20s -hold 20s \
  -connect-timeout 60s -stats http://ws-lab-aquifer.internal:9090/stats"

# Message throughput: add -rate (commands/s per session).
fly ssh console -a ws-lab-client -C "/bench ... -connections 500 -ramp 2s -hold 30s -rate 10"
```

- **Aquifer:** runs under the soak supervisor, which raises the open-file limit and serves process stats on `:9090`.
- **Valkey:** its start command raises the limit too.
- **The bench binary:** raises its own limit.

For EZThrottle Local, deploy `fly.ezthrottle.toml` from that repository's checkout. On Fly's private network it must listen on `::`, but its `config/runtime.exs` binds `0.0.0.0` on purpose for Fly's public proxy, so the lab used a copy with `ip: {0, 0, 0, 0, 0, 0, 0, 0}`.

Tear down with `fly apps destroy` for each app.
