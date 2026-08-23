# Aquifer behind a Gateway API proxy (Envoy Gateway)

A fourth deployment shape alongside sidecar, standalone, and embedded library (see the root
[README's "Deployment model"](../../README.md#deployment-model)): Aquifer as a normal Deployment,
reached through a [Gateway API](https://gateway.k8s.io/) proxy instead of a sidecar. The Gateway
owns routing, TLS termination, and the cluster's single ingress point; Aquifer owns the queue behind
it — not a way to horizontally scale Aquifer itself.

Tested against **Gateway API v1.6.0** and **Envoy Gateway v1.9.0** (current stable as of writing).
Pin your own cluster to a specific release rather than "latest" — both projects ship frequently.

## Why `replicas: 1`

This is not a simplification to loosen later. Aquifer's durable queue is a local SQLite file per
instance, and pool membership plus the SSE broker's subscriber map are in-process only. Scaling is by
**partitioning** — running one instance per upstream domain or tenant — not by adding replicas behind
one Service. Multiple replicas sharing this manifest's `Service` would risk double-dispatch on
retries and would make pool members registered on one replica invisible to another. If you need more
capacity, deploy a second, separately partitioned copy of this example (different namespace, own PVC,
own `HTTPRoute` path or hostname), not a higher `replicas` count.

## Prerequisites

- A Kubernetes cluster (this was verified end-to-end against a local [`kind`](https://kind.sigs.k8s.io/)
  cluster — `kind create cluster` is the fastest way to try this yourself)
- [Envoy Gateway](https://gateway.envoyproxy.io/docs/install/) installed in-cluster (v1.9.0+) via
  Helm:
  ```bash
  helm install eg oci://docker.io/envoyproxy/gateway-helm --version v1.9.0 \
    -n envoy-gateway-system --create-namespace
  kubectl wait --timeout=180s -n envoy-gateway-system deployment/envoy-gateway --for=condition=Available
  ```
  **Don't install the Gateway API CRDs separately first.** Envoy Gateway's Helm chart bundles its own
  copy and installs them via server-side apply — applying the standalone
  [Gateway API release manifest](https://github.com/kubernetes-sigs/gateway-api/releases) first (via
  plain `kubectl apply`, which uses client-side apply) causes a field-manager conflict and the Helm
  install fails outright. Let the chart provide the CRDs; that's the only ordering that worked in
  testing. Confirmed no default `GatewayClass` ships with the chart — `gatewayclass.yaml` below is
  required, not optional.
- A `ReadWriteOnce`-capable `StorageClass` available in your cluster (`kind` provides one by default)

## Deploy

```bash
kubectl apply -k .
```

This creates the `aquifer` namespace, a 1Gi PVC, a single-replica Aquifer Deployment (`AQUIFER_ADAPTER=http`),
a `ClusterIP` Service, and the Gateway API resources (`GatewayClass`, `Gateway`, `HTTPRoute`) routing
all traffic to it.

## Verify

```bash
kubectl get pods -n aquifer
kubectl get httproute -n aquifer aquifer -o jsonpath='{.status.parents[0].conditions}'
```

The `HTTPRoute` status conditions should show `Accepted` and `ResolvedRefs` once Envoy Gateway has
picked it up. Find the Envoy proxy Service Envoy Gateway provisions for this Gateway
(`kubectl get svc -n envoy-gateway-system | grep aquifer-gateway`) and confirm `GET /health` responds
through it, then submit a real job through `POST /jobs` on the same address and confirm it reaches
`"status": "completed"`.

**Verified end-to-end** against a local `kind` cluster with Gateway API v1.6.0 and Envoy Gateway
v1.9.0: `HTTPRoute` came up `Accepted`/`ResolvedRefs`, `GET /health` returned 200 through the Envoy
proxy, a real job (`POST /jobs` → `postman-echo.com`) returned 201 with a job ID, and polling
`GET /jobs/:id` afterward showed `"status": "completed"` — full round trip through the Gateway, not
just the manifests applying cleanly.

## Customizing

- **Image**: `deployment.yaml` hardcodes `ghcr.io/rjpruitt16/aquifer:latest` — pin a specific tag for
  anything beyond local testing, or repoint at your own fork's registry.
- **Sizing**: the CPU/memory requests in `deployment.yaml` are a starting point, not a benchmarked
  recommendation — see the root [benchmark.md](../../benchmark.md) and the README's "Choosing a
  machine size" section before sizing for real traffic.
- **TLS**: this example is HTTP-only on the Gateway listener, to keep the example focused on the
  Gateway API wiring itself. A production deployment would add an HTTPS listener with a cert (e.g.
  via [cert-manager](https://cert-manager.io/)) — not included here.
