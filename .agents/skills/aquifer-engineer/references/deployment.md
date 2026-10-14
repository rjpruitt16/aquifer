# Aquifer Partitioning And Scaling Guide

## Choose A Partition

- Use static partitioning when one Aquifer instance or fleet protects a known resource with one aggregate capacity budget.
- Use account queues to isolate tenants sharing that resource while keeping their aggregate traffic inside its measured capacity.
- Use regional cluster routing to partition by `user_id` when callers can reach any Aquifer node and requests for one user should converge on one active owner.
- Use pools when Aquifer selects among interchangeable backend instances using declared capacity and reputation. Pool state is local today, so keep a given `pool_id` on one Aquifer instance.
- Use drain mode when an Aquifer instance is assigned to one tenant at a time and releases that assignment after draining.

Avoid pointing independent, uncoordinated Aquifer instances at the same rate-limited resource with each configured for the resource's full capacity.

## Valkey Coordination

For load-aware regional routing, configure `AQUIFER_CLUSTER_PROVIDER=valkey` and a shared regional endpoint. Aquifer heartbeats nodes, atomically claims user assignments, prefers candidates below active-user capacity, and falls back to the least-loaded active node when all are above the soft target.

Production requirements:

- one writable primary or managed primary endpoint;
- persistence appropriate to the deployment's recovery objective;
- healthy replicas and automatic failover tested under load;
- latency comfortably below `AQUIFER_CLUSTER_VALKEY_TIMEOUT_MS`;
- memory and `maxmemory` policy sized for coordination, streams, and optional remote results;
- monitoring for command latency, saturation, replication lag, failovers, rejected writes, and evictions.

A read replica is a recovery/failover component, not the endpoint for assignment writes. Current regional assignment expects one primary/endpoint and a non-sharded keyspace because its Lua transaction touches multiple keys.

Aquifer remains availability-first when coordination times out: it routes from cached deterministic state. Temporary duplicate owners and account queues are therefore possible. Preserve idempotency and test convergence after failover.

## Autoscaling Inputs

Use several signals together:

- `X-Aqueduct-Fullness` and `GET /health` admission state;
- active queues, queue backlog, upstream backlog, and admission pressure;
- inbound advertised RPS, p95 insert latency, learned threshold, and backlog trend;
- CPU, memory, disk latency/capacity, file descriptors, and network connections;
- Valkey command latency, connection count, memory, replication health, and failover state;
- backend-specific saturation such as database latency, cache latency, worker occupancy, or GPU queue/cache use.

Scale early enough to cover provisioning delay. Validate that new nodes pass `/ready`, join cluster membership, receive assignments, and start conservatively before old nodes drain. Keep termination grace longer than Aquifer's shutdown timeout.

Scaling Aquifer cannot repair a saturated shared backend. When backend capacity is the constraint, scale that backend or lower the Aqueduct limits first.
