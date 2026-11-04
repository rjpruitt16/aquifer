# Several Aquifer nodes

Part of the aquifer-pilot skill, step 3. Read it when the pilot runs more than one Aquifer node.

Aquifer scales by partitioning, not by sharing state:
- **Static:** dedicate a node to one protected resource, such as one GPU pool or one third-party API.
- **Cluster routing:** nodes share a Valkey that keeps each tenant on one node.
- **Drain mode:** a node takes one tenant's burst at a time.
- **Canalis:** a gateway in front of many nodes.

Partitioning matters for the pacing math. **Pacing is per node and per queue.** A rate the backend advertises applies to each queue that received it. A node keeps the total across its own queues only within its configured budget, and nodes don't coordinate with each other. So the backend sees up to:

> advertised rate × active queues per node × number of nodes

Account for that in the AIMD:
- **Count the senders.** Read `X-Aqueduct-Active-Queues` per node. Tell nodes apart by `X-Aqueduct-Node-Id`, which every request carries. Add them up.
- **Divide.** Advertise each queue `budget ÷ total active queues`, not the whole budget. That's an even split: when tenants are very uneven, it caps the big tenant at an even share even while the small ones leave theirs unused. For uneven tenants, prefer the static ceiling, or size each queue's rate from what that queue actually uses.
- **Scale the step.** An additive step of +5 rps lands on every queue at once. With 20 queues across 4 nodes, that's +100 rps at the backend. Size the increase for the total, and cut on the total.

A feedback loop on the backend's own saturation signal corrects itself either way, because it measures the real total. But dividing first means fewer overshoots.
