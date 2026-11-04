// The docs site's pages, in sidebar order. Each comes from a Markdown file in
// the repository, so the files stay readable on GitHub and the site is built
// from them; nothing here is written by hand twice.
export const site = 'https://rjpruitt16.github.io';
export const base = '/aquifer';
export const repo = 'https://github.com/rjpruitt16/aquifer';

export const groups = [
	{
		label: 'Start here',
		pages: [
			{ src: 'docs/pages/overview.md', slug: 'index', md: 'overview', title: 'Why Aquifer', description: 'What Aquifer is, the problem it solves, and where it sits next to a gateway, Canalis and Inlet.' },
			{ src: 'docs/pages/quick-start.md', slug: 'quick-start', title: 'Quick start', description: 'Run Aquifer from the binary, Docker Compose, Fly.io, AWS, or a Canalis fleet.' },
			{ src: 'docs/pages/how-it-works.md', slug: 'how-it-works', title: 'How it works', description: 'The job lifecycle and the delivery guarantee (at least once).' },
			{ src: 'docs/pages/use-cases.md', slug: 'use-cases', title: 'Use cases', description: 'MCP tools, protecting an HTTP API, webhook intake, outbound pacing, the /proxy reverse proxy, WebSockets and cross-region redirect.' },
		],
	},
	{
		label: 'Guides',
		pages: [
			{ src: 'docs/pages/pacing.md', slug: 'pacing', title: 'Dynamic pacing', description: 'How the target API sets the pace with X-Aqueduct-* headers: per-tenant queues, fair admission, sizing the backlog budget, slow start and the ORCA fallback.' },
			{ src: 'docs/pages/streaming.md', slug: 'streaming', title: 'Streaming', description: 'Stay on the line over SSE or /proxy, or hang up and get a webhook; when the webhook is skipped.' },
			{ src: 'docs/pages/pools.md', slug: 'pools', title: 'Pools', description: 'Load balancing across registered members, weighted by declared capacity and live reputation.' },
			{ src: 'docs/pages/partitioning.md', slug: 'partitioning', title: 'Partitioning', description: 'Static partitioning, regional cluster routing, drain mode and external registration.' },
			{ src: 'docs/pages/deployment.md', slug: 'deployment', title: 'Deployment', description: 'Sidecar, standalone, embedded or behind a Gateway API proxy; choosing a machine size.' },
			{ src: 'docs/pages/integrations.md', slug: 'integrations', title: 'Integrations', description: 'Inlet, for provider webhook storms (GitHub, Shopify, Stripe, Anthropic).' },
			{ src: 'docs/pages/l8.md', slug: 'l8', title: 'L8 protocol', description: 'Webhook delivery signed with public keys, optional payload encryption and request schemas.' },
			{ src: 'docs/pages/reliability.md', slug: 'reliability', title: 'Reliability', description: 'The durable queue, crash and panic recovery, and job TTLs.' },
		],
	},
	{
		label: 'Reference',
		pages: [
			{ src: 'docs/pages/api-overview.md', slug: 'api-overview', title: 'API overview', description: 'The POST /jobs request and the warning about untrusted callers.' },
			{ src: 'API.md', slug: 'api', title: 'API reference', description: 'Every endpoint: jobs, /proxy, streams, WebSockets, health, metrics, webhooks, admission and cluster routing.' },
			{ src: 'docs/pages/configuration.md', slug: 'configuration', title: 'Configuration', description: 'Rate limits per target API in YAML, and the environment variables.' },
			{ src: 'CONFIGURATION.md', slug: 'configuration-rationale', title: 'Configuration rationale', description: 'Why the admission defaults are what they are.' },
			{ src: 'docs/pages/adapters.md', slug: 'adapters', title: 'Framework adapters', description: 'The HTTP, MCP stdio and A2A front doors.' },
			{ src: 'ADAPTERS.md', slug: 'adapters-reference', title: 'Adapters reference', description: 'The MCP tools, the A2A Agent Card, and writing your own adapter, storage backend or metrics adapter.' },
			{ src: 'DRAIN_MODE.md', slug: 'drain-mode', title: 'Drain mode', description: 'Handing an instance to one tenant at a time: the state machine, settings and ledger payloads.' },
			{ src: 'REGISTRATION.md', slug: 'registration', title: 'External registration', description: 'Reporting an instance to an external control plane: settings and payloads.' },
			{ src: 'benchmark.md', slug: 'benchmarks', title: 'Benchmarks', description: 'Throughput, crash recovery, memory, capacity per machine size, and the GPU run.' },
		],
	},
];

export const pages = groups.flatMap((g) => g.pages);
