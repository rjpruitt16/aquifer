# Integrations

## Inlet: webhook storms

**[Inlet](https://github.com/rjpruitt16/inlet)** is the easy way to put provider webhooks in front of Aquifer without exposing Aquifer to the internet. It runs as an AWS Lambda (or a Cloudflare Worker). For each webhook it:

1. Verifies the provider's signature.
2. Durably queues the event before acknowledging the provider.
3. Pours the event into Aquifer's `POST /jobs`.

Each event uses the sender as `user_id` and the provider's event ID as the idempotency key. Aquifer's fairness and pacing then keep one noisy shop or installation from swamping your handlers.

| Provider | Default fairness identity |
|---|---|
| GitHub | installation, organization, repository owner, or sender |
| Shopify | shop domain |
| Stripe | event account |
| Anthropic | workspace or organization |

Deploy one Inlet stack per provider. See the [Inlet README](https://github.com/rjpruitt16/inlet#readme) for setup.
