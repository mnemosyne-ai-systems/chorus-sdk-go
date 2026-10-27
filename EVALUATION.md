# Evaluate Chorus

The Chorus Go SDK and protobuf definitions are public. The Chorus core is
proprietary and private. This page is the access path for teams that want to
evaluate the full coordination layer.

## What evaluation includes

- A 30-minute call to understand the workload and failure model.
- Access to a hosted sandbox when available, or a guided private deployment.
- A working end-to-end example: session state, claim, failover, and stale-write
  rejection.
- Benchmark guidance for your own environment.
- Direct feedback into the design while the API is still `v0.1.x`.

Evaluation is time-boxed and intended for hands-on technical validation. It is
not a free production service and does not include an SLA.

## What we need from you

- The workload: which agent framework, how long tasks run, and which tool calls
  have side effects.
- The failure mode you care about: duplicate workers, stale writes, restart
  recovery, or shard movement.
- Deployment constraints: cloud/self-hosted, Kubernetes, network, and data
  residency.
- A technical contact and a timeline.

Please do not include credentials, customer data, or secrets in a public issue.

## Request access

Open an evaluation request:

https://github.com/mnemosyne-ai-systems/chorus-sdk-go/issues/new?template=evaluation-request.yml

If the form is unavailable, open a normal issue with the subject
`[Evaluation] Chorus access` and include the details above.

## Public boundary

- Public: SDK, protobuf definitions, route-key helpers, examples, Apache 2.0.
- Private: core, control plane, and production deployment guidance.
- Commercial: production use requires a commercial license.
