# Chorus Go SDK

Go client for [Chorus](https://github.com/mnemosyne-ai-systems), a state and coordination layer for long-running agent sessions.

The SDK caches the shard routing table locally, routes session-scoped requests by session ID, and implements the retry, redirect, and CAS policies required by the Chorus API.

## Status

Public beta. The `v0.1.x` line targets the Chorus `chorus.v1` API. APIs may change before `v1.0.0`; pin an exact version if you need stability.

The SDK is Apache 2.0. The Chorus server is proprietary and private. To run the
quick start, request evaluation access: [EVALUATION.md](EVALUATION.md).

## Requirements

- Go 1.22 or newer
- A reachable Chorus node endpoint (evaluation access required)

## Install

```bash
go get github.com/mnemosyne-ai-systems/chorus-sdk-go@v0.1.0
```

## Quick start

```go
package main

import (
	"context"
	"log"
	"time"

	sdk "github.com/mnemosyne-ai-systems/chorus-sdk-go"
	"github.com/mnemosyne-ai-systems/chorus-sdk-go/grpctransport"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dialer := grpctransport.NewDialer()
	defer dialer.Close()

	routes, err := dialer.RouteSource("127.0.0.1:9101")
	if err != nil {
		log.Fatal(err)
	}

	client := sdk.NewClient(routes, dialer)
	if err := client.Reload(ctx); err != nil {
		log.Fatal(err)
	}
	client.StartRoutePoll(ctx)

	resp, err := client.PutSessionState(ctx, "sid-1", "status", []byte("running"), 0)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("stored version %d", resp.Version)
}
```

Use one long-lived `Client` per process. `StartRoutePoll` polls the route version and rebuilds the local cache only when the control plane publishes a newer version.

## Stateless routing and failover

Chorus nodes are stateless: any node can serve any request, and the routing
interface only exposes a virtual `lb` node. Two options let the SDK choose real
nodes itself:

```go
client := sdk.NewClient(nil, dialer,
    // Route queries fail over across these seeds in order. The last healthy
    // seed is remembered; if all are down the cached route table is kept and
    // polling retries after the normal backoff.
    sdk.WithSeedEndpoints("10.0.0.1:9101", "10.0.0.2:9101"),

    // Data-plane requests choose a node per session ID using rendezvous
    // hashing. If a node is unreachable, the next node in hash order is tried.
    // Required in stateless mode because ListNodes only returns "lb".
    sdk.WithNodeEndpoints("10.0.0.1:9101", "10.0.0.2:9101", "10.0.0.3:9101"),
)
```

`WithSeedEndpoints` requires a dialer that implements `sdk.RouteSourceProvider`;
`grpctransport.Dialer` does. When neither option is set, the SDK keeps the
original single-endpoint behavior and uses the endpoint from the route cache.

Session-scoped gRPC calls also carry `x-chorus-sid`; spawn-scoped calls carry
`x-chorus-spawn` as well, so a layer-7 load balancer can hash consistently.

Spawn writes (`PutSpawnState`, `DeleteSpawnState`) require a non-zero claim
epoch. Pass the epoch returned by `ClaimSpawn`/`RenewSpawnClaim`; epoch `0` is
rejected locally with `ErrEpochRequired`.

## Failover demo

See [`examples/worker-lease`](examples/worker-lease) for a runnable demo: a
worker crashes, another takes over, and the old worker's stale write is rejected
instead of duplicating a side effect.

```bash
go run ./examples/worker-lease -endpoint 127.0.0.1:9101
```

## What the client handles

- Local shard lookup: `hash(session_id)` is evaluated against the cached shard ranges, with no network round trip per lookup.
- Seed failover: route polling rotates through `WithSeedEndpoints`, remembers the last healthy seed, and keeps the old cache when all seeds are down.
- Node selection: `WithNodeEndpoints` uses rendezvous hashing per session ID and falls through to the next node on failure.
- Redirects: a `REDIRECT` response updates the local route immediately and retries against the new owner.
- Retries: `RETRY` responses use bounded jittered backoff, followed by one full route reload.
- CAS: `Client.CAS` and `Client.CASSpawnState` run the bounded read/modify/write loop.
- Stop signals: `EPOCH_STALE`, `LEASE_EXPIRED`, and failed preconditions are returned to the caller without hidden retries.
- Spawn writes require a non-zero claim epoch (`ErrEpochRequired` otherwise).

## Packages

| Package | Purpose |
| --- | --- |
| `github.com/mnemosyne-ai-systems/chorus-sdk-go` | High-level client, CAS loops, route cache, error types |
| `.../grpctransport` | gRPC dialer, `Conn`, and `RouteSource` implementations |
| `.../api/proto/v1` | Generated `chorus.v1` protobuf and gRPC bindings |
| `.../routekey` | Stable session hash and shard range helpers |

The `grpctransport.Conn` type also exposes the raw entity and route service clients when you need APIs that the high-level client does not wrap.

## Development

```bash
go test ./...
go test -race ./...
go vet ./...
```

## AI-assisted development

Architecture, API design, and correctness decisions are made by the maintainers. Implementation and tests are written with AI coding assistants under maintainer direction and review. All changes are reviewed and validated by the project's CI before release.

## License

Apache License 2.0. See [LICENSE](LICENSE).

The Chorus server is proprietary software and is licensed separately.
