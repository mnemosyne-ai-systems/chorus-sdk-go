# Chorus Go SDK

Go client for [Chorus](https://github.com/mnemosyne-ai-systems/chorus), a state and coordination layer for long-running agent sessions.

The SDK caches the shard routing table locally, routes session-scoped requests by session ID, and implements the retry, redirect, and CAS policies required by the Chorus API.

## Status

Public beta. The `v0.1.x` line targets the Chorus `chorus.v1` API. APIs may change before `v1.0.0`; pin an exact version if you need stability.

## Requirements

- Go 1.22 or newer
- A reachable Chorus node endpoint

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

	routes, err := dialer.RouteSource("127.0.0.1:9000")
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

## What the client handles

- Local shard lookup: `hash(session_id)` is evaluated against the cached shard ranges, with no network round trip per lookup.
- Redirects: a `REDIRECT` response updates the local route immediately and retries against the new owner.
- Retries: `RETRY` responses use bounded jittered backoff, followed by one full route reload.
- CAS: `Client.CAS` and `Client.CASSpawnState` run the bounded read/modify/write loop.
- Stop signals: `EPOCH_STALE`, `LEASE_EXPIRED`, and failed preconditions are returned to the caller without hidden retries.

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
