# Worker lease demo

This demo shows the failure mode Chorus is designed to prevent: a worker dies,
another worker takes over, and the old worker cannot commit a duplicate side
effect with its stale claim epoch.

## What happens

1. `worker-a` claims the spawn and writes `side_effects=1`.
2. `worker-a` stops renewing its lease, simulating a crash.
3. `worker-b` waits for the lease to expire and claims the same spawn with a
   newer epoch.
4. `worker-a` wakes up and tries to write with its old epoch.
5. Chorus rejects the stale write with `EPOCH_STALE`, and `side_effects` stays
   at `1`.

## Prerequisites

- A running Chorus node reachable at `-endpoint` (default `127.0.0.1:9101`).
- At least one shard covering the session key space. For the local compose
  stack:

```bash
docker compose -f deploy/docker-compose.yaml run --rm chorusctl nodes
docker compose -f deploy/docker-compose.yaml run --rm chorusctl \
  -confirm create-shard 0 0 n1
```

- Go 1.22 or newer.

## Run

From the SDK repository:

```bash
go run ./examples/worker-lease -endpoint 127.0.0.1:9101
```

The command creates a unique workspace and session on every run, so it can be
repeated without cleanup. The total lease wait is about 15 seconds.

Expected tail:

```text
worker-a claimed: epoch=1 renew_after=5000ms
worker-a wrote side_effects=1 version=<tso-version>
simulating worker-a crash: stop renewing the claim
worker-b waiting: owner=worker-a epoch=1 (attempt 1)
worker-b waiting: owner=worker-a epoch=1 (attempt 5)
worker-b waiting: owner=worker-a epoch=1 (attempt 10)
worker-b waiting: owner=worker-a epoch=1 (attempt 15)
worker-b claimed: epoch=2
worker-a wakes up and tries a stale write
stale write rejected: EPOCH_STALE
PASS: side_effects=1 (not duplicated)
```

## Notes

- The demo uses one process for reproducibility, but the API calls are the same
  ones separate workers would make.
- This is a correctness demo, not a benchmark.
- The Chorus core is proprietary and private. Request evaluation access from
  the [SDK README](../../README.md#status).
