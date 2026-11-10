# Changelog

## Unreleased (suggested v0.2.0)

### Added

- `WithSeedEndpoints`: route polling now fails over across a list of seed
  endpoints. The last successful seed is remembered; if every seed is
  unreachable the cached route table is kept intact and polling retries after
  the normal backoff.
- `WithNodeEndpoints`: optional client-side node selection for stateless
  clusters. Nodes are chosen per session ID with rendezvous hashing, and a
  failed node falls through to the next node in hash order.
- gRPC routing metadata: session-scoped calls send `x-chorus-sid`; spawn-scoped
  calls also send `x-chorus-spawn`, so layer-7 load balancers can hash
  consistently.

### Fixed

- When only `WithNodeEndpoints` is set, those endpoints now also serve as route
  polling seeds. Route discovery no longer depends on the construction-time
  endpoint (typically the virtual `lb`), and `routes` may be `nil` when the
  dialer implements `sdk.RouteSourceProvider`.

### Changed (breaking)

- `PutSpawnState` and `DeleteSpawnState` now reject `claimEpoch == 0` locally
  with `ErrEpochRequired` instead of sending a request that the stateless
  server cannot accept. Callers that previously relied on the server to reject
  epoch 0 must pass the epoch returned by `ClaimSpawn` / `RenewSpawnClaim`.
- Because this rejects previously accepted calls, the next release should be a
  minor version bump: **v0.2.0**.
