package sdk

import "context"

// ---- 路由查询（§8.4）----

// Shard 是路由表中的分片条目（对应 /routes/shards/{id} 的对外视图）。
type Shard struct {
	ShardID   uint64
	RangeLo   uint64
	RangeHi   uint64
	OwnerNode string
	Epoch     uint64
	State     string // "active" | "migrating"
}

// Node 是节点对外视图（endpoint 供 REDIRECT 与直连）。
type Node struct {
	NodeID   string
	Endpoint string
	Status   string
}

// RouteSource 是路由查询面（§8.4 + ListNodes）。
// ListNodes 是 §8.4 的显式扩展（偏差报告 P-2）：全量重建路由需要 node→endpoint 映射，
// 仅靠 get_route(sid) 无法预知未访问过的分片的 endpoint。
type RouteSource interface {
	GetRoutesVersion(ctx context.Context) (uint64, error)
	ListShards(ctx context.Context) ([]Shard, error)
	ListNodes(ctx context.Context) ([]Node, error)
}

// Route 是 get_route(session_id) 的返回（§8.4）。
type Route struct {
	ShardID     uint64
	ShardEpoch  uint64
	OwnerNode   string
	Endpoint    string
	OwnerStatus string
}

// ---- 数据面请求/响应（§8.2 状态读写、§8.3 协调原语）----

type PutSessionStateReq struct {
	Sid             string
	Key             string
	Value           []byte
	ExpectedVersion uint64
	ShardEpoch      uint64 // 客户端已知的分片 epoch（§9.1 步骤3 防御校验；0=不带）
}

type PutSessionStateResp struct{ Version uint64 }

type GetSessionStateReq struct{ Sid, Key string }

type GetSessionStateResp struct {
	Value   []byte
	Version uint64
}

type DeleteSessionStateReq struct {
	Sid             string
	Key             string
	ExpectedVersion uint64
	ShardEpoch      uint64
}

type ListSessionStateReq struct {
	Sid       string
	KeyPrefix string
	PageToken string
	Limit     int
}

type StateItem struct {
	Key     string
	Value   []byte
	Version uint64
}

type ListSessionStateResp struct {
	Items         []StateItem
	NextPageToken string
}

type PutSpawnStateReq struct {
	Sid             string
	SpawnID         string
	Key             string
	Value           []byte
	ExpectedVersion uint64
	Epoch           uint64 // Claim Epoch（§8.2 spawn 写的 epoch 参数）
}

type PutSpawnStateResp struct{ Version uint64 }

type GetSpawnStateReq struct{ Sid, SpawnID, Key string }

type GetSpawnStateResp struct {
	Value   []byte
	Version uint64
}

type DeleteSpawnStateReq struct {
	Sid             string
	SpawnID         string
	Key             string
	ExpectedVersion uint64
	Epoch           uint64
}

type ListSpawnStateReq struct {
	Sid       string
	SpawnID   string
	KeyPrefix string
	PageToken string
	Limit     int
}

type ListSpawnStateResp struct {
	Items         []StateItem
	NextPageToken string
}

type ClaimSpawnReq struct {
	Sid        string
	SpawnID    string
	ExecutorID string
}

type ClaimSpawnResp struct {
	Epoch          uint64
	LeaseExpiresAt uint64
	RenewAfterMs   int
}

type RenewSpawnClaimReq struct {
	Sid     string
	SpawnID string
	Epoch   uint64
}

type RenewSpawnClaimResp struct{ LeaseExpiresAt uint64 }

type ReleaseSpawnClaimReq struct {
	Sid     string
	SpawnID string
	Epoch   uint64
}

type GetSpawnClaimReq struct{ Sid, SpawnID string }

type GetSpawnClaimResp struct {
	OwnerID        string
	Epoch          uint64
	LeaseExpiresAt uint64
}

// Conn 是数据面连接抽象：Phase 2 进程内实现；gRPC 层后续实现同一接口。
type Conn interface {
	PutSessionState(ctx context.Context, req PutSessionStateReq) (PutSessionStateResp, error)
	GetSessionState(ctx context.Context, req GetSessionStateReq) (GetSessionStateResp, error)
	DeleteSessionState(ctx context.Context, req DeleteSessionStateReq) error
	ListSessionState(ctx context.Context, req ListSessionStateReq) (ListSessionStateResp, error)

	PutSpawnState(ctx context.Context, req PutSpawnStateReq) (PutSpawnStateResp, error)
	GetSpawnState(ctx context.Context, req GetSpawnStateReq) (GetSpawnStateResp, error)
	DeleteSpawnState(ctx context.Context, req DeleteSpawnStateReq) error
	ListSpawnState(ctx context.Context, req ListSpawnStateReq) (ListSpawnStateResp, error)

	ClaimSpawn(ctx context.Context, req ClaimSpawnReq) (ClaimSpawnResp, error)
	RenewSpawnClaim(ctx context.Context, req RenewSpawnClaimReq) (RenewSpawnClaimResp, error)
	ReleaseSpawnClaim(ctx context.Context, req ReleaseSpawnClaimReq) error
	GetSpawnClaim(ctx context.Context, req GetSpawnClaimReq) (GetSpawnClaimResp, error)
}

// Dialer 按 endpoint 建立数据面连接。
type Dialer interface {
	Dial(ctx context.Context, endpoint string) (Conn, error)
}
