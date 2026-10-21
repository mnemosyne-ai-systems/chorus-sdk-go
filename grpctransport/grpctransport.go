// Package grpctransport 是客户端 SDK 的 gRPC 传输实现（§3：SDK ──gRPC──> 计算节点）。
//
// 它实现 SDK 的三组抽象：
//   - sdk.Dialer：按 endpoint 建立数据面连接
//   - sdk.Conn：§8.2 状态读写 + §8.3 协调原语
//   - sdk.RouteSource：§8.4 路由查询
//
// 路由查询面为什么由节点提供：§3 的拓扑是"SDK → gRPC → 计算节点"，且 §12.3 明确
// 控制面不做数据面代理。每个节点经 poller 都持有全量路由表、也能直读 TiKV 的 /routes，
// 因此由节点暴露只读的路由查询面，SDK 只需一份 seed 节点地址即可启动（不需要控制面地址配置）。
// 偏差报告 P-1 记录此项：§8.4 未指明 route 查询由谁承载。
//
// 连接是"按 endpoint 懒建 + 复用"：REDIRECT 到新节点会新建连接；
// 旧连接不主动关闭（节点数有限，且关早了会把正在重试的请求打断）。
package grpctransport

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	sdk "github.com/mnemosyne-ai-systems/chorus-sdk-go"
	chorusv1 "github.com/mnemosyne-ai-systems/chorus-sdk-go/api/proto/v1"
)

// Dialer 是 sdk.Dialer 的 gRPC 实现。
type Dialer struct {
	opts []grpc.DialOption

	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]*Conn
	closed  bool
}

// NewDialer 构造拨号器。默认使用 insecure（§4：集群内网，不引入 TLS 依赖）；
// 需要 TLS 时用 WithDialOptions 覆盖。
func NewDialer(opts ...grpc.DialOption) *Dialer {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &Dialer{
		opts:    opts,
		conns:   map[string]*grpc.ClientConn{},
		clients: map[string]*Conn{},
	}
}

// Dial 实现 sdk.Dialer：返回该 endpoint 的数据面连接（同一 endpoint 复用同一连接对象）。
func (d *Dialer) Dial(_ context.Context, endpoint string) (sdk.Conn, error) {
	return d.DialConn(endpoint)
}

// DialConn 与 Dial 相同，但返回具体类型，便于取用实体面（§8.1）与其他服务客户端。
func (d *Dialer) DialConn(endpoint string) (*Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, fmt.Errorf("grpctransport: dialer closed")
	}
	if c, ok := d.clients[endpoint]; ok {
		return c, nil
	}
	// DialContext 是惰性连接（未设 WithBlock，不阻塞）——首次 RPC 才真正建连，
	// 因此"节点已死"表现为 RPC 失败而非 Dial 失败，正合 SDK 的 RETRY/REDIRECT 语义。
	cc, err := grpc.DialContext(context.Background(), endpoint, d.opts...)
	if err != nil {
		return nil, fmt.Errorf("grpctransport: dial %s: %w", endpoint, err)
	}
	c := &Conn{endpoint: endpoint, cc: cc}
	d.conns[endpoint] = cc
	d.clients[endpoint] = c
	return c, nil
}

// RouteSource 返回某个 endpoint 的路由查询面（实现 sdk.RouteSource）。
func (d *Dialer) RouteSource(endpoint string) (*RouteSource, error) {
	c, err := d.DialConn(endpoint)
	if err != nil {
		return nil, err
	}
	return &RouteSource{route: c.Route()}, nil
}

// Close 关闭所有已建立的连接（进程退出或客户端重建时调用）。
func (d *Dialer) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	var first error
	for ep, cc := range d.conns {
		if err := cc.Close(); err != nil && first == nil {
			first = fmt.Errorf("close %s: %w", ep, err)
		}
	}
	d.conns = map[string]*grpc.ClientConn{}
	d.clients = map[string]*Conn{}
	return first
}

// Conn 是到单个节点 endpoint 的 gRPC 连接，实现 sdk.Conn。
type Conn struct {
	endpoint string
	cc       *grpc.ClientConn
}

// Endpoint 返回该连接的目标地址。
func (c *Conn) Endpoint() string { return c.endpoint }

// Entity 返回实体生命周期客户端（§8.1）。实体面暂以 proto 类型直出，
// 原生类型包装待外部框架接入时按实际用法定义（避免先造一堆用不上的类型）。
func (c *Conn) Entity() chorusv1.EntityServiceClient {
	return chorusv1.NewEntityServiceClient(c.cc)
}

// Route 返回路由查询客户端（§8.4）。
func (c *Conn) Route() chorusv1.RouteServiceClient {
	return chorusv1.NewRouteServiceClient(c.cc)
}

// ---- §8.2 session 级状态 ----

func (c *Conn) PutSessionState(ctx context.Context, req sdk.PutSessionStateReq) (sdk.PutSessionStateResp, error) {
	resp, err := chorusv1.NewStateServiceClient(c.cc).PutSessionState(ctx, &chorusv1.PutSessionStateRequest{
		Sid:             req.Sid,
		Key:             req.Key,
		Value:           req.Value,
		ExpectedVersion: req.ExpectedVersion,
		ShardEpoch:      req.ShardEpoch,
	})
	if err != nil {
		return sdk.PutSessionStateResp{}, fromStatus(err)
	}
	return sdk.PutSessionStateResp{Version: resp.GetVersion()}, nil
}

func (c *Conn) GetSessionState(ctx context.Context, req sdk.GetSessionStateReq) (sdk.GetSessionStateResp, error) {
	resp, err := chorusv1.NewStateServiceClient(c.cc).GetSessionState(ctx, &chorusv1.GetSessionStateRequest{
		Sid: req.Sid,
		Key: req.Key,
	})
	if err != nil {
		return sdk.GetSessionStateResp{}, fromStatus(err)
	}
	return sdk.GetSessionStateResp{Value: resp.GetValue(), Version: resp.GetVersion()}, nil
}

func (c *Conn) DeleteSessionState(ctx context.Context, req sdk.DeleteSessionStateReq) error {
	_, err := chorusv1.NewStateServiceClient(c.cc).DeleteSessionState(ctx, &chorusv1.DeleteSessionStateRequest{
		Sid:             req.Sid,
		Key:             req.Key,
		ExpectedVersion: req.ExpectedVersion,
		ShardEpoch:      req.ShardEpoch,
	})
	return fromStatus(err)
}

func (c *Conn) ListSessionState(ctx context.Context, req sdk.ListSessionStateReq) (sdk.ListSessionStateResp, error) {
	resp, err := chorusv1.NewStateServiceClient(c.cc).ListSessionState(ctx, &chorusv1.ListSessionStateRequest{
		Sid:       req.Sid,
		KeyPrefix: req.KeyPrefix,
		Page:      page(req.PageToken, req.Limit),
	})
	if err != nil {
		return sdk.ListSessionStateResp{}, fromStatus(err)
	}
	return sdk.ListSessionStateResp{Items: items(resp.GetItems()), NextPageToken: resp.GetNextPageToken()}, nil
}

// ---- §8.2 spawn 级状态 ----

func (c *Conn) PutSpawnState(ctx context.Context, req sdk.PutSpawnStateReq) (sdk.PutSpawnStateResp, error) {
	resp, err := chorusv1.NewStateServiceClient(c.cc).PutSpawnState(ctx, &chorusv1.PutSpawnStateRequest{
		Sid:             req.Sid,
		SpawnId:         req.SpawnID,
		Key:             req.Key,
		Value:           req.Value,
		ExpectedVersion: req.ExpectedVersion,
		Epoch:           req.Epoch,
	})
	if err != nil {
		return sdk.PutSpawnStateResp{}, fromStatus(err)
	}
	return sdk.PutSpawnStateResp{Version: resp.GetVersion()}, nil
}

func (c *Conn) GetSpawnState(ctx context.Context, req sdk.GetSpawnStateReq) (sdk.GetSpawnStateResp, error) {
	resp, err := chorusv1.NewStateServiceClient(c.cc).GetSpawnState(ctx, &chorusv1.GetSpawnStateRequest{
		Sid:     req.Sid,
		SpawnId: req.SpawnID,
		Key:     req.Key,
	})
	if err != nil {
		return sdk.GetSpawnStateResp{}, fromStatus(err)
	}
	return sdk.GetSpawnStateResp{Value: resp.GetValue(), Version: resp.GetVersion()}, nil
}

func (c *Conn) DeleteSpawnState(ctx context.Context, req sdk.DeleteSpawnStateReq) error {
	_, err := chorusv1.NewStateServiceClient(c.cc).DeleteSpawnState(ctx, &chorusv1.DeleteSpawnStateRequest{
		Sid:             req.Sid,
		SpawnId:         req.SpawnID,
		Key:             req.Key,
		ExpectedVersion: req.ExpectedVersion,
		Epoch:           req.Epoch,
	})
	return fromStatus(err)
}

func (c *Conn) ListSpawnState(ctx context.Context, req sdk.ListSpawnStateReq) (sdk.ListSpawnStateResp, error) {
	resp, err := chorusv1.NewStateServiceClient(c.cc).ListSpawnState(ctx, &chorusv1.ListSpawnStateRequest{
		Sid:       req.Sid,
		SpawnId:   req.SpawnID,
		KeyPrefix: req.KeyPrefix,
		Page:      page(req.PageToken, req.Limit),
	})
	if err != nil {
		return sdk.ListSpawnStateResp{}, fromStatus(err)
	}
	return sdk.ListSpawnStateResp{Items: items(resp.GetItems()), NextPageToken: resp.GetNextPageToken()}, nil
}

// ---- §8.3 协调原语 ----

func (c *Conn) ClaimSpawn(ctx context.Context, req sdk.ClaimSpawnReq) (sdk.ClaimSpawnResp, error) {
	resp, err := chorusv1.NewClaimServiceClient(c.cc).ClaimSpawn(ctx, &chorusv1.ClaimSpawnRequest{
		Sid:        req.Sid,
		SpawnId:    req.SpawnID,
		ExecutorId: req.ExecutorID,
	})
	if err != nil {
		return sdk.ClaimSpawnResp{}, fromStatus(err)
	}
	return sdk.ClaimSpawnResp{
		Epoch:          resp.GetEpoch(),
		LeaseExpiresAt: resp.GetLeaseExpiresAt(),
		RenewAfterMs:   int(resp.GetRenewAfterMs()),
	}, nil
}

func (c *Conn) RenewSpawnClaim(ctx context.Context, req sdk.RenewSpawnClaimReq) (sdk.RenewSpawnClaimResp, error) {
	resp, err := chorusv1.NewClaimServiceClient(c.cc).RenewSpawnClaim(ctx, &chorusv1.RenewSpawnClaimRequest{
		Sid:     req.Sid,
		SpawnId: req.SpawnID,
		Epoch:   req.Epoch,
	})
	if err != nil {
		return sdk.RenewSpawnClaimResp{}, fromStatus(err)
	}
	return sdk.RenewSpawnClaimResp{LeaseExpiresAt: resp.GetLeaseExpiresAt()}, nil
}

func (c *Conn) ReleaseSpawnClaim(ctx context.Context, req sdk.ReleaseSpawnClaimReq) error {
	_, err := chorusv1.NewClaimServiceClient(c.cc).ReleaseSpawnClaim(ctx, &chorusv1.ReleaseSpawnClaimRequest{
		Sid:     req.Sid,
		SpawnId: req.SpawnID,
		Epoch:   req.Epoch,
	})
	return fromStatus(err)
}

func (c *Conn) GetSpawnClaim(ctx context.Context, req sdk.GetSpawnClaimReq) (sdk.GetSpawnClaimResp, error) {
	resp, err := chorusv1.NewClaimServiceClient(c.cc).GetSpawnClaim(ctx, &chorusv1.GetSpawnClaimRequest{
		Sid:     req.Sid,
		SpawnId: req.SpawnID,
	})
	if err != nil {
		return sdk.GetSpawnClaimResp{}, fromStatus(err)
	}
	return sdk.GetSpawnClaimResp{
		OwnerID:        resp.GetOwnerId(),
		Epoch:          resp.GetEpoch(),
		LeaseExpiresAt: resp.GetLeaseExpiresAt(),
	}, nil
}

// ---- §8.4 路由查询 ----

// RouteSource 实现 sdk.RouteSource。
type RouteSource struct{ route chorusv1.RouteServiceClient }

// GetRoutesVersion 读 /routes/version（§11.1：每秒一次的单 key 读）。
func (r *RouteSource) GetRoutesVersion(ctx context.Context) (uint64, error) {
	resp, err := r.route.GetRoutesVersion(ctx, &chorusv1.GetRoutesVersionRequest{})
	if err != nil {
		return 0, fromStatus(err)
	}
	return resp.GetVersion(), nil
}

// ListShards 返回全量分片（§8.4 list_shards）。
func (r *RouteSource) ListShards(ctx context.Context) ([]sdk.Shard, error) {
	resp, err := r.route.ListShards(ctx, &chorusv1.ListShardsRequest{})
	if err != nil {
		return nil, fromStatus(err)
	}
	out := make([]sdk.Shard, 0, len(resp.GetShards()))
	for _, s := range resp.GetShards() {
		out = append(out, sdk.Shard{
			ShardID:   s.GetShardId(),
			RangeLo:   s.GetRangeLo(),
			RangeHi:   s.GetRangeHi(),
			OwnerNode: s.GetOwnerNode(),
			Epoch:     s.GetEpoch(),
			State:     s.GetState(),
		})
	}
	return out, nil
}

// ListNodes 返回全量节点记录（全量重建路由需要 node→endpoint 映射）。
func (r *RouteSource) ListNodes(ctx context.Context) ([]sdk.Node, error) {
	resp, err := r.route.ListNodes(ctx, &chorusv1.ListNodesRequest{})
	if err != nil {
		return nil, fromStatus(err)
	}
	out := make([]sdk.Node, 0, len(resp.GetNodes()))
	for _, n := range resp.GetNodes() {
		out = append(out, sdk.Node{
			NodeID:   n.GetNodeId(),
			Endpoint: n.GetEndpoint(),
			Status:   n.GetStatus(),
		})
	}
	return out, nil
}

// ---- 小工具 ----

// page 组装分页参数：limit<=0 交给服务端取默认（§8.2 默认 1000）。
func page(token string, limit int) *chorusv1.PageRequest {
	if token == "" && limit <= 0 {
		return nil
	}
	p := &chorusv1.PageRequest{PageToken: token}
	if limit > 0 {
		p.Limit = int32(limit)
	}
	return p
}

func items(in []*chorusv1.StateItem) []sdk.StateItem {
	out := make([]sdk.StateItem, 0, len(in))
	for _, it := range in {
		out = append(out, sdk.StateItem{Key: it.GetKey(), Value: it.GetValue(), Version: it.GetVersion()})
	}
	return out
}
