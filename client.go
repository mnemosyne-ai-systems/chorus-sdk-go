package sdk

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/mnemosyne-ai-systems/chorus-sdk-go/routekey"
)

const (
	// defaultPollInterval §11.1：每 1s 读一次 /routes/version。
	defaultPollInterval = time.Second
	// maxRequestRetries §11.3/G4：同一请求 RETRY 上限 3 次，之后拉全量路由再试一次。
	maxRequestRetries = 3
	// maxCasRounds §7.5：CAS 循环最多 10 轮。
	maxCasRounds = 10
)

// Option 配置 Client。
type Option func(*Client)

// WithPollInterval 覆盖路由轮询周期（测试用短周期）。
func WithPollInterval(d time.Duration) Option {
	return func(c *Client) { c.pollInterval = d }
}

// Client 是 §11 的客户端 SDK：只缓存路由（G1），不缓存业务状态。
type Client struct {
	routes RouteSource
	dialer Dialer

	pollInterval time.Duration

	mu      sync.RWMutex
	version uint64
	shards  []Shard
	nodes   map[string]Node
	loaded  bool
}

// NewClient 构造客户端。
func NewClient(routes RouteSource, dialer Dialer, opts ...Option) *Client {
	c := &Client{
		routes:       routes,
		dialer:       dialer,
		pollInterval: defaultPollInterval,
		nodes:        make(map[string]Node),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// StartRoutePoll 启动 §11.1 的版本轮询：remote > local 时全量重建。
func (c *Client) StartRoutePoll(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(c.pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			v, err := c.routes.GetRoutesVersion(ctx)
			if err != nil {
				continue
			}
			c.mu.RLock()
			local := c.version
			loaded := c.loaded
			c.mu.RUnlock()
			if !loaded || v > local {
				_ = c.Reload(ctx)
			}
		}
	}()
}

// Reload 全量拉取路由（§11.1）：version 读后读两次，
// 不稳定时用较旧的 version 标记 → 下一轮轮询必然再拉（绝不漏更新）。
func (c *Client) Reload(ctx context.Context) error {
	v1, err := c.routes.GetRoutesVersion(ctx)
	if err != nil {
		return err
	}
	shards, err := c.routes.ListShards(ctx)
	if err != nil {
		return err
	}
	nodes, err := c.routes.ListNodes(ctx)
	if err != nil {
		return err
	}
	v2, err := c.routes.GetRoutesVersion(ctx)
	if err != nil {
		return err
	}
	version := v1
	if v2 == v1 {
		version = v2
	} else {
		version = v1 // 偏旧：下轮轮询 v > local 必触发重拉
	}
	c.mu.Lock()
	c.version = version
	c.shards = shards
	c.nodes = make(map[string]Node, len(nodes))
	for _, n := range nodes {
		c.nodes[n.NodeID] = n
	}
	c.loaded = true
	c.mu.Unlock()
	return nil
}

// Route 是 §8.4 get_route(session_id)：本地路由表计算，不打网络。
func (c *Client) Route(sid string) (Route, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	h := routekey.Hash(sid)
	for _, s := range c.shards {
		if !routekey.Contains(s.RangeLo, s.RangeHi, h) {
			continue
		}
		n, ok := c.nodes[s.OwnerNode]
		if !ok {
			return Route{}, &RetryError{RetryAfterMs: retryAfter()}
		}
		return Route{ShardID: s.ShardID, ShardEpoch: s.Epoch, OwnerNode: s.OwnerNode, Endpoint: n.Endpoint, OwnerStatus: n.Status}, nil
	}
	return Route{}, ErrNotFound
}

// RoutesVersion 返回缓存的 /routes/version。
func (c *Client) RoutesVersion() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.version
}

// ListShards 返回缓存的全量分片（§8.4）。
func (c *Client) ListShards() []Shard {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Shard, len(c.shards))
	copy(out, c.shards)
	return out
}

// applyRedirect §11.2/G2：收到 REDIRECT 立即更新本地路由，不等下一次轮询。
//
// epoch 单调性（四之四十 §5）：REDIRECT 携带的 ShardEpoch 低于本地已知 epoch
// 时拒绝覆盖——transfer/split 收敛窗口内，落后节点会带着旧 owner+旧 epoch
// 返回 REDIRECT，盲目覆盖会把本地路由打回旧态、与新 owner 拉锯（实测把
// 收敛窗口拉长到撞防环阈值、~0.1% 请求上抛）。ShardEpoch==0 视为无法证明
// 新鲜度，同样拒绝（fail-closed，调用方全量刷新后以控制面为准）。
//
// 返回值表示**是否真的更新到了本地路由表**。返回 false 的情形：
//   - REDIRECT 指向的分片在本地表里不存在 —— 这在 **split** 之后必然发生
//     （切界会产生一个新分片 id，载荷里只有新 owner/endpoint/epoch，**补不出
//     该分片的区间**）。此时仅靠载荷无法修正路由，必须回退到全量刷新；
//   - epoch 落后（见上）—— 分歧以控制面全量表为准裁决。
//
// 两种情形调用方（do）都立即刷新，而不是继续在旧 owner 上撞墙直到防环
// 阈值把 REDIRECT 抛给调用方。
func (c *Client) applyRedirect(e *RedirectError) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.shards {
		if c.shards[i].ShardID == e.ShardID && e.ShardEpoch < c.shards[i].Epoch {
			return false // 旧 epoch：不覆盖本地新路由，交给全量刷新裁决
		}
	}
	applied := false
	for i := range c.shards {
		if c.shards[i].ShardID == e.ShardID {
			c.shards[i].OwnerNode = e.NewOwner
			if e.ShardEpoch != 0 {
				c.shards[i].Epoch = e.ShardEpoch
			}
			applied = true
		}
	}
	if e.Endpoint != "" {
		if n, ok := c.nodes[e.NewOwner]; ok {
			n.Endpoint = e.Endpoint
			c.nodes[e.NewOwner] = n
		} else {
			c.nodes[e.NewOwner] = Node{NodeID: e.NewOwner, Endpoint: e.Endpoint, Status: "active"}
		}
	}
	return applied
}

// routeOnce 解析 sid → (shard, node)；未加载/未覆盖时先全量刷新一次。
func (c *Client) routeOnce(ctx context.Context, sid string) (Shard, Node, error) {
	try := func() (Shard, Node, error) {
		c.mu.RLock()
		defer c.mu.RUnlock()
		h := routekey.Hash(sid)
		for _, s := range c.shards {
			if routekey.Contains(s.RangeLo, s.RangeHi, h) {
				n, ok := c.nodes[s.OwnerNode]
				if !ok {
					return Shard{}, Node{}, &RetryError{RetryAfterMs: retryAfter()}
				}
				return s, n, nil
			}
		}
		return Shard{}, Node{}, ErrNotFound
	}
	sh, node, err := try()
	if err == nil {
		return sh, node, nil
	}
	// 路由解析失败（未覆盖/endpoint 缺失）→ 全量刷新一次再定位
	if rerr := c.Reload(ctx); rerr == nil {
		return try()
	}
	return Shard{}, Node{}, err
}

// do 是 §11.2/§11.3 的重试策略核心：
//   - RETRY：按 retry_after_ms + jitter 退避，同一请求最多 3 次（G4）；
//     超过后拉全量路由 → 定位新 owner → 再试一次 → 仍失败上抛
//   - REDIRECT：立即更新路由（G2）→ 重试到新节点；反复失败拉全量
//   - EPOCH_STALE / LEASE_EXPIRED 等：直接上抛，不静默重试（G5）
func (c *Client) do(ctx context.Context, sid string, op func(conn Conn, sh Shard, node Node) error) error {
	retries := 0
	redirects := 0
	reloaded := false
	for {
		sh, node, err := c.routeOnce(ctx, sid)
		if err != nil {
			if !reloaded && IsRetryable(err) {
				if rerr := c.Reload(ctx); rerr == nil {
					reloaded = true
					continue
				}
			}
			return err
		}
		conn, err := c.dialer.Dial(ctx, node.Endpoint)
		if err != nil {
			// 网络错误按 RETRY 预算处理（有界，G4：不做无限重试）
			retries++
			if retries >= maxRequestRetries {
				if !reloaded {
					if rerr := c.Reload(ctx); rerr != nil {
						return err
					}
					reloaded = true
					continue
				}
				return err
			}
			sleepWithJitter(retryAfter())
			continue
		}
		err = op(conn, sh, node)
		if err == nil {
			return nil
		}
		var red *RedirectError
		var retry *RetryError
		switch {
		case errors.As(err, &red):
			applied := c.applyRedirect(red) // G2
			redirects++
			// 分片不在本地表里（split 切界会产生新分片 id）：载荷补不出区间，只能全量刷新，
			// 且必须**立刻**做——否则这段时间的请求会一直撞到防环阈值、把 REDIRECT 上抛给调用方
			// （四之二十四：真集群 split 期间实测 0.6% 的请求如此）。reloaded 保证每个请求
			// 最多因此多刷一次，不会形成刷新风暴。
			if !applied && !reloaded {
				if rerr := c.Reload(ctx); rerr != nil {
					return err
				}
				reloaded = true
				continue
			}
			if redirects >= maxRequestRetries && !reloaded {
				if rerr := c.Reload(ctx); rerr != nil {
					return err
				}
				reloaded = true
			}
			if redirects >= 2*maxRequestRetries {
				return err // 防环：全量刷新后仍反复 REDIRECT → 上抛
			}
			continue
		case errors.As(err, &retry):
			retries++
			if retries >= maxRequestRetries {
				if !reloaded {
					// §11.3：超过 3 次 → 拉取全量路由 → 定位新 owner → 重试
					if rerr := c.Reload(ctx); rerr != nil {
						return err
					}
					reloaded = true
					continue
				}
				return err // 仍失败 → 向上抛
			}
			sleepWithJitter(retry.RetryAfterMs)
			continue
		default:
			return err // G5：EPOCH_STALE/LEASE_EXPIRED/CONFLICT/... 原样上抛
		}
	}
}

// ---- §8.2 状态读写 ----

func (c *Client) PutSessionState(ctx context.Context, sid, key string, value []byte, expectedVersion uint64) (PutSessionStateResp, error) {
	var out PutSessionStateResp
	err := c.do(ctx, sid, func(conn Conn, sh Shard, _ Node) error {
		// 始终携带已知 shard epoch（§9.1 步骤3 防御性校验）
		v, err := conn.PutSessionState(ctx, PutSessionStateReq{
			Sid: sid, Key: key, Value: value, ExpectedVersion: expectedVersion, ShardEpoch: sh.Epoch,
		})
		out = v
		return err
	})
	return out, err
}

func (c *Client) GetSessionState(ctx context.Context, sid, key string) (GetSessionStateResp, error) {
	var out GetSessionStateResp
	err := c.do(ctx, sid, func(conn Conn, _ Shard, _ Node) error {
		v, err := conn.GetSessionState(ctx, GetSessionStateReq{Sid: sid, Key: key})
		out = v
		return err
	})
	return out, err
}

func (c *Client) DeleteSessionState(ctx context.Context, sid, key string, expectedVersion uint64) error {
	return c.do(ctx, sid, func(conn Conn, sh Shard, _ Node) error {
		return conn.DeleteSessionState(ctx, DeleteSessionStateReq{
			Sid: sid, Key: key, ExpectedVersion: expectedVersion, ShardEpoch: sh.Epoch,
		})
	})
}

func (c *Client) ListSessionState(ctx context.Context, sid, keyPrefix, pageToken string, limit int) (ListSessionStateResp, error) {
	var out ListSessionStateResp
	err := c.do(ctx, sid, func(conn Conn, _ Shard, _ Node) error {
		v, err := conn.ListSessionState(ctx, ListSessionStateReq{
			Sid: sid, KeyPrefix: keyPrefix, PageToken: pageToken, Limit: limit,
		})
		out = v
		return err
	})
	return out, err
}

// ---- §8.2 spawn 级状态 ----

func (c *Client) PutSpawnState(ctx context.Context, sid, spawnID, key string, value []byte, expectedVersion, claimEpoch uint64) (PutSpawnStateResp, error) {
	var out PutSpawnStateResp
	err := c.do(ctx, sid, func(conn Conn, _ Shard, _ Node) error {
		v, err := conn.PutSpawnState(ctx, PutSpawnStateReq{
			Sid: sid, SpawnID: spawnID, Key: key, Value: value,
			ExpectedVersion: expectedVersion, Epoch: claimEpoch,
		})
		out = v
		return err
	})
	return out, err
}

func (c *Client) GetSpawnState(ctx context.Context, sid, spawnID, key string) (GetSpawnStateResp, error) {
	var out GetSpawnStateResp
	err := c.do(ctx, sid, func(conn Conn, _ Shard, _ Node) error {
		v, err := conn.GetSpawnState(ctx, GetSpawnStateReq{Sid: sid, SpawnID: spawnID, Key: key})
		out = v
		return err
	})
	return out, err
}

func (c *Client) DeleteSpawnState(ctx context.Context, sid, spawnID, key string, expectedVersion, claimEpoch uint64) error {
	return c.do(ctx, sid, func(conn Conn, _ Shard, _ Node) error {
		return conn.DeleteSpawnState(ctx, DeleteSpawnStateReq{
			Sid: sid, SpawnID: spawnID, Key: key, ExpectedVersion: expectedVersion, Epoch: claimEpoch,
		})
	})
}

func (c *Client) ListSpawnState(ctx context.Context, sid, spawnID, keyPrefix, pageToken string, limit int) (ListSpawnStateResp, error) {
	var out ListSpawnStateResp
	err := c.do(ctx, sid, func(conn Conn, _ Shard, _ Node) error {
		v, err := conn.ListSpawnState(ctx, ListSpawnStateReq{
			Sid: sid, SpawnID: spawnID, KeyPrefix: keyPrefix, PageToken: pageToken, Limit: limit,
		})
		out = v
		return err
	})
	return out, err
}

// ---- §8.3 协调原语 ----

func (c *Client) ClaimSpawn(ctx context.Context, sid, spawnID, executorID string) (ClaimSpawnResp, error) {
	var out ClaimSpawnResp
	err := c.do(ctx, sid, func(conn Conn, _ Shard, _ Node) error {
		v, err := conn.ClaimSpawn(ctx, ClaimSpawnReq{Sid: sid, SpawnID: spawnID, ExecutorID: executorID})
		out = v
		return err
	})
	return out, err
}

func (c *Client) RenewSpawnClaim(ctx context.Context, sid, spawnID string, epoch uint64) (RenewSpawnClaimResp, error) {
	var out RenewSpawnClaimResp
	err := c.do(ctx, sid, func(conn Conn, _ Shard, _ Node) error {
		v, err := conn.RenewSpawnClaim(ctx, RenewSpawnClaimReq{Sid: sid, SpawnID: spawnID, Epoch: epoch})
		out = v
		return err
	})
	return out, err
}

func (c *Client) ReleaseSpawnClaim(ctx context.Context, sid, spawnID string, epoch uint64) error {
	return c.do(ctx, sid, func(conn Conn, _ Shard, _ Node) error {
		return conn.ReleaseSpawnClaim(ctx, ReleaseSpawnClaimReq{Sid: sid, SpawnID: spawnID, Epoch: epoch})
	})
}

func (c *Client) GetSpawnClaim(ctx context.Context, sid, spawnID string) (GetSpawnClaimResp, error) {
	var out GetSpawnClaimResp
	err := c.do(ctx, sid, func(conn Conn, _ Shard, _ Node) error {
		v, err := conn.GetSpawnClaim(ctx, GetSpawnClaimReq{Sid: sid, SpawnID: spawnID})
		out = v
		return err
	})
	return out, err
}

// ---- 退避（G3：所有退避带 jitter）----

func retryAfter() int { return 200 + rand.Intn(200) } // §10：200 + jitter(0~200)

// sleepWithJitter 按 baseMs 退避并叠加 0~20% jitter。
func sleepWithJitter(baseMs int) {
	if baseMs < 0 {
		baseMs = 0
	}
	d := time.Duration(baseMs)*time.Millisecond + time.Duration(rand.Intn(baseMs+1))*time.Millisecond/5
	time.Sleep(d)
}
