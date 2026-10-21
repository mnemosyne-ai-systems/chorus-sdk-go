package sdk

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mnemosyne-ai-systems/chorus-sdk-go/routekey"
)

// ---- 测试桩 ----

type fakeRoutes struct {
	mu      sync.Mutex
	version uint64
	shards  []Shard
	nodes   []Node
}

func (f *fakeRoutes) set(v uint64, shards []Shard, nodes []Node) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.version, f.shards, f.nodes = v, shards, nodes
}

func (f *fakeRoutes) GetRoutesVersion(context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.version, nil
}

func (f *fakeRoutes) ListShards(context.Context) ([]Shard, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Shard, len(f.shards))
	copy(out, f.shards)
	return out, nil
}

func (f *fakeRoutes) ListNodes(context.Context) ([]Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Node, len(f.nodes))
	copy(out, f.nodes)
	return out, nil
}

type fakeConn struct {
	mu       sync.Mutex
	putCalls int
	getCalls int

	putFn func(PutSessionStateReq) (PutSessionStateResp, error)
	getFn func(GetSessionStateReq) (GetSessionStateResp, error)
}

func (f *fakeConn) PutSessionState(_ context.Context, req PutSessionStateReq) (PutSessionStateResp, error) {
	f.mu.Lock()
	f.putCalls++
	fn := f.putFn
	f.mu.Unlock()
	if fn == nil {
		return PutSessionStateResp{}, nil
	}
	return fn(req)
}

func (f *fakeConn) GetSessionState(_ context.Context, req GetSessionStateReq) (GetSessionStateResp, error) {
	f.mu.Lock()
	f.getCalls++
	fn := f.getFn
	f.mu.Unlock()
	if fn == nil {
		return GetSessionStateResp{}, nil
	}
	return fn(req)
}

func (f *fakeConn) DeleteSessionState(context.Context, DeleteSessionStateReq) error { return nil }
func (f *fakeConn) ListSessionState(context.Context, ListSessionStateReq) (ListSessionStateResp, error) {
	return ListSessionStateResp{}, nil
}
func (f *fakeConn) PutSpawnState(context.Context, PutSpawnStateReq) (PutSpawnStateResp, error) {
	return PutSpawnStateResp{}, nil
}
func (f *fakeConn) GetSpawnState(context.Context, GetSpawnStateReq) (GetSpawnStateResp, error) {
	return GetSpawnStateResp{}, nil
}
func (f *fakeConn) DeleteSpawnState(context.Context, DeleteSpawnStateReq) error { return nil }
func (f *fakeConn) ListSpawnState(context.Context, ListSpawnStateReq) (ListSpawnStateResp, error) {
	return ListSpawnStateResp{}, nil
}
func (f *fakeConn) ClaimSpawn(context.Context, ClaimSpawnReq) (ClaimSpawnResp, error) {
	return ClaimSpawnResp{}, nil
}
func (f *fakeConn) RenewSpawnClaim(context.Context, RenewSpawnClaimReq) (RenewSpawnClaimResp, error) {
	return RenewSpawnClaimResp{}, nil
}
func (f *fakeConn) ReleaseSpawnClaim(context.Context, ReleaseSpawnClaimReq) error { return nil }
func (f *fakeConn) GetSpawnClaim(context.Context, GetSpawnClaimReq) (GetSpawnClaimResp, error) {
	return GetSpawnClaimResp{}, nil
}

type fakeDialer struct {
	mu    sync.Mutex
	conns map[string]*fakeConn
	dials map[string]int
	fail  error
}

func newDialer() *fakeDialer {
	return &fakeDialer{conns: map[string]*fakeConn{}, dials: map[string]int{}}
}

func (d *fakeDialer) setEndpoint(ep string, conn *fakeConn) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.conns[ep] = conn
}

func (d *fakeDialer) dialCount(ep string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dials[ep]
}

func (d *fakeDialer) totalDials() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, v := range d.dials {
		n += v
	}
	return n
}

func (d *fakeDialer) Dial(_ context.Context, endpoint string) (Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dials[endpoint]++
	if d.fail != nil {
		return nil, d.fail
	}
	c, ok := d.conns[endpoint]
	if !ok {
		return nil, errors.New("dial: no such endpoint " + endpoint)
	}
	return c, nil
}

// 半区间：两分片按 hash 划分，覆盖全空间。
func splitTwoShards(ownerA, ownerB string) []Shard {
	return []Shard{
		{ShardID: 1, RangeLo: 0, RangeHi: 1 << 63, OwnerNode: ownerA, Epoch: 1, State: "active"},
		{ShardID: 2, RangeLo: 1 << 63, RangeHi: 0, OwnerNode: ownerB, Epoch: 1, State: "active"},
	}
}

func twoNodeRoutes(ownerA, ownerB string) []Node {
	return []Node{
		{NodeID: ownerA, Endpoint: "ep-" + ownerA, Status: "active"},
		{NodeID: ownerB, Endpoint: "ep-" + ownerB, Status: "active"},
	}
}

// ---- 测试 ----

func TestRouteLookupByHash(t *testing.T) {
	fr := &fakeRoutes{}
	fr.set(1, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	c := NewClient(fr, newDialer())
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 找一个落在 shard1 / shard2 的 sid
	sid1, sid2 := findSIDs(t)
	r1, err := c.Route(sid1)
	if err != nil || r1.OwnerNode != "nA" || r1.Endpoint != "ep-nA" || r1.ShardID != 1 {
		t.Fatalf("route(%s)=%+v err=%v want nA/shard1", sid1, r1, err)
	}
	r2, err := c.Route(sid2)
	if err != nil || r2.OwnerNode != "nB" || r2.ShardID != 2 {
		t.Fatalf("route(%s)=%+v err=%v want nB/shard2", sid2, r2, err)
	}
}

func findSIDs(t *testing.T) (string, string) {
	t.Helper()
	// 复用 routekey 的 Hash（与 route 同一判定源）
	hashFn := routekey.Hash
	var a, b string
	for i := 0; i < 10000; i++ {
		s := "sid-" + strconv.Itoa(i)
		h := hashFn(s)
		if h < 1<<63 && a == "" {
			a = s
		}
		if h >= 1<<63 && b == "" {
			b = s
		}
		if a != "" && b != "" {
			return a, b
		}
	}
	t.Fatal("no sids found")
	return "", ""
}

// split 切界会产生一个**新分片 id**，而 REDIRECT 载荷里只有新 owner/endpoint/epoch，
// **补不出该分片的区间** ⇒ applyRedirect 只能返回"没更新到"。此时必须立刻全量刷新，
// 否则请求会一直撞在旧 owner 上直到防环阈值，把 REDIRECT 抛给调用方
// （真集群实测：split 期间 0.6% 的请求如此，见四之二十四）。
func TestRedirectToUnknownShardReloadsImmediately(t *testing.T) {
	sid1, _ := findSIDs(t) // sid1 落在 [0, 2^63)
	mid := routekey.Hash(sid1)
	fr := &fakeRoutes{}
	// 客户端初始只看到 split 之前的单分片：全空间、全归 nA
	fr.set(1, []Shard{{ShardID: 1, RangeLo: 0, RangeHi: 0, OwnerNode: "nA", Epoch: 1, State: "active"}},
		twoNodeRoutes("nA", "nB"))
	d := newDialer()
	connA := &fakeConn{}
	connA.putFn = func(PutSessionStateReq) (PutSessionStateResp, error) {
		// 切界已发生，但客户端还不知道新分片 7 的存在 ⇒ 只能 REDIRECT
		return PutSessionStateResp{}, &RedirectError{
			ShardID: 7, NewOwner: "nB", Endpoint: "ep-nB", ShardEpoch: 1,
		}
	}
	connB := &fakeConn{}
	connB.putFn = func(PutSessionStateReq) (PutSessionStateResp, error) {
		return PutSessionStateResp{Version: 9}, nil
	}
	d.setEndpoint("ep-nA", connA)
	d.setEndpoint("ep-nB", connB)

	// 关掉后台轮询：只测"REDIRECT 驱动"的刷新（否则是轮询把新分片带进来的，测不出这条）。
	c := NewClient(fr, d, WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 刷新源此刻已能给出切界后的真实表：分片 1 = [0, hash(sid1))、分片 7 = [hash(sid1), 2^64)
	fr.set(2, []Shard{
		{ShardID: 1, RangeLo: 0, RangeHi: mid, OwnerNode: "nA", Epoch: 2, State: "active"},
		{ShardID: 7, RangeLo: mid, RangeHi: 0, OwnerNode: "nB", Epoch: 1, State: "active"},
	}, twoNodeRoutes("nA", "nB"))

	resp, err := c.PutSessionState(context.Background(), sid1, "k", []byte("v"), 0)
	if err != nil {
		t.Fatalf("REDIRECT 指向本地表里没有的分片时必须立即全量刷新后重试，而不是撞满防环阈值上抛：%v", err)
	}
	if resp.Version != 9 {
		t.Fatalf("version = %d, want 9（应落到新 owner nB）", resp.Version)
	}
	if got := connA.putCalls; got != 1 {
		t.Fatalf("旧 owner 只应被撞一次（撞完立刻刷新），实际 %d 次 —— 说明仍在反复重试旧路由", got)
	}
}

func TestRedirectUpdatesCacheImmediately(t *testing.T) {
	sid1, _ := findSIDs(t)
	fr := &fakeRoutes{}
	fr.set(1, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	d := newDialer()
	connA := &fakeConn{}
	attempt := 0
	connA.putFn = func(PutSessionStateReq) (PutSessionStateResp, error) {
		attempt++
		if attempt == 1 {
			return PutSessionStateResp{}, &RedirectError{
				ShardID: 1, NewOwner: "nB", Endpoint: "ep-nB", ShardEpoch: 2,
			}
		}
		return PutSessionStateResp{Version: 7}, nil
	}
	connB := &fakeConn{}
	connB.putFn = func(PutSessionStateReq) (PutSessionStateResp, error) {
		return PutSessionStateResp{Version: 9}, nil
	}
	d.setEndpoint("ep-nA", connA)
	d.setEndpoint("ep-nB", connB)

	c := NewClient(fr, d, WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	resp, err := c.PutSessionState(context.Background(), sid1, "k", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Version != 9 {
		t.Fatalf("want version 9 from nB, got %d", resp.Version)
	}
	// G2：REDIRECT 后本地缓存立即指向 nB（未触发任何轮询/reload）
	r, err := c.Route(sid1)
	if err != nil || r.OwnerNode != "nB" || r.ShardEpoch != 2 {
		t.Fatalf("cache not updated after redirect: %+v err=%v", r, err)
	}
	// 后续请求直接打 nB（nA 不再被拨号）
	resp, err = c.PutSessionState(context.Background(), sid1, "k", []byte("v2"), 9)
	if err != nil || resp.Version != 9 {
		t.Fatalf("second put: %+v err=%v", resp, err)
	}
	if d.dialCount("ep-nA") != 1 {
		t.Fatalf("nA dialed %d times, want 1", d.dialCount("ep-nA"))
	}
}

func TestRetryBudgetThreeThenReloadThenOne(t *testing.T) {
	sid1, _ := findSIDs(t)
	fr := &fakeRoutes{}
	fr.set(1, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	d := newDialer()
	connA := &fakeConn{}
	connA.putFn = func(PutSessionStateReq) (PutSessionStateResp, error) {
		return PutSessionStateResp{}, &RetryError{RetryAfterMs: 1}
	}
	d.setEndpoint("ep-nA", connA)
	d.setEndpoint("ep-nB", &fakeConn{})

	c := NewClient(fr, d, WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := c.PutSessionState(context.Background(), sid1, "k", []byte("v"), 0)
	var re *RetryError
	if !errors.As(err, &re) {
		t.Fatalf("want *RetryError, got %v", err)
	}
	// §11.3：3 次 RETRY → 全量 reload → 再试 1 次 → 上抛 = 4 次数据面调用
	if connA.putCalls != 4 {
		t.Fatalf("data-plane calls = %d, want 4 (3 retries + 1 post-reload)", connA.putCalls)
	}
}

// —— G4护栏：REDIRECT 防环必须有界 ——
//
// 真实故障场景：两个节点的路由表互相矛盾时，节点 A 让我去 B、B 又让我回 A。
// 若 applyRedirect + 重试没有防环，这就是一次**无限循环**——客户端会持续打两个
// 节点、持续占用连接池，直到上层超时。而 §11.2/G4 要求"不做无限重试"。
//
// 断言口径是"数据面调用次数有硬上界"，而不是"某个具体次数"：上界随
// maxRequestRetries 的调整可以变，但必须是常数。
func TestRedirectCycleIsBounded(t *testing.T) {
	sid1, _ := findSIDs(t)
	fr := &fakeRoutes{}
	fr.set(1, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	d := newDialer()

	// 两个节点互指对方：无限循环的放大器
	connA := &fakeConn{}
	connA.putFn = func(PutSessionStateReq) (PutSessionStateResp, error) {
		return PutSessionStateResp{}, &RedirectError{ShardID: 1, NewOwner: "nB", Endpoint: "ep-nB", ShardEpoch: 2}
	}
	connB := &fakeConn{}
	connB.putFn = func(PutSessionStateReq) (PutSessionStateResp, error) {
		return PutSessionStateResp{}, &RedirectError{ShardID: 1, NewOwner: "nA", Endpoint: "ep-nA", ShardEpoch: 3}
	}
	d.setEndpoint("ep-nA", connA)
	d.setEndpoint("ep-nB", connB)

	c := NewClient(fr, d, WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := c.PutSessionState(context.Background(), sid1, "k", []byte("v"), 0)
	var red *RedirectError
	if !errors.As(err, &red) {
		t.Fatalf("want *RedirectError 上抛, got %v", err)
	}
	total := connA.putCalls + connB.putCalls
	// 上界 = 2*maxRequestRetries（防环阈值），留一倍余量容忍实现调整
	if limit := 2 * 2 * maxRequestRetries; total > limit {
		t.Fatalf("互指 REDIRECT 下数据面调用 %d 次，超过上界 %d —— 防环失效（G4）", total, limit)
	}
	if total == 0 {
		t.Fatal("测试未生效：没有任何数据面调用")
	}
	t.Logf("互指 REDIRECT：%d 次数据面调用后上抛（有界）", total)
}

// —— epoch 单调性：旧 epoch 的 REDIRECT 不得覆盖本地新路由（四之四十 §5）——
//
// 真实故障场景：手动 transfer 期间，落后节点带着旧 owner+旧 epoch 返回
// REDIRECT；若 applyRedirect 盲目覆盖，本地路由被打回旧态、与新 owner
// 拉锯，收敛窗口被拉长到撞防环阈值（真集群实测 ~0.1% 请求上抛）。
// 断言口径：旧 epoch 拒绝且本地表不变；相等/更新 epoch 正常应用；
// epoch=0 无法证明新鲜度，按 fail-closed 拒绝。
func TestApplyRedirectRejectsStaleEpoch(t *testing.T) {
	fr := &fakeRoutes{}
	fr.set(1, []Shard{{ShardID: 1, RangeLo: 0, RangeHi: 0, OwnerNode: "nB", Epoch: 5, State: "active"}},
		twoNodeRoutes("nA", "nB"))
	c := NewClient(fr, newDialer(), WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 旧 epoch（4 < 5）：拒绝覆盖，返回"没更新到"，本地表保持 nB/epoch5
	if applied := c.applyRedirect(&RedirectError{ShardID: 1, NewOwner: "nA", Endpoint: "ep-nA", ShardEpoch: 4}); applied {
		t.Fatal("旧 epoch 的 REDIRECT 必须被拒绝（不得覆盖本地新路由）")
	}
	if sh, _ := c.Route("s"); sh.OwnerNode != "nB" || sh.ShardEpoch != 5 {
		t.Fatalf("拒绝后本地路由 = %s/epoch%d, want nB/epoch5 —— 陈旧 REDIRECT 泄漏进了路由表", sh.OwnerNode, sh.ShardEpoch)
	}

	// epoch=0：无法证明新鲜度，fail-closed 同样拒绝
	if applied := c.applyRedirect(&RedirectError{ShardID: 1, NewOwner: "nA", Endpoint: "ep-nA", ShardEpoch: 0}); applied {
		t.Fatal("ShardEpoch=0 的 REDIRECT 必须被拒绝（无法证明新鲜度，fail-closed）")
	}

	// 相等 epoch：幂等应用（endpoint 可能刷新）
	if applied := c.applyRedirect(&RedirectError{ShardID: 1, NewOwner: "nB", Endpoint: "ep-nB2", ShardEpoch: 5}); !applied {
		t.Fatal("相等 epoch 的 REDIRECT 应正常应用")
	}

	// 更新 epoch：正常学习
	if applied := c.applyRedirect(&RedirectError{ShardID: 1, NewOwner: "nA", Endpoint: "ep-nA", ShardEpoch: 6}); !applied {
		t.Fatal("更新 epoch 的 REDIRECT 应正常应用")
	}
	if sh, _ := c.Route("s"); sh.OwnerNode != "nA" || sh.ShardEpoch != 6 {
		t.Fatalf("应用后路由 = %s/epoch%d, want nA/epoch6", sh.OwnerNode, sh.ShardEpoch)
	}
}

// —— G4 护栏：CAS 的双层循环放大必须有界 ——
//
// cas.go 外层最多 maxCasRounds(10)轮，每轮内部调用 do()，而 do() 自己还会
// 重试（RETRY≤3+1、REDIRECT≤6）。两層相乘意味着最坏情况下的网络往返是
// 10 × 10 = 100 次。这个上界必须被测试钉住：将来任何人在任一层加重试或去掉
// 防环，这里都会立刻失败，而不是等故障期才发现客户端在打满连接池。
func TestCasLoopTotalCallsAreBounded(t *testing.T) {
	sid1, _ := findSIDs(t)
	fr := &fakeRoutes{}
	fr.set(1, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	d := newDialer()
	connA := &fakeConn{}
	// 永远 CONFLICT：逼CAS 循环跑满 maxCasRounds
	connA.putFn = func(PutSessionStateReq) (PutSessionStateResp, error) {
		return PutSessionStateResp{}, ErrConflict
	}
	d.setEndpoint("ep-nA", connA)
	d.setEndpoint("ep-nB", &fakeConn{})

	c := NewClient(fr, d, WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := c.CAS(context.Background(), sid1, "k", func(cur []byte, exists bool, version uint64) ([]byte, error) {
		return []byte("v"), nil
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("满轮冲突后应上抛 CONFLICT，got %v", err)
	}
	// 每轮 CAS 只调一次数据面（CONFLICT 立即 continue，不进 do() 的重试分支），
	// 所以总调用应恰好等于轮数；留一倍余量。
	if limit := 2 * maxCasRounds; connA.putCalls > limit {
		t.Fatalf("CAS 满轮冲突产生 %d 次数据面调用，超过上界 %d", connA.putCalls, limit)
	}
	if connA.putCalls < 2 {
		t.Fatalf("CAS 循环疑似未重试（calls=%d），测试口径失效", connA.putCalls)
	}
	t.Logf("CAS 满轮冲突：%d 次数据面调用（轮数上界 %d）", connA.putCalls, maxCasRounds)
}

func TestEpochStaleSurfacesWithoutRetry(t *testing.T) {
	sid1, _ := findSIDs(t)
	fr := &fakeRoutes{}
	fr.set(1, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	d := newDialer()
	connA := &fakeConn{}
	connA.putFn = func(PutSessionStateReq) (PutSessionStateResp, error) {
		return PutSessionStateResp{}, ErrEpochStale
	}
	d.setEndpoint("ep-nA", connA)
	d.setEndpoint("ep-nB", &fakeConn{})

	c := NewClient(fr, d, WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := c.PutSessionState(context.Background(), sid1, "k", []byte("v"), 0)
	if !errors.Is(err, ErrEpochStale) {
		t.Fatalf("want EPOCH_STALE, got %v", err)
	}
	if connA.putCalls != 1 {
		t.Fatalf("G5: EPOCH_STALE must not retry, calls=%d", connA.putCalls)
	}
}

func TestPollRebuildsOnVersionBump(t *testing.T) {
	sid1, _ := findSIDs(t)
	fr := &fakeRoutes{}
	fr.set(1, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	c := NewClient(fr, newDialer(), WithPollInterval(5*time.Millisecond))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.RoutesVersion() != 1 {
		t.Fatalf("version=%d", c.RoutesVersion())
	}
	// 迁移：shard1 → nB
	fr.set(2, []Shard{
		{ShardID: 1, RangeLo: 0, RangeHi: 1 << 63, OwnerNode: "nB", Epoch: 2, State: "active"},
		{ShardID: 2, RangeLo: 1 << 63, RangeHi: 0, OwnerNode: "nB", Epoch: 1, State: "active"},
	}, twoNodeRoutes("nA", "nB"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c.StartRoutePoll(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r, err := c.Route(sid1)
		if err == nil && r.OwnerNode == "nB" && r.ShardEpoch == 2 && c.RoutesVersion() == 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("poll did not rebuild route cache")
}

func TestCASConflictRetriesAndSucceeds(t *testing.T) {
	sid1, _ := findSIDs(t)
	fr := &fakeRoutes{}
	fr.set(1, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	d := newDialer()
	connA := &fakeConn{}
	readN, writeN := 0, 0
	connA.getFn = func(GetSessionStateReq) (GetSessionStateResp, error) {
		readN++
		return GetSessionStateResp{Value: []byte("old"), Version: uint64(readN)}, nil
	}
	connA.putFn = func(PutSessionStateReq) (PutSessionStateResp, error) {
		writeN++
		if writeN <= 2 {
			return PutSessionStateResp{}, ErrConflict
		}
		return PutSessionStateResp{Version: uint64(writeN)}, nil
	}
	d.setEndpoint("ep-nA", connA)
	d.setEndpoint("ep-nB", &fakeConn{})

	c := NewClient(fr, d, WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	rounds := 0
	v, err := c.CAS(context.Background(), sid1, "k", func(old []byte, exists bool, version uint64) ([]byte, error) {
		rounds++
		if !exists || string(old) != "old" {
			t.Fatalf("bad read: exists=%v old=%q", exists, old)
		}
		return []byte("new"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if rounds != 3 || v != 3 {
		t.Fatalf("rounds=%d version=%d, want 3/3", rounds, v)
	}
}

func TestCASNotFoundStartsFresh(t *testing.T) {
	sid1, _ := findSIDs(t)
	fr := &fakeRoutes{}
	fr.set(1, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	d := newDialer()
	connA := &fakeConn{}
	connA.getFn = func(GetSessionStateReq) (GetSessionStateResp, error) {
		return GetSessionStateResp{}, ErrNotFound
	}
	connA.putFn = func(PutSessionStateReq) (PutSessionStateResp, error) {
		return PutSessionStateResp{Version: 1}, nil
	}
	d.setEndpoint("ep-nA", connA)
	d.setEndpoint("ep-nB", &fakeConn{})

	c := NewClient(fr, d, WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := c.CAS(context.Background(), sid1, "k", func(old []byte, exists bool, version uint64) ([]byte, error) {
		if exists || version != 0 || old != nil {
			t.Fatalf("want fresh read, got exists=%v version=%d old=%q", exists, version, old)
		}
		return []byte("first"), nil
	})
	if err != nil || v != 1 {
		t.Fatalf("v=%d err=%v", v, err)
	}
}

func TestCASBreaksOnEpochStale(t *testing.T) {
	sid1, _ := findSIDs(t)
	fr := &fakeRoutes{}
	fr.set(1, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	d := newDialer()
	connA := &fakeConn{}
	connA.putFn = func(PutSessionStateReq) (PutSessionStateResp, error) {
		return PutSessionStateResp{}, ErrEpochStale
	}
	d.setEndpoint("ep-nA", connA)
	d.setEndpoint("ep-nB", &fakeConn{})

	c := NewClient(fr, d, WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	rounds := 0
	_, err := c.CAS(context.Background(), sid1, "k", func([]byte, bool, uint64) ([]byte, error) {
		rounds++
		return []byte("x"), nil
	})
	if !errors.Is(err, ErrEpochStale) {
		t.Fatalf("want EPOCH_STALE break, got %v", err)
	}
	if rounds != 1 {
		t.Fatalf("G5: rounds=%d want 1", rounds)
	}
}

func TestDialFailureBounded(t *testing.T) {
	sid1, _ := findSIDs(t)
	fr := &fakeRoutes{}
	fr.set(1, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	d := newDialer()
	d.fail = errors.New("conn refused")

	c := NewClient(fr, d, WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := c.PutSessionState(context.Background(), sid1, "k", []byte("v"), 0)
	if err == nil {
		t.Fatal("want dial error surfaced")
	}
	// G4 有界重试：3 次 → reload → 1 次 → 上抛 = 4 次拨号
	if got := d.totalDials(); got != 4 {
		t.Fatalf("dials=%d, want 4", got)
	}
}
