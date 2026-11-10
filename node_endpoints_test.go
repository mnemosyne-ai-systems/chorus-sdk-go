package sdk

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func statelessRoutes() *fakeRoutes {
	fr := &fakeRoutes{}
	fr.set(1,
		[]Shard{{ShardID: 1, RangeLo: 0, RangeHi: 0, OwnerNode: "lb", Epoch: 1, State: "active"}},
		[]Node{{NodeID: "lb", Endpoint: "lb", Status: "active"}},
	)
	return fr
}

func TestNodeEndpointsStickyBySID(t *testing.T) {
	d := newDialer()
	for _, ep := range []string{"n1", "n2", "n3"} {
		d.setEndpoint(ep, &fakeConn{})
	}
	c := NewClient(statelessRoutes(), d,
		WithNodeEndpoints("n1", "n2", "n3"), WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	const sid = "sid-sticky"
	for i := 0; i < 10; i++ {
		if _, err := c.GetSessionState(context.Background(), sid, "k"); err != nil {
			t.Fatal(err)
		}
	}
	used := 0
	for _, ep := range []string{"n1", "n2", "n3"} {
		if d.dialCount(ep) > 0 {
			used++
			if got := d.dialCount(ep); got != 10 {
				t.Fatalf("%s dialed %d times, want 10 (same sid must stick to one node)", ep, got)
			}
		}
	}
	if used != 1 {
		t.Fatalf("%d different nodes used for the same sid, want 1", used)
	}
	if got := d.dialCount("lb"); got != 0 {
		t.Fatalf("virtual lb endpoint was dialed %d times; WithNodeEndpoints must override it", got)
	}
}

func TestNodeEndpointFailoverUsesNextInHashOrder(t *testing.T) {
	order := []string{"n1", "n2", "n3"}
	var sid string
	for i := 0; i < 10000; i++ {
		candidate := fmt.Sprintf("sid-%d", i)
		got := orderNodeEndpoints(candidate, order)
		if got[0] == "n1" && got[1] == "n2" {
			sid = candidate
			break
		}
	}
	if sid == "" {
		t.Fatal("no sid found with n1,n2 ordering")
	}

	d := newDialer()
	// n1 不可达：不注册连接，Dial 会返回错误。
	d.setEndpoint("n2", &fakeConn{})
	d.setEndpoint("n3", &fakeConn{})
	c := NewClient(statelessRoutes(), d,
		WithNodeEndpoints(order...), WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, err := c.GetSessionState(context.Background(), sid, "k"); err != nil {
		t.Fatalf("request should fail over to the second node: %v", err)
	}
	if got := d.dialCount("n1"); got != 1 {
		t.Fatalf("n1 dialed %d times, want 1", got)
	}
	if got := d.dialCount("n2"); got != 1 {
		t.Fatalf("n2 dialed %d times, want 1", got)
	}
	if got := d.dialCount("n3"); got != 0 {
		t.Fatalf("n3 dialed %d times, want 0 (second choice should succeed)", got)
	}
}

// gRPC 的 Dial 是惰性的：节点已死通常表现为第一次 RPC 返回 Unavailable
// （映射为 *RetryError），而不是 Dial 返回错误。这条测试覆盖真实路径。
func TestNodeEndpointFailoverOnRetryError(t *testing.T) {
	order := []string{"n1", "n2"}
	var sid string
	for i := 0; i < 10000; i++ {
		candidate := fmt.Sprintf("sid-%d", i)
		if orderNodeEndpoints(candidate, order)[0] == "n1" {
			sid = candidate
			break
		}
	}
	if sid == "" {
		t.Fatal("no sid found with n1 first")
	}

	d := newDialer()
	connN1 := &fakeConn{}
	connN1.getFn = func(GetSessionStateReq) (GetSessionStateResp, error) {
		return GetSessionStateResp{}, &RetryError{RetryAfterMs: 1}
	}
	connN2 := &fakeConn{}
	d.setEndpoint("n1", connN1)
	d.setEndpoint("n2", connN2)

	c := NewClient(statelessRoutes(), d, WithNodeEndpoints(order...), WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetSessionState(context.Background(), sid, "k"); err != nil {
		t.Fatalf("request should fail over to n2 after RETRY from n1: %v", err)
	}
	if got := d.dialCount("n1"); got != 1 {
		t.Fatalf("n1 dialed %d times, want 1", got)
	}
	if got := d.dialCount("n2"); got != 1 {
		t.Fatalf("n2 dialed %d times, want 1", got)
	}
}

// 只设置 WithNodeEndpoints、不设置 WithSeedEndpoints 时，路由轮询必须把节点
// endpoint 当种子；否则会一直使用构造时传入的单个 endpoint（无状态模式下通常
// 是虚拟的 lb，可能不可达）。
func TestNodeEndpointsUsedAsRouteSeedsWhenNoSeedEndpoints(t *testing.T) {
	lb := &flakyRoutes{fakeRoutes: statelessRoutes()}
	lb.setFail(true) // 虚拟 lb 不可达

	good := &fakeRoutes{}
	good.set(21, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))

	d := newSeedDialer()
	d.setSource("n1", nil, errors.New("n1 down"))
	d.setSource("n2", good, nil)

	c := NewClient(lb, d, WithNodeEndpoints("n1", "n2"), WithPollInterval(5*time.Millisecond))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatalf("Reload should fail over across node endpoints when WithSeedEndpoints is unset: %v", err)
	}
	if got := c.RoutesVersion(); got != 21 {
		t.Fatalf("RoutesVersion=%d, want 21 from node endpoint n2", got)
	}

	// 后台轮询也必须走节点 endpoint，而不是不可达的 lb。
	good.set(22, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c.StartRoutePoll(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.RoutesVersion() == 22 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.RoutesVersion(); got != 22 {
		t.Fatalf("route poll did not use node endpoints: version=%d, want 22", got)
	}
}

func TestNodeEndpointsRouteSeedsWithNilRouteSource(t *testing.T) {
	good := &fakeRoutes{}
	good.set(31, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	d := newSeedDialer()
	d.setSource("n1", good, nil)

	c := NewClient(nil, d, WithNodeEndpoints("n1"), WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatalf("Reload with nil routes and only node endpoints: %v", err)
	}
	if got := c.RoutesVersion(); got != 31 {
		t.Fatalf("RoutesVersion=%d, want 31", got)
	}
}

func TestNodeEndpointDeadlineExceededDoesNotSwitch(t *testing.T) {
	order := []string{"n1", "n2"}
	var sid string
	for i := 0; i < 10000; i++ {
		candidate := fmt.Sprintf("sid-%d", i)
		if orderNodeEndpoints(candidate, order)[0] == "n1" {
			sid = candidate
			break
		}
	}
	if sid == "" {
		t.Fatal("no sid found with n1 first")
	}

	d := newDialer()
	connN1 := &fakeConn{}
	connN1.getFn = func(GetSessionStateReq) (GetSessionStateResp, error) {
		return GetSessionStateResp{}, context.DeadlineExceeded
	}
	connN2 := &fakeConn{}
	d.setEndpoint("n1", connN1)
	d.setEndpoint("n2", connN2)

	c := NewClient(statelessRoutes(), d, WithNodeEndpoints(order...), WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := c.GetSessionState(context.Background(), sid, "k")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("GetSessionState = %v, want context.DeadlineExceeded", err)
	}
	if got := d.dialCount("n1"); got != 1 {
		t.Fatalf("n1 dialed %d times, want 1", got)
	}
	if got := d.dialCount("n2"); got != 0 {
		t.Fatalf("DeadlineExceeded switched to n2 (dials=%d), want 0", got)
	}
	if connN1.getCalls != 1 || connN2.getCalls != 0 {
		t.Fatalf("data calls n1=%d n2=%d, want 1/0", connN1.getCalls, connN2.getCalls)
	}
}

func TestNodeEndpointFailoverIsBounded(t *testing.T) {
	order := []string{"n1", "n2", "n3"}
	d := newDialer()
	conns := make(map[string]*fakeConn, len(order))
	for _, ep := range order {
		conn := &fakeConn{}
		conn.getFn = func(GetSessionStateReq) (GetSessionStateResp, error) {
			return GetSessionStateResp{}, &RetryError{RetryAfterMs: 1}
		}
		conns[ep] = conn
		d.setEndpoint(ep, conn)
	}

	c := NewClient(statelessRoutes(), d, WithNodeEndpoints(order...), WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := c.GetSessionState(context.Background(), "sid-bounded", "k")
	var re *RetryError
	if !errors.As(err, &re) {
		t.Fatalf("GetSessionState = %v, want *RetryError", err)
	}

	total := 0
	for _, conn := range conns {
		total += conn.getCalls
	}
	limit := (maxRequestRetries + 1) * len(order)
	if total > limit {
		t.Fatalf("data-plane calls=%d, over bound %d: node failover is not bounded", total, limit)
	}
	if total < len(order) {
		t.Fatalf("data-plane calls=%d, want at least one full pass over %d nodes", total, len(order))
	}
	t.Logf("all nodes RETRY: %d data-plane calls (bound %d)", total, limit)
}

func TestNodeEndpointListChangeMovesAboutOneOverN(t *testing.T) {
	three := []string{"n1", "n2", "n3"}
	four := []string{"n1", "n2", "n3", "n4"}
	const n = 3000

	movedOnAdd := 0
	movedOnRemove := 0
	for i := 0; i < n; i++ {
		sid := fmt.Sprintf("sid-%d", i)
		a := orderNodeEndpoints(sid, three)[0]
		b := orderNodeEndpoints(sid, four)[0]
		if a != b {
			movedOnAdd++
		}
		if a != b {
			// 从 four 删除 n4 时，只有原本落在 n4 的 sid 会移动。
			movedOnRemove++
		}
	}

	addRatio := float64(movedOnAdd) / n
	if addRatio < 0.17 || addRatio > 0.33 {
		t.Fatalf("adding one node moved %.1f%% of sids, want about 1/4", addRatio*100)
	}
	removeRatio := float64(movedOnRemove) / n
	if removeRatio < 0.17 || removeRatio > 0.33 {
		t.Fatalf("removing one node moved %.1f%% of sids, want about 1/4", removeRatio*100)
	}
}

func TestNodeEndpointsUnsetKeepsRouteBehavior(t *testing.T) {
	fr := statelessRoutes()
	d := newDialer()
	d.setEndpoint("lb", &fakeConn{})
	c := NewClient(fr, d, WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetSessionState(context.Background(), "sid", "k"); err != nil {
		t.Fatal(err)
	}
	if got := d.dialCount("lb"); got != 1 {
		t.Fatalf("lb dialed %d times, want 1", got)
	}
}
