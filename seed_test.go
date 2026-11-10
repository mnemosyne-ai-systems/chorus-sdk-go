package sdk

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// seedDialer 同时提供数据面拨号和按 endpoint 构造 RouteSource 的能力。
// 生产实现是 grpctransport.Dialer.RouteSourceAt。
type seedDialer struct {
	*fakeDialer

	mu      sync.Mutex
	sources map[string]RouteSource
	errs    map[string]error
	calls   []string
}

func newSeedDialer() *seedDialer {
	return &seedDialer{
		fakeDialer: newDialer(),
		sources:    map[string]RouteSource{},
		errs:       map[string]error{},
	}
}

func (d *seedDialer) RouteSourceAt(endpoint string) (RouteSource, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, endpoint)
	if err := d.errs[endpoint]; err != nil {
		return nil, err
	}
	src, ok := d.sources[endpoint]
	if !ok {
		return nil, errors.New("no route source for " + endpoint)
	}
	return src, nil
}

func (d *seedDialer) setSource(endpoint string, src RouteSource, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err != nil {
		d.errs[endpoint] = err
		return
	}
	delete(d.errs, endpoint)
	d.sources[endpoint] = src
}

func (d *seedDialer) resetCalls() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = nil
}

func (d *seedDialer) callLog() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.calls))
	copy(out, d.calls)
	return out
}

func TestSeedFailoverToSecondEndpoint(t *testing.T) {
	good := &fakeRoutes{}
	good.set(7, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))

	d := newSeedDialer()
	d.setSource("seed-1", nil, errors.New("seed-1 unreachable"))
	d.setSource("seed-2", good, nil)

	c := NewClient(nil, d, WithSeedEndpoints("seed-1", "seed-2"), WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatalf("Reload should fail over to the second seed: %v", err)
	}
	if got := c.RoutesVersion(); got != 7 {
		t.Fatalf("RoutesVersion=%d, want 7 from seed-2", got)
	}
	sid, _ := findSIDs(t)
	if r, err := c.Route(sid); err != nil || r.OwnerNode != "nA" {
		t.Fatalf("Route=%+v err=%v, want route from seed-2", r, err)
	}

	// 成功后记住 seed-2：下一次刷新应直接从它开始，不再撞 seed-1。
	d.resetCalls()
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, ep := range d.callLog() {
		if ep == "seed-1" {
			t.Fatalf("after success the client went back to seed-1: calls=%v", d.callLog())
		}
	}
}

// flakyRoutes 模拟节点可达性变化：源对象本身还在，但 RPC 失败。
type flakyRoutes struct {
	*fakeRoutes
	mu   sync.Mutex
	fail bool
}

func (f *flakyRoutes) setFail(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = v
}

func (f *flakyRoutes) failing() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fail
}

func (f *flakyRoutes) GetRoutesVersion(ctx context.Context) (uint64, error) {
	if f.failing() {
		return 0, errors.New("node down")
	}
	return f.fakeRoutes.GetRoutesVersion(ctx)
}

func (f *flakyRoutes) ListShards(ctx context.Context) ([]Shard, error) {
	if f.failing() {
		return nil, errors.New("node down")
	}
	return f.fakeRoutes.ListShards(ctx)
}

func (f *flakyRoutes) ListNodes(ctx context.Context) ([]Node, error) {
	if f.failing() {
		return nil, errors.New("node down")
	}
	return f.fakeRoutes.ListNodes(ctx)
}

func TestSeedAllUnreachableKeepsCachedRoutes(t *testing.T) {
	good := &fakeRoutes{}
	good.set(3, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	seed1 := &flakyRoutes{fakeRoutes: good}
	seed2 := &flakyRoutes{fakeRoutes: good}

	d := newSeedDialer()
	d.setSource("seed-1", seed1, nil)
	d.setSource("seed-2", seed2, nil)

	c := NewClient(nil, d, WithSeedEndpoints("seed-1", "seed-2"), WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	sid, _ := findSIDs(t)
	before, err := c.Route(sid)
	if err != nil {
		t.Fatal(err)
	}

	// 全部种子不可达：Reload 返回错误，但旧缓存必须原样保留，且不能 panic。
	seed1.setFail(true)
	seed2.setFail(true)
	if err := c.Reload(context.Background()); err == nil {
		t.Fatal("Reload should return an error when every seed is unreachable")
	}
	if got := c.RoutesVersion(); got != 3 {
		t.Fatalf("RoutesVersion=%d after failed reload, want cached 3", got)
	}
	after, err := c.Route(sid)
	if err != nil {
		t.Fatalf("cached route disappeared after failed reload: %v", err)
	}
	if after != before {
		t.Fatalf("cached route changed after failed reload: before=%+v after=%+v", before, after)
	}

	// 轮询也必须安全：全部失败时只是继续退避，不清缓存。
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	c.StartRoutePoll(ctx)
	time.Sleep(20 * time.Millisecond)
	if got := c.RoutesVersion(); got != 3 {
		t.Fatalf("poll with all seeds down cleared cache: version=%d", got)
	}
}
