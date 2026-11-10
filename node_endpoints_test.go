package sdk

import (
	"context"
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