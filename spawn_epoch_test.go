package sdk

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPutSpawnStateRejectsZeroEpoch(t *testing.T) {
	sid, _ := findSIDs(t)
	fr := &fakeRoutes{}
	fr.set(1, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	d := newDialer()
	connA := &fakeConn{}
	d.setEndpoint("ep-nA", connA)
	d.setEndpoint("ep-nB", &fakeConn{})

	c := NewClient(fr, d, WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, err := c.PutSpawnState(context.Background(), sid, "sp", "k", []byte("v"), 0, 0)
	if !errors.Is(err, ErrEpochRequired) {
		t.Fatalf("PutSpawnState(claimEpoch=0) = %v, want ErrEpochRequired", err)
	}
	if connA.spawnPutCalls != 0 {
		t.Fatalf("PutSpawnState(claimEpoch=0) sent %d requests, want 0", connA.spawnPutCalls)
	}
}

func TestCASSpawnStateRejectsZeroEpochBeforeRead(t *testing.T) {
	sid, _ := findSIDs(t)
	fr := &fakeRoutes{}
	fr.set(1, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	d := newDialer()
	connA := &fakeConn{}
	d.setEndpoint("ep-nA", connA)
	d.setEndpoint("ep-nB", &fakeConn{})

	c := NewClient(fr, d, WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	called := false
	_, err := c.CASSpawnState(context.Background(), sid, "sp", "k", 0,
		func([]byte, bool, uint64) ([]byte, error) {
			called = true
			return []byte("v"), nil
		})
	if !errors.Is(err, ErrEpochRequired) {
		t.Fatalf("CASSpawnState(claimEpoch=0) = %v, want ErrEpochRequired", err)
	}
	if called {
		t.Fatal("CASSpawnState(claimEpoch=0) ran the user callback")
	}
	if connA.getCalls != 0 || connA.spawnPutCalls != 0 {
		t.Fatalf("CASSpawnState(claimEpoch=0) sent requests: get=%d put=%d", connA.getCalls, connA.spawnPutCalls)
	}
}

func TestDeleteSpawnStateRejectsZeroEpoch(t *testing.T) {
	sid, _ := findSIDs(t)
	fr := &fakeRoutes{}
	fr.set(1, splitTwoShards("nA", "nB"), twoNodeRoutes("nA", "nB"))
	d := newDialer()
	connA := &fakeConn{}
	d.setEndpoint("ep-nA", connA)
	d.setEndpoint("ep-nB", &fakeConn{})

	c := NewClient(fr, d, WithPollInterval(time.Hour))
	if err := c.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	err := c.DeleteSpawnState(context.Background(), sid, "sp", "k", 0, 0)
	if !errors.Is(err, ErrEpochRequired) {
		t.Fatalf("DeleteSpawnState(claimEpoch=0) = %v, want ErrEpochRequired", err)
	}
	if connA.spawnDeleteCalls != 0 {
		t.Fatalf("DeleteSpawnState(claimEpoch=0) sent %d requests, want 0", connA.spawnDeleteCalls)
	}
}
