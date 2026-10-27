// Command worker-lease demonstrates the failure mode Chorus is built for:
// a worker dies, another worker takes over, and the old worker cannot commit a
// duplicate side effect with its stale claim epoch.
//
// The demo uses one process to keep the recording reproducible. It creates a
// unique workspace, session, and spawn, then:
//
//  1. worker-a claims the spawn and writes side_effects=1;
//  2. worker-a stops renewing its lease (simulating a crash);
//  3. worker-b waits for the lease to expire and claims the same spawn;
//  4. worker-a wakes up and tries to write with its old epoch;
//  5. Chorus rejects the stale write, and side_effects stays at 1.
//
// Run it against a Chorus node with a shard that covers the session key space:
//
//	go run ./examples/worker-lease -endpoint 127.0.0.1:9101
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	sdk "github.com/mnemosyne-ai-systems/chorus-sdk-go"
	chorusv1 "github.com/mnemosyne-ai-systems/chorus-sdk-go/api/proto/v1"
	"github.com/mnemosyne-ai-systems/chorus-sdk-go/grpctransport"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	endpoint := flag.String("endpoint", "127.0.0.1:9101", "Chorus node endpoint")
	prefix := flag.String("prefix", "worker-lease", "prefix for demo workspace and session IDs")
	timeout := flag.Duration("timeout", 90*time.Second, "overall demo timeout")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	if err := run(ctx, *endpoint, *prefix); err != nil {
		log.Fatalf("demo failed: %v", err)
	}
}

func run(ctx context.Context, endpoint, prefix string) error {
	dialer := grpctransport.NewDialer()
	defer dialer.Close()

	raw, err := dialer.Dial(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("dial %s: %w", endpoint, err)
	}
	conn, ok := raw.(*grpctransport.Conn)
	if !ok {
		return fmt.Errorf("unexpected connection type %T", raw)
	}
	entity := conn.Entity()

	stamp := time.Now().UnixNano()
	wsID := fmt.Sprintf("%s-ws-%d", prefix, stamp)
	sid := fmt.Sprintf("%s-session-%d", prefix, stamp)
	spawnID := "worker"

	if _, err := entity.CreateWorkspace(ctx, &chorusv1.CreateWorkspaceRequest{
		WsId: wsID,
		Name: "worker lease demo",
	}); err != nil {
		return fmt.Errorf("create workspace: %w", err)
	}
	if _, err := entity.CreateSession(ctx, &chorusv1.CreateSessionRequest{
		WsId: wsID,
		Sid:  sid,
	}); err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	if _, err := entity.CreateSpawn(ctx, &chorusv1.CreateSpawnRequest{
		Sid:     sid,
		SpawnId: spawnID,
	}); err != nil {
		return fmt.Errorf("create spawn: %w", err)
	}

	routes, err := dialer.RouteSource(endpoint)
	if err != nil {
		return fmt.Errorf("route source: %w", err)
	}
	client := sdk.NewClient(routes, dialer)
	if err := client.Reload(ctx); err != nil {
		return fmt.Errorf("reload routes: %w", err)
	}
	if len(client.ListShards()) == 0 {
		return fmt.Errorf("no shards found: create one with chorusctl -confirm create-shard 0 0 <node-id>")
	}
	client.StartRoutePoll(ctx)

	fmt.Printf("session=%s\nspawn=%s\n", sid, spawnID)

	a, err := client.ClaimSpawn(ctx, sid, spawnID, "worker-a")
	if err != nil {
		return fmt.Errorf("worker-a claim: %w", err)
	}
	fmt.Printf("worker-a claimed: epoch=%d renew_after=%dms\n", a.Epoch, a.RenewAfterMs)

	put, err := client.PutSpawnState(ctx, sid, spawnID, "side_effects", []byte("1"), 0, a.Epoch)
	if err != nil {
		return fmt.Errorf("worker-a side effect: %w", err)
	}
	fmt.Printf("worker-a wrote side_effects=1 version=%d\n", put.Version)

	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go renewLoop(hbCtx, client, sid, spawnID, a.Epoch, time.Duration(a.RenewAfterMs)*time.Millisecond)

	if err := sleep(ctx, 2*time.Second); err != nil {
		return err
	}
	fmt.Println("simulating worker-a crash: stop renewing the claim")
	hbCancel()

	b, err := claimWithRetry(ctx, client, sid, spawnID, "worker-b")
	if err != nil {
		return err
	}
	fmt.Printf("worker-b claimed: epoch=%d\n", b.Epoch)

	fmt.Println("worker-a wakes up and tries a stale write")
	_, err = client.PutSpawnState(ctx, sid, spawnID, "side_effects", []byte("2"), put.Version, a.Epoch)
	if err == nil {
		return errors.New("stale write unexpectedly succeeded")
	}
	if !errors.Is(err, sdk.ErrEpochStale) && !errors.Is(err, sdk.ErrLeaseExpired) {
		return fmt.Errorf("stale write returned %v, want EPOCH_STALE or LEASE_EXPIRED", err)
	}
	fmt.Printf("stale write rejected: %v\n", err)

	state, err := client.GetSpawnState(ctx, sid, spawnID, "side_effects")
	if err != nil {
		return fmt.Errorf("read side effect: %w", err)
	}
	if string(state.Value) != "1" {
		return fmt.Errorf("side_effects=%q, want 1", state.Value)
	}
	fmt.Printf("PASS: side_effects=%s (not duplicated)\n", state.Value)
	return nil
}

func claimWithRetry(ctx context.Context, client *sdk.Client, sid, spawnID, executor string) (sdk.ClaimSpawnResp, error) {
	attempt := 0
	for {
		resp, err := client.ClaimSpawn(ctx, sid, spawnID, executor)
		if err == nil {
			return resp, nil
		}
		var claimed *sdk.AlreadyClaimedError
		if !errors.As(err, &claimed) {
			return sdk.ClaimSpawnResp{}, fmt.Errorf("%s claim: %w", executor, err)
		}
		attempt++
		if attempt == 1 || attempt%5 == 0 {
			fmt.Printf("worker-b waiting: owner=%s epoch=%d (attempt %d)\n", claimed.CurrentOwner, claimed.CurrentEpoch, attempt)
		}
		if err := sleep(ctx, time.Second); err != nil {
			return sdk.ClaimSpawnResp{}, err
		}
	}
}

func renewLoop(ctx context.Context, client *sdk.Client, sid, spawnID string, epoch uint64, every time.Duration) {
	if every <= 0 {
		every = 5 * time.Second
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := client.RenewSpawnClaim(ctx, sid, spawnID, epoch); err != nil {
				if ctx.Err() == nil {
					log.Printf("worker-a renew stopped: %v", err)
				}
				return
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
