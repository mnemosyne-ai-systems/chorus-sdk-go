package grpctransport

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	sdk "github.com/mnemosyne-ai-systems/chorus-sdk-go"
)

// TestRoutingMetadata 用客户端拦截器断言所有带 sid 的调用都携带 x-chorus-sid，
// spawn 级调用再额外携带 x-chorus-spawn。七层负载均衡依赖这两个头做一致性哈希。
func TestRoutingMetadata(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	var last metadata.MD
	cc, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			md, _ := metadata.FromOutgoingContext(ctx)
			last = md.Copy()
			// 只验证元数据，不真正发 RPC。
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	conn := &Conn{endpoint: "bufnet", cc: cc}
	const (
		sid   = "sid-1"
		spawn = "sp-1"
	)
	ctx := context.Background()

	sessionCalls := []struct {
		name string
		call func() error
	}{
		{"PutSessionState", func() error {
			_, err := conn.PutSessionState(ctx, sdk.PutSessionStateReq{Sid: sid, Key: "k"})
			return err
		}},
		{"GetSessionState", func() error {
			_, err := conn.GetSessionState(ctx, sdk.GetSessionStateReq{Sid: sid, Key: "k"})
			return err
		}},
		{"DeleteSessionState", func() error {
			return conn.DeleteSessionState(ctx, sdk.DeleteSessionStateReq{Sid: sid, Key: "k"})
		}},
		{"ListSessionState", func() error {
			_, err := conn.ListSessionState(ctx, sdk.ListSessionStateReq{Sid: sid})
			return err
		}},
	}
	for _, tc := range sessionCalls {
		t.Run(tc.name, func(t *testing.T) {
			last = nil
			if err := tc.call(); err != nil {
				t.Fatal(err)
			}
			assertMetadata(t, last, sid, "")
		})
	}

	spawnCalls := []struct {
		name string
		call func() error
	}{
		{"PutSpawnState", func() error {
			_, err := conn.PutSpawnState(ctx, sdk.PutSpawnStateReq{Sid: sid, SpawnID: spawn, Key: "k", Epoch: 1})
			return err
		}},
		{"GetSpawnState", func() error {
			_, err := conn.GetSpawnState(ctx, sdk.GetSpawnStateReq{Sid: sid, SpawnID: spawn, Key: "k"})
			return err
		}},
		{"DeleteSpawnState", func() error {
			return conn.DeleteSpawnState(ctx, sdk.DeleteSpawnStateReq{Sid: sid, SpawnID: spawn, Key: "k", Epoch: 1})
		}},
		{"ListSpawnState", func() error {
			_, err := conn.ListSpawnState(ctx, sdk.ListSpawnStateReq{Sid: sid, SpawnID: spawn})
			return err
		}},
		{"ClaimSpawn", func() error {
			_, err := conn.ClaimSpawn(ctx, sdk.ClaimSpawnReq{Sid: sid, SpawnID: spawn, ExecutorID: "exec"})
			return err
		}},
		{"RenewSpawnClaim", func() error {
			_, err := conn.RenewSpawnClaim(ctx, sdk.RenewSpawnClaimReq{Sid: sid, SpawnID: spawn, Epoch: 1})
			return err
		}},
		{"ReleaseSpawnClaim", func() error {
			return conn.ReleaseSpawnClaim(ctx, sdk.ReleaseSpawnClaimReq{Sid: sid, SpawnID: spawn, Epoch: 1})
		}},
		{"GetSpawnClaim", func() error {
			_, err := conn.GetSpawnClaim(ctx, sdk.GetSpawnClaimReq{Sid: sid, SpawnID: spawn})
			return err
		}},
	}
	for _, tc := range spawnCalls {
		t.Run(tc.name, func(t *testing.T) {
			last = nil
			if err := tc.call(); err != nil {
				t.Fatal(err)
			}
			assertMetadata(t, last, sid, spawn)
		})
	}
}

func assertMetadata(t *testing.T, md metadata.MD, wantSID, wantSpawn string) {
	t.Helper()
	if got := md.Get("x-chorus-sid"); len(got) != 1 || got[0] != wantSID {
		t.Fatalf("x-chorus-sid = %v, want [%s]", got, wantSID)
	}
	gotSpawn := md.Get("x-chorus-spawn")
	if wantSpawn == "" {
		if len(gotSpawn) != 0 {
			t.Fatalf("x-chorus-spawn = %v, want absent", gotSpawn)
		}
		return
	}
	if len(gotSpawn) != 1 || gotSpawn[0] != wantSpawn {
		t.Fatalf("x-chorus-spawn = %v, want [%s]", gotSpawn, wantSpawn)
	}
}

