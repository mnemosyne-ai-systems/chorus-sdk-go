package grpctransport

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sdk "github.com/mnemosyne-ai-systems/chorus-sdk-go"
)

// TestDegradedFallbackKeepsStopSemantics 锁住「详情被中间层剥离」这一降级路径的语义。
//
// ChorusError 详情走 grpc-status-details-bin trailer，它不是 gRPC 强制转发的部分——
// 反向代理 / API 网关常把它剥掉。此时 fromStatus 只能靠 gRPC code 兜底，而兜底口径
// 错一次就会把 fencing 失败变成版本冲突：sdk.CAS 的重试循环对 CONFLICT 是
// 「重新读取后重试」（§7.5），于是客户端会一直重试到耗尽轮次，而不是按 G5
// 立即停止执行——对一个已经被 fencing 掉的 executor，这是最危险的错误方向。
//
// 因此约束只有一条，但必须双向：
//   - codes.Aborted **只能**由 CONFLICT 投影（它唯一对应的语义就是"可重试"）；
//   - 其余"停止执行"类信号（EPOCH_STALE / LEASE_EXPIRED / DELETING 共用
//     codes.FailedPrecondition）降级后**绝不能**变成 ErrConflict。
func TestDegradedFallbackKeepsStopSemantics(t *testing.T) {
	t.Run("aborted_is_conflict", func(t *testing.T) {
		err := fromStatus(status.New(codes.Aborted, "no detail").Err())
		if !errors.Is(err, sdk.ErrConflict) {
			t.Fatalf("Aborted fallback must be ErrConflict, got %v", err)
		}
	})

	stopCodes := []struct {
		name string
		code codes.Code
	}{
		{"epoch_stale", codes.FailedPrecondition},
		{"lease_expired", codes.FailedPrecondition},
		{"deleting", codes.FailedPrecondition},
	}
	for _, c := range stopCodes {
		t.Run(c.name+"_never_degrades_to_conflict", func(t *testing.T) {
			err := fromStatus(status.New(c.code, "no detail").Err())
			if errors.Is(err, sdk.ErrConflict) {
				t.Fatalf("%v degraded to ErrConflict: CAS 循环会重试，违反 G5", c.code)
			}
			if sdk.IsRetryable(err) {
				t.Fatalf("%v degraded to a retryable error: %v", c.code, err)
			}
		})
	}
}
