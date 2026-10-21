package grpctransport

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sdk "github.com/mnemosyne-ai-systems/chorus-sdk-go"
	chorusv1 "github.com/mnemosyne-ai-systems/chorus-sdk-go/api/proto/v1"
)

// fromStatus 把 gRPC status 还原成 SDK 的错误类型（§10 / §11.2 / §11.3 / G5）。
//
// 权威语义在 ChorusError 详情里（见 api/proto/v1/chorus.proto 的错误传递约定）；
// gRPC status code 只作为详情缺失时的兜底。
//
// G5 的红线：LEASE_EXPIRED / EPOCH_STALE 必须原样上抛为可识别的错误类型，
// 让调用方停止执行——绝不在传输层静默重试（重试策略只认 RETRY/REDIRECT，见 sdk.IsRetryable）。
func fromStatus(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err // 非 gRPC 错误（连接层/上下文取消等）：原样上抛
	}

	detail := detailOf(st)
	switch detail.GetCode() {
	case chorusv1.ErrorCode_CONFLICT:
		return sdk.ErrConflict
	case chorusv1.ErrorCode_EPOCH_STALE:
		return sdk.ErrEpochStale
	case chorusv1.ErrorCode_ALREADY_CLAIMED:
		return &sdk.AlreadyClaimedError{
			CurrentOwner:          detail.GetCurrentOwner(),
			CurrentEpoch:          detail.GetCurrentEpoch(),
			CurrentLeaseExpiresAt: detail.GetCurrentLeaseExpiresAt(),
		}
	case chorusv1.ErrorCode_NOT_FOUND:
		return sdk.ErrNotFound
	case chorusv1.ErrorCode_LEASE_EXPIRED:
		return sdk.ErrLeaseExpired
	case chorusv1.ErrorCode_DELETING:
		return &sdk.FailedPreconditionError{Reason: "DELETING"}
	case chorusv1.ErrorCode_FAILED_PRECONDITION:
		return &sdk.FailedPreconditionError{Reason: detail.GetMessage()}
	case chorusv1.ErrorCode_ALREADY_EXISTS:
		return sdk.ErrAlreadyExists
	case chorusv1.ErrorCode_INVALID_ARGUMENT:
		return fmt.Errorf("%w: %s", sdk.ErrInvalidArgument, detail.GetMessage())
	case chorusv1.ErrorCode_REDIRECT:
		return &sdk.RedirectError{
			ShardID:    detail.GetShardId(),
			NewOwner:   detail.GetNewOwner(),
			Endpoint:   detail.GetEndpoint(),
			ShardEpoch: detail.GetShardEpoch(),
		}
	case chorusv1.ErrorCode_RETRY:
		return &sdk.RetryError{RetryAfterMs: int(detail.GetRetryAfterMs())}
	case chorusv1.ErrorCode_INTERNAL:
		return fmt.Errorf("%w: %s", sdk.ErrInternal, detail.GetMessage())
	case chorusv1.ErrorCode_ERROR_CODE_UNSPECIFIED:
		// 详情缺失（例如对端不是本项目实现）→ 退回 gRPC code 兜底。
	default:
	}

	return fallback(st)
}

// detailOf 从 status 里取出 ChorusError 详情；不存在时返回空详情。
func detailOf(st *status.Status) *chorusv1.ChorusError {
	for _, d := range st.Details() {
		if ce, ok := d.(*chorusv1.ChorusError); ok {
			return ce
		}
	}
	return &chorusv1.ChorusError{}
}

// fallback 在无详情时按 gRPC code 给出最接近的语义。
// RETRY 用 200ms 兜底（§10 的约定是 200 + jitter，这里取地板值，宁可退避不足也别不动）。
func fallback(st *status.Status) error {
	switch st.Code() {
	case codes.NotFound:
		return sdk.ErrNotFound
	case codes.AlreadyExists:
		return sdk.ErrAlreadyExists
	case codes.Aborted:
		return sdk.ErrConflict
	case codes.InvalidArgument:
		return fmt.Errorf("%w: %s", sdk.ErrInvalidArgument, st.Message())
	case codes.FailedPrecondition:
		return &sdk.FailedPreconditionError{Reason: st.Message()}
	case codes.Unavailable:
		return &sdk.RetryError{RetryAfterMs: 200}
	case codes.DeadlineExceeded, codes.Canceled:
		return st.Err() // 超时/取消：由调用方按上下文语义处理，不伪装成 RETRY
	default:
		return fmt.Errorf("%w: %s", sdk.ErrInternal, st.Message())
	}
}
