// Package sdk 是客户端 SDK（§11）：只缓存路由（G1）、版本轮询、REDIRECT/RETRY 策略、
// CAS 重试循环。传输层为可插拔接口（Conn / Dialer / RouteSource），生产实现（gRPC）
// 在 grpctransport 子包。
package sdk

import (
	"errors"
	"fmt"
)

// §10 错误码（SDK 对外的 Go 形态；wire 映射在 gRPC 层完成）。
var (
	ErrConflict        = errors.New("CONFLICT")
	ErrNotFound        = errors.New("NOT_FOUND")
	ErrEpochStale      = errors.New("EPOCH_STALE")
	ErrLeaseExpired    = errors.New("LEASE_EXPIRED")
	ErrAlreadyExists   = errors.New("ALREADY_EXISTS")
	ErrInvalidArgument = errors.New("INVALID_ARGUMENT")
	ErrInternal        = errors.New("INTERNAL")
)

// RetryError 对应 RETRY：临时错误，按 retry_after_ms 退避后重试。
type RetryError struct {
	RetryAfterMs int
}

func (e *RetryError) Error() string {
	return fmt.Sprintf("RETRY: retry_after_ms=%d", e.RetryAfterMs)
}

// RedirectError 对应 REDIRECT：分片已迁移，附 new_owner + endpoint，
// 客户端立即更新路由缓存后重试（§11.2 / G2）。
type RedirectError struct {
	ShardID    uint64
	NewOwner   string
	Endpoint   string
	ShardEpoch uint64
}

func (e *RedirectError) Error() string {
	return fmt.Sprintf("REDIRECT: shard=%d owner=%s endpoint=%s epoch=%d",
		e.ShardID, e.NewOwner, e.Endpoint, e.ShardEpoch)
}

// AlreadyClaimedError 对应 ALREADY_CLAIMED（附当前持有者信息）。
type AlreadyClaimedError struct {
	CurrentOwner          string
	CurrentEpoch          uint64
	CurrentLeaseExpiresAt uint64
}

func (e *AlreadyClaimedError) Error() string {
	return fmt.Sprintf("ALREADY_CLAIMED: owner=%s epoch=%d expires=%d",
		e.CurrentOwner, e.CurrentEpoch, e.CurrentLeaseExpiresAt)
}

// FailedPreconditionError 对应 FAILED_PRECONDITION 子码（DELETING / SESSION_ENDED 等）。
type FailedPreconditionError struct {
	Reason string
}

func (e *FailedPreconditionError) Error() string { return "FAILED_PRECONDITION: " + e.Reason }

// IsRetryable 报告错误是否属于可重试类（RETRY / REDIRECT / 网络类）。
func IsRetryable(err error) bool {
	var r *RetryError
	var d *RedirectError
	return errors.As(err, &r) || errors.As(err, &d)
}
