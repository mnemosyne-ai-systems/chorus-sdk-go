package sdk

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// CAS 是 §7.5 的客户端重试循环（session 级）：
//
//	loop (max 10):
//	  读 key → (value, exists, version)   // 不存在记 version=0
//	  fn 构造新值（返回 nil → 删除）
//	  put(expected_version=version)
//	  成功 → return；CONFLICT → 重读再来
//	  EPOCH_STALE / LEASE_EXPIRED / DELETING / NOT_FOUND → break 上抛（G5）
//	  RETRY → 指数退避（10ms 起 ×2 上限 1s 带 jitter）
//
// REDIRECT/网络类由底层 do() 策略吸收，这里只处理业务层结果。
// fn 返回错误即中止（交回上层）。
func (c *Client) CAS(ctx context.Context, sid, key string,
	fn func(old []byte, exists bool, version uint64) (newVal []byte, err error),
) (uint64, error) {
	return c.casLoop(ctx, maxCasRounds,
		func() ([]byte, bool, uint64, error) {
			v, err := c.GetSessionState(ctx, sid, key)
			if errors.Is(err, ErrNotFound) {
				return nil, false, 0, nil
			}
			if err != nil {
				return nil, false, 0, err
			}
			return v.Value, true, v.Version, nil
		},
		func(newVal []byte, expected uint64) (uint64, error) {
			if newVal == nil { // B5：空值走真实删除
				if err := c.DeleteSessionState(ctx, sid, key, expected); err != nil {
					return 0, err
				}
				return expected, nil
			}
			v, err := c.PutSessionState(ctx, sid, key, newVal, expected)
			return v.Version, err
		},
		fn,
	)
}

// CASSpawnState 是 spawn 级的 §7.5 循环（claim epoch 透传）。
func (c *Client) CASSpawnState(ctx context.Context, sid, spawnID, key string, claimEpoch uint64,
	fn func(old []byte, exists bool, version uint64) (newVal []byte, err error),
) (uint64, error) {
	return c.casLoop(ctx, maxCasRounds,
		func() ([]byte, bool, uint64, error) {
			v, err := c.GetSpawnState(ctx, sid, spawnID, key)
			if errors.Is(err, ErrNotFound) {
				return nil, false, 0, nil
			}
			if err != nil {
				return nil, false, 0, err
			}
			return v.Value, true, v.Version, nil
		},
		func(newVal []byte, expected uint64) (uint64, error) {
			if newVal == nil {
				if err := c.DeleteSpawnState(ctx, sid, spawnID, key, expected, claimEpoch); err != nil {
					return 0, err
				}
				return expected, nil
			}
			v, err := c.PutSpawnState(ctx, sid, spawnID, key, newVal, expected, claimEpoch)
			return v.Version, err
		},
		fn,
	)
}

func (c *Client) casLoop(
	ctx context.Context,
	maxRounds int,
	read func() (old []byte, exists bool, version uint64, err error),
	write func(newVal []byte, expected uint64) (uint64, error),
	fn func(old []byte, exists bool, version uint64) (newVal []byte, err error),
) (uint64, error) {
	backoff := 10 * time.Millisecond // §7.5：10ms 起，×2，上限 1s，带 jitter
	for round := 0; round < maxRounds; round++ {
		old, exists, version, err := read()
		if err != nil {
			if isCasRetryable(err) {
				sleepWithJitter(int(backoff / time.Millisecond))
				if backoff < time.Second {
					backoff *= 2
				}
				continue // 读遇到 RETRY → 退避重来（不消耗业务轮次语义：计入 round 防死循环）
			}
			return 0, err
		}
		newVal, err := fn(old, exists, version)
		if err != nil {
			return 0, err // 业务判断中止（交回上层）
		}
		newVersion, err := write(newVal, version)
		if err == nil {
			return newVersion, nil
		}
		switch {
		case errors.Is(err, ErrConflict):
			continue // 重读再来
		case isCasRetryable(err):
			sleepWithJitter(int(backoff / time.Millisecond))
			if backoff < time.Second {
				backoff *= 2
			}
		default:
			return 0, err // G5：EPOCH_STALE/LEASE_EXPIRED/DELETING/NOT_FOUND → break
		}
	}
	// 轮次耗尽：必须返回一个**可分类**的错误。
	//
	// 修复前返回 errors.New("CAS: exceeded max rounds")——一句话，没有包装任何
	// sentinel。后果是调用方无法用 errors.Is 判断"这轮失败到底该不该重试"：
	// 本质是 CONFLICT（另一个写者赢了10 次），语义上应归入冲突；可若改成裸
	// ErrConflict 又会丢掉"轮次耗尽"这个诊断信息。折中：包装 ErrConflict，
	// 让 errors.Is(err, ErrConflict) 成立（分类），同时保留可读原因（诊断）。
	return 0, fmt.Errorf("CAS: exceeded max rounds (%d): %w", maxRounds, ErrConflict)
}

// isCasRetryable：§7.5 里 RETRY/网络类 → 退避重试；REDIRECT 已由 do() 吸收。
func isCasRetryable(err error) bool {
	var r *RetryError
	return errors.As(err, &r)
}
