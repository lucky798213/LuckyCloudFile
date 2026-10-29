package database

import (
	"context"
	"errors"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

// MySQL 的并发事务冲突错误码。InnoDB 遇到这两种错误时会回滚整笔事务，
// 因此调用方可以安全地重新开启事务重做，而不是把错误当成业务失败。
const (
	errLockDeadlock    = 1213 // ER_LOCK_DEADLOCK
	errLockWaitTimeout = 1205 // ER_LOCK_WAIT_TIMEOUT
)

// maxRetryDelayShift 限制指数退避的位数，避免重试次数变大时等待失控。
const maxRetryDelayShift = 4

// IsRetryableConflict 判断错误是否来自 MySQL 死锁或锁等待超时。
//
// 只认驱动返回的错误码，不按错误字符串猜测；其它数据库错误（包括唯一键
// 冲突和连接失败）仍然交给调用方按自己的语义处理。
func IsRetryableConflict(err error) bool {
	var mysqlErr *mysqlDriver.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	return mysqlErr.Number == errLockDeadlock ||
		mysqlErr.Number == errLockWaitTimeout
}

// WaitForRetry 等待第 attempt 次冲突后的退避时间；attempt 从 0 开始。
//
// 等待期间响应调用方取消，避免请求已经断开后还在后台重试。
func WaitForRetry(ctx context.Context, attempt int) error {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > maxRetryDelayShift {
		attempt = maxRetryDelayShift
	}

	timer := time.NewTimer(time.Duration(5*(1<<attempt)) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
