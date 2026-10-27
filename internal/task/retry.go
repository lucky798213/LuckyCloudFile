package task

import "errors"

// retryableError 标记“这次失败可能只是暂时的，重新执行有机会成功”。
//
// 业务在自己的边界决定哪些错误值得重试：来源不存在、越权、大小限制等
// 永久问题不要包；数据库/存储抖动、提交结果不明确等才包。调度器只认
// 这个标记，不猜错误字符串。
type retryableError struct {
	err error
}

func (e *retryableError) Error() string { return e.err.Error() }

func (e *retryableError) Unwrap() error { return e.err }

func (e *retryableError) Retryable() bool { return true }

// Retryable 把一个错误标记为可重试。nil 仍然是 nil。
//
// 重复包装不会改变行为；调用方可以在多个边界分别标记，最后由调度器
// 用 IsRetryable 统一判断。
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &retryableError{err: err}
}

// IsRetryable 判断错误链上是否有可重试标记。
func IsRetryable(err error) bool {
	var retryable interface{ Retryable() bool }
	return errors.As(err, &retryable) && retryable.Retryable()
}
