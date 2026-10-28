package task

import (
	"errors"
	"fmt"
)

// panicError 把业务 panic 转成普通错误，交给持久化状态收尾处理。
//
// 它刻意不带 Retryable 标记：panic 通常是代码缺陷，重跑同一份输入很可能
// 再次 panic。调度器会先核对是否已有发布结果；没有结果时按永久失败收尾。
type panicError struct {
	value any
}

func (e *panicError) Error() string {
	return fmt.Sprintf("task panic: %v", e.value)
}

func (e *panicError) Unwrap() error {
	err, _ := e.value.(error)
	return err
}

func isPanicError(err error) bool {
	var target *panicError
	return errors.As(err, &target)
}
