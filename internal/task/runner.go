package task

import (
	"context"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/queue"
)

// Runner 执行一条已经从数据库记录重建出来的任务。
//
// 它接收的是普通值，不是 queue.Job 指针：调用方可以在执行前重新读取
// 数据库、重新取得业务锁，而不是复用提交时留下的闭包或文件句柄。
// 返回值是任务的结果文件 ID；没有结果文件时返回空串。
//
// report 可以为空，用于把展示用进度交给内存队列；它不是检查点，
// 恢复执行不能依赖它。
type Runner interface {
	Run(
		ctx context.Context,
		record model.Task,
		report queue.ProgressFunc,
	) (resultFileID string, err error)
}

// RunnerFunc 方便用普通函数实现一个 Runner。
type RunnerFunc func(
	ctx context.Context,
	record model.Task,
	report queue.ProgressFunc,
) (string, error)

func (f RunnerFunc) Run(
	ctx context.Context,
	record model.Task,
	report queue.ProgressFunc,
) (string, error) {
	return f(ctx, record, report)
}
