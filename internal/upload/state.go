// Package upload 管理上传会话的状态规则。
//
// 本阶段只定义“状态怎么变”和“确认完成时要不要创建文件”，
// 不实现字节传输，也不提供 HTTP 接口。创建/查询会话接口在下一步实现。
package upload

import (
	"errors"
	"fmt"

	"luckyCloudFile/internal/model"
)

var (
	// ErrInvalidTransition 表示当前状态不允许变成目标状态。
	ErrInvalidTransition = errors.New("invalid upload session status transition")

	// ErrNotCompletable 表示会话还没有进入 ready，不能确认完成。
	ErrNotCompletable = errors.New("upload session is not ready to complete")

	// ErrMissingResultFile 表示会话已经是 completed，却没有记录结果文件。
	// 正常流程不会出现，说明数据库数据已经不一致。
	ErrMissingResultFile = errors.New("completed upload session has no result file")
)

// CanTransition 判断会话能否从 from 变成 to。
//
// 允许的转换：
//
//	pending   → receiving / canceled / expired
//	receiving → ready / pending / canceled / expired
//	ready     → completed / canceled / expired
//	completed → completed（重复确认完成，幂等重放）
//
// receiving → pending 表示这次写入失败且已经停止，客户端可以重新上传。
// completed 不能再取消或过期，所以它只能停留在 completed。
// canceled / expired 也都是终态：取消不能把 expired 改写成 canceled，
// 过期扫描也不能把 canceled 改写成 expired；两者只继续推进物理清理。
func CanTransition(from, to string) bool {
	switch from {
	case model.UploadStatusPending:
		switch to {
		case model.UploadStatusReceiving, model.UploadStatusCanceled, model.UploadStatusExpired:
			return true
		}
	case model.UploadStatusReceiving:
		switch to {
		case model.UploadStatusReady, model.UploadStatusPending,
			model.UploadStatusCanceled, model.UploadStatusExpired:
			return true
		}
	case model.UploadStatusReady:
		switch to {
		case model.UploadStatusCompleted, model.UploadStatusCanceled, model.UploadStatusExpired:
			return true
		}
	case model.UploadStatusCompleted:
		// 重复确认完成不是一次新的状态转换：调用方应当直接返回已有的
		// ResultFileID，不能再创建文件。这里返回 true 只是让幂等重放
		// 有一个统一入口，Transition 不会修改任何字段。
		return to == model.UploadStatusCompleted
	}
	return false
}

// Transition 在内存里应用一次状态转换。
//
// 它只负责状态本身，不写数据库；调用方需要在事务里把新的 Status 保存回去。
// 非法转换返回 ErrInvalidTransition，并保持 session 原样。
//
// 注意：completed → completed 是允许的，但它不会改变任何字段，
// 也不会创建文件。确认完成必须先用 PlanCompletion 判断是否需要创建文件。
func Transition(session *model.UploadSession, to string) error {
	if session == nil {
		return fmt.Errorf("%w: nil session", ErrInvalidTransition)
	}
	from := session.Status
	if !CanTransition(from, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	if from == to {
		// 只有 completed → completed 会走到这里，是幂等重放。
		return nil
	}
	session.Status = to
	return nil
}

// CompletionPlan 说明一次“确认完成”请求应该怎么处理。
type CompletionPlan struct {
	// 需要创建结果文件。true 表示会话刚刚从 ready 变成 completed，
	// 调用方应当先创建 File，再把它的 ID 写回 ResultFileID。
	CreateFile bool

	// 已经完成过时，直接返回这个文件 ID，不再创建文件。
	ResultFileID string

	// 需要把 session.Status 从 ready 更新成 completed。
	MarkCompleted bool
}

// PlanCompletion 决定确认完成时要不要创建文件。
//
// 规则：
//   - 只有 ready 可以确认完成；
//   - 已经 completed 的会话重复确认时返回已有 ResultFileID，
//     CreateFile=false，调用方不能再次创建文件；
//   - 其他状态一律返回 ErrNotCompletable。
//
// 这个函数不修改 session，也不写数据库：它只把状态规则集中在一处，
// 避免调用方在检查状态之前就创建出重复文件。
func PlanCompletion(session *model.UploadSession) (CompletionPlan, error) {
	if session == nil {
		return CompletionPlan{}, fmt.Errorf("%w: nil session", ErrNotCompletable)
	}

	switch session.Status {
	case model.UploadStatusReady:
		return CompletionPlan{CreateFile: true, MarkCompleted: true}, nil

	case model.UploadStatusCompleted:
		if session.ResultFileID == nil || *session.ResultFileID == "" {
			return CompletionPlan{}, ErrMissingResultFile
		}
		return CompletionPlan{ResultFileID: *session.ResultFileID}, nil

	default:
		return CompletionPlan{}, fmt.Errorf(
			"%w: status=%s",
			ErrNotCompletable, session.Status,
		)
	}
}
