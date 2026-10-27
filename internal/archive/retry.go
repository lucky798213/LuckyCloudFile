package archive

import (
	"errors"

	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/task"
)

// classifyRunError 决定一次执行失败是否值得自动重试。
//
// 只有包装层做这个判断：业务函数仍然返回原始错误，调度器只认 task.Retryable
// 标记。来源被删除、快照变化、越权、配额/大小限制、同名冲突、参数或阶段
// 不认识，这些都是重跑也不会变的永久错误；数据库/存储抖动、提交结果不明确
// 等未识别的错误才进入有上限的退避重试。
func classifyRunError(err error) error {
	if err == nil {
		return nil
	}
	if isPermanentRunError(err) {
		return err
	}
	return task.Retryable(err)
}

// isPermanentRunError 列出“重跑不会改变结果”的错误。
//
// 清单保持显式：新增业务错误时如果它其实是暂时的，不要加进来；如果
// 是永久错误却不在这里，最多浪费有上限的几次重试，不会无限循环。
func isPermanentRunError(err error) bool {
	permanent := []error{
		ErrNoSources,
		ErrTooManyFiles,
		ErrDuplicateFile,
		ErrInvalidFileID,
		ErrUnsafeName,
		ErrDuplicateName,
		ErrInvalidName,
		ErrSourceTooLarge,
		ErrArchiveTooLarge,
		ErrUnknownPhase,

		file.ErrNotFound,
		file.ErrInvalidSource,
		file.ErrSourceChanged,
		file.ErrTooLarge,
		file.ErrNotDirectory,
		file.ErrIsDirectory,
		file.ErrInvalidName,
		file.ErrAlreadyExists,
		file.ErrRootDirectory,
		file.ErrDirectoryNotEmpty,
		file.ErrInvalidMove,
		file.ErrQuotaExceeded,
		file.ErrInvalidUploadMode,
		file.ErrUploadModeMismatch,
		file.ErrInvalidChunkIndex,
		file.ErrInvalidSize,
		file.ErrUploadSessionNotFound,
		file.ErrUploadSessionNotCompletable,
		file.ErrUploadSessionInconsistent,
	}
	for _, sentinel := range permanent {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	return false
}
