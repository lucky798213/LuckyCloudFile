package file

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
)

// ErrTaskResultExists 表示同一个任务已经有一份发布结果。
//
// 发布事务会把新文件、容量结算和 task_results 一起回滚；调用方应取消
// 本次多余的上传会话，再查询已发布结果，而不是换一个名字再发一份。
var ErrTaskResultExists = errors.New("task result already exists")

// FindTaskResult 返回任务已经发布的结果文件 ID。
//
// 不存在（或不属于该用户）返回 ErrNotFound。它不检查 File 是否还存在：
// 用户后来删除结果文件时，旧任务恢复必须复用这个关联并收敛，不能重新发布。
func (s *Service) FindTaskResult(
	ctx context.Context,
	ownerID int,
	taskID string,
) (string, error) {
	if taskID == "" {
		return "", fmt.Errorf("%w: task ID is required", ErrInvalidSource)
	}

	var result model.TaskResult
	err := s.db.WithContext(ctx).
		Where("task_id = ? AND owner_id = ?", taskID, ownerID).
		First(&result).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("query task result: %w", err)
	}
	return result.FileID, nil
}
