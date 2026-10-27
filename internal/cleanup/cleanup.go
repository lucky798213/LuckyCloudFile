// Package cleanup 把已有的过期会话扫描和无引用内容回收包装成可恢复任务。
//
// 它不重写删除逻辑：真正的工作仍然由 file.Service.CleanupPending 完成，
// 继续保持那里的状态重查、容量释放和引用检查。这里只负责：
//
//   - 清理失败时让任务进入 retry_wait，而不是静默丢掉责任；
//   - 回收已经完成任务的过期工作目录，并清除 cleanup_pending；
//   - 由周期触发器保证同一类型最多只有一条未终态清理任务。
package cleanup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/queue"
	"luckyCloudFile/internal/task"
)

const (
	// TaskType 是持久化清理任务在 tasks.type 里的稳定取值。
	TaskType = "storage_cleanup_v1"

	// StateVersion 是清理任务检查点的格式版本。清理没有业务参数，
	// 但版本仍然落库，方便以后扩展扫描范围时拒绝旧格式。
	StateVersion = 1

	// DefaultBatchSize 是一轮清理最多处理多少条记录。
	DefaultBatchSize = file.DefaultCleanupBatchSize
)

// FileService 是清理任务需要的最小文件业务能力。
// 生产实现是 *file.Service，测试可以注入假实现。
type FileService interface {
	CleanupPending(ctx context.Context, batchSize int) (file.CleanupResult, error)
}

// Store 是清理任务需要的持久化能力。生产实现是 *task.Service。
type Store interface {
	ListCleanupPending(ctx context.Context, limit int) ([]model.Task, error)
	ClearCleanupPending(ctx context.Context, id string) error
}

// Options 是清理任务的依赖和参数。
type Options struct {
	// WorkDir 是任务工作目录根。清理任务会删除已经完成任务的
	// task-<taskID> 子目录；为空时跳过目录回收并明确报错。
	WorkDir string

	// BatchSize 为零时使用 DefaultBatchSize。
	BatchSize int

	// Logger 为空时使用 slog.Default()。
	Logger *slog.Logger
}

// NewTaskFactory 返回 storage_cleanup_v1 的重建工厂。
//
// 每次执行都重新读取数据库里的清理责任，不依赖任何旧 Job 指针。
func NewTaskFactory(
	files FileService,
	store Store,
	opts Options,
) task.Factory {
	return task.FactoryFunc(func(record model.Task) (task.Runner, error) {
		if record.Type != TaskType {
			return nil, fmt.Errorf("cleanup: unsupported task type %q", record.Type)
		}
		if record.StateVersion != StateVersion {
			return nil, fmt.Errorf(
				"cleanup: unsupported task state version %d", record.StateVersion,
			)
		}
		if files == nil || store == nil {
			return nil, errors.New("cleanup: file service and task store are required")
		}

		batchSize := opts.BatchSize
		if batchSize <= 0 {
			batchSize = DefaultBatchSize
		}
		logger := opts.Logger
		if logger == nil {
			logger = slog.Default()
		}

		return task.RunnerFunc(func(
			ctx context.Context,
			_ model.Task,
			_ queue.ProgressFunc,
		) (string, error) {
			var firstErr error

			// 先回收已经完成任务的过期工作目录。它和内容回收互不依赖，
			// 一条失败不影响另一条继续推进。
			dirErr := cleanupTaskDirs(ctx, store, opts.WorkDir, batchSize, logger)
			if dirErr != nil {
				firstErr = dirErr
			}

			result, err := files.CleanupPending(ctx, batchSize)
			if err != nil {
				return "", task.Retryable(fmt.Errorf("cleanup pending content: %w", err))
			}
			if result.Deferred > 0 {
				return "", task.Retryable(fmt.Errorf(
					"cleanup deferred %d item(s)", result.Deferred,
				))
			}
			if firstErr != nil {
				return "", task.Retryable(firstErr)
			}
			return "", nil
		}), nil
	})
}

// cleanupTaskDirs 删除 terminal 任务留下并已标记 cleanup_pending 的目录。
//
// 顺序不能反：先删除成功，再清除数据库里的责任标记。删除失败时保留标记，
// 下一次清理任务会重试；主任务已经完成，不会再因为清理失败重新发布结果。
func cleanupTaskDirs(
	ctx context.Context,
	store Store,
	workDir string,
	batchSize int,
	logger *slog.Logger,
) error {
	tasks, err := store.ListCleanupPending(ctx, batchSize)
	if err != nil {
		return fmt.Errorf("list tasks with cleanup pending: %w", err)
	}

	var firstErr error
	for _, record := range tasks {
		if err := ctx.Err(); err != nil {
			return err
		}

		dir, err := taskDir(workDir, record.ID)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			logger.Warn("cleanup task directory", "task_id", record.ID, "error", err)
			continue
		}

		if err := os.RemoveAll(dir); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("remove task directory of %q: %w", record.ID, err)
			}
			logger.Warn("remove task directory",
				"task_id", record.ID, "path", dir, "error", err)
			continue
		}

		if err := store.ClearCleanupPending(ctx, record.ID); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("clear cleanup pending of %q: %w", record.ID, err)
			}
			logger.Warn("clear cleanup pending", "task_id", record.ID, "error", err)
		}
	}
	return firstErr
}

// taskDir 返回任务独占的工作目录。任务 ID 由服务端生成，但数据库里的
// 异常值仍然不能把删除动作带到工作根目录之外。
func taskDir(workDir, taskID string) (string, error) {
	if strings.TrimSpace(workDir) == "" {
		return "", errors.New("cleanup: task work directory is required")
	}
	if !safeTaskID(taskID) {
		return "", fmt.Errorf("cleanup: unsafe task ID %q", taskID)
	}
	return filepath.Join(workDir, "task-"+taskID), nil
}

func safeTaskID(id string) bool {
	if id == "" || id == "." || id == ".." {
		return false
	}
	if strings.ContainsRune(id, 0) {
		return false
	}
	return !strings.ContainsAny(id, `/\`)
}
