package archive

import (
	"context"
	"encoding/json"
	"fmt"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/queue"
	"luckyCloudFile/internal/task"
)

const (
	// TaskType 是 ZIP 打包任务在 tasks.type 里的稳定取值。
	// 恢复时用它把数据库记录路由回这个业务。
	TaskType = "archive_zip_v1"

	// TaskStateVersion 是 TaskState 的格式版本。
	// 版本不认识时明确失败，不能按零值猜参数继续打包。
	TaskStateVersion = 1
)

// TaskState 是可以落库、重启后重建 ZIP 任务的全部参数。
//
// 它只存输入参数（来源、目标、结果名），不存打开的文件句柄、锁令牌或
// 临时路径：那些东西每次执行都必须重新取得。
type TaskState struct {
	SourceIDs []string `json:"source_ids"`
	ParentID  string   `json:"parent_id"`
	Name      string   `json:"name"`
}

// NewTaskState 校验并返回可落库的任务参数。
//
// 提交时用它做静态校验（数量、空 ID、重复 ID、结果名），执行时再把
// 同一份数据交给 New 重建任务；两处共用同一套规则，避免提交能过、
// 执行才发现参数非法。
func NewTaskState(
	sourceIDs []string,
	parentID, name string,
	opts Options,
) (TaskState, error) {
	if len(sourceIDs) == 0 {
		return TaskState{}, ErrNoSources
	}
	if !safeEntryName(name) {
		return TaskState{}, fmt.Errorf("%w: %q", ErrInvalidName, name)
	}

	maxFiles := opts.MaxFiles
	if maxFiles <= 0 {
		maxFiles = DefaultMaxFiles
	}
	if len(sourceIDs) > maxFiles {
		return TaskState{}, fmt.Errorf("%w: limit is %d", ErrTooManyFiles, maxFiles)
	}

	seen := make(map[string]struct{}, len(sourceIDs))
	for _, id := range sourceIDs {
		if id == "" {
			return TaskState{}, ErrInvalidFileID
		}
		if _, ok := seen[id]; ok {
			return TaskState{}, fmt.Errorf("%w: %s", ErrDuplicateFile, id)
		}
		seen[id] = struct{}{}
	}

	return TaskState{
		SourceIDs: append([]string(nil), sourceIDs...),
		ParentID:  parentID,
		Name:      name,
	}, nil
}

// NewTaskRunner 返回从 tasks 记录重建并执行 ZIP 任务的 Runner。
//
// 每次执行都重新构造 Job：不依赖提交时的闭包、旧队列对象或旧锁。
// 文件是否还存在、是否仍属于该用户，都在执行时由 file.Service 检查。
func NewTaskRunner(files FileService, opts Options) task.Runner {
	return task.RunnerFunc(func(
		ctx context.Context,
		record model.Task,
		report queue.ProgressFunc,
	) (string, error) {
		if record.Type != TaskType {
			return "", fmt.Errorf("archive: unsupported task type %q", record.Type)
		}
		if record.StateVersion != TaskStateVersion {
			return "", fmt.Errorf(
				"archive: unsupported task state version %d", record.StateVersion,
			)
		}

		var state TaskState
		if err := json.Unmarshal([]byte(record.StateJSON), &state); err != nil {
			return "", fmt.Errorf("archive: decode task state: %w", err)
		}

		job, err := New(
			files,
			record.OwnerID,
			state.SourceIDs,
			state.ParentID,
			state.Name,
			opts,
		)
		if err != nil {
			return "", err
		}
		return job.RunWithProgress(ctx, report)
	})
}
