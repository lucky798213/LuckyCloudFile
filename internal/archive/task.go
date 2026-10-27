package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

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

// ZIP 阶段。phase 永远表示“下一步做什么”：
//
//	prepare  → 准备任务工作目录
//	compress → 生成完整 ZIP，并保存路径、大小、摘要
//	publish  → 把已提交的 ZIP 发布成普通文件
//	finalize → 核对结果文件，收尾并允许任务标记 completed
//
// 因此 phase=publish 的含义是：完整 ZIP 已经落盘、元数据已经写进
// state_json，并且这条检查点已经提交；它不是“界面进度显示 100%”。
const (
	PhasePrepare  = "prepare"
	PhaseCompress = "compress"
	PhasePublish  = "publish"
	PhaseFinalize = "finalize"
)

// ErrUnknownPhase 表示数据库里保存了一个当前代码不认识的阶段。
var ErrUnknownPhase = errors.New("archive: unknown task phase")

// ArchiveArtifact 是完整 ZIP 在任务工作目录里的位置和校验信息。
//
// Path 是相对 Options.WorkDir 的路径，由服务器生成，不来自客户端；
// Size / SHA256 用于恢复 publish 前核对产物，避免上传损坏或错的 ZIP。
type ArchiveArtifact struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// TaskState 是可以落库、重启后重建 ZIP 任务的全部数据。
//
// 它只保存输入、已完成的产物和结果文件 ID；不保存函数、context、
// reader、锁令牌或旧 Job 指针——那些东西重启后无法还原，也不该落库。
// 阶段本身保存在 tasks.phase，这里不再重复一份，避免两个事实来源。
type TaskState struct {
	SourceIDs []string `json:"source_ids"`
	ParentID  string   `json:"parent_id"`
	Name      string   `json:"name"`

	// Archive 在 compress 成功并提交 publish 检查点后才有值。
	Archive *ArchiveArtifact `json:"archive,omitempty"`

	// ResultFileID 在 publish 成功并提交 finalize 检查点后才有值。
	// finalize 不再重新上传，只核对它并清理临时产物。
	ResultFileID string `json:"result_file_id,omitempty"`
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

// NewTaskFactory 返回 archive_zip_v1 的重建工厂。
//
// 文件服务、检查点入口、工作目录和限制都在应用启动时注入；每次执行只用
// tasks 记录里的 owner、phase 和 state_json 重新构造 Job，不依赖提交时的
// 闭包、旧队列对象或旧锁。
func NewTaskFactory(
	files FileService,
	checkpoints task.Checkpointer,
	opts Options,
) task.Factory {
	return task.FactoryFunc(func(record model.Task) (task.Runner, error) {
		if record.Type != TaskType {
			return nil, fmt.Errorf("archive: unsupported task type %q", record.Type)
		}
		if record.StateVersion != TaskStateVersion {
			return nil, fmt.Errorf(
				"archive: unsupported task state version %d", record.StateVersion,
			)
		}

		var state TaskState
		if err := json.Unmarshal([]byte(record.StateJSON), &state); err != nil {
			return nil, fmt.Errorf("archive: decode task state: %w", err)
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
			return nil, err
		}

		// runner 只持有这一次重建出来的 Job；执行结束后不会再被复用。
		// 恢复时文件是否还在、是否仍属于该用户，仍由 file.Service 检查。
		return task.RunnerFunc(func(
			ctx context.Context,
			_ model.Task,
			report queue.ProgressFunc,
		) (string, error) {
			return job.resume(ctx, record, state, checkpoints, report)
		}), nil
	})
}

// resume 从记录的 phase 开始执行，直到 finalize 完成。
//
// 每完成一个阶段，先把“下一步阶段 + 完整 state_json”提交为检查点，成功
// 之后才继续。这样进程在任意边界退出，下一次执行都能从已提交的阶段继续，
// 而不是重新猜参数或重做已经发布的结果。
func (j *Job) resume(
	ctx context.Context,
	record model.Task,
	state TaskState,
	checkpoints task.Checkpointer,
	report queue.ProgressFunc,
) (string, error) {
	dir, err := j.taskDir(record.ID)
	if err != nil {
		return "", err
	}

	phase := record.Phase
	if phase == "" {
		// 兼容没有写 phase 的旧记录：从第一步开始，而不是按零值乱跑。
		phase = PhasePrepare
	}

	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}

		switch phase {
		case PhasePrepare:
			if err := j.prepare(ctx, dir); err != nil {
				return "", err
			}
			if err := j.saveCheckpoint(ctx, checkpoints, record, PhaseCompress, state); err != nil {
				return "", err
			}
			phase = PhaseCompress

		case PhaseCompress:
			artifact, err := j.compress(ctx, dir, report)
			if err != nil {
				return "", err
			}
			state.Archive = &artifact
			if err := j.saveCheckpoint(ctx, checkpoints, record, PhasePublish, state); err != nil {
				return "", err
			}
			phase = PhasePublish

		case PhasePublish:
			resultFileID, err := j.publishArtifact(ctx, state, report)
			if err != nil {
				return "", err
			}
			state.ResultFileID = resultFileID
			if err := j.saveCheckpoint(ctx, checkpoints, record, PhaseFinalize, state); err != nil {
				return "", err
			}
			phase = PhaseFinalize

		case PhaseFinalize:
			if err := j.finalize(ctx, dir, state); err != nil {
				return "", err
			}
			return state.ResultFileID, nil

		default:
			return "", fmt.Errorf("%w: %q", ErrUnknownPhase, phase)
		}
	}
}

func (j *Job) prepare(ctx context.Context, dir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("archive: create task directory: %w", err)
	}
	return nil
}

// compress 在任务工作目录里重建完整 ZIP。
//
// 先写 .partial，写完、Close、Sync 成功后才重命名为正式产物；只有整个
// 过程成功才返回元数据，调用方随后提交 phase=publish 检查点。
func (j *Job) compress(
	ctx context.Context,
	dir string,
	report queue.ProgressFunc,
) (ArchiveArtifact, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return ArchiveArtifact{}, fmt.Errorf("archive: create task directory: %w", err)
	}

	finalPath := filepath.Join(dir, "archive.zip")
	partialPath := finalPath + ".partial"

	size, digest, err := j.writeArchive(ctx, partialPath, report)
	if err != nil {
		_ = os.Remove(partialPath)
		return ArchiveArtifact{}, err
	}

	// 覆盖上一次尝试留下的正式产物；此时它一定没有已提交的 publish 检查点。
	if err := os.Remove(finalPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = os.Remove(partialPath)
		return ArchiveArtifact{}, fmt.Errorf("archive: replace archive: %w", err)
	}
	if err := os.Rename(partialPath, finalPath); err != nil {
		_ = os.Remove(partialPath)
		return ArchiveArtifact{}, fmt.Errorf("archive: commit archive: %w", err)
	}

	rel, err := filepath.Rel(j.workDir, finalPath)
	if err != nil {
		return ArchiveArtifact{}, fmt.Errorf("archive: resolve archive path: %w", err)
	}
	return ArchiveArtifact{Path: rel, Size: size, SHA256: digest}, nil
}

// publishArtifact 只发布已经提交的完整产物。
//
// 没有产物元数据、路径逃出工作目录、文件丢失或大小/摘要不一致时明确失败；
// 不会拿半成品或不匹配的内容去创建上传会话。
func (j *Job) publishArtifact(
	ctx context.Context,
	state TaskState,
	report queue.ProgressFunc,
) (string, error) {
	if state.Archive == nil {
		return "", errors.New("archive: phase=publish has no committed archive artifact")
	}
	if state.Archive.Size < 0 || state.Archive.SHA256 == "" {
		return "", errors.New("archive: committed archive metadata is incomplete")
	}

	path, err := j.artifactPath(state.Archive.Path)
	if err != nil {
		return "", err
	}
	if err := verifyArtifact(path, *state.Archive); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return j.publish(ctx, path, state.Archive.Size, report)
}

// finalize 核对结果文件 ID，并清理只在本任务内使用的临时产物。
//
// 清理失败只记录日志：结果已经发布，不能因为删不掉临时目录就把任务判成
// 失败，否则下一次重试会再发布一份。
func (j *Job) finalize(ctx context.Context, dir string, state TaskState) error {
	if state.ResultFileID == "" {
		return errors.New("archive: phase=finalize has no result file ID")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.RemoveAll(dir); err != nil {
		j.logger.Warn("archive: remove task directory", "path", dir, "error", err)
	}
	return nil
}

// saveCheckpoint 提交“当前阶段已完成、下一步是 nextPhase”。
//
// StateJSON 每次都写完整快照；仓储层不做部分字段合并，所以进度、产物和
// 结果不会被迟到的旧检查点拼在一起。
func (j *Job) saveCheckpoint(
	ctx context.Context,
	checkpoints task.Checkpointer,
	record model.Task,
	nextPhase string,
	state TaskState,
) error {
	if checkpoints == nil {
		return errors.New("archive: checkpoint store is required")
	}

	encoded, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("archive: encode task state: %w", err)
	}
	return checkpoints.SaveCheckpoint(ctx, record.ID, record.Attempts, task.Checkpoint{
		Phase:        nextPhase,
		StateVersion: TaskStateVersion,
		StateJSON:    string(encoded),
	})
}

// taskDir 返回本任务独占的工作目录。任务 ID 由服务端生成，但仍然拒绝
// 路径分隔符，避免数据库里的异常值把文件写到工作根目录之外。
func (j *Job) taskDir(taskID string) (string, error) {
	if !safeEntryName(taskID) {
		return "", fmt.Errorf("archive: unsafe task ID %q", taskID)
	}
	return filepath.Join(j.workDir, "task-"+taskID), nil
}

// artifactPath 把检查点里的相对路径还原成工作目录内的绝对路径。
func (j *Job) artifactPath(rel string) (string, error) {
	if rel == "" || !filepath.IsLocal(rel) {
		return "", fmt.Errorf("archive: unsafe archive path %q", rel)
	}
	return filepath.Join(j.workDir, rel), nil
}

// verifyArtifact 在发布前核对产物仍是检查点里记录的那一份。
//
// 只比较大小和 SHA-256，不解析 ZIP；内容如果变了，摘要必然对不上。
func verifyArtifact(path string, want ArchiveArtifact) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("archive: committed archive is missing: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("archive: committed archive is not a regular file")
	}
	if info.Size() != want.Size {
		return fmt.Errorf(
			"archive: committed archive size changed: got %d, want %d",
			info.Size(), want.Size,
		)
	}

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("archive: open committed archive: %w", err)
	}
	defer file.Close()

	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return fmt.Errorf("archive: read committed archive: %w", err)
	}
	got := hex.EncodeToString(digest.Sum(nil))
	if !strings.EqualFold(got, want.SHA256) {
		return fmt.Errorf(
			"archive: committed archive checksum changed: got %s, want %s",
			got, want.SHA256,
		)
	}
	return nil
}
