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

	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/queue"
	"luckyCloudFile/internal/task"
)

const (
	// TaskType 是 ZIP 打包任务在 tasks.type 里的稳定取值。
	// 恢复时用它把数据库记录路由回这个业务。
	TaskType = "archive_zip_v1"

	// TaskStateVersion 是 TaskState 的格式版本。
	// 版本 2 在来源快照里增加了 parent_id；旧快照无法判断来源是否被移动，
	// 因此按未知版本明确失败，而不是把缺失的父目录当成合法值。
	TaskStateVersion = 2
)

// ZIP 阶段。phase 永远表示“下一步做什么”：
//
//	prepare  → 校验来源和目标，保存来源快照
//	compress → 生成完整 ZIP，并保存路径、大小、摘要
//	publish  → 把已提交的 ZIP 发布成普通文件，保存任务结果关联
//	finalize → 核对结果文件，收尾并记录清理责任
//
// 因此 phase=publish 的含义是：完整 ZIP 已经落盘、元数据已经写进
// state_json，并且这条检查点已经提交；它不是“界面进度显示 100%”。
const (
	PhasePrepare  = "prepare"
	PhaseCompress = "compress"
	PhasePublish  = "publish"
	PhaseFinalize = "finalize"
)

var (
	// ErrUnknownPhase 表示数据库里保存了一个当前代码不认识的阶段。
	ErrUnknownPhase = errors.New("archive: unknown task phase")

	// ErrArchiveCorrupt 表示 phase=publish 指向的之前产物不可用：
	// 路径缺失、文件不存在、大小或摘要对不上。恢复时必须退回 compress
	// 重建，绝不能把半成品或错误内容发布出去。
	ErrArchiveCorrupt = errors.New("archive: committed archive is missing or corrupt")
)

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
// 它只保存输入、来源快照、已完成的产物和结果文件 ID；不保存函数、
// context、reader、锁令牌或旧 Job 指针——那些东西重启后无法还原，
// 也不该落库。阶段本身保存在 tasks.phase，这里不再重复一份。
type TaskState struct {
	SourceIDs []string `json:"source_ids"`
	ParentID  string   `json:"parent_id"`
	Name      string   `json:"name"`

	// Sources 是 prepare 提交的来源身份快照。
	// 压缩前会重新取得来源保护，并逐项核对这里记录的实体、名字和大小。
	Sources []file.SourceSnapshot `json:"sources,omitempty"`

	// Archive 在 compress 成功并提交 publish 检查点后才有值。
	Archive *ArchiveArtifact `json:"archive,omitempty"`

	// ResultFileID 在 publish 成功并提交 finalize 检查点后才有值。
	// finalize 不再重新上传，只核对它并清理临时产物。
	ResultFileID string `json:"result_file_id,omitempty"`
}

// TaskStore 是 ZIP 任务需要的持久化能力。
//
// 生产实现是 *task.Service：检查点用 attempts 做围栏，清理标记和任务
// 状态分开记录。业务不直接持有数据库连接。
type TaskStore interface {
	task.Checkpointer

	// SetCleanupPending 记录是否还有工作目录需要清理。
	SetCleanupPending(ctx context.Context, id string, attempt int, pending bool) error
}

var _ TaskStore = (*task.Service)(nil)

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
	store TaskStore,
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
		// 恢复时来源是否仍一致、目标是否可用，都由本次执行重新检查。
		return task.RunnerFunc(func(
			ctx context.Context,
			_ model.Task,
			report queue.ProgressFunc,
		) (string, error) {
			return job.resume(ctx, record, state, store, report)
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
	store TaskStore,
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

	// 产物丢失或损坏时只允许本次执行退回 compress 一次，避免
	// “重建 → 校验失败 → 再重建”的死循环。
	rebuilt := false

	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}

		switch phase {
		case PhasePrepare:
			if err := j.prepare(ctx, dir, &state); err != nil {
				return "", err
			}
			if err := j.saveCheckpoint(ctx, store, record, PhaseCompress, state); err != nil {
				return "", err
			}
			phase = PhaseCompress

		case PhaseCompress:
			artifact, err := j.compress(ctx, dir, &state, report)
			if err != nil {
				return "", err
			}
			state.Archive = &artifact
			if err := j.saveCheckpoint(ctx, store, record, PhasePublish, state); err != nil {
				return "", err
			}
			phase = PhasePublish

		case PhasePublish:
			resultFileID, err := j.publishArtifact(ctx, record.ID, state, report)
			if err != nil {
				if errors.Is(err, ErrArchiveCorrupt) && !rebuilt {
					// 完整产物丢了或被篡改：退回 compress 用同一份来源快照重建。
					// 来源也已经变化时，compress 会明确失败，不会悄悄换内容。
					rebuilt = true
					j.logger.Warn("archive: committed artifact unusable; rebuilding",
						"task_id", record.ID,
						"error", err,
					)
					phase = PhaseCompress
					continue
				}
				return "", err
			}
			state.ResultFileID = resultFileID
			if err := j.saveCheckpoint(ctx, store, record, PhaseFinalize, state); err != nil {
				return "", err
			}
			phase = PhaseFinalize

		case PhaseFinalize:
			if err := j.finalize(ctx, dir, state, record, store); err != nil {
				return "", err
			}
			return state.ResultFileID, nil

		default:
			return "", fmt.Errorf("%w: %q", ErrUnknownPhase, phase)
		}
	}
}

// prepare 校验来源和目标，并保存来源身份快照。
//
// 快照之后压缩时只认这份身份：来源被改名、移动、删除或换内容时明确失败，
// 不会按“现在同名的那份文件”继续打出一份不同的 ZIP。
func (j *Job) prepare(ctx context.Context, dir string, state *TaskState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := j.files.ValidateTarget(ctx, j.ownerID, j.parentID, j.name); err != nil {
		return fmt.Errorf("archive: validate target: %w", err)
	}

	snapshots, err := j.files.SnapshotSources(ctx, j.ownerID, j.sourceIDs)
	if err != nil {
		return fmt.Errorf("archive: snapshot sources: %w", err)
	}
	if err := validateSourceSnapshots(j.sourceIDs, snapshots); err != nil {
		return err
	}
	state.Sources = snapshots

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("archive: create task directory: %w", err)
	}
	return nil
}

// compress 在任务工作目录里重建完整 ZIP。
//
// 每次压缩都重新取得来源保护，并用 prepare 保存的快照核对；先写 .partial，
// 写完、Close、Sync 成功后才重命名为正式产物。只有整个过程成功才返回
// 元数据，调用方随后提交 phase=publish 检查点。
func (j *Job) compress(
	ctx context.Context,
	dir string,
	state *TaskState,
	report queue.ProgressFunc,
) (ArchiveArtifact, error) {
	if len(state.Sources) == 0 {
		// 旧记录可能没有来源快照：第一次压缩时补一份，并随下一次检查点保存。
		snapshots, err := j.files.SnapshotSources(ctx, j.ownerID, j.sourceIDs)
		if err != nil {
			return ArchiveArtifact{}, fmt.Errorf("archive: snapshot sources: %w", err)
		}
		if err := validateSourceSnapshots(j.sourceIDs, snapshots); err != nil {
			return ArchiveArtifact{}, err
		}
		state.Sources = snapshots
	}
	if err := validateSourceSnapshots(j.sourceIDs, state.Sources); err != nil {
		return ArchiveArtifact{}, err
	}

	// 恢复/重试都重新申请锁，不复用任何旧令牌；压缩结束立刻释放。
	guard, err := j.files.ProtectSources(ctx, j.ownerID, state.Sources)
	if err != nil {
		return ArchiveArtifact{}, fmt.Errorf("archive: protect sources: %w", err)
	}
	defer guard.Release()

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return ArchiveArtifact{}, fmt.Errorf("archive: create task directory: %w", err)
	}

	finalPath := filepath.Join(dir, "archive.zip")
	partialPath := finalPath + ".partial"

	size, digest, err := j.writeArchive(ctx, partialPath, state.Sources, guard, report)
	if err != nil {
		_ = os.Remove(partialPath)
		return ArchiveArtifact{}, err
	}
	// 写完立即释放来源保护，发布阶段不再占用来源路径。
	guard.Release()

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

// publishArtifact 发布已经提交的完整产物。
//
// 先查任务结果关联：已经有结果时直接复用，不重新压缩、不再上传、不再计费。
// 没有结果时才核对产物；产物缺失、路径逃逸或大小/摘要不一致返回
// ErrArchiveCorrupt，由 resume 退回 compress 重建。
func (j *Job) publishArtifact(
	ctx context.Context,
	taskID string,
	state TaskState,
	report queue.ProgressFunc,
) (string, error) {
	if taskID != "" {
		existing, err := j.files.FindTaskResult(ctx, j.ownerID, taskID)
		switch {
		case err == nil:
			return existing, nil
		case errors.Is(err, file.ErrNotFound):
			// 还没有发布结果，继续走正常发布。
		default:
			return "", fmt.Errorf("archive: query task result: %w", err)
		}
	}

	if state.Archive == nil {
		return "", fmt.Errorf("%w: no artifact metadata", ErrArchiveCorrupt)
	}
	if state.Archive.Size < 0 || state.Archive.SHA256 == "" {
		return "", fmt.Errorf("%w: incomplete artifact metadata", ErrArchiveCorrupt)
	}

	path, err := j.artifactPath(state.Archive.Path)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrArchiveCorrupt, err)
	}
	if err := verifyArtifact(path, *state.Archive); err != nil {
		return "", fmt.Errorf("%w: %v", ErrArchiveCorrupt, err)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return j.publish(ctx, path, state.Archive.Size, taskID, report)
}

// finalize 核对结果文件 ID，记录清理责任，再清理任务工作目录。
//
// 顺序不能反：先把 cleanup_pending 持久化，再删除目录。
//   - 删除失败：责任已经在数据库里，任务可以完成，后续清理任务按标记重试；
//   - 删除成功：再清零；清零失败只会保守地留下 true，不会丢责任；
//   - 责任写不进去：直接返回错误，绝不在没有持久化责任的情况下删除并完成。
//
// 结果已经发布时，删除失败不能把任务判成失败，否则下一次重试会再发布
// 第二份文件；这里只把责任留给后续清理。
func (j *Job) finalize(
	ctx context.Context,
	dir string,
	state TaskState,
	record model.Task,
	store TaskStore,
) error {
	if state.ResultFileID == "" {
		return errors.New("archive: phase=finalize has no result file ID")
	}
	// 有结果关联时必须和检查点里的结果一致，不能拿旧检查点覆盖新结果。
	// 旧版本可能只有 result_file_id 而没有 task_results，此时找不到关联也接受。
	if record.ID != "" {
		existing, err := j.files.FindTaskResult(ctx, j.ownerID, record.ID)
		switch {
		case err == nil:
			if existing != state.ResultFileID {
				return fmt.Errorf(
					"archive: task result %q does not match checkpoint result %q",
					existing, state.ResultFileID,
				)
			}
		case errors.Is(err, file.ErrNotFound):
		default:
			return fmt.Errorf("archive: query task result: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil {
		return errors.New("archive: cleanup store is required")
	}

	// 先落库责任，再动手。反过来“先删后记”一旦两条路径都失败就会静默完成。
	if err := store.SetCleanupPending(ctx, record.ID, record.Attempts, true); err != nil {
		return fmt.Errorf("archive: persist cleanup responsibility: %w", err)
	}

	if err := os.RemoveAll(dir); err != nil {
		j.logger.Warn("archive: remove task directory", "path", dir, "error", err)
		// cleanup_pending=true 已经提交；结果已发布，任务可以完成。
		return nil
	}

	// 删除成功后才收回责任；这里失败只会留下一个保守的 true。
	if err := store.SetCleanupPending(ctx, record.ID, record.Attempts, false); err != nil {
		j.logger.Warn("archive: clear cleanup responsibility",
			"task_id", record.ID, "error", err)
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

// validateSourceSnapshots 确认快照数量和顺序与任务输入一致。
// 数据库里的 state_json 被破坏时不能拿一半来源去拼 ZIP。
func validateSourceSnapshots(ids []string, snapshots []file.SourceSnapshot) error {
	if len(ids) != len(snapshots) {
		return fmt.Errorf(
			"archive: source snapshot count %d does not match input count %d",
			len(snapshots), len(ids),
		)
	}
	for i, id := range ids {
		if snapshots[i].FileID != id {
			return fmt.Errorf(
				"archive: source snapshot %d is %q, want %q",
				i, snapshots[i].FileID, id,
			)
		}
	}
	return nil
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
		return fmt.Errorf("committed archive is missing: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("committed archive is not a regular file")
	}
	if info.Size() != want.Size {
		return fmt.Errorf(
			"committed archive size changed: got %d, want %d",
			info.Size(), want.Size,
		)
	}

	fileHandle, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open committed archive: %w", err)
	}
	defer fileHandle.Close()

	digest := sha256.New()
	if _, err := io.Copy(digest, fileHandle); err != nil {
		return fmt.Errorf("read committed archive: %w", err)
	}
	got := hex.EncodeToString(digest.Sum(nil))
	if !strings.EqualFold(got, want.SHA256) {
		return fmt.Errorf(
			"committed archive checksum changed: got %s, want %s",
			got, want.SHA256,
		)
	}
	return nil
}
