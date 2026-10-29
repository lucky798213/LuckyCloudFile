// Package verify 把“校验已有内容并回填可信摘要”做成一种可领取的后台任务。
//
// 它复用秒传已经建立的内容指纹体系：
//
//	提交已有文件的校验任务
//	    ↓
//	worker 读取 Local/S3 内容
//	    ↓
//	计算完整 SHA-256，核对大小
//	    ↓
//	回填 entities.content_sha256，登记或复用 content_keys
//	    ↓
//	在同一个事务里标记任务完成
//
// 第一版不做历史副本合并，也不搬迁已有引用：它只让旧内容获得可信摘要，
// 从而能被之后的秒传命中。
package verify

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/failpoint"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/queue"
	"luckyCloudFile/internal/storage"
	"luckyCloudFile/internal/task"
)

const (
	// TaskType 是内容校验任务在 tasks.type 里的稳定取值。
	TaskType = "content_verify_v1"

	// StateVersion 是 TaskState 的格式版本。
	StateVersion = 1

	// DemoFailpoint 是演示租约接管用的阻塞点名称。
	// 只有显式设置 CLOUDREVE_FAILPOINT 时才会生效。
	DemoFailpoint = "content-verify-before-commit"

	// demoPause 是阻塞点停留的时间，足够外部暂停或杀掉 worker，
	// 并等过一个租约周期。
	demoPause = 60 * time.Second

	// emptyContentSHA256 是空内容的 SHA-256。空文件不读对象，
	// 但仍然要回填一条可信摘要。
	emptyContentSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	// commitAttempts 是回填事务遇到数据库死锁时的重试上限。
	// InnoDB 会回滚整笔事务，重新走一遍 Complete 不会重复写入。
	commitAttempts = 3
)

var (
	// ErrTargetGone 表示任务指向的文件/实体已经不存在，或已经不再可用。
	// 它是永久错误：任务应收敛为失败，并且绝不能登记内容映射。
	ErrTargetGone = errors.New("verify: target is missing or no longer verifiable")

	// ErrSizeMismatch 表示实际读到的字节数和实体声明的大小不一致。
	// 内容不可信，同样不能登记映射。
	ErrSizeMismatch = errors.New("verify: stored content size does not match")

	// ErrDigestMismatch 表示重新读取得到的摘要和实体上已有的可信摘要不同。
	// 这说明内容在写入后被改过，必须保留原摘要并让人排查。
	ErrDigestMismatch = errors.New("verify: stored digest does not match re-read content")
)

// TaskState 是内容校验任务的持久化参数。
//
// 只保存一个文件 ID：owner、存储策略和大小都从数据库当前状态读取，
// 不复制进 state_json，避免恢复时使用过期的快照。
type TaskState struct {
	FileID string `json:"file_id"`
}

// Result 是任务成功后的结果投影（写入 tasks.result_json）。
//
// 真正的业务事实是 entities.content_sha256 和 content_keys 行；
// 这里只是让接口和演示能直接看到这次校验得到了什么。
type Result struct {
	FileID   string `json:"file_id"`
	EntityID int64  `json:"entity_id"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
}

// NewState 校验并返回可落库的任务参数。提交时用它做静态校验。
func NewState(fileID string) (TaskState, error) {
	if fileID == "" {
		return TaskState{}, errors.New("verify: file id is required")
	}
	return TaskState{FileID: fileID}, nil
}

// Options 是内容校验任务的依赖。
type Options struct {
	// DB 用来读取目标内容并完成回填事务。
	DB *gorm.DB

	// Stores 按存储策略 ID 找到真正保存内容的后端（Local 或 S3）。
	Stores *storage.Registry

	// Tasks 提供带租约围栏的 Complete。业务结果必须通过它提交。
	Tasks *task.Service
}

// NewFactory 创建内容校验任务的工厂。
//
// 依赖在 worker 启动时注入一次；每次执行都从 tasks 记录重建，
// 不在内存里保留上一次执行的 Job 或 claim。
func NewFactory(options Options) task.Factory {
	return &factory{options: options}
}

type factory struct {
	options Options
}

// Rebuild 从一条已经被领取的任务记录重建执行器。
func (f *factory) Rebuild(record model.Task) (task.Runner, error) {
	if record.Type != TaskType {
		return nil, fmt.Errorf("verify: unexpected task type %q", record.Type)
	}
	if record.StateVersion != StateVersion {
		return nil, fmt.Errorf(
			"verify: unsupported state version %d", record.StateVersion,
		)
	}
	if record.Status != model.TaskStatusRunning ||
		record.WorkerID == "" || record.LeaseUntil == nil {
		return nil, fmt.Errorf(
			"verify: task %s does not carry an active worker claim", record.ID,
		)
	}

	var state TaskState
	if err := json.Unmarshal([]byte(record.StateJSON), &state); err != nil {
		return nil, fmt.Errorf("verify: decode state of task %s: %w", record.ID, err)
	}
	if state.FileID == "" {
		return nil, fmt.Errorf("verify: task %s has no file id", record.ID)
	}

	return &runner{
		options: f.options,
		state:   state,
		claim: task.Claim{
			TaskID:     record.ID,
			WorkerID:   record.WorkerID,
			Attempt:    record.Attempts,
			LeaseUntil: *record.LeaseUntil,
			Record:     record,
		},
	}, nil
}

type runner struct {
	options Options
	state   TaskState
	claim   task.Claim
}

var _ task.Runner = (*runner)(nil)

// Run 读取内容、计算摘要，并在一个事务里回填和完成任务。
//
// 读取发生在事务之外：大文件可能读很久，不能一直持有数据库行锁。
// 真正的“仍有资格”检查和业务写入放在 Complete 的同一个事务里，
// 所以读取期间发生接管时，旧执行者会在最后一步被拒绝。
func (r *runner) Run(
	ctx context.Context,
	record model.Task,
	_ queue.ProgressFunc,
) (string, error) {
	target, err := r.loadTarget(ctx, record.OwnerID)
	if err != nil {
		return "", err
	}

	store, err := r.options.Stores.Get(target.StoragePolicyID)
	if err != nil {
		return "", fmt.Errorf(
			"verify: resolve storage policy %d: %w", target.StoragePolicyID, err,
		)
	}

	// 演示/故障注入：在写结果之前停住，方便观察租约过期和接管。
	// 不设置 CLOUDREVE_FAILPOINT 时这里是空操作。
	failpoint.Sleep(DemoFailpoint, demoPause)

	digest, err := hashContent(ctx, store, target.StorageKey, target.Size)
	if err != nil {
		return "", err
	}
	if target.ContentSHA256 != nil && *target.ContentSHA256 != "" &&
		*target.ContentSHA256 != digest {
		return "", fmt.Errorf(
			"%w: entity %d", ErrDigestMismatch, target.ID,
		)
	}

	result := Result{
		FileID:   r.state.FileID,
		EntityID: target.ID,
		Size:     target.Size,
		SHA256:   digest,
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("verify: encode result: %w", err)
	}

	// 回填摘要、登记映射和标记完成必须在同一个事务里提交。
	// Complete 会先锁任务行并核对 worker / attempt / 租约，再执行 apply。
	//
	// 死锁/锁等待超时由 InnoDB 回滚整笔事务，可以在本进程内有界重试；
	// 重试仍失败时标记为可重试，交给任务调度器按 attempt 上限再排一次。
	for attempt := 0; attempt < commitAttempts; attempt++ {
		err = r.options.Tasks.Complete(
			ctx, r.claim, string(resultJSON),
			func(tx *gorm.DB) error {
				return r.backfill(tx, record.OwnerID, target, digest)
			},
		)
		if err == nil {
			break
		}
		if !database.IsRetryableConflict(err) {
			return "", err
		}
		if attempt+1 < commitAttempts {
			if waitErr := database.WaitForRetry(ctx, attempt); waitErr != nil {
				return "", waitErr
			}
		}
	}
	if err != nil {
		return "", task.Retryable(err)
	}
	return string(resultJSON), nil
}

// loadTarget 读取任务指向的文件和实体快照。
//
// 这里只做快速筛选；读取完成后还会在事务里重新核对一次，防止读取期间
// 文件被删除或换内容。
func (r *runner) loadTarget(ctx context.Context, ownerID int) (*model.Entity, error) {
	var record model.File
	err := r.options.DB.WithContext(ctx).
		Where("id = ? AND owner_id = ? AND kind = ?",
			r.state.FileID, ownerID, model.KindFile).
		First(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("%w: file %s", ErrTargetGone, r.state.FileID)
	}
	if err != nil {
		return nil, fmt.Errorf("verify: query file %s: %w", r.state.FileID, err)
	}
	if record.EntityID == nil {
		return nil, fmt.Errorf("%w: file %s has no content", ErrTargetGone, record.ID)
	}

	var entity model.Entity
	err = r.options.DB.WithContext(ctx).First(&entity, *record.EntityID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("%w: entity %d", ErrTargetGone, *record.EntityID)
	}
	if err != nil {
		return nil, fmt.Errorf("verify: query entity %d: %w", *record.EntityID, err)
	}
	if entity.Status != model.EntityStatusActive || entity.RefCount <= 0 {
		return nil, fmt.Errorf(
			"%w: entity %d is %s with ref_count %d",
			ErrTargetGone, entity.ID, entity.Status, entity.RefCount,
		)
	}
	return &entity, nil
}

// backfill 在 Complete 的事务里执行：
//
//	锁文件行 → 核对实体仍有效 → 回填摘要 → 登记或复用内容映射
//
// 加锁顺序固定为 File → Entity → ContentKey，和发布/删除链路一致；
// 内容键永远最后锁，不能反过来先锁 ContentKey 再锁 Entity。
//
// 任何一步发现目标已经不可校验，都返回 ErrTargetGone 让整个事务回滚，
// 任务随后收敛为失败，不会留下错误的内容映射。
func (r *runner) backfill(
	tx *gorm.DB,
	ownerID int,
	verified *model.Entity,
	digest string,
) error {
	var record model.File
	err := tx.Clauses(clause.Locking{Strength: clause.LockingStrengthUpdate}).
		Where("id = ? AND owner_id = ?", r.state.FileID, ownerID).
		First(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("%w: file %s", ErrTargetGone, r.state.FileID)
	}
	if err != nil {
		return fmt.Errorf("verify: lock file %s: %w", r.state.FileID, err)
	}
	if record.Kind != model.KindFile || record.EntityID == nil ||
		*record.EntityID != verified.ID {
		return fmt.Errorf(
			"%w: file %s no longer points at entity %d",
			ErrTargetGone, record.ID, verified.ID,
		)
	}

	var entity model.Entity
	err = tx.Clauses(clause.Locking{Strength: clause.LockingStrengthUpdate}).
		First(&entity, verified.ID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("%w: entity %d", ErrTargetGone, verified.ID)
	}
	if err != nil {
		return fmt.Errorf("verify: lock entity %d: %w", verified.ID, err)
	}
	if entity.Status != model.EntityStatusActive || entity.RefCount <= 0 ||
		entity.StorageKey != verified.StorageKey || entity.Size != verified.Size {
		return fmt.Errorf(
			"%w: entity %d changed while verifying", ErrTargetGone, verified.ID,
		)
	}

	// 回填可信摘要。值已经相同的时候 MySQL 会报告 0 行受影响，
	// 所以不能用 RowsAffected 判断失败。
	if err := tx.Model(&model.Entity{}).
		Where("id = ? AND status = ?", entity.ID, model.EntityStatusActive).
		UpdateColumn("content_sha256", digest).Error; err != nil {
		return fmt.Errorf("verify: backfill digest of entity %d: %w", entity.ID, err)
	}

	return registerContentKey(tx, ownerID, entity, digest)
}

// registerContentKey 让这份已验证内容进入秒传映射。
//
// 作用域和秒传一致：同一用户、同一存储策略、同一摘要和大小。
// 已有映射可用时直接复用；映射指向的实体已经失效时改指向本次核验的实体。
// 唯一键冲突说明另一个事务先登记了等价内容，忽略即可。
func registerContentKey(
	tx *gorm.DB,
	ownerID int,
	entity model.Entity,
	digest string,
) error {
	var key model.ContentKey
	err := tx.Clauses(clause.Locking{Strength: clause.LockingStrengthUpdate}).
		Where(
			"owner_id = ? AND storage_policy_id = ? AND sha256 = ? AND size = ?",
			ownerID, entity.StoragePolicyID, digest, entity.Size,
		).
		First(&key).Error

	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		key = model.ContentKey{
			OwnerID:         ownerID,
			StoragePolicyID: entity.StoragePolicyID,
			SHA256:          digest,
			Size:            entity.Size,
			EntityID:        entity.ID,
		}
		if err := tx.Create(&key).Error; err != nil {
			if errors.Is(err, gorm.ErrDuplicatedKey) {
				// 另一个事务先登记了同一份内容：结果等价，不需要重试。
				return nil
			}
			return fmt.Errorf("verify: register content key: %w", err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("verify: query content key: %w", err)
	}

	// 已有映射：确认它仍然指向可用实体，过期时改指向本次核验的实体。
	var mapped model.Entity
	err = tx.Where("id = ?", key.EntityID).First(&mapped).Error
	if err == nil &&
		mapped.Status == model.EntityStatusActive && mapped.RefCount > 0 {
		return nil
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf(
			"verify: query mapped entity %d: %w", key.EntityID, err,
		)
	}
	if err := tx.Model(&model.ContentKey{}).
		Where("id = ?", key.ID).
		UpdateColumn("entity_id", entity.ID).Error; err != nil {
		return fmt.Errorf("verify: repoint content key %d: %w", key.ID, err)
	}
	return nil
}

// hashContent 按实体声明的大小读取内容并计算 SHA-256。
//
// 读取长度必须正好等于声明大小：多一个字节说明对象比记录的大，
// 少一个字节说明内容缺失或被截断，两种情况都不能登记为可信内容。
func hashContent(
	ctx context.Context,
	store storage.Store,
	storageKey string,
	expectedSize int64,
) (string, error) {
	if expectedSize == 0 {
		return emptyContentSHA256, nil
	}

	reader, err := store.Open(ctx, storageKey)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf(
				"%w: content object %q is missing", ErrTargetGone, storageKey,
			)
		}
		// 存储暂时不可用属于可重试错误：租约释放后由下一轮再试。
		return "", task.Retryable(fmt.Errorf(
			"verify: open content %q: %w", storageKey, err,
		))
	}
	defer reader.Close()

	hasher := sha256.New()
	read, err := io.Copy(hasher, io.LimitReader(reader, expectedSize+1))
	if err != nil {
		return "", task.Retryable(fmt.Errorf(
			"verify: read content %q: %w", storageKey, err,
		))
	}
	if read != expectedSize {
		return "", fmt.Errorf(
			"%w: content object %q has %d bytes, want %d",
			ErrSizeMismatch, storageKey, read, expectedSize,
		)
	}
	return sumSHA256(hasher), nil
}

// sumSHA256 把 hasher 的当前状态编码成小写十六进制摘要。
func sumSHA256(hasher hash.Hash) string {
	return hex.EncodeToString(hasher.Sum(nil))
}
