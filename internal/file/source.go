package file

import (
	"context"
	"errors"
	"fmt"
	"io"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/pathlock"
)

var (
	// ErrSourceChanged 表示来源文件在任务准备好之后被改名、移动、
	// 换内容或删除，已经和保存的来源快照不一致。
	//
	// 可恢复任务必须明确失败，不能按“当前同名文件”继续压缩一份不同的内容。
	ErrSourceChanged = errors.New("archive source changed")

	// ErrInvalidSource 表示来源清单本身不合法，例如空 ID 或重复 ID。
	ErrInvalidSource = errors.New("invalid archive source")
)

// SourceSnapshot 是一个来源文件在 prepare 阶段确定下来的身份。
//
// EntityID 是内容身份：文件改名或移动后 EntityID 不变，但 Name 会变；
// 换一份内容会得到新的 EntityID。ParentID 是位置身份：文件被移动到
// 另一个目录后名字和内容都可以不变，只有父目录会变。
//
// 压缩前必须同时核对位置、名字、内容和大小，才能保证恢复任务既没有
// 悄悄换成另一份内容，也没有使用一个已经被移走的来源。
type SourceSnapshot struct {
	FileID   string `json:"file_id"`
	EntityID int64  `json:"entity_id"`
	ParentID string `json:"parent_id"`
	Name     string `json:"name"`
	Size     int64  `json:"size"`
}

// SourceGuard 是一次执行期间持有的来源保护。
//
// 它只持有当前进程的路径锁和来源快照，不保存数据库行锁或旧令牌。
// 每次恢复都会重新调用 ProtectSources 取得新保护；Release 必须调用。
type SourceGuard interface {
	// Open 按快照顺序打开一个来源，返回的内容读取器由调用方关闭。
	Open(ctx context.Context, index int) (SourceSnapshot, io.ReadCloser, error)

	// Release 释放本次执行取得的全部路径保护，可以重复调用。
	Release()
}

// SnapshotSources 读取 ownerID 用户自己的普通文件，返回可落库的来源快照。
//
// 它只做校验和读取，不申请长期锁：prepare 阶段记录身份，真正压缩时
// 再由 ProtectSources 重新取得保护并核对。
func (s *Service) SnapshotSources(
	ctx context.Context,
	ownerID int,
	ids []string,
) ([]SourceSnapshot, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: no source files", ErrInvalidSource)
	}

	seen := make(map[string]struct{}, len(ids))
	snapshots := make([]SourceSnapshot, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			return nil, fmt.Errorf("%w: source file ID is empty", ErrInvalidSource)
		}
		if _, ok := seen[id]; ok {
			return nil, fmt.Errorf("%w: duplicate source file %s", ErrInvalidSource, id)
		}
		seen[id] = struct{}{}

		record, entity, err := s.querySource(ctx, ownerID, id)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, SourceSnapshot{
			FileID:   record.ID,
			EntityID: entity.ID,
			ParentID: sourceParentID(record),
			Name:     record.Name,
			Size:     entity.Size,
		})
	}
	return snapshots, nil
}

// ValidateTarget 校验发布目标：parentID（为空表示根目录）属于 ownerID
// 且是目录，name 是合法文件名。返回解析后的父目录 ID。
//
// 这里不加锁；真正发布时 CreateTaskUploadSession 和完成事务会按各自的
// 锁与快照规则重新核对。
func (s *Service) ValidateTarget(
	ctx context.Context,
	ownerID int,
	parentID, name string,
) (string, error) {
	target, err := s.resolveTargetPath(ctx, ownerID, parentID, name)
	if err != nil {
		return "", err
	}
	if len(target.Ancestors) == 0 {
		return "", fmt.Errorf("archive target has no parent directory")
	}
	return target.Ancestors[len(target.Ancestors)-1].ID, nil
}

// ProtectSources 为一次压缩重新取得来源保护。
//
// 步骤是“解析路径 → 批量加锁 → 加锁后复查”：
//  1. 按快照里的 FileID 重新解析虚拟路径，不信任保存的旧路径；
//  2. 一次性申请全部来源节点的锁，冲突时短暂重试；
//  3. 加锁后重新读取文件、实体和路径，与保存的快照逐项比较；
//  4. 只有完全一致才返回 guard。
//
// 所以任务恢复、重试或换一个进程执行时，都不会复用旧锁令牌；
// 来源被改名、移动、删除或换内容时返回 ErrSourceChanged，不会继续压缩。
func (s *Service) ProtectSources(
	ctx context.Context,
	ownerID int,
	snapshots []SourceSnapshot,
) (SourceGuard, error) {
	if len(snapshots) == 0 {
		return nil, fmt.Errorf("%w: no source snapshots", ErrInvalidSource)
	}

	type target struct {
		snapshot SourceSnapshot
		path     string
	}
	targets := make([]target, 0, len(snapshots))
	requests := make([]pathlock.Request, 0, len(snapshots))
	seen := make(map[string]struct{}, len(snapshots))

	for _, snapshot := range snapshots {
		if snapshot.FileID == "" {
			return nil, fmt.Errorf("%w: source file ID is empty", ErrInvalidSource)
		}
		if _, ok := seen[snapshot.FileID]; ok {
			return nil, fmt.Errorf("%w: duplicate source file %s", ErrInvalidSource, snapshot.FileID)
		}
		seen[snapshot.FileID] = struct{}{}

		resolved, err := s.resolvePath(ctx, ownerID, snapshot.FileID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, fmt.Errorf(
					"%w: source %s no longer exists",
					ErrSourceChanged, snapshot.FileID,
				)
			}
			return nil, err
		}
		if resolved.Path == "" {
			return nil, fmt.Errorf("%w: source %s has no path", ErrInvalidSource, snapshot.FileID)
		}
		targets = append(targets, target{snapshot: snapshot, path: resolved.Path})
		requests = append(requests, pathlock.Request{
			Namespace: pathlock.Namespace(ownerID),
			Path:      resolved.Path,
			Scope:     pathlock.Node,
		})
	}

	leases, err := s.createSourceLocks(ctx, requests)
	if err != nil {
		return nil, err
	}
	releaseOnError := true
	defer func() {
		if releaseOnError {
			s.releaseSourceLocks(leases)
		}
	}()

	locations := make([]sourceLocation, 0, len(targets))
	for _, target := range targets {
		// 加锁前可能刚好发生过改名/移动；加锁后必须重新解析路径。
		resolved, err := s.resolvePath(ctx, ownerID, target.snapshot.FileID)
		if err != nil {
			return nil, fmt.Errorf("%w: source %s cannot be resolved: %v", ErrSourceChanged, target.snapshot.FileID, err)
		}
		if resolved.Path != target.path {
			return nil, fmt.Errorf(
				"%w: source %s moved from %s to %s",
				ErrSourceChanged, target.snapshot.FileID, target.path, resolved.Path,
			)
		}

		record, entity, err := s.querySource(ctx, ownerID, target.snapshot.FileID)
		if err != nil {
			return nil, fmt.Errorf("%w: source %s cannot be read: %v", ErrSourceChanged, target.snapshot.FileID, err)
		}
		if record.Name != target.snapshot.Name ||
			sourceParentID(record) != target.snapshot.ParentID ||
			entity.ID != target.snapshot.EntityID ||
			entity.Size != target.snapshot.Size {
			return nil, fmt.Errorf(
				"%w: source %s is no longer the recorded file",
				ErrSourceChanged, target.snapshot.FileID,
			)
		}
		locations = append(locations, sourceLocation{
			snapshot:        target.snapshot,
			storagePolicyID: entity.StoragePolicyID,
			storageKey:      entity.StorageKey,
		})
	}

	releaseOnError = false
	return &sourceLease{
		service:   s,
		locations: locations,
		leases:    leases,
	}, nil
}

// sourceParentID 把可空的父目录 ID 转成快照里的字符串形式。
// 来源必须是普通文件，正常一定有父目录；空值只用于兼容异常数据。
func sourceParentID(record model.File) string {
	if record.ParentID == nil {
		return ""
	}
	return *record.ParentID
}

// querySource 读取一个来源文件的数据库身份。
func (s *Service) querySource(
	ctx context.Context,
	ownerID int,
	id string,
) (model.File, model.Entity, error) {
	var record model.File
	err := s.db.WithContext(ctx).
		Preload("Entity").
		Where("id = ? AND owner_id = ?", id, ownerID).
		First(&record).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.File{}, model.Entity{}, ErrNotFound
		}
		return model.File{}, model.Entity{}, fmt.Errorf("query source file: %w", err)
	}
	if record.Kind != model.KindFile {
		return model.File{}, model.Entity{}, ErrIsDirectory
	}
	if record.Entity == nil {
		return model.File{}, model.Entity{}, fmt.Errorf("source file %s has no content", id)
	}
	if record.Entity.Status != model.EntityStatusActive {
		return model.File{}, model.Entity{}, fmt.Errorf(
			"source file %s content is %s", id, record.Entity.Status,
		)
	}
	return record, *record.Entity, nil
}

// createSourceLocks 批量申请来源节点锁。冲突表示另一个写操作正在
// 改名/移动/删除来源，短暂重试后仍然冲突就返回 ErrResourceLocked。
func (s *Service) createSourceLocks(
	ctx context.Context,
	requests []pathlock.Request,
) ([]pathlock.Lease, error) {
	var lastErr error
	for attempt := 0; attempt < pathLockRetryAttempts; attempt++ {
		leases, err := s.pathLocks.Create(requests...)
		if err == nil {
			return leases, nil
		}
		lastErr = err
		if !errors.Is(err, pathlock.ErrConflict) {
			return nil, err
		}
		if err := waitPathLockRetry(ctx); err != nil {
			return nil, err
		}
	}
	return nil, wrapResourceLocked(lastErr)
}

func (s *Service) releaseSourceLocks(leases []pathlock.Lease) {
	if len(leases) == 0 {
		return
	}
	tokens := make([]string, 0, len(leases))
	for _, lease := range leases {
		tokens = append(tokens, lease.Token)
	}
	// namespace 对同一批来源相同；释放失败只可能来自已经解锁，不影响正确性。
	if err := s.pathLocks.Unlock(leases[0].Request.Namespace, tokens...); err != nil {
		s.logger.Warn("release archive source locks", "error", err)
	}
}

// sourceLocation 是 guard.Open 需要的存储位置，不暴露给业务层。
type sourceLocation struct {
	snapshot        SourceSnapshot
	storagePolicyID int
	storageKey      string
}

// sourceLease 是 SourceGuard 的实现：保护来自当前进程的路径锁，
// 每个任务每次执行都重新申请。
type sourceLease struct {
	service   *Service
	locations []sourceLocation
	leases    []pathlock.Lease
	released  bool
}

func (g *sourceLease) Open(
	ctx context.Context,
	index int,
) (SourceSnapshot, io.ReadCloser, error) {
	if g.released {
		return SourceSnapshot{}, nil, errors.New("archive source guard is released")
	}
	if index < 0 || index >= len(g.locations) {
		return SourceSnapshot{}, nil, fmt.Errorf("archive source index %d out of range", index)
	}
	if err := ctx.Err(); err != nil {
		return SourceSnapshot{}, nil, err
	}

	location := g.locations[index]
	store, err := g.service.stores.Get(location.storagePolicyID)
	if err != nil {
		return SourceSnapshot{}, nil, fmt.Errorf(
			"resolve storage policy %d of source %s: %w",
			location.storagePolicyID, location.snapshot.FileID, err,
		)
	}
	content, err := store.Open(ctx, location.storageKey)
	if err != nil {
		return SourceSnapshot{}, nil, fmt.Errorf(
			"open source %s content: %w", location.snapshot.FileID, err,
		)
	}
	return location.snapshot, content, nil
}

func (g *sourceLease) Release() {
	if g.released {
		return
	}
	g.released = true
	g.service.releaseSourceLocks(g.leases)
	g.leases = nil
}
