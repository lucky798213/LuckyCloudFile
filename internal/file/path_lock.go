package file

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/pathlock"
)

const (
	// 路径冲突通常只持续一个短事务；短暂等待后重试，避免把并发写直接变成 500。
	pathLockRetryAttempts = 100
	pathLockRetryDelay    = 5 * time.Millisecond
)

// maxPathDepth 限制从节点走到根目录的层数，异常数据不能把请求拖死。
const maxPathDepth = 1000

// ErrResourceChanged 表示读取路径快照之后、取得锁之前，数据库里的节点变了。
// 调用方应释放旧快照并重新发起操作，不能继续按旧路径执行。
var ErrResourceChanged = errors.New("file path changed while acquiring lock")

// ErrResourceLocked 表示目标路径正被另一个操作占用，例如进行中的上传会话
// 已经占住了最终目标。HTTP 层把它映射成 409。
var ErrResourceLocked = pathlock.ErrConflict

// nodeSnapshot 是路径上一个节点的身份和层级信息。
type nodeSnapshot struct {
	ID       string
	ParentID *string
	Name     string
	OwnerID  int
	Kind     string
}

// pathSnapshot 是一次路径读取的完整快照。
//
// Ancestors 按“根目录 → 目标节点”排列，包含目标节点及其全部祖先。
// 新建目标还不存在时，Path 已经包含新名字，但 Ancestors 只包含已经存在的
// 父目录及其祖先，因为目标节点还没有数据库记录可以复查。
type pathSnapshot struct {
	Namespace string
	Path      string
	Ancestors []nodeSnapshot
}

// pathLockRequest 描述一个需要加锁并复查的路径快照。
type pathLockRequest struct {
	Snapshot pathSnapshot
	Scope    pathlock.Scope

	// ExpiresAt 是这把锁的过期时间。零值表示永不过期；
	// 上传会话锁直接用会话的 ExpiresAt，避免上传会话还活着但锁先失效。
	ExpiresAt time.Time
}

// resolvePath 从 nodeID 沿 parent_id 向上读取，构造用户虚拟路径。
//
// nodeID 为空时解析当前用户的根目录。每一层都必须属于 ownerID，祖先必须是
// 目录，最终必须到达该用户的根目录；访问集合和深度上限用来拒绝环和异常层级。
func (s *Service) resolvePath(ctx context.Context, ownerID int, nodeID string) (pathSnapshot, error) {
	rootID, err := rootDirectoryID(ctx, s.db, ownerID)
	if err != nil {
		return pathSnapshot{}, err
	}
	if nodeID == "" {
		nodeID = rootID
	}

	visited := make(map[string]struct{})
	nodes := make([]nodeSnapshot, 0, 8)
	current := nodeID
	reachedRoot := false

	for depth := 0; depth < maxPathDepth; depth++ {
		if _, ok := visited[current]; ok {
			return pathSnapshot{}, fmt.Errorf("file tree contains a cycle at %s", current)
		}
		visited[current] = struct{}{}

		var record model.File
		err := s.db.WithContext(ctx).
			Where("id = ? AND owner_id = ?", current, ownerID).
			First(&record).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return pathSnapshot{}, ErrNotFound
			}
			return pathSnapshot{}, fmt.Errorf("query path node: %w", err)
		}

		// 目标节点可以是文件或目录，但它上面的每一层都必须是目录。
		if len(nodes) > 0 && record.Kind != model.KindDirectory {
			return pathSnapshot{}, fmt.Errorf(
				"ancestor %s of node %s is not a directory", record.ID, nodeID,
			)
		}

		nodes = append(nodes, nodeSnapshot{
			ID:       record.ID,
			ParentID: record.ParentID,
			Name:     record.Name,
			OwnerID:  record.OwnerID,
			Kind:     record.Kind,
		})

		if record.ParentID == nil {
			if record.ID != rootID {
				return pathSnapshot{}, fmt.Errorf(
					"path from node %s does not end at user %d root", nodeID, ownerID,
				)
			}
			if record.Kind != model.KindDirectory {
				return pathSnapshot{}, fmt.Errorf("root %s is not a directory", record.ID)
			}
			reachedRoot = true
			break
		}
		current = *record.ParentID
	}

	if !reachedRoot {
		return pathSnapshot{}, fmt.Errorf(
			"path from node %s is too deep or does not reach the root", nodeID,
		)
	}

	// 收集时是目标 → 根目录，翻转成根目录 → 目标，方便拼路径和复查。
	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}

	elements := make([]string, 0, len(nodes)-1)
	for _, node := range nodes {
		if node.ParentID == nil {
			continue
		}
		// 数据库里的名字也必须在拼路径前合法，否则虚拟路径会有歧义。
		if err := validateName(node.Name); err != nil {
			return pathSnapshot{}, fmt.Errorf("node %s has invalid name: %w", node.ID, err)
		}
		elements = append(elements, node.Name)
	}

	return pathSnapshot{
		Namespace: pathlock.Namespace(ownerID),
		Path:      path.Join(append([]string{"/"}, elements...)...),
		Ancestors: nodes,
	}, nil
}

// resolveTargetPath 解析“父目录 + 新名字”对应的目标虚拟路径。
//
// 先解析父目录：父目录必须存在、属于当前用户、而且是目录。然后用和写接口
// 相同的规则校验新名字，最后拼出目标路径。返回的快照只包含已经存在的
// 父目录及其祖先，目标节点尚未落库。
func (s *Service) resolveTargetPath(
	ctx context.Context,
	ownerID int,
	parentID, name string,
) (pathSnapshot, error) {
	parent, err := s.resolvePath(ctx, ownerID, parentID)
	if err != nil {
		return pathSnapshot{}, err
	}
	if err := validateName(name); err != nil {
		return pathSnapshot{}, err
	}
	if len(parent.Ancestors) == 0 {
		return pathSnapshot{}, fmt.Errorf("parent path has no nodes")
	}

	parentNode := parent.Ancestors[len(parent.Ancestors)-1]
	if parentNode.Kind != model.KindDirectory {
		return pathSnapshot{}, ErrNotDirectory
	}

	return pathSnapshot{
		Namespace: parent.Namespace,
		Path:      path.Join(parent.Path, name),
		Ancestors: parent.Ancestors,
	}, nil
}

// verifyPathSnapshot 用服务自己的连接复查快照，供测试和事务外调用使用。
func (s *Service) verifyPathSnapshot(ctx context.Context, snapshot pathSnapshot) error {
	return verifyPathSnapshotWith(ctx, s.db, snapshot)
}

// verifyPathSnapshotWith 重新查询快照里的每一个节点，并逐字段比较。
//
// 传入 tx 时，复查发生在已经开始的数据库事务里。必须按快照遍历，
// 而不是只遍历查询结果：如果某个节点已经被删除，查询结果里少了它，
// 也只有按快照核对才能发现。
func verifyPathSnapshotWith(ctx context.Context, db *gorm.DB, snapshot pathSnapshot) error {
	for _, node := range snapshot.Ancestors {
		var record model.File
		err := db.WithContext(ctx).First(&record, "id = ?", node.ID).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w: node %s no longer exists", ErrResourceChanged, node.ID)
			}
			return fmt.Errorf("verify path snapshot: %w", err)
		}

		if record.ID != node.ID ||
			!equalParentID(record.ParentID, node.ParentID) ||
			record.Name != node.Name ||
			record.OwnerID != node.OwnerID ||
			record.Kind != node.Kind {
			return fmt.Errorf("%w: node %s changed", ErrResourceChanged, node.ID)
		}
	}
	return nil
}

// pathLockGuard 保存本次批量申请的锁和对应的 Confirm 短时保护。
type pathLockGuard struct {
	manager  *pathlock.Manager
	leases   []pathlock.Lease
	releases []func()
}

// acquirePathLocks 批量申请路径锁，并为每把锁进入一次 Confirm 短时保护。
//
// 锁的粒度由调用方按路径和操作类型决定。成功后必须调用 release：
// 先退出短时保护，再 Unlock 本次申请的全部锁。
func (s *Service) acquirePathLocks(requests ...pathLockRequest) (*pathLockGuard, error) {
	guard := &pathLockGuard{manager: s.pathLocks}
	if len(requests) == 0 {
		return guard, nil
	}

	locks := make([]pathlock.Request, len(requests))
	for i, request := range requests {
		locks[i] = pathlock.Request{
			Namespace: request.Snapshot.Namespace,
			Path:      request.Snapshot.Path,
			Scope:     request.Scope,
			ExpiresAt: request.ExpiresAt,
		}
	}

	leases, err := s.pathLocks.Create(locks...)
	if err != nil {
		return nil, err
	}
	guard.leases = leases

	for _, lease := range leases {
		release, err := s.pathLocks.Confirm(
			lease.Request.Namespace,
			lease.Request.Path,
			lease.Token,
		)
		if err != nil {
			guard.release()
			return nil, err
		}
		guard.releases = append(guard.releases, release)
	}
	return guard, nil
}

// release 先退出全部短时保护，再释放本次申请的资源锁。
func (g *pathLockGuard) release() {
	if g == nil {
		return
	}
	g.releaseProtection()
	g.unlock()
}

// releaseProtection 只退出本次短时保护，保留资源锁供后续 Confirm。
//
// 上传会话创建成功后用它结束创建请求的保护：锁本身要跨请求保留到
// 会话完成、取消或过期。
func (g *pathLockGuard) releaseProtection() {
	if g == nil {
		return
	}
	for _, release := range g.releases {
		release()
	}
	g.releases = nil
}

// unlock 释放本次申请的资源锁。调用方必须先退出自己的短时保护。
func (g *pathLockGuard) unlock() {
	if g == nil {
		return
	}
	for _, lease := range g.leases {
		_ = g.manager.Unlock(lease.Request.Namespace, lease.Token)
	}
	g.leases = nil
}

// withLockedTx 在资源锁和短时保护内开启事务，先复查快照，再执行 fn。
func (s *Service) withLockedTx(
	ctx context.Context,
	requests []pathLockRequest,
	fn func(tx *gorm.DB) error,
) error {
	guard, err := s.acquirePathLocks(requests...)
	if err != nil {
		return err
	}
	defer guard.release()

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := verifyPathSnapshotsWith(ctx, tx, requests); err != nil {
			return err
		}
		return fn(tx)
	})
}

func verifyPathSnapshotsWith(ctx context.Context, db *gorm.DB, requests []pathLockRequest) error {
	for _, request := range requests {
		if err := verifyPathSnapshotWith(ctx, db, request.Snapshot); err != nil {
			return err
		}
	}
	return nil
}

// runLockedOperation 先解析快照，再在锁内执行事务。
//
// 另一请求正持有一把冲突的锁，或者解析之后路径被改动时，重新解析快照并重试。
// 这样同一个路径上的并发操作会串行完成，而不是直接返回一个内部错误。
func (s *Service) runLockedOperation(
	ctx context.Context,
	prepare func() ([]pathLockRequest, error),
	fn func(tx *gorm.DB) error,
) error {
	var lastErr error
	for attempt := 0; attempt < pathLockRetryAttempts; attempt++ {
		requests, err := prepare()
		if err != nil {
			return err
		}

		lastErr = s.withLockedTx(ctx, requests, fn)
		if !isRetryablePathError(lastErr) {
			return lastErr
		}
		if err := waitPathLockRetry(ctx); err != nil {
			return err
		}
	}
	return lastErr
}

// acquirePathLocksWithRetry 用于 Upload：在读取内容之前先拿到目标路径锁。
func (s *Service) acquirePathLocksWithRetry(
	ctx context.Context,
	prepare func() ([]pathLockRequest, error),
) (*pathLockGuard, error) {
	var lastErr error
	for attempt := 0; attempt < pathLockRetryAttempts; attempt++ {
		requests, err := prepare()
		if err != nil {
			return nil, err
		}

		guard, err := s.acquirePathLocks(requests...)
		if err == nil {
			return guard, nil
		}
		if !errors.Is(err, pathlock.ErrConflict) {
			return nil, err
		}

		lastErr = err
		if err := waitPathLockRetry(ctx); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func isRetryablePathError(err error) bool {
	return errors.Is(err, pathlock.ErrConflict) || errors.Is(err, ErrResourceChanged)
}

func waitPathLockRetry(ctx context.Context) error {
	timer := time.NewTimer(pathLockRetryDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// snapshotContainsNode 判断快照的祖先链里是否包含指定节点。
// 用于目录移动的成环预检：看目标父目录是不是源目录本身或它的后代。
func snapshotContainsNode(snapshot pathSnapshot, nodeID string) bool {
	for _, ancestor := range snapshot.Ancestors {
		if ancestor.ID == nodeID {
			return true
		}
	}
	return false
}

func equalParentID(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
