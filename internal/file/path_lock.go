package file

import (
	"context"
	"errors"
	"fmt"
	"path"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/pathlock"
)

// maxPathDepth 限制从节点走到根目录的层数，异常数据不能把请求拖死。
const maxPathDepth = 1000

// ErrResourceChanged 表示读取路径快照之后、取得锁之前，数据库里的节点变了。
// 调用方应释放旧快照并重新发起操作，不能继续按旧路径执行。
var ErrResourceChanged = errors.New("file path changed while acquiring lock")

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

// verifyPathSnapshot 重新查询快照里的每一个节点，并逐字段比较。
//
// 必须按快照遍历，而不是只遍历查询结果：如果某个节点已经被删除，查询结果里
// 少了它，也只有按快照核对才能发现。
func (s *Service) verifyPathSnapshot(ctx context.Context, snapshot pathSnapshot) error {
	for _, node := range snapshot.Ancestors {
		var record model.File
		err := s.db.WithContext(ctx).First(&record, "id = ?", node.ID).Error
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

// acquirePathLocks 批量申请路径锁，并在锁内复查所有快照。
//
// 成功时返回的 release 必须由调用方在操作结束后调用。复查发现节点被删除或
// 字段变化时，先释放本次申请的全部锁，再返回 ErrResourceChanged，调用方应
// 重新解析路径并重试，不能继续使用旧快照。
func (s *Service) acquirePathLocks(
	ctx context.Context,
	requests ...pathLockRequest,
) (func(), error) {
	if len(requests) == 0 {
		return func() {}, nil
	}

	locks := make([]pathlock.Request, len(requests))
	for i, request := range requests {
		locks[i] = pathlock.Request{
			Namespace: request.Snapshot.Namespace,
			Path:      request.Snapshot.Path,
			Scope:     request.Scope,
		}
	}

	leases, err := s.pathLocks.Create(locks...)
	if err != nil {
		return nil, err
	}

	release := func() {
		for _, lease := range leases {
			_ = s.pathLocks.Unlock(lease.Request.Namespace, lease.Token)
		}
	}

	for _, request := range requests {
		if err := s.verifyPathSnapshot(ctx, request.Snapshot); err != nil {
			release()
			return nil, err
		}
	}

	return release, nil
}

func equalParentID(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
