// Package file 是文件的业务层：生成 ID、操作数据库记录、协调物理存储。
//
// 它不碰 HTTP（那是 internal/httpapi），也不碰 os.Open/os.Rename（那是 internal/storage）。
// 它只决定“这件事该按什么顺序做、失败了怎么收拾”。
package file

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"luckyCloudFile/internal/idgen"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
)

const (
	// MaxUploadBytes 是单个文件内容的大小上限：32 MiB。
	// 注意它限制的是“文件内容”，不包含 multipart 的表单头和边界。
	MaxUploadBytes int64 = 32 << 20

	// defaultPageSize / maxPageSize 限制列表接口一次返回多少条。
	defaultPageSize = 20
	maxPageSize     = 100

	// maxNameRunes 和 files.name 的 varchar(255) 对齐。
	maxNameRunes = 255

	// orphanCleanupTimeout 是上传成功后数据库写入失败时，补偿删除内容的超时时间。
	// 它不长：删除只是一个本地文件操作或一次对象存储请求；
	// 但没有上限的话，存储端卡住会把请求一直拖住。
	orphanCleanupTimeout = 10 * time.Second
)

var (
	// ErrNotFound 表示这个 ID 没有对应的节点，或者不属于当前用户。
	ErrNotFound = errors.New("file not found")

	// ErrTooLarge 表示上传的文件内容超过了 MaxUploadBytes。
	ErrTooLarge = errors.New("file too large")

	// ErrNotDirectory 表示 parent_id 指向的不是目录。
	ErrNotDirectory = errors.New("parent is not a directory")

	// ErrIsDirectory 表示对一个目录调用了只适用于文件的操作（下载、作为复制源）。
	ErrIsDirectory = errors.New("file is a directory")

	// ErrInvalidName 表示名字为空、是 "." / ".."、含路径分隔符或太长。
	ErrInvalidName = errors.New("invalid file name")

	// ErrAlreadyExists 表示同一目录下已经有同名节点。
	ErrAlreadyExists = errors.New("an entry with the same name already exists")

	// ErrRootDirectory 表示试图修改用户根目录（改名、移动或删除）。
	ErrRootDirectory = errors.New("the user root directory cannot be modified")

	// ErrDirectoryNotEmpty 表示试图删除还有子节点的目录。
	ErrDirectoryNotEmpty = errors.New("directory is not empty")

	// ErrInvalidMove 表示把目录移动到自身或自己的后代，会让目录树成环。
	ErrInvalidMove = errors.New("cannot move a directory into itself or its descendants")
)

// File 是返回给 HTTP 层的节点信息（不包含数据库内部字段）。
//
// 它和 model.File 不是同一个类型：
// model.File 是数据库记录，下面这个只是“客户端需要看到的字段”。
// 目录没有 Entity，所以 Size 是 0。
type File struct {
	ID       string  `json:"id"`
	Name     string  `json:"name"`
	Size     int64   `json:"size"`
	Kind     string  `json:"kind"`
	ParentID *string `json:"parent_id"`
}

// ListResult 是目录列表的一页。
type ListResult struct {
	Items    []File `json:"items"`
	Total    int64  `json:"total"`
	Page     int    `json:"page"`
	PageSize int    `json:"page_size"`
}

// Service 负责文件业务：把内容交给 Store 保存 + 写数据库。
//
// 它依赖的是 storage.Store 接口，而不是具体实现：
// 现在注入 Local，以后换成 S3 时这里不需要改。
//
// 多个存储位置由 storage.Registry 按策略 ID 管理；
// Service 不自己选目录，也不回退到“默认位置”。
type Service struct {
	db     *gorm.DB
	stores *storage.Registry
	logger *slog.Logger
}

// NewService 创建文件服务。
func NewService(db *gorm.DB, stores *storage.Registry) *Service {
	return &Service{
		db:     db,
		stores: stores,
		// 默认丢弃日志：构造 Service 的人不一定关心后台清理，
		// 生产代码可以用 WithLogger 换成真正的 logger。
		logger: slog.New(slog.DiscardHandler),
	}
}

// WithLogger 给后台清理任务设置日志输出，返回自身方便链式调用。
func (s *Service) WithLogger(logger *slog.Logger) *Service {
	if logger != nil {
		s.logger = logger
	}
	return s
}

// Upload 把 r 的内容存成 ownerID 用户 parentID 目录下的文件。
//
// ownerID 只能来自登录会话，不能来自请求体或 URL。
// parentID 为空时使用当前用户的根目录。
//
// 顺序是：
//  1. 先确认父目录存在、属于当前用户、而且真的是目录
//  2. 生成两个随机 ID：File 的公开 ID、Entity 的存储 key
//  3. 通过 Store 保存内容（具体怎么写由存储实现决定）
//  4. 再开一个短事务：重新核对父目录 → 写 Entity 和 File
//
// 为什么不把整个上传包在事务里？因为 32 MiB 可能要传几十秒，
// 一直占着数据库事务会拖住其他请求。
func (s *Service) Upload(ctx context.Context, ownerID int, parentID, name string, r io.Reader) (File, error) {
	if err := validateName(name); err != nil {
		return File{}, err
	}

	parent, err := s.requireDirectory(ctx, ownerID, parentID)
	if err != nil {
		return File{}, err
	}

	// 整个上传只在这里选一次策略：后面即使服务端改了用户默认策略，
	// 本次内容仍然写到已经选定的位置，Entity 也记录同一个策略 ID。
	policyID, store, err := s.uploadStore(ctx, ownerID)
	if err != nil {
		return File{}, err
	}

	fileID, err := idgen.New()
	if err != nil {
		return File{}, err
	}
	storageKey, err := idgen.New()
	if err != nil {
		return File{}, err
	}

	// 先落盘。写入失败时数据库还没动过，直接返回即可。
	size, err := store.Save(ctx, storageKey, r, MaxUploadBytes)
	if err != nil {
		if errors.Is(err, storage.ErrTooLarge) {
			return File{}, ErrTooLarge
		}
		return File{}, err
	}

	// 再写数据库。失败就删除已写入的内容，保证两边一致。
	if err := s.saveRecords(ctx, ownerID, parent.ID, fileID, storageKey, policyID, name, size); err != nil {
		// 补偿删除不能复用请求的 ctx：客户端断开或超时后 ctx 已经被取消，
		// Remove 会立刻返回取消错误，内容就永远留在存储里了。
		// 用 WithoutCancel 保留值但去掉取消/超时，再套一个独立的有限超时。
		// 这里必须用本次实际写入的 store，而不是重新按默认策略查一次。
		removeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), orphanCleanupTimeout)
		removeErr := store.Remove(removeCtx, storageKey)
		cancel()
		if removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			s.logger.Warn("remove orphan content",
				"storage_key", storageKey,
				"storage_policy_id", policyID,
				"error", removeErr,
			)
		}
		return File{}, err
	}

	return File{
		ID:       fileID,
		Name:     name,
		Size:     size,
		Kind:     model.KindFile,
		ParentID: &parent.ID,
	}, nil
}

// uploadStore 读取服务端分配给用户的上传策略，并取出对应的 Store。
//
// 这是上传过程中唯一一次选择位置：返回的策略 ID 会原样写入 Entity，
// 后续下载和清理都以 Entity 上的 ID 为准，不再看用户当前默认策略。
func (s *Service) uploadStore(ctx context.Context, ownerID int) (int, storage.Store, error) {
	var user model.User
	err := s.db.WithContext(ctx).
		Select("id", "default_storage_policy_id").
		First(&user, ownerID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil, fmt.Errorf("user %d not found", ownerID)
		}
		return 0, nil, fmt.Errorf("query user storage policy: %w", err)
	}
	if user.DefaultStoragePolicyID <= 0 {
		return 0, nil, fmt.Errorf("user %d has no default storage policy", ownerID)
	}

	store, err := s.stores.Get(user.DefaultStoragePolicyID)
	if err != nil {
		return 0, nil, fmt.Errorf(
			"resolve upload storage policy %d: %w",
			user.DefaultStoragePolicyID, err,
		)
	}
	return user.DefaultStoragePolicyID, store, nil
}

// saveRecords 在一个短事务里写入 Entity 和 File。
//
// 两个表必须一起成功：只有 File 没有 Entity，就不知道内容在哪；
// 只有 Entity 没有 File，就是一份没人引用的孤儿内容。
//
// 事务里会重新核对一次父目录：接收文件的过程中，目录可能已经被删掉，
// 或者被换到了别人名下。
func (s *Service) saveRecords(ctx context.Context, ownerID int, parentID, fileID, storageKey string, policyID int, name string, size int64) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, err := queryDirectory(ctx, tx, ownerID, parentID); err != nil {
			return err
		}

		entity := model.Entity{
			StorageKey:      storageKey,
			Size:            size,
			StoragePolicyID: policyID,
			RefCount:        1,
			// 新内容一定可以被引用。以后做去重、复用现有 Entity 时，
			// 必须先锁住 Entity 并确认它仍是 active，pending_delete 不允许再加引用。
			Status: model.EntityStatusActive,
		}
		// Create 会把自增 ID 回填到 entity.ID，下面 File 要用。
		if err := tx.Create(&entity).Error; err != nil {
			return fmt.Errorf("create entity: %w", err)
		}

		record := model.File{
			ID:       fileID,
			Name:     name,
			Kind:     model.KindFile,
			ParentID: &parentID,
			EntityID: &entity.ID,
			OwnerID:  ownerID,
		}
		if err := tx.Create(&record).Error; err != nil {
			if errors.Is(err, gorm.ErrDuplicatedKey) {
				return ErrAlreadyExists
			}
			return fmt.Errorf("create file: %w", err)
		}
		return nil
	})
}

// Copy 在 parentID（为空表示根目录）下创建 id 文件的一份副本，副本名为 name。
//
// 复制不搬字节：新 File 直接指向源文件原来的 Entity，再把 Entity 的引用数加一。
// 所以磁盘上只有一份内容，删除源文件或副本时只要还有另一份引用，内容就不会被清理。
//
// 加锁顺序和 Delete 一致：先锁源 File，再锁 Entity。这样复制和删除同一份文件时
// 不会互相等待成环，也不会出现“刚复制完，Entity 已被标记为待删除”的中间状态。
func (s *Service) Copy(ctx context.Context, ownerID int, id, parentID, name string) (File, error) {
	if err := validateName(name); err != nil {
		return File{}, err
	}

	// 先在事务外查一次目标目录，快速拒绝明显不合法的请求。
	// 事务里还会再查一次，防止校验之后目录被改动。
	parent, err := s.requireDirectory(ctx, ownerID, parentID)
	if err != nil {
		return File{}, err
	}

	newID, err := idgen.New()
	if err != nil {
		return File{}, err
	}

	var copied model.File
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		source, err := lockOwnedFile(ctx, tx, ownerID, id)
		if err != nil {
			return err
		}
		if source.Kind != model.KindFile {
			return ErrIsDirectory
		}
		if source.EntityID == nil {
			return fmt.Errorf("file %s has no entity", id)
		}

		entity, err := lockEntity(ctx, tx, *source.EntityID)
		if err != nil {
			return err
		}
		// 源 File 仍然引用这个 Entity，正常数据里 ref_count 至少是 1。
		// 状态不是 active 或引用数已经归零，都说明数据库已经不一致；
		// 这时继续复制只会把错误放大，宁可回滚。
		if entity.Status != model.EntityStatusActive || entity.RefCount <= 0 {
			return fmt.Errorf(
				"entity %d cannot be referenced: status=%q ref_count=%d",
				entity.ID, entity.Status, entity.RefCount,
			)
		}

		// 事务内重新核对目标目录：它必须仍然属于当前用户、而且仍然是目录。
		target, err := queryDirectory(ctx, tx, ownerID, parent.ID)
		if err != nil {
			return err
		}

		record := model.File{
			ID:       newID,
			Name:     name,
			Kind:     model.KindFile,
			ParentID: &target.ID,
			EntityID: &entity.ID,
			OwnerID:  ownerID,
		}
		// (owner_id, parent_id, name) 唯一索引负责最终的同名检查。
		// 重名时整个事务回滚，源 Entity 的引用数不会被加一。
		if err := tx.Create(&record).Error; err != nil {
			if errors.Is(err, gorm.ErrDuplicatedKey) {
				return ErrAlreadyExists
			}
			return fmt.Errorf("create copied file: %w", err)
		}

		incremented := tx.WithContext(ctx).
			Model(&model.Entity{}).
			Where("id = ? AND status = ? AND ref_count > 0", entity.ID, model.EntityStatusActive).
			UpdateColumn("ref_count", gorm.Expr("ref_count + 1"))
		if incremented.Error != nil {
			return fmt.Errorf("increment ref count: %w", incremented.Error)
		}
		if incremented.RowsAffected != 1 {
			return fmt.Errorf(
				"increment ref count of entity %d: expected 1 row affected, got %d",
				entity.ID, incremented.RowsAffected,
			)
		}

		// 返回值里带 Size：toFile 从关联的 Entity 读取大小。
		record.Entity = entity
		copied = record
		return nil
	})
	if err != nil {
		return File{}, err
	}
	return toFile(copied), nil
}

// Rename 修改 ownerID 用户名下节点（普通文件或目录）的显示名。
//
// 只更新 files.name 一列：Entity、引用计数、磁盘对象都不动，
// 所以改名后下载到的内容不变。
//
// 名字合法性在查询数据库之前校验；同目录重名由
// (owner_id, parent_id, name) 唯一索引拒绝，返回 ErrAlreadyExists。
// 根目录是整棵树的起点（唯一 parent_id 为 NULL 的节点），
// 名字由服务端固定为 "/"，不允许改名，返回 ErrRootDirectory。
func (s *Service) Rename(ctx context.Context, ownerID int, id, name string) (File, error) {
	if err := validateName(name); err != nil {
		return File{}, err
	}

	var renamed model.File
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		record, err := lockOwnedFile(ctx, tx, ownerID, id)
		if err != nil {
			return err
		}
		// 目录和文件都能改名，唯独根目录不行。
		if record.ParentID == nil {
			return ErrRootDirectory
		}

		// 名字没变时 MySQL 的 RowsAffected 是 0（值没有变化），
		// 这里直接当作成功，避免把“改成同一个名字”误判为失败。
		if record.Name != name {
			updated := tx.WithContext(ctx).
				Model(&model.File{}).
				Where("id = ? AND owner_id = ?", record.ID, ownerID).
				UpdateColumn("name", name)
			if updated.Error != nil {
				if errors.Is(updated.Error, gorm.ErrDuplicatedKey) {
					return ErrAlreadyExists
				}
				return fmt.Errorf("rename file: %w", updated.Error)
			}
			if updated.RowsAffected != 1 {
				return fmt.Errorf(
					"rename file %s: expected 1 row affected, got %d",
					id, updated.RowsAffected,
				)
			}
			record.Name = name
		}

		// 在事务里读 Entity：此时 File 行锁还在，别的删除事务无法把它清理掉。
		if err := attachEntity(ctx, tx, record); err != nil {
			return err
		}

		renamed = *record
		return nil
	})
	if err != nil {
		return File{}, err
	}

	return toFile(renamed), nil
}

// Move 把 ownerID 用户名下的节点（文件或目录）移动到 parentID 目录下。
//
// parentID 为空表示当前用户的根目录。
//
// 只更新 files.parent_id：磁盘内容、Entity 和引用计数都不动，
// 所以移动后下载到的内容不变。旧的父目录列表里不再出现这个节点，
// 新的父目录列表里能看到它。
//
// 目标目录必须属于当前用户、而且真的是目录。
// 目录不能移动到自身或自己的后代，否则目录树会成环，返回 ErrInvalidMove。
// 目标目录已有同名节点时由唯一索引拒绝，返回 ErrAlreadyExists。
// 用户根目录是整棵树的起点，不能移动，返回 ErrRootDirectory。
func (s *Service) Move(ctx context.Context, ownerID int, id, parentID string) (File, error) {
	var moved model.File

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		record, err := lockOwnedFile(ctx, tx, ownerID, id)
		if err != nil {
			return err
		}
		// 根目录没有父目录，移动它等于换掉整棵树的起点，直接拒绝。
		if record.ParentID == nil {
			return ErrRootDirectory
		}

		// 目标目录必须在事务内再核对一次，防止校验之后被删除或换到别人名下。
		target, err := queryDirectory(ctx, tx, ownerID, parentID)
		if err != nil {
			return err
		}

		// 只有目录可能把“自己”装进子树；文件没有子节点，不需要检查。
		if record.Kind == model.KindDirectory {
			cyclic, err := isDescendant(ctx, tx, record.ID, target.ID)
			if err != nil {
				return err
			}
			if cyclic {
				return ErrInvalidMove
			}
		}

		// 已经在目标目录里：幂等成功。这样也不用担心 MySQL 的 RowsAffected = 0。
		if record.ParentID != nil && *record.ParentID == target.ID {
			if err := attachEntity(ctx, tx, record); err != nil {
				return err
			}
			moved = *record
			return nil
		}

		updated := tx.WithContext(ctx).
			Model(&model.File{}).
			Where("id = ? AND owner_id = ?", record.ID, ownerID).
			UpdateColumn("parent_id", target.ID)
		if updated.Error != nil {
			if errors.Is(updated.Error, gorm.ErrDuplicatedKey) {
				return ErrAlreadyExists
			}
			return fmt.Errorf("move file: %w", updated.Error)
		}
		if updated.RowsAffected != 1 {
			return fmt.Errorf(
				"move file %s: expected 1 row affected, got %d",
				id, updated.RowsAffected,
			)
		}

		parentID := target.ID
		record.ParentID = &parentID
		if err := attachEntity(ctx, tx, record); err != nil {
			return err
		}
		moved = *record
		return nil
	})
	if err != nil {
		return File{}, err
	}

	return toFile(moved), nil
}

// attachEntity 在事务里补上节点的 Entity，让返回体能带 size。
// 目录没有 Entity，保持 nil（size 为 0）。
func attachEntity(ctx context.Context, tx *gorm.DB, record *model.File) error {
	if record.EntityID == nil {
		return nil
	}
	var entity model.Entity
	if err := tx.WithContext(ctx).First(&entity, *record.EntityID).Error; err != nil {
		return fmt.Errorf("query entity: %w", err)
	}
	record.Entity = &entity
	return nil
}

// isDescendant 判断 nodeID 是不是 ancestorID 本身或它的后代。
//
// 从 nodeID 沿着 parent_id 往上走：走到 ancestorID 说明成环。
// 加深度上限是为了让异常数据（已经存在环）报错退出，而不是死循环。
func isDescendant(ctx context.Context, db *gorm.DB, ancestorID, nodeID string) (bool, error) {
	const maxDepth = 1000

	current := nodeID
	for depth := 0; depth < maxDepth; depth++ {
		if current == ancestorID {
			return true, nil
		}

		var node model.File
		err := db.WithContext(ctx).
			Select("id", "parent_id").
			First(&node, "id = ?", current).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return false, nil
			}
			return false, fmt.Errorf("walk directory tree: %w", err)
		}
		if node.ParentID == nil {
			return false, nil
		}
		current = *node.ParentID
	}

	return false, fmt.Errorf("directory tree is too deep or contains a cycle at %s", nodeID)
}

// CreateDirectory 在 parentID（为空表示根目录）下创建一个空目录。
//
// 目录只是 files 表里的一行：不写磁盘、不创建 Entity。
func (s *Service) CreateDirectory(ctx context.Context, ownerID int, parentID, name string) (File, error) {
	if err := validateName(name); err != nil {
		return File{}, err
	}

	parent, err := s.requireDirectory(ctx, ownerID, parentID)
	if err != nil {
		return File{}, err
	}

	id, err := idgen.New()
	if err != nil {
		return File{}, err
	}

	record := model.File{
		ID:       id,
		Name:     name,
		Kind:     model.KindDirectory,
		ParentID: &parent.ID,
		OwnerID:  ownerID,
	}
	if err := s.db.WithContext(ctx).Create(&record).Error; err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return File{}, ErrAlreadyExists
		}
		return File{}, fmt.Errorf("create directory: %w", err)
	}

	return toFile(record), nil
}

// ListChildren 返回 parentID 目录下的直接子节点，不递归。
//
// page 从 1 开始，pageSize 会被限制在 1～100。
func (s *Service) ListChildren(ctx context.Context, ownerID int, parentID string, page, pageSize int) (ListResult, error) {
	parent, err := s.requireDirectory(ctx, ownerID, parentID)
	if err != nil {
		return ListResult{}, err
	}

	page, pageSize = normalizePagination(page, pageSize)
	const where = "owner_id = ? AND parent_id = ?"

	var total int64
	if err := s.db.WithContext(ctx).
		Model(&model.File{}).
		Where(where, ownerID, parent.ID).
		Count(&total).Error; err != nil {
		return ListResult{}, fmt.Errorf("count directory children: %w", err)
	}

	var records []model.File
	if err := s.db.WithContext(ctx).
		Preload("Entity").
		Where(where, ownerID, parent.ID).
		Order("name ASC").
		Order("id ASC").
		Limit(pageSize).
		Offset((page - 1) * pageSize).
		Find(&records).Error; err != nil {
		return ListResult{}, fmt.Errorf("query directory children: %w", err)
	}

	items := make([]File, 0, len(records))
	for _, record := range records {
		items = append(items, toFile(record))
	}

	return ListResult{
		Items:    items,
		Total:    total,
		Page:     page,
		PageSize: pageSize,
	}, nil
}

// Open 只打开 ownerID 用户自己的普通文件。
//
// “ID 存在”不代表“当前用户能下载”：查询必须同时匹配文件 ID 和 owner_id。
// 别人的文件与不存在的文件都返回 ErrNotFound，避免泄露文件是否存在。
// 目录没有内容，明确返回 ErrIsDirectory，而不是让上层去读空关联。
func (s *Service) Open(ctx context.Context, ownerID int, id string) (File, io.ReadCloser, error) {
	var record model.File
	err := s.db.WithContext(ctx).
		Preload("Entity").
		Where("id = ? AND owner_id = ?", id, ownerID).
		First(&record).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return File{}, nil, ErrNotFound
		}
		return File{}, nil, fmt.Errorf("query file: %w", err)
	}
	if record.Kind != model.KindFile {
		return File{}, nil, ErrIsDirectory
	}

	entity := record.Entity
	if entity == nil {
		return File{}, nil, fmt.Errorf("file %s has no entity", id)
	}

	// 按内容自己的 StoragePolicyID 找位置，不看用户当前默认策略：
	// 服务端把默认位置从 A 调到 B 之后，A 里的旧内容仍然必须从 A 读。
	store, err := s.stores.Get(entity.StoragePolicyID)
	if err != nil {
		return File{}, nil, fmt.Errorf(
			"resolve storage policy %d of entity %d: %w",
			entity.StoragePolicyID, entity.ID, err,
		)
	}

	f, err := store.Open(ctx, entity.StorageKey)
	if err != nil {
		// 数据库有记录但磁盘文件没了：对外也当作“不存在”。
		if errors.Is(err, fs.ErrNotExist) {
			return File{}, nil, ErrNotFound
		}
		return File{}, nil, fmt.Errorf("open content: %w", err)
	}

	return toFile(record), f, nil
}

// Delete 删除 ownerID 用户自己的节点。
//
// 普通文件：在事务里删 File、扣 Entity 引用，提交后再尝试清理磁盘。
// 空目录：只删 files 这一行，不碰 Entity 和磁盘。
// 非空目录：返回 ErrDirectoryNotEmpty，要求客户端先清空；
// 这一阶段不做递归删除，避免留下半棵树。
// 用户根目录不能删除：它是整棵树的起点，返回 ErrRootDirectory。
//
// 文件删除分两段：先在事务里只改逻辑关系，提交成功后再尝试清理磁盘。
//
//	BEGIN
//	  SELECT files   ... FOR UPDATE   (id + owner_id)
//	  SELECT entities ... FOR UPDATE
//	  DELETE files WHERE id = ...
//	  UPDATE entities SET ref_count = ref_count - 1 WHERE id = ... AND ref_count > 0
//	  ref_count == 0 → status = pending_delete
//	COMMIT
//	  → 立即尝试一次物理清理；失败只记日志，不回滚已经提交的逻辑删除
//
// 为什么先锁 File 再锁 Entity：这是所有事务统一的加锁顺序
// （Copy 也遵守：先锁源 File、再锁 Entity），
// 否则两个事务可能一个先锁 File、另一个先锁 Entity，互相等待成环。
//
// 为什么不在事务里删磁盘：磁盘操作可能很慢，未来换成 S3 更慢。
// 事务只保护“引用计数 + 逻辑记录”这一小段，物理清理交给 cleanup.go。
//
// 空目录检查也在同一个事务里：父目录行已经被 FOR UPDATE 锁住，
// 并发的上传会等这个事务结束，不会出现“检查时是空的、删完又多出孩子”的情况。
func (s *Service) Delete(ctx context.Context, ownerID int, id string) error {
	var pendingEntityID int64

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		record, err := lockOwnedFile(ctx, tx, ownerID, id)
		if err != nil {
			return err
		}
		// 根目录同样 parent_id 为 NULL，必须在按目录处理之前拦住。
		if record.ParentID == nil {
			return ErrRootDirectory
		}

		// 目录只允许删除空的：先数直接子节点，非空就拒绝。
		if record.Kind == model.KindDirectory {
			var children int64
			if err := tx.WithContext(ctx).
				Model(&model.File{}).
				Where("owner_id = ? AND parent_id = ?", ownerID, record.ID).
				Count(&children).Error; err != nil {
				return fmt.Errorf("count directory children: %w", err)
			}
			if children > 0 {
				return ErrDirectoryNotEmpty
			}
			return deleteFileRow(ctx, tx, ownerID, record.ID)
		}

		if record.EntityID == nil {
			return fmt.Errorf("file %s has no entity", id)
		}

		entity, err := lockEntity(ctx, tx, *record.EntityID)
		if err != nil {
			return err
		}
		// 到这里 File 和 Entity 都被锁住了，entity.RefCount 是事务内的最新值，
		// 不再是事务外预读的旧值。
		if entity.RefCount <= 0 {
			return fmt.Errorf(
				"entity %d has ref_count %d while file %s still references it",
				entity.ID, entity.RefCount, id,
			)
		}

		// 先删 File 行。RowsAffected 必须正好是 1，否则说明这一行已经被别的事务删掉了，
		// 继续扣引用就会重复计数，所以直接回滚。
		if err := deleteFileRow(ctx, tx, ownerID, record.ID); err != nil {
			return err
		}

		// 用带 ref_count > 0 条件的原子自减，防止重复扣引用。
		decremented := tx.WithContext(ctx).
			Model(&model.Entity{}).
			Where("id = ? AND ref_count > 0", entity.ID).
			UpdateColumn("ref_count", gorm.Expr("ref_count - 1"))
		if decremented.Error != nil {
			return fmt.Errorf("decrement ref count: %w", decremented.Error)
		}
		if decremented.RowsAffected != 1 {
			return fmt.Errorf(
				"decrement ref count of entity %d: expected 1 row affected, got %d",
				entity.ID, decremented.RowsAffected,
			)
		}

		// 还有其他 File 引用这份内容：保持 active，不碰磁盘。
		if entity.RefCount > 1 {
			return nil
		}

		// 最后一个引用也删掉了。这里不删 Entity，只标记 pending_delete，
		// 交给 cleanup.go 在事务提交后删物理对象；这样两个崩溃点都能收敛。
		marked := tx.WithContext(ctx).
			Model(&model.Entity{}).
			Where("id = ? AND ref_count = 0 AND status = ?", entity.ID, model.EntityStatusActive).
			UpdateColumn("status", model.EntityStatusPendingDelete)
		if marked.Error != nil {
			return fmt.Errorf("mark entity pending deletion: %w", marked.Error)
		}
		if marked.RowsAffected != 1 {
			return fmt.Errorf(
				"mark entity %d pending deletion: expected 1 row affected, got %d",
				entity.ID, marked.RowsAffected,
			)
		}
		pendingEntityID = entity.ID
		return nil
	})
	if err != nil {
		return err
	}

	// 事务已经提交，逻辑删除完成。物理清理失败不能让接口返回错误：
	// 否则客户端会以为没删掉，而数据库里其实已经删了。
	if pendingEntityID != 0 {
		if cleanupErr := s.cleanupEntity(ctx, pendingEntityID); cleanupErr != nil {
			s.logger.Warn("physical cleanup deferred",
				"entity_id", pendingEntityID,
				"error", cleanupErr,
			)
		}
	}
	return nil
}

// deleteFileRow 删除一行 files，并确认真的删掉了一行。
//
// 文件和空目录共用这段逻辑：两者在 files 表里都只是一行。
func deleteFileRow(ctx context.Context, tx *gorm.DB, ownerID int, id string) error {
	deleted := tx.WithContext(ctx).
		Where("id = ? AND owner_id = ?", id, ownerID).
		Delete(&model.File{})
	if deleted.Error != nil {
		return fmt.Errorf("delete file: %w", deleted.Error)
	}
	if deleted.RowsAffected != 1 {
		return fmt.Errorf(
			"delete file %s: expected 1 row affected, got %d",
			id, deleted.RowsAffected,
		)
	}
	return nil
}

// lockOwnedFile 在事务里按 id + owner_id 锁住一行 files。
//
// SELECT ... FOR UPDATE 会一直持有到事务提交或回滚。重复删除同一个文件时，
// 第二个事务会在这里等第一个提交，然后看到记录已经不存在（ErrNotFound），
// 不会重复扣 Entity 的引用数。
func lockOwnedFile(ctx context.Context, tx *gorm.DB, ownerID int, id string) (*model.File, error) {
	var record model.File
	err := tx.WithContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ? AND owner_id = ?", id, ownerID).
		First(&record).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("lock file: %w", err)
	}
	return &record, nil
}

// lockEntity 在事务里锁住一行 entities，并读出最新的 ref_count 和 status。
//
// 调用者必须已经持有对应的 File 行锁，保证所有事务的加锁顺序都是 File → Entity。
func lockEntity(ctx context.Context, tx *gorm.DB, entityID int64) (*model.Entity, error) {
	var entity model.Entity
	err := tx.WithContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		First(&entity, entityID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// File 指向不存在的 Entity：数据库已经不一致，不能继续扣引用。
			return nil, fmt.Errorf("entity %d not found for file reference", entityID)
		}
		return nil, fmt.Errorf("lock entity: %w", err)
	}
	return &entity, nil
}

// requireDirectory 校验 parentID 是 ownerID 自己的目录。
//
// parentID 为空时先解析成当前用户的根目录 ID。
//
// “不存在”和“属于别人”都返回 ErrNotFound：不能通过错误类型区分，
// 否则客户端可以拿它探测别人的目录 ID 是否存在。
func (s *Service) requireDirectory(ctx context.Context, ownerID int, parentID string) (*model.File, error) {
	return queryDirectory(ctx, s.db, ownerID, parentID)
}

// queryDirectory 是 requireDirectory 的底层实现，传入 tx 就能在事务里复用。
func queryDirectory(ctx context.Context, db *gorm.DB, ownerID int, parentID string) (*model.File, error) {
	if parentID == "" {
		resolved, err := rootDirectoryID(ctx, db, ownerID)
		if err != nil {
			return nil, err
		}
		parentID = resolved
	}

	var parent model.File
	err := db.WithContext(ctx).
		Where("id = ? AND owner_id = ?", parentID, ownerID).
		First(&parent).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query parent directory: %w", err)
	}
	if parent.Kind != model.KindDirectory {
		return nil, ErrNotDirectory
	}
	return &parent, nil
}

// rootDirectoryID 取出用户的根目录 ID。
func rootDirectoryID(ctx context.Context, db *gorm.DB, ownerID int) (string, error) {
	var user model.User
	err := db.WithContext(ctx).
		Select("id", "root_file_id").
		First(&user, ownerID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", fmt.Errorf("user %d not found", ownerID)
		}
		return "", fmt.Errorf("query user root directory: %w", err)
	}
	if user.RootFileID == nil || *user.RootFileID == "" {
		return "", fmt.Errorf("user %d has no root directory", ownerID)
	}
	return *user.RootFileID, nil
}

// validateName 拒绝不能安全落库的名字。
//
// 不允许路径分隔符：这个名字只用于展示，绝不能参与拼磁盘路径。
// 根目录 "/" 由服务端自己创建，不会走这里。
func validateName(name string) error {
	if name == "" || name == "." || name == ".." {
		return ErrInvalidName
	}
	if !utf8.ValidString(name) || strings.ContainsRune(name, 0) {
		return ErrInvalidName
	}
	if strings.ContainsAny(name, `/\`) {
		return ErrInvalidName
	}
	if utf8.RuneCountInString(name) > maxNameRunes {
		return ErrInvalidName
	}
	return nil
}

// normalizePagination 把越界的页码收敛到合法范围，而不是报错。
func normalizePagination(page, pageSize int) (int, int) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	return page, pageSize
}

// toFile 把数据库记录转成 HTTP 层要展示的结构。
// 目录没有 Entity，Size 保持 0。
func toFile(record model.File) File {
	var size int64
	if record.Entity != nil {
		size = record.Entity.Size
	}
	return File{
		ID:       record.ID,
		Name:     record.Name,
		Size:     size,
		Kind:     record.Kind,
		ParentID: record.ParentID,
	}
}
