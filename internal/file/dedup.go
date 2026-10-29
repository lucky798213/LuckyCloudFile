package file

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"luckyCloudFile/internal/model"
)

// contentPublishAttempts 是发布事务因唯一键冲突或数据库死锁时的重试上限。
//
// 冲突不代表“已经存在”，而是说明另一个事务先提交了映射；死锁说明两个
// 事务发生了锁竞争。两者都由数据库回滚了整笔事务，重开后重新读取获胜
// 实体再决定复用还是继续插入；超过上限仍失败就按普通错误返回。
const contentPublishAttempts = 3

// errContentKeyRace 是内部信号：ContentKey 唯一键被并发事务抢先写入。
// 调用方必须重试整个发布事务，不能把它当成普通的“文件已存在”。
var errContentKeyRace = errors.New("content key was created concurrently")

// lookupContentKey 在当前用户、当前策略作用域内读取内容映射。
//
// 这里刻意使用快照读而不是 FOR UPDATE：如果先锁 ContentKey 再锁 Entity，
// 会和“锁 Entity → 锁 ContentKey”的删除/校验链路反向加锁。
// 这个结果只是候选线索；真正能否复用由 lockReusableContent 在锁内复核。
func lookupContentKey(
	tx *gorm.DB,
	ownerID, policyID int,
	digest string,
	size int64,
) (*model.ContentKey, error) {
	var key model.ContentKey
	err := tx.Where(
		"owner_id = ? AND storage_policy_id = ? AND sha256 = ? AND size = ?",
		ownerID, policyID, digest, size,
	).First(&key).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("query content key: %w", err)
	}
	return &key, nil
}

// lockContentKey 按主键锁住一条内容映射。
//
// 调用方必须已经持有该映射指向的 Entity 行锁，保证内容发布、校验回填和
// 删除都按 Entity → ContentKey 的顺序加锁，不会形成等待环。
func lockContentKey(tx *gorm.DB, id int64) (*model.ContentKey, error) {
	var key model.ContentKey
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ?", id).First(&key).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("lock content key %d: %w", id, err)
	}
	return &key, nil
}

// lockReusableContent 查找并锁定当前作用域下可复用的内容。
//
// 先锁候选 Entity，再锁 ContentKey 并复核映射没有被并发删除或改写。
// 这既避免了“ContentKey → Entity”和“Entity → ContentKey”的锁顺序倒置，
// 也保证返回的 Entity 在事务剩余时间里不会被删除或改写。
//
// 返回 (nil, nil) 表示未命中：没有映射、映射已过期，或映射在加锁期间
// 被并发改写。调用方按未命中继续，唯一键竞争仍由 insertContentKey 兜底。
func lockReusableContent(
	tx *gorm.DB,
	ownerID, policyID int,
	digest string,
	size int64,
) (*model.Entity, error) {
	hint, err := lookupContentKey(tx, ownerID, policyID, digest, size)
	if err != nil {
		return nil, err
	}
	if hint == nil {
		return nil, nil
	}

	entity, err := lockReusableEntity(tx, hint)
	if err != nil {
		return nil, err
	}
	if entity == nil {
		// 映射指向的内容已经不可引用。锁住映射后确认它仍指向同一份过期内容，
		// 再删掉它，让后续上传按未命中重新登记。
		if err := dropStaleContentKey(tx, hint); err != nil {
			return nil, err
		}
		return nil, nil
	}

	current, err := lockContentKey(tx, hint.ID)
	if err != nil {
		return nil, err
	}
	if current == nil || current.EntityID != entity.ID {
		// 映射在提示读和 Entity 锁之间被别的发布事务改写。
		// 不复用旧候选；调用方继续登记时会由唯一键触发有界重试。
		return nil, nil
	}
	return entity, nil
}

// dropStaleContentKey 删除一条仍指向同一份过期内容的映射。
//
// 如果映射已经被并发删除或改写，这里什么都不做：那条新映射属于另一个
// 发布事务，不能按旧候选把它删掉。
func dropStaleContentKey(tx *gorm.DB, stale *model.ContentKey) error {
	current, err := lockContentKey(tx, stale.ID)
	if err != nil {
		return err
	}
	if current == nil || current.EntityID != stale.EntityID {
		return nil
	}
	if err := tx.Where("id = ?", current.ID).Delete(&model.ContentKey{}).Error; err != nil {
		return fmt.Errorf("delete stale content key: %w", err)
	}
	return nil
}

// lockReusableEntity 锁住映射指向的 Entity，并确认它仍然可以被引用。
//
// 返回 (nil, nil) 表示映射已经过期（Entity 不存在或不再是 active），
// 调用方应当删除这条映射，再按“未命中”继续。
func lockReusableEntity(tx *gorm.DB, key *model.ContentKey) (*model.Entity, error) {
	var entity model.Entity
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		First(&entity, key.EntityID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("lock content entity: %w", err)
	}
	if entity.Status != model.EntityStatusActive || entity.RefCount <= 0 {
		return nil, nil
	}
	if entity.Size != key.Size ||
		entity.StoragePolicyID != key.StoragePolicyID ||
		entity.ContentSHA256 == nil ||
		*entity.ContentSHA256 != key.SHA256 {
		return nil, fmt.Errorf(
			"content key %d points to inconsistent entity %d", key.ID, entity.ID,
		)
	}
	return &entity, nil
}

// reuseExistingContent 尝试创建一条指向已有内容的 File。
//
// 命中时会创建 File 并把已有 Entity 的引用数加一。返回 true 表示复用了
// 已有内容，调用方不能再创建新的 active Entity。
//
// 映射过期（Entity 不存在或已不是 active）时删除这条映射并返回 false。
// 唯一键竞争由 insertContentKey 负责；这里不吞掉任何数据库错误。
func reuseExistingContent(
	tx *gorm.DB,
	ownerID int,
	parentID, fileID, name string,
	policyID int,
	digest string,
	size int64,
) (bool, error) {
	entity, err := lockReusableContent(tx, ownerID, policyID, digest, size)
	if err != nil {
		return false, err
	}
	if entity == nil {
		return false, nil
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
			return false, ErrAlreadyExists
		}
		return false, fmt.Errorf("create reused file: %w", err)
	}

	incremented := tx.Model(&model.Entity{}).Where("id = ? AND status = ? AND ref_count > 0",
		entity.ID, model.EntityStatusActive).
		UpdateColumn("ref_count", gorm.Expr("ref_count + 1"))
	if incremented.Error != nil {
		return false, fmt.Errorf("increment reused ref count: %w", incremented.Error)
	}
	if incremented.RowsAffected != 1 {
		return false, fmt.Errorf(
			"%w: entity %d can no longer be referenced",
			ErrUploadSessionConflict, entity.ID,
		)
	}
	return true, nil
}

// insertContentKey 登记“这份内容应该复用哪个 Entity”。
//
// 唯一键冲突返回 errContentKeyRace，由调用方重开事务、读取获胜实体后重试；
// 其他数据库错误原样返回，不能包装成“已经存在”。
func insertContentKey(
	tx *gorm.DB,
	ownerID, policyID int,
	digest string,
	size, entityID int64,
) error {
	key := model.ContentKey{
		OwnerID:         ownerID,
		StoragePolicyID: policyID,
		SHA256:          digest,
		Size:            size,
		EntityID:        entityID,
	}
	if err := tx.Create(&key).Error; err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return errContentKeyRace
		}
		return fmt.Errorf("create content key: %w", err)
	}
	return nil
}

// insertDiscardableEntity 为本次上传多出来的物理对象登记一条 pending_delete
// Entity，让现有 GC 在事务提交后负责删除。
//
// 直接上传在命中去重时不会创建正式 Entity；如果没有这条记录，物理对象就
// 只存在于存储里，失败后没有任何数据库线索可以重试清理。
func insertDiscardableEntity(
	tx *gorm.DB,
	storageKey string,
	size int64,
	policyID int,
	digest string,
) (int64, error) {
	entity := model.Entity{
		StorageKey:      storageKey,
		Size:            size,
		ContentSHA256:   &digest,
		StoragePolicyID: policyID,
		RefCount:        0,
		Status:          model.EntityStatusPendingDelete,
	}
	if err := tx.Create(&entity).Error; err != nil {
		return 0, fmt.Errorf("create discardable entity: %w", err)
	}
	return entity.ID, nil
}

// cleanPublishedLeftover 在发布事务提交后尽力删除多余物理对象。
//
// 删除失败不回滚已经成功的发布：Entity 仍是 pending_delete，下一轮 GC
// 会继续处理。使用独立 context，避免请求取消后清理被一起取消。
func (s *Service) cleanPublishedLeftover(ctx context.Context, entityID int64) {
	if entityID == 0 {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), orphanCleanupTimeout,
	)
	defer cancel()

	if err := s.cleanupEntity(cleanupCtx, entityID); err != nil {
		s.logger.Warn("deferred cleanup of deduplicated content",
			"entity_id", entityID,
			"error", err,
		)
	}
}
