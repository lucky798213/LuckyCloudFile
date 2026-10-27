// 这个文件负责“延迟物理清理”：把已经没有人引用的磁盘内容删掉。
//
// Delete 提交事务时只把 Entity 标成 pending_delete，真正删磁盘放在这里。
// 这样数据库事务不用等磁盘（以后接 S3 时尤其重要），代价是允许出现
// “记录已提交、对象暂时还在”的中间状态。下面两个崩溃点都能靠重复扫描收敛：
//
//	标记已提交，磁盘还没删 → 下次扫描到标记，继续删
//	磁盘已删，Entity 还没删 → 再删发现对象不存在，视为成功，再清记录
//
// 清理逻辑本身不直接把 active 内容标成 pending_delete：它只处理已经处于
// 该状态的 Entity。上传会话失效（到期，或服务重启后被启动恢复作废）时，
// CleanupPending 会先走一遍到期扫描和终态恢复扫描，
// 由 handoffTerminatedSession 在确认写入退出后产生新的 pending_delete。
package file

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"luckyCloudFile/internal/model"
)

const (
	// DefaultCleanupBatchSize 是一条 SELECT 最多取多少条待清理实体。
	// 扫描会按 id 递增继续取，直到当前积压处理完。
	DefaultCleanupBatchSize = 100

	// DefaultCleanupInterval 是服务运行期间两次扫描之间的间隔。
	DefaultCleanupInterval = time.Minute
)

// CleanupResult 是一轮清理的统计，用来写日志和做测试断言。
type CleanupResult struct {
	Scanned  int // 检查过的 pending_delete 实体数
	Removed  int // 已经清掉的对象/记录数（对象本来就不存在也算）
	Deferred int // 暂时失败、保留记录等下一轮的数量
}

// CleanupPending 扫描并清理所有 pending_delete、ref_count=0 的 Entity。
//
// 每一轮按固定顺序做三件事：
//  1. 扫描到期仍未完成的会话，标记 expired，并尝试移交占位内容；
//  2. 恢复已经 canceled / expired、但占位 Entity 还是 uploading 的会话，
//     补上上次崩溃前没来得及完成的移交；
//  3. 清理所有 pending_delete 实体。
//
// 第 1、2 步都可能产生新的 pending_delete，所以必须放在清理之前。
// 它们不会删除已发布的内容：只有仍处于 uploading/0、没有 File 引用的
// 占位实体才会被移交。
//
// 每处理一个实体都会：
//  1. 在一个短事务里锁住 Entity，再次确认没有 File 引用；
//  2. 提交事务后才删物理对象（磁盘/S3 操作不能长期占着数据库事务）；
//  3. 成功后再用一个短事务删 Entity 记录。
//
// 失败不会中断整轮扫描：该实体保留 pending_delete，下一轮再试。
// 按 id 递增扫描，所以同一轮里失败的行不会反复取到同一个。
func (s *Service) CleanupPending(ctx context.Context, batchSize int) (CleanupResult, error) {
	if batchSize <= 0 {
		batchSize = DefaultCleanupBatchSize
	}

	var result CleanupResult

	// 第 1 步：先把已经到期的未完成会话标成 expired。
	// 到期扫描内部按条处理，单条失败不会中断整轮；这里的错误通常来自
	// 候选查询本身，记日志后仍然继续清理已经存在的 pending_delete。
	if err := s.ExpireUploadSessions(ctx, time.Now(), batchSize); err != nil {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		s.logger.Warn("scan expired upload sessions", "error", err)
	}

	// 第 2 步：补做已经 canceled / expired 但还没移交的占位内容。
	// 服务可能在“会话已经是终态、Entity 仍是 uploading/0”时崩溃；
	// 启动恢复也会在服务开始接受请求前把旧会话统一标成 expired，
	// 那些残留同样由这一步接手。
	s.resumeTerminatedSessions(ctx, batchSize)

	var cursor int64
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		var entities []model.Entity
		err := s.db.WithContext(ctx).
			Select("id", "storage_key", "storage_policy_id").
			Where("status = ? AND ref_count = 0 AND id > ?", model.EntityStatusPendingDelete, cursor).
			Order("id ASC").
			Limit(batchSize).
			Find(&entities).Error
		if err != nil {
			return result, fmt.Errorf("query entities pending deletion: %w", err)
		}
		if len(entities) == 0 {
			return result, nil
		}

		for _, entity := range entities {
			cursor = entity.ID
			result.Scanned++

			if err := s.cleanupEntity(ctx, entity.ID); err != nil {
				result.Deferred++
				s.logger.Warn("defer content cleanup",
					"entity_id", entity.ID,
					"storage_key", entity.StorageKey,
					"storage_policy_id", entity.StoragePolicyID,
					"error", err,
				)
				continue
			}
			result.Removed++
		}
	}
}

// CleanupLoop 立即扫描一次，之后每隔 interval 再扫描一次，直到 ctx 被取消。
//
// 这是 P7 的进程内循环，保留给直接调用和旧测试；应用启动现在改为确保一条
// storage_cleanup_v1 持久化任务，由任务调度器调用 CleanupPending，
// 这样清理失败和重启后的责任都记录在数据库里。
func (s *Service) CleanupLoop(ctx context.Context, interval time.Duration, batchSize int) {
	if interval <= 0 {
		interval = DefaultCleanupInterval
	}

	s.runCleanupOnce(ctx, batchSize)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runCleanupOnce(ctx, batchSize)
		}
	}
}

// runCleanupOnce 扫描一次并把失败记录到日志，不向调用方返回错误：
// 后台任务失败不应该让进程退出，下一轮会自动重试。
func (s *Service) runCleanupOnce(ctx context.Context, batchSize int) {
	result, err := s.CleanupPending(ctx, batchSize)
	if err != nil {
		if ctx.Err() != nil {
			// 关闭过程中主动取消，不是故障。
			return
		}
		s.logger.Error("scan pending content", "error", err)
		return
	}
	if result.Scanned > 0 {
		s.logger.Info("content cleanup finished",
			"scanned", result.Scanned,
			"removed", result.Removed,
			"deferred", result.Deferred,
		)
	}
}

// cleanupEntity 清理一个待删除的 Entity。
//
// 三步刻意分开：
//  1. 短事务里锁住 Entity、确认状态并检查引用；
//  2. 事务外删物理对象；
//  3. 再一个短事务删 Entity 记录。
//
// 如果第 2 步成功、第 3 步之前崩溃，下一轮会看到对象不存在，直接视为删除成功。
func (s *Service) cleanupEntity(ctx context.Context, entityID int64) error {
	policyID, storageKey, err := s.prepareCleanup(ctx, entityID)
	if err != nil {
		return err
	}
	if storageKey == "" {
		// 已经不是 pending_delete + ref_count=0：可能被别的扫描清理掉了，
		// 也可能被人工改回了 active。这里什么都不做。
		return nil
	}

	// 去内容实际写入的位置删除。策略缺失就保留记录等下一轮，
	// 绝不能换成别的 Store 去删（可能删到同 key 的另一份内容）。
	store, err := s.stores.Get(policyID)
	if err != nil {
		return fmt.Errorf("resolve storage policy %d: %w", policyID, err)
	}

	// 物理删除必须在事务之外。对象已经不存在（上次删到一半崩溃）也算成功。
	if err := store.Remove(ctx, storageKey); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove object %q from policy %d: %w", storageKey, policyID, err)
	}

	return s.deleteCleanedEntity(ctx, entityID)
}

// prepareCleanup 在一个短事务里确认 Entity 可以清理，并返回它记录的位置：
// 策略 ID + storage key。两者必须一起返回，清理才能去正确的地方删。
//
// 锁顺序：清理只锁 Entity，不锁 File 行，因此不会和 Delete 的
// “File → Entity” 顺序成环。这里对 files 的 COUNT 是普通快照读：
// pending_delete 实体不允许新增引用，所以计数只会因为 File 被删而变小。
func (s *Service) prepareCleanup(ctx context.Context, entityID int64) (int, string, error) {
	var policyID int
	var storageKey string

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var entity model.Entity
		err := tx.WithContext(ctx).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND status = ? AND ref_count = 0", entityID, model.EntityStatusPendingDelete).
			First(&entity).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// 已经清理过，或者状态已经变化。交给调用方按成功处理。
				return nil
			}
			return fmt.Errorf("lock pending entity %d: %w", entityID, err)
		}

		var references int64
		if err := tx.WithContext(ctx).
			Model(&model.File{}).
			Where("entity_id = ?", entityID).
			Count(&references).Error; err != nil {
			return fmt.Errorf("count references to entity %d: %w", entityID, err)
		}
		if references > 0 {
			// 正常流程不会出现：引用的 File 和 pending_delete 状态是互斥的。
			// 但这里再检查一次，宁可保留对象，也不能删掉还在用的内容。
			return fmt.Errorf(
				"entity %d is pending deletion but still has %d file reference(s)",
				entityID, references,
			)
		}

		policyID = entity.StoragePolicyID
		storageKey = entity.StorageKey
		return nil
	})
	if err != nil {
		return 0, "", err
	}
	return policyID, storageKey, nil
}

// deleteCleanedEntity 删除已经完成物理清理的 Entity 记录。
//
// 条件里再写一遍 status / ref_count，并加 NOT EXISTS：
// 即使这一行在磁盘删除期间被改动，也不会误删仍可能被引用的内容。
// RowsAffected == 0 表示别的扫描已经删掉了，按成功处理。
func (s *Service) deleteCleanedEntity(ctx context.Context, entityID int64) error {
	result := s.db.WithContext(ctx).
		Where("id = ? AND status = ? AND ref_count = 0", entityID, model.EntityStatusPendingDelete).
		Where("NOT EXISTS (SELECT 1 FROM files WHERE files.entity_id = ?)", entityID).
		Delete(&model.Entity{})
	if result.Error != nil {
		return fmt.Errorf("delete cleaned entity %d: %w", entityID, result.Error)
	}
	return nil
}
