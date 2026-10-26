package file

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"luckyCloudFile/internal/model"
)

// terminatedUploadStatuses 是已经结束、但可能还留着占位内容的会话状态。
//
// 取消和过期共用同一条清理路径：两者都只说明“这次上传不会再发布了”，
// 清理动作完全一样。不要把 expired 再改写成 canceled，否则会丢失结束原因。
var terminatedUploadStatuses = []string{
	model.UploadStatusCanceled,
	model.UploadStatusExpired,
}

// unfinishedUploadStatuses 是仍可能继续、因此允许被“结束”的状态。
// 到期扫描和启动恢复都只处理这三种；completed 已经发布文件，必须跳过；
// canceled / expired 已经是终态。
var unfinishedUploadStatuses = []string{
	model.UploadStatusPending,
	model.UploadStatusReceiving,
	model.UploadStatusReady,
}

// handoffTerminatedSession 把 canceled / expired 会话的占位 Entity 移交给物理清理。
//
// 它会被四个地方调用：取消接口、到期扫描、内容写入退出路径、清理循环的恢复扫描。
// 调用前必须确认没有活跃写入；本方法也会再检查一次，避免误删正在写的内容。
//
// 只有 canceled / expired 两个终态才会被处理；其他状态一律不碰。
//
// 清理分两步：
//  1. 短事务里先锁 Session、再锁 Entity，确认实体仍是 uploading/0、
//     没有被任何 File 引用，然后标记 pending_delete；
//  2. 事务提交后复用 cleanupEntity 删除物理对象和 Entity 记录。
//
// 物理删除失败时保留 pending_delete，交给现有清理器重试；
// 绝不能先删除 Entity 记录，否则磁盘删除失败后就失去了追踪依据。
func (s *Service) handoffTerminatedSession(
	ctx context.Context,
	ownerID int,
	sessionID string,
) error {
	if s.uploads.isActive(sessionID) {
		// 写入还没退出：现在删内容会被重新写回来。等它的退出路径再移交。
		// 这里不能等待 done：一次阻塞的 Read 可能很久才返回，而到期扫描
		// 不应该被一个传输请求拖住。活跃登记表就是“写入是否已退出”的依据。
		return nil
	}

	handoffCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), orphanCleanupTimeout)
	defer cancel()

	var entityID int64
	err := s.db.WithContext(handoffCtx).Transaction(func(tx *gorm.DB) error {
		var session model.UploadSession
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND owner_id = ?", sessionID, ownerID).
			First(&session).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// 会话已经不在了，没有可移交的内容。
				return nil
			}
			return fmt.Errorf("lock terminated upload session: %w", err)
		}
		if !isTerminatedUploadStatus(session.Status) {
			// 会话不是 canceled / expired：不碰它的内容。
			// 这一步同时挡住“完成接口刚把会话变成 completed”的竞争：
			// 已经发布的结果不能被过期扫描删掉。
			return nil
		}

		// 会话已经进入终态，分片账本不会再有合法请求使用。
		// 物理清理可能失败，但账本不需要跟着重试。
		s.chunks.remove(sessionID)

		var entity model.Entity
		err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&entity, session.EntityID).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// 占位实体已经被清理过，重复取消/过期仍然成功。
				return nil
			}
			return fmt.Errorf("lock terminated upload entity: %w", err)
		}

		// 只处理仍未被引用的上传占位内容。active / pending_delete
		// 或引用数不为 0 的内容一律不碰。
		if entity.Status != model.EntityStatusUploading || entity.RefCount != 0 {
			return nil
		}

		var references int64
		if err := tx.WithContext(handoffCtx).
			Model(&model.File{}).
			Where("entity_id = ?", entity.ID).
			Count(&references).Error; err != nil {
			return fmt.Errorf("count references to terminated entity %d: %w", entity.ID, err)
		}
		if references > 0 {
			// 正常流程不会出现；宁可保留内容，也不能删掉仍被引用的对象。
			return nil
		}

		marked := tx.Model(&model.Entity{}).
			Where("id = ? AND status = ? AND ref_count = 0",
				entity.ID, model.EntityStatusUploading).
			UpdateColumn("status", model.EntityStatusPendingDelete)
		if marked.Error != nil {
			return fmt.Errorf("mark terminated entity pending deletion: %w", marked.Error)
		}
		if marked.RowsAffected != 1 {
			return fmt.Errorf(
				"mark terminated entity %d pending deletion: expected 1 row affected, got %d",
				entity.ID, marked.RowsAffected,
			)
		}

		entityID = entity.ID
		return nil
	})
	if err != nil {
		return err
	}
	if entityID == 0 {
		return nil
	}

	// 复用现有清理逻辑：物理删除失败会保留 pending_delete，后续扫描重试。
	return s.cleanupEntity(handoffCtx, entityID)
}

// resumeTerminatedSessions 让已经结束但还没完成清理的会话继续收尾。
//
// 服务可能在“会话已 canceled / expired、Entity 还是 uploading”时崩溃；
// 这轮扫描会重新移交，不需要等下一次上传请求。
//
// 只查仍需要处理的会话（Entity 还在 uploading / 0），
// 已经变成 pending_delete 的交给 CleanupPending 正常重试。
//
// 和到期扫描一样按 id 游标分页。单条移交失败只记录日志，游标照常前进：
// 不能让一个持续失败的故障条目占满第一批，否则后面的残留永远轮不到。
// 失败条目下一轮从 id 开头重新扫描时会再次被选中。
func (s *Service) resumeTerminatedSessions(ctx context.Context, batchSize int) {
	if batchSize <= 0 {
		batchSize = DefaultCleanupBatchSize
	}

	var cursor string
	for {
		if err := ctx.Err(); err != nil {
			return
		}

		var sessions []model.UploadSession
		err := s.db.WithContext(ctx).
			Model(&model.UploadSession{}).
			Joins("JOIN entities ON entities.id = upload_sessions.entity_id").
			Where("upload_sessions.status IN ? AND entities.status = ? AND entities.ref_count = 0 AND upload_sessions.id > ?",
				terminatedUploadStatuses, model.EntityStatusUploading, cursor).
			Order("upload_sessions.id ASC").
			Limit(batchSize).
			Find(&sessions).Error
		if err != nil {
			s.logger.Warn("scan terminated upload sessions", "error", err)
			return
		}
		if len(sessions) == 0 {
			return
		}

		for _, session := range sessions {
			if err := ctx.Err(); err != nil {
				return
			}

			// 先推进游标再移交：这一条失败也不能让它反复占住当前批次。
			cursor = session.ID

			if err := s.handoffTerminatedSession(ctx, session.OwnerID, session.ID); err != nil {
				s.logger.Warn("resume terminated upload session",
					"session_id", session.ID,
					"error", err,
				)
				continue
			}
		}
	}
}

// isTerminatedUploadStatus 判断会话是否已经结束、只剩清理工作。
func isTerminatedUploadStatus(status string) bool {
	return status == model.UploadStatusCanceled || status == model.UploadStatusExpired
}
