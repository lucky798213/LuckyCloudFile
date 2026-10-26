package file

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/upload"
)

// DefaultExpireBatchSize 是到期扫描一条 SELECT 最多取多少条候选会话。
// 和清理器一样按 id 递增继续取，直到当前积压处理完。
const DefaultExpireBatchSize = DefaultCleanupBatchSize

// ExpireUploadSessions 把已经超过 expires_at 的未完成上传会话标记为 expired。
//
// 它只处理“超时”这一种失效原因；服务重启造成的失效由
// InvalidateUnfinishedUploads 在启动时完成。两者最终都走同一条清理链：
// 会话处于 expired，resumeTerminatedSessions 再回收残留占位内容。
//
// now 由调用方显式传入，测试可以直接构造“已经到期”的时刻，
// 不需要真的等待一小时。生产路径由 CleanupPending 传 time.Now()。
//
// 扫描顺序：
//  1. 按 id 游标分页查询 pending/receiving/ready 且 expires_at <= now 的会话；
//  2. 每条会话单独开一个短事务：锁住会话，重新核对状态和到期时间，
//     确认仍然未完成后才改成 expired；提交事务；
//  3. 事务提交后通知上传控制器停止，再尝试把占位 Entity 移交给清理。
//
// 候选查询不是最终依据：会话可能在候选查询和加锁之间被完成、取消或续期，
// 所以事务内必须重新检查。与完成接口竞争时两者锁同一条 Session：
// 完成先提交 → 这里看到 completed 并跳过；过期先提交 → 完成接口不能再发布文件。
//
// 某一条处理失败不会中断整轮扫描：游标先推进到该条，后面仍会继续处理。
// 失败的那条下一轮会从 id 开头重新被选中并重试，因此不会被前面的失败饿死，
// 也不会在同一个游标位置反复占住第一批。
//
// 本方法不持有数据库事务等待上传停止或删除磁盘；这些动作都在事务提交之后，
// 由 handoffTerminatedSession 在活跃写入登记表确认写入已退出后再完成。
func (s *Service) ExpireUploadSessions(
	ctx context.Context,
	now time.Time,
	batchSize int,
) error {
	if batchSize <= 0 {
		batchSize = DefaultExpireBatchSize
	}

	var (
		cursor       string
		expiredCount int
	)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var candidates []model.UploadSession
		err := s.db.WithContext(ctx).
			Select("id", "owner_id").
			Where("status IN ? AND expires_at <= ? AND id > ?",
				unfinishedUploadStatuses, now, cursor).
			Order("id ASC").
			Limit(batchSize).
			Find(&candidates).Error
		if err != nil {
			return fmt.Errorf("query expired upload sessions: %w", err)
		}
		if len(candidates) == 0 {
			if expiredCount > 0 {
				s.logger.Info("expired upload sessions", "count", expiredCount)
			}
			return nil
		}

		for _, candidate := range candidates {
			if err := ctx.Err(); err != nil {
				return err
			}

			// 先推进游标再处理：这一条失败也不能让它反复占住当前批次，
			// 更不能因为它而跳过后面已经到期的会话。
			cursor = candidate.ID

			expired, expireErr := s.expireUploadSession(
				ctx, candidate.OwnerID, candidate.ID, now,
			)
			if expired {
				expiredCount++
			}
			if expireErr != nil {
				s.logger.Warn("expire upload session",
					"session_id", candidate.ID,
					"owner_id", candidate.OwnerID,
					"error", expireErr,
				)
				continue
			}
		}
	}
}

// expireUploadSession 处理一条到期候选。
//
// 返回 expired 表示本次是否真的把会话改成了 expired；返回 error 表示
// 状态落库或后续移交失败。已经提交的 expired 状态不会因为清理失败而回滚，
// 清理残留由 resumeTerminatedSessions / CleanupPending 继续重试。
func (s *Service) expireUploadSession(
	ctx context.Context,
	ownerID int,
	sessionID string,
	now time.Time,
) (bool, error) {
	expired := false

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var session model.UploadSession
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND owner_id = ?", sessionID, ownerID).
			First(&session).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// 候选查询之后会话被删掉了，跳过即可。
				return nil
			}
			return fmt.Errorf("lock upload session for expiry: %w", err)
		}

		// 候选查询只是缩小范围，事务内必须按最新提交的数据重新核对。
		// 这里直接复用状态规则：completed 已经发布文件，绝不能被改成
		// expired；canceled / expired 已经是终态，也不需要重复处理。
		if !upload.CanTransition(session.Status, model.UploadStatusExpired) {
			return nil
		}
		if session.ExpiresAt.After(now) {
			// 在候选查询之后被续期了：这次不能过期。
			return nil
		}

		updated := tx.Model(&model.UploadSession{}).
			Where("id = ? AND owner_id = ? AND status IN ? AND expires_at <= ?",
				session.ID, ownerID, unfinishedUploadStatuses, now).
			UpdateColumn("status", model.UploadStatusExpired)
		if updated.Error != nil {
			return fmt.Errorf("expire upload session: %w", updated.Error)
		}
		if updated.RowsAffected != 1 {
			return fmt.Errorf(
				"expire upload session %s: expected 1 row affected, got %d",
				session.ID, updated.RowsAffected,
			)
		}

		expired = true
		return nil
	})
	if err != nil {
		return expired, err
	}
	if !expired {
		return false, nil
	}

	// 事务已经提交：会话不可再发布。先通知正在写的内容停止；
	// 真正删除必须等 handoff 确认没有活跃写入后才进行。
	// 这里不等待 done：阻塞的 Read 可能很久才退出，过期扫描不能被它拖住。
	s.uploads.cancel(sessionID)

	if err := s.handoffTerminatedSession(ctx, ownerID, sessionID); err != nil {
		// 会话已经是 expired，失败只会让 Entity 留在 uploading/pending_delete，
		// 下一轮清理会继续处理。把错误返回给调用方记录日志即可。
		return true, err
	}
	return true, nil
}
