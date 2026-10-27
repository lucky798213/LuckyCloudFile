package file

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/upload"
)

// DefaultRecoveryBatchSize 是启动恢复一条 SELECT 最多取多少条未完成会话。
// 和清理器用同一个默认值：批次只决定一次查询取多少条候选，
// 每条会话仍然各自开一个事务，不改变最终结果。
const DefaultRecoveryBatchSize = DefaultCleanupBatchSize

// InvalidateUnfinishedUploads 作废上一次进程留下的所有未完成上传会话。
//
// 当前是单实例部署：旧进程退出后不允许上传跨重启续传，所以重启前没完成的
// 会话必须立刻失去继续上传和确认完成的资格。判断依据只能是“是否已经完成”，
// 不能是 expires_at：旧会话可能还有很久才到期，但它已经不属于这次进程。
//
// 状态规则：
//
//	pending / receiving / ready → expired
//	completed / canceled / expired → 保持不变
//
// 这里刻意不加 “expires_at <= now”：需要作废的是重启前的全部未完成会话。
// 重复执行是幂等的：第二轮查询不会再选中已经变成 expired 的记录。
//
// 每条会话单独开一个短事务，而不是只做一次批量 UPDATE：进入终态必须和
// 释放预留一起提交。事务里先锁用户、再锁会话，按会话上持久化的
// ReservedBytes 释放并清零；批量更新无法逐条知道该释放多少，会让预留永久占用。
// 单条失败时直接返回错误（调用方会终止启动），游标不推进，避免跳过没作废的会话。
//
// 本方法只改数据库状态和容量账目，不删除磁盘内容、Entity 或 Session：
// 会话变成 expired 后，现有清理链（CleanupPending → resumeTerminatedSessions）
// 会把它当作普通终态会话回收。启动只需要等“资格切断”完成，不需要等物理删除；
// 删除失败会保留记录，由下一轮清理重试，且不会再改动容量。
//
// 它必须在服务开始接受 HTTP 请求之前同步执行，而且失败必须终止启动：
// 如果只记日志继续启动，没被作废的旧会话仍然可以上传和发布。
// 也不要放进 NewService / database.Open：创建业务对象或连接数据库
// 都不等于“进程重启”，测试和初始化命令也会走那些路径。
func (s *Service) InvalidateUnfinishedUploads(ctx context.Context, batchSize int) error {
	if batchSize <= 0 {
		batchSize = DefaultRecoveryBatchSize
	}

	var (
		cursor      string
		invalidated int64
	)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// 按 id 游标分批。expired / completed 不在候选范围内，
		// 所以已经处理过的批次不会再次出现。
		var candidates []model.UploadSession
		err := s.db.WithContext(ctx).
			Select("id", "owner_id").
			Where("status IN ? AND id > ?", unfinishedUploadStatuses, cursor).
			Order("id ASC").
			Limit(batchSize).
			Find(&candidates).Error
		if err != nil {
			return fmt.Errorf("query unfinished upload sessions: %w", err)
		}
		if len(candidates) == 0 {
			if invalidated > 0 {
				s.logger.Info("invalidated unfinished upload sessions", "count", invalidated)
			}
			return nil
		}

		for _, candidate := range candidates {
			if err := ctx.Err(); err != nil {
				return err
			}

			// 只有事务提交成功才推进游标：失败时必须返回错误终止启动，
			// 而不是跳过这条没作废的会话继续跑。
			updated, err := s.invalidateUnfinishedUploadSession(
				ctx, candidate.OwnerID, candidate.ID,
			)
			if err != nil {
				return err
			}
			cursor = candidate.ID
			if updated {
				invalidated++
			}
		}
	}
}

// invalidateUnfinishedUploadSession 在事务里作废一条重启前的未完成会话。
//
// 返回 true 表示本次真的把会话改成了 expired。已经完成或已经终态的会话
// 保持原样并返回 false。
func (s *Service) invalidateUnfinishedUploadSession(
	ctx context.Context,
	ownerID int,
	sessionID string,
) (bool, error) {
	invalidated := false

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 加锁顺序固定为 User → Session；释放预留和写终态在同一个事务里。
		session, err := lockUserAndUploadSession(ctx, tx, ownerID, sessionID)
		if err != nil {
			if errors.Is(err, ErrUploadSessionNotFound) {
				// 候选查询之后会话被删掉了，没有需要作废的内容。
				return nil
			}
			return fmt.Errorf("lock unfinished upload session: %w", err)
		}

		// 状态条件仍然要检查：候选查询和加锁之间会话可能被并发完成，
		// completed 绝不能被启动恢复改成 expired。
		if !upload.CanTransition(session.Status, model.UploadStatusExpired) {
			return nil
		}

		if _, err := releaseUploadReservation(tx, session); err != nil {
			return err
		}

		updated := tx.Model(&model.UploadSession{}).
			Where("id = ? AND owner_id = ? AND status IN ?",
				session.ID, ownerID, unfinishedUploadStatuses).
			UpdateColumns(map[string]any{
				"status":         model.UploadStatusExpired,
				"reserved_bytes": 0,
			})
		if updated.Error != nil {
			return fmt.Errorf("mark unfinished upload session expired: %w", updated.Error)
		}
		if updated.RowsAffected != 1 {
			return fmt.Errorf(
				"invalidate upload session %s: expected 1 row affected, got %d",
				session.ID, updated.RowsAffected,
			)
		}

		invalidated = true
		return nil
	})
	if err != nil {
		return false, err
	}
	if invalidated {
		// 重启边界不恢复旧令牌：这里只是清掉当前进程里可能残留的绑定。
		// 新进程本来就没有绑定，Unlock 是空操作。
		s.closeUploadSessionLock(sessionID)
	}
	return invalidated, nil
}
