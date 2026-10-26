package file

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
)

// DefaultRecoveryBatchSize 是启动恢复一条 SELECT 最多取多少条未完成会话。
// 和清理器用同一个默认值：批次只决定一次事务涉及多少行，不改变最终结果。
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
// 本方法只改数据库状态，不删除磁盘内容、Entity 或 Session：
// 会话变成 expired 后，现有清理链（CleanupPending → resumeTerminatedSessions）
// 会把它当作普通终态会话回收。启动只需要等“资格切断”完成，不需要等物理删除；
// 删除失败会保留记录，由下一轮清理重试。
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
			Select("id").
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

		ids := make([]string, len(candidates))
		for i, candidate := range candidates {
			ids[i] = candidate.ID
		}

		// 一批一个短事务，提交后才推进游标：回滚时必须保持游标不动，
		// 让调用方看到错误并终止启动，而不是跳过没作废的会话继续跑。
		var affected int64
		err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			// 状态条件仍然写在 UPDATE 里：候选查询和更新之间会话可能被
			// 并发完成，completed 绝不能被这条语句改成 expired。
			updated := tx.Model(&model.UploadSession{}).
				Where("id IN ? AND status IN ?", ids, unfinishedUploadStatuses).
				UpdateColumn("status", model.UploadStatusExpired)
			if updated.Error != nil {
				return fmt.Errorf("mark unfinished upload sessions expired: %w", updated.Error)
			}
			affected = updated.RowsAffected
			return nil
		})
		if err != nil {
			return err
		}

		// 事务已经提交：本批再也不会被重复处理，游标可以向前推进。
		cursor = candidates[len(candidates)-1].ID
		invalidated += affected
	}
}
