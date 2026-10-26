package file

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"luckyCloudFile/internal/model"
)

// ErrUploadSessionCompleted 表示会话已经发布成正式文件，不能再取消。
var ErrUploadSessionCompleted = errors.New("upload session already completed")

// CancelUploadSession 取消一个还没有发布文件的上传会话。
//
// 成功返回会话当前的终态：普通取消是 canceled；如果会话已经因为到期
// 变成 expired，则保持 expired。HTTP 层用 202 表示“取消已受理”，
// 因为正在传输的请求可能还没有完全退出，物理清理也不保证已经完成。
//
// 终态规则：
//   - pending/receiving/ready → canceled；
//   - expired → 保持 expired，仍然继续推进清理；
//   - canceled → 幂等返回，仍然继续推进清理；
//   - completed → 拒绝，已经发布的结果不能被取消。
//
// 不能通过 CancelUploadSession 实现过期：那会把“为什么结束”从 expired
// 改成 canceled。过期有自己的扫描入口，见 ExpireUploadSessions。
//
// 顺序：
//  1. 在短事务里锁住会话，只把未完成状态改成 canceled；
//  2. 提交事务，先切断“再发布文件”的可能；
//  3. 通知活跃上传停止；
//  4. 确认没有活跃写入后，尝试把占位 Entity 移交给清理。
//
// 不持有数据库事务等待上传停止。active upload 如果还在运行，
// 它退出时会再次调用 handoffTerminatedSession 完成清理。
func (s *Service) CancelUploadSession(
	ctx context.Context,
	ownerID int,
	sessionID string,
) (UploadSessionInfo, error) {
	// 取消一旦被受理就应该尽量落库：客户端断开或请求超时不能阻止
	// 数据库收尾。清理有独立上限，不会无限占用请求。
	opCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), orphanCleanupTimeout)
	defer cancel()

	var info UploadSessionInfo
	err := s.db.WithContext(opCtx).Transaction(func(tx *gorm.DB) error {
		// 先锁会话。取消和完成争的是同一行：
		// 完成先提交 → 这里看到 completed，返回冲突；
		// 取消先提交 → 完成接口看到 canceled，不能再发布。
		var session model.UploadSession
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND owner_id = ?", sessionID, ownerID).
			First(&session).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrUploadSessionNotFound
			}
			return fmt.Errorf("lock upload session for cancellation: %w", err)
		}

		switch session.Status {
		case model.UploadStatusCompleted:
			// 已经发布成正式文件，不能取消，也不能删除结果。
			return ErrUploadSessionCompleted
		case model.UploadStatusCanceled, model.UploadStatusExpired:
			// 两者都是终态：取消不能把 expired 改写成 canceled，
			// 否则就丢失了“这次结束是因为到期”的语义。
			// 保持原状态，下面仍然尝试推进残留内容的清理。
		default:
			// pending / receiving / ready → canceled。
			updated := tx.Model(&model.UploadSession{}).
				Where("id = ? AND owner_id = ?", session.ID, ownerID).
				UpdateColumn("status", model.UploadStatusCanceled)
			if updated.Error != nil {
				return fmt.Errorf("cancel upload session: %w", updated.Error)
			}
			if updated.RowsAffected != 1 {
				return fmt.Errorf(
					"cancel upload session %s: expected 1 row affected, got %d",
					session.ID, updated.RowsAffected,
				)
			}
			session.Status = model.UploadStatusCanceled
		}

		info = toUploadSessionInfo(session)
		return nil
	})
	if err != nil {
		return UploadSessionInfo{}, err
	}

	// 事务已经提交：新的内容请求抢不到 pending，正在写的内容也不能再变 ready。
	// 通知活跃写入停止；如果没有活跃写入，done 立刻关闭。
	s.uploads.cancel(sessionID)

	// 只有确认没有活跃写入，handoff 才会真正把 Entity 标记为待清理。
	// 如果上传还在退出过程中，它会在 defer 里再次尝试；失败也不影响取消结果。
	if err := s.handoffTerminatedSession(opCtx, ownerID, sessionID); err != nil {
		s.logger.Warn("terminated upload session cleanup deferred",
			"session_id", sessionID,
			"error", err,
		)
	}
	return info, nil
}
