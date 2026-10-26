package file

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"luckyCloudFile/internal/idgen"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/quota"
	"luckyCloudFile/internal/upload"
)

var (
	// ErrUploadSessionNotCompletable 表示会话还不是 ready，不能首次完成。
	// 状态规则集中在 internal/upload，这里只把它暴露给 HTTP 层。
	ErrUploadSessionNotCompletable = upload.ErrNotCompletable

	// ErrUploadSessionInconsistent 表示会话已经是 completed，却没有 result_file_id。
	// 这是数据异常，不能通过重新创建文件来“补救”。
	ErrUploadSessionInconsistent = upload.ErrMissingResultFile
)

// CompleteUploadSession 把已经 ready 的上传内容正式发布到用户目录。
//
// 它不接收也不搬运文件：内容已经在内容上传阶段写进了会话占位 Entity，
// 这里只创建正式 File、激活 Entity、把会话标成 completed。
//
// 顺序：
//  1. 在事务里先锁用户行，再按 sessionID + ownerID 锁会话；
//  2. completed 直接返回原 ResultFileID（先于过期检查，支持幂等重放）；
//  3. ready 且未过期才继续，其余状态拒绝；
//  4. 重新校验目标目录和占位 Entity（uploading / 0，三处大小一致）；
//  5. 把会话预留量转成已用量（reserved → used），
//     创建 File → 激活 Entity（active / 1）→ 会话标记 completed、预留清零。
//
// 容量结算是这个事务的一部分，但不在事务提交后的回调里做：
// 否则会出现“文件发布了，但账没记上”的窗口。结算按会话上持久化的
// ReservedBytes 进行，不看 ExpectedSize；重复完成直接返回原结果，不会二次计费。
//
// 容量结算、File、Entity 和会话状态必须一起提交。事务失败后不会删除已经传好的内容：
// 明确回滚时会话仍是 ready、预留原样保留，客户端可以重试；提交结果不确定时，
// 用独立且有超时的 context 加锁核对 completed + ResultFileID。
func (s *Service) CompleteUploadSession(
	ctx context.Context,
	ownerID int,
	sessionID string,
) (UploadSessionInfo, error) {
	var (
		completed UploadSessionInfo
		fileID    string
	)

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 先锁用户、再锁会话：和取消、过期、重启恢复保持同一个加锁顺序。
		if _, err := quota.LockUser(tx, ownerID); err != nil {
			return err
		}

		// 并发的完成请求会在会话这一行上排队：
		// 后到的请求等到前一个提交后读到 completed，直接返回同一个结果文件。
		var session model.UploadSession
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND owner_id = ?", sessionID, ownerID).
			First(&session).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrUploadSessionNotFound
			}
			return fmt.Errorf("lock upload session: %w", err)
		}

		plan, err := upload.PlanCompletion(&session)
		if err != nil {
			return err
		}
		// 已经完成过：先判断 completed，再检查过期。
		// 上传结果一旦发布，即使会话后来过期，重复确认也应返回原文件 ID。
		if !plan.CreateFile {
			completed = toUploadSessionInfo(session)
			return nil
		}

		// 首次完成只允许 ready，而且必须在有效期内。
		if !session.ExpiresAt.After(time.Now()) {
			return ErrUploadSessionExpired
		}

		// 接收内容期间目录可能被删除、改名或换到别人名下，事务内必须重新核对。
		if _, err := queryDirectory(ctx, tx, ownerID, session.ParentID); err != nil {
			return err
		}

		// 保持“先 Session 再 Entity”的加锁顺序，和内容收尾、核对流程一致。
		var entity model.Entity
		err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&entity, session.EntityID).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf(
					"%w: placeholder entity %d not found",
					ErrUploadSessionConflict, session.EntityID,
				)
			}
			return fmt.Errorf("lock upload placeholder entity: %w", err)
		}
		if entity.Status != model.EntityStatusUploading || entity.RefCount != 0 {
			return fmt.Errorf(
				"%w: entity %d is %s/%d, want uploading/0",
				ErrUploadSessionConflict, entity.ID, entity.Status, entity.RefCount,
			)
		}
		// 客户端创建会话时声明的大小、实际收到的大小、Entity 记录的大小
		// 必须完全一致，否则不能把这份内容发布成正式文件。
		if session.ExpectedSize != session.ActualSize || session.ActualSize != entity.Size {
			return fmt.Errorf(
				"%w: expected_size=%d actual_size=%d entity_size=%d",
				ErrUploadSessionConflict,
				session.ExpectedSize, session.ActualSize, entity.Size,
			)
		}
		// 预留量必须正好覆盖这次要发布的内容。少了就说明账目和内容对不上，
		// 不能“先发布、以后再补”：那会让 used+reserved 越过配额上界。
		if session.ReservedBytes != entity.Size {
			return fmt.Errorf(
				"%w: reserved_bytes=%d entity_size=%d",
				ErrUploadSessionConflict, session.ReservedBytes, entity.Size,
			)
		}

		// 空的分片会话没有调用过分片接口，磁盘上还没有对象；直接发布会让
		// 完成后的文件下载不到。发布前幂等地补一个 0 字节对象。
		//
		// 用 Save 写空内容：同一个 key 重复写结果仍然是 0 字节，重试完成
		// 不会产生多份内容。放在事务内是为了和会话/Entity 的发布共用同一个
		// 提交边界，避免取消或过期清理与这次补写交错。
		if session.ExpectedSize == 0 {
			store, err := s.stores.Get(entity.StoragePolicyID)
			if err != nil {
				return fmt.Errorf(
					"resolve storage policy %d for empty upload session %s: %w",
					entity.StoragePolicyID, session.ID, err,
				)
			}
			if _, err := store.Save(ctx, entity.StorageKey, bytes.NewReader(nil), 0); err != nil {
				return fmt.Errorf("create empty upload content: %w", err)
			}
		}

		// 先把预留转成已用。它和创建 File、激活 Entity、标记 completed
		// 在同一个事务里，失败时全部回滚，预留仍然保留，客户端可以重试完成。
		if err := quota.Settle(tx, ownerID, session.ReservedBytes); err != nil {
			return err
		}

		// 文件名字、目录、所有者都来自会话，完成时不允许客户端重新指定。
		fileID, err = idgen.New()
		if err != nil {
			return err
		}
		entityID := entity.ID
		parentID := session.ParentID
		record := model.File{
			ID:       fileID,
			Name:     session.Name,
			Kind:     model.KindFile,
			ParentID: &parentID,
			EntityID: &entityID,
			OwnerID:  session.OwnerID,
		}
		// 同一目录同名由唯一索引兜底：冲突时整个事务回滚，
		// 会话仍是 ready，已经传好的物理内容不会被删除。
		if err := tx.Create(&record).Error; err != nil {
			if errors.Is(err, gorm.ErrDuplicatedKey) {
				return ErrAlreadyExists
			}
			return fmt.Errorf("create file: %w", err)
		}

		// 占位内容正式激活：引用计数从 0 变成 1。
		activated := tx.Model(&model.Entity{}).
			Where("id = ? AND status = ? AND ref_count = 0",
				entity.ID, model.EntityStatusUploading).
			UpdateColumns(map[string]any{
				"status":    model.EntityStatusActive,
				"ref_count": 1,
			})
		if activated.Error != nil {
			return fmt.Errorf("activate upload entity: %w", activated.Error)
		}
		if activated.RowsAffected != 1 {
			return fmt.Errorf(
				"%w: entity %d changed while completing",
				ErrUploadSessionConflict, entity.ID,
			)
		}

		marked := tx.Model(&model.UploadSession{}).
			Where("id = ? AND owner_id = ? AND status = ?",
				session.ID, ownerID, model.UploadStatusReady).
			UpdateColumns(map[string]any{
				"status":         model.UploadStatusCompleted,
				"result_file_id": fileID,
				"reserved_bytes": 0,
			})
		if marked.Error != nil {
			return fmt.Errorf("complete upload session: %w", marked.Error)
		}
		if marked.RowsAffected != 1 {
			return fmt.Errorf(
				"%w: upload session %s is no longer ready",
				ErrUploadSessionConflict, session.ID,
			)
		}

		session.Status = model.UploadStatusCompleted
		session.ResultFileID = &fileID
		session.ReservedBytes = 0
		completed = toUploadSessionInfo(session)
		return nil
	})
	if err == nil {
		return completed, nil
	}

	// 事务可能已经提交但返回了错误（例如提交响应丢失）。这时不能删除内容，
	// 而是用独立 context 加锁重新核对；确认 completed 就返回已有结果。
	verified, verifyErr := s.verifyCompletedUploadSession(ctx, ownerID, sessionID)
	if verifyErr == nil && verified != nil {
		return *verified, nil
	}
	if verifyErr != nil {
		s.logger.Warn("cannot verify upload completion; keeping content",
			"session_id", sessionID,
			"error", err,
			"verify_error", verifyErr,
		)
	}
	return UploadSessionInfo{}, err
}

// verifyCompletedUploadSession 用独立、有超时的 context 加锁核对完成结果。
//
// 它和 verifyReadyUploadSession 的区别：
//   - 这里确认的是 completed + ResultFileID，不是 ready + uploading/0 Entity；
//   - 返回 (nil, nil) 表示确定事务没有提交，会话仍在 ready，内容必须保留；
//   - 返回 (nil, err) 表示无法确定，调用方同样不能删除内容。
//
// 加锁读取会等待原事务结束，避免把“正在提交”的 completed 误判成 ready。
func (s *Service) verifyCompletedUploadSession(
	ctx context.Context,
	ownerID int,
	sessionID string,
) (*UploadSessionInfo, error) {
	verifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), orphanCleanupTimeout)
	defer cancel()

	var verified *UploadSessionInfo
	err := s.db.WithContext(verifyCtx).Transaction(func(tx *gorm.DB) error {
		var session model.UploadSession
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND owner_id = ?", sessionID, ownerID).
			First(&session).Error
		if err != nil {
			return fmt.Errorf("lock upload session for completion verification: %w", err)
		}

		if session.Status != model.UploadStatusCompleted {
			// 能加锁读到非 completed，说明原事务确实没有提交。
			return nil
		}
		if session.ResultFileID == nil || *session.ResultFileID == "" {
			return ErrUploadSessionInconsistent
		}

		info := toUploadSessionInfo(session)
		verified = &info
		return nil
	})
	if err != nil {
		return nil, err
	}
	return verified, nil
}
