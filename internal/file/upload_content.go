package file

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
)

var (
	// ErrUploadSessionExpired 表示会话已经超过 expires_at，不能再写入内容。
	ErrUploadSessionExpired = errors.New("upload session expired")

	// ErrUploadSessionConflict 表示会话当前不能接收内容：
	// 已经是 receiving/ready/completed，或者占位实体不再是 uploading/0。
	ErrUploadSessionConflict = errors.New("upload session is not writable")

	// ErrUploadSizeMismatch 表示实际收到的字节数不等于会话声明的 ExpectedSize。
	ErrUploadSizeMismatch = errors.New("uploaded content size does not match expected size")
)

// UploadSessionContent 接收一个会话的完整文件内容，写完后把会话置为 ready。
//
// 本阶段只写占位 Entity 对应的内容，不创建正式 File：
// 正式发布留给“确认完成”接口。
//
// 处理顺序：
//  1. 按 sessionID + ownerID 查询会话，检查过期、状态和占位实体；
//  2. 用 Entity.StoragePolicyID 找存储位置，不使用用户当前默认策略；
//  3. 用条件 UPDATE 原子地把 pending 抢成 receiving；
//  4. 流式写入内容，按 ExpectedSize 限制读取并核对实际大小；
//  5. 在短事务里把 Session 置为 ready、Entity.Size 改成实际大小。
//
// 写入失败后会清理本次内容并尽量把会话放回 pending，允许客户端整文件重传。
// 清理失败时保留 receiving，避免在有残留内容时又放行一次写入。
func (s *Service) UploadSessionContent(
	ctx context.Context,
	ownerID int,
	sessionID string,
	r io.Reader,
) (UploadSessionInfo, error) {
	session, err := s.findOwnedUploadSession(ctx, ownerID, sessionID)
	if err != nil {
		return UploadSessionInfo{}, err
	}
	if !session.ExpiresAt.After(time.Now()) {
		return UploadSessionInfo{}, ErrUploadSessionExpired
	}
	if session.Status != model.UploadStatusPending {
		return UploadSessionInfo{}, ErrUploadSessionConflict
	}

	var entity model.Entity
	err = s.db.WithContext(ctx).First(&entity, session.EntityID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return UploadSessionInfo{}, ErrUploadSessionConflict
		}
		return UploadSessionInfo{}, fmt.Errorf("query upload placeholder entity: %w", err)
	}
	if entity.Status != model.EntityStatusUploading || entity.RefCount != 0 {
		return UploadSessionInfo{}, ErrUploadSessionConflict
	}

	// 创建会话时已经选好位置。这里只认 Entity 记录的策略，
	// 即使服务端后来调整了用户默认策略也不能换位置。
	store, err := s.stores.Get(entity.StoragePolicyID)
	if err != nil {
		return UploadSessionInfo{}, fmt.Errorf(
			"resolve storage policy %d of upload session %s: %w",
			entity.StoragePolicyID, session.ID, err,
		)
	}

	claimed, err := s.claimUploadSession(ctx, ownerID, sessionID)
	if err != nil {
		return UploadSessionInfo{}, err
	}
	if !claimed {
		// 可能是另一个请求先抢到了，也可能在预检查之后刚好过期。
		current, findErr := s.findOwnedUploadSession(ctx, ownerID, sessionID)
		if findErr == nil && !current.ExpiresAt.After(time.Now()) {
			return UploadSessionInfo{}, ErrUploadSessionExpired
		}
		return UploadSessionInfo{}, ErrUploadSessionConflict
	}

	// 按声明大小读取，最多多读 1 个字节来发现“多传”。
	actualSize, err := store.Save(ctx, entity.StorageKey, r, session.ExpectedSize)
	if err != nil {
		if errors.Is(err, storage.ErrTooLarge) {
			err = ErrUploadSizeMismatch
		}
		return UploadSessionInfo{}, s.failUploadContent(
			ctx, ownerID, session, entity.StorageKey, store, err,
		)
	}
	if actualSize != session.ExpectedSize {
		return UploadSessionInfo{}, s.failUploadContent(
			ctx, ownerID, session, entity.StorageKey, store, ErrUploadSizeMismatch,
		)
	}

	info, err := s.markUploadSessionReady(ctx, ownerID, session, actualSize)
	if err == nil {
		return info, nil
	}

	// 收尾事务可能已经提交但返回了错误，不能直接删对象。
	// 先核对数据库结果：确认 ready 就按成功返回；确认仍在 receiving
	// 才清理内容并放回 pending；查不清就保留现场，不放行新写入。
	verified, verifyErr := s.verifyReadyUploadSession(ctx, ownerID, session.ID, actualSize)
	if verifyErr != nil {
		s.logger.Warn("cannot verify upload finalization; keeping content",
			"session_id", session.ID,
			"storage_key", entity.StorageKey,
			"error", err,
			"verify_error", verifyErr,
		)
		return UploadSessionInfo{}, fmt.Errorf("finalize upload session: %w", err)
	}
	if verified != nil {
		return *verified, nil
	}

	return UploadSessionInfo{}, s.failUploadContent(
		ctx, ownerID, session, entity.StorageKey, store, err,
	)
}

// findOwnedUploadSession 同时按会话 ID 和 owner ID 查询，避免读到别人的会话。
func (s *Service) findOwnedUploadSession(ctx context.Context, ownerID int, sessionID string) (*model.UploadSession, error) {
	var session model.UploadSession
	err := s.db.WithContext(ctx).
		Where("id = ? AND owner_id = ?", sessionID, ownerID).
		First(&session).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrUploadSessionNotFound
		}
		return nil, fmt.Errorf("query upload session: %w", err)
	}
	return &session, nil
}

// claimUploadSession 用带条件的 UPDATE 抢本次写入资格。
//
// 只有 RowsAffected == 1 的请求才能继续写；先查再改会让两个请求
// 同时看到 pending，然后一起写同一个对象。
func (s *Service) claimUploadSession(ctx context.Context, ownerID int, sessionID string) (bool, error) {
	result := s.db.WithContext(ctx).
		Model(&model.UploadSession{}).
		Where("id = ? AND owner_id = ? AND status = ? AND expires_at > ?",
			sessionID, ownerID, model.UploadStatusPending, time.Now()).
		UpdateColumn("status", model.UploadStatusReceiving)
	if result.Error != nil {
		return false, fmt.Errorf("claim upload session: %w", result.Error)
	}
	return result.RowsAffected == 1, nil
}

// markUploadSessionReady 在一个短事务里完成收尾：
// Session 变成 ready、ActualSize 记录实际大小、Entity.Size 同步更新。
//
// Entity 仍然是 uploading / ref_count=0：正式发布留给确认完成接口。
func (s *Service) markUploadSessionReady(
	ctx context.Context,
	ownerID int,
	session *model.UploadSession,
	actualSize int64,
) (UploadSessionInfo, error) {
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		updated := tx.Model(&model.UploadSession{}).
			Where("id = ? AND owner_id = ? AND status = ? AND expires_at > ?",
				session.ID, ownerID, model.UploadStatusReceiving, time.Now()).
			UpdateColumns(map[string]any{
				"actual_size": actualSize,
				"status":      model.UploadStatusReady,
			})
		if updated.Error != nil {
			return fmt.Errorf("mark upload session ready: %w", updated.Error)
		}
		if updated.RowsAffected != 1 {
			var current model.UploadSession
			if err := tx.Where("id = ? AND owner_id = ?", session.ID, ownerID).
				First(&current).Error; err == nil &&
				!current.ExpiresAt.After(time.Now()) {
				return ErrUploadSessionExpired
			}
			return ErrUploadSessionConflict
		}

		// 锁住占位实体再改大小。不能用 RowsAffected 判断“实体是否有效”：
		// 空文件要写 size=0，而它在创建时已经是 0，MySQL 会把这次 UPDATE
		// 报告成 0 行受影响。先 SELECT ... FOR UPDATE 校验状态，再按需更新。
		var entity model.Entity
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND status = ? AND ref_count = 0",
				session.EntityID, model.EntityStatusUploading).
			First(&entity).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrUploadSessionConflict
			}
			return fmt.Errorf("lock upload placeholder entity: %w", err)
		}
		if entity.Size != actualSize {
			if err := tx.Model(&model.Entity{}).
				Where("id = ?", entity.ID).
				UpdateColumn("size", actualSize).Error; err != nil {
				return fmt.Errorf("update upload placeholder entity: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return UploadSessionInfo{}, err
	}

	info := toUploadSessionInfo(*session)
	info.ActualSize = actualSize
	info.Status = model.UploadStatusReady
	return info, nil
}

// errVerifyStillReceiving 是 verifyReadyUploadSession 的内部信号：
// 加锁读到会话仍在 receiving，说明收尾事务没有提交，可以安全清理。
var errVerifyStillReceiving = errors.New("upload session still receiving")

// verifyReadyUploadSession 用独立 context、在事务里加锁核对收尾事务的结果。
//
// 这里不能用普通 SELECT：如果原收尾事务正在把会话改成 ready 但还没提交，
// 普通查询会读到旧的 receiving，调用方就会误删内容；随后原事务提交，
// 数据库中留下一个 ready 会话却没有内容。
//
// SELECT ... FOR UPDATE 会等待持有该行锁的事务结束，再读取最新提交结果。
// 锁等待超时或查询失败时返回错误，调用方必须保留内容，不能删除。
//
// 返回值：
//   - (info, nil)：数据库已经是 ready，而且 Entity 大小一致，按成功处理；
//   - (nil, nil)：加锁确认仍在 receiving，可以安全清理内容并放回 pending；
//   - (nil, err)：无法确定或数据不一致，调用方必须保留内容，不能贸然删除。
func (s *Service) verifyReadyUploadSession(
	ctx context.Context,
	ownerID int,
	sessionID string,
	actualSize int64,
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
			return fmt.Errorf("lock upload session for verification: %w", err)
		}

		if session.Status != model.UploadStatusReady {
			if session.Status == model.UploadStatusReceiving {
				return errVerifyStillReceiving
			}
			return fmt.Errorf(
				"unexpected upload session status %q after finalize error",
				session.Status,
			)
		}
		if session.ActualSize != actualSize {
			return fmt.Errorf(
				"upload session %s is ready with actual_size=%d, want %d",
				session.ID, session.ActualSize, actualSize,
			)
		}

		// 会话和实体按同一顺序加锁：先 session，再 entity。
		// 与 markUploadSessionReady 的顺序一致，避免互相等待成环。
		var entity model.Entity
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&entity, session.EntityID).Error; err != nil {
			return fmt.Errorf("lock upload placeholder entity for verification: %w", err)
		}
		if entity.Size != actualSize ||
			entity.Status != model.EntityStatusUploading ||
			entity.RefCount != 0 {
			return fmt.Errorf(
				"upload placeholder entity %d is inconsistent: size=%d status=%q ref_count=%d",
				entity.ID, entity.Size, entity.Status, entity.RefCount,
			)
		}

		info := toUploadSessionInfo(session)
		verified = &info
		return nil
	})
	if errors.Is(err, errVerifyStillReceiving) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return verified, nil
}

// failUploadContent 处理“内容已经停止写入、但会话还不能 ready”的收尾。
//
// 先删本次残留内容，删除成功后才把 receiving 放回 pending。
// 清理失败时保留 receiving：数据库里仍有可追踪记录，也不会放行第二次写入
// 去覆盖可能还存在的残留内容。
//
// 清理和状态回退都使用独立且有超时的 context，因为请求 ctx 可能已经取消。
func (s *Service) failUploadContent(
	ctx context.Context,
	ownerID int,
	session *model.UploadSession,
	storageKey string,
	store storage.Store,
	cause error,
) error {
	cleanupCtx, cancelCleanup := context.WithTimeout(
		context.WithoutCancel(ctx), orphanCleanupTimeout,
	)
	removeErr := store.Remove(cleanupCtx, storageKey)
	cancelCleanup()
	if removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
		s.logger.Warn("upload failed and content cleanup failed; keeping session receiving",
			"session_id", session.ID,
			"storage_key", storageKey,
			"error", cause,
			"cleanup_error", removeErr,
		)
		return fmt.Errorf("upload failed (%v); cleanup failed: %w", cause, removeErr)
	}

	revertCtx, cancelRevert := context.WithTimeout(
		context.WithoutCancel(ctx), orphanCleanupTimeout,
	)
	defer cancelRevert()
	result := s.db.WithContext(revertCtx).
		Model(&model.UploadSession{}).
		Where("id = ? AND owner_id = ? AND status = ?",
			session.ID, ownerID, model.UploadStatusReceiving).
		UpdateColumn("status", model.UploadStatusPending)
	if result.Error != nil {
		s.logger.Warn("upload failed and session revert failed",
			"session_id", session.ID,
			"error", cause,
			"revert_error", result.Error,
		)
		return fmt.Errorf("upload failed (%v); revert session: %w", cause, result.Error)
	}
	if result.RowsAffected != 1 {
		s.logger.Warn("upload failed but session was no longer receiving",
			"session_id", session.ID,
			"error", cause,
		)
	}
	return cause
}
