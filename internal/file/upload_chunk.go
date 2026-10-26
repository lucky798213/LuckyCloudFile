package file

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
	"luckyCloudFile/internal/upload"
)

// UploadSessionChunk 接收 chunked 会话的一个分片。
//
// 分片必须按 0、1、2… 的顺序逐片上传，服务端不接受跳片或乱序：
// 内容直接追加在同一个占位文件后面，ActualSize 就是下一片的起始偏移。
// 这样不用额外记录每片的状态，取消/过期清理也仍然只删一个存储对象。
//
// 一片的处理顺序：
//  1. 查会话并确认是 chunked、未失效、pending，序号正好是下一片；
//  2. 用 Entity.StoragePolicyID 找到会话创建时选定的存储位置；
//  3. 登记活跃写入，再把 pending 抢占成 receiving；
//  4. 从 ActualSize 开始写入这一片，最多写入本片应有的字节数；
//  5. 短事务里把 ActualSize 前移；最后一片完成后会话变成 ready。
//
// 写入失败时只把会话放回 pending，不删除已经确认的前几片；
// 未确认的残留尾巴会在下一次写入同一片时被覆盖并截断。
func (s *Service) UploadSessionChunk(
	ctx context.Context,
	ownerID int,
	sessionID string,
	index int,
	r io.Reader,
) (UploadSessionInfo, error) {
	session, err := s.findOwnedUploadSession(ctx, ownerID, sessionID)
	if err != nil {
		return UploadSessionInfo{}, err
	}
	if isUploadSessionInvalid(*session) {
		return UploadSessionInfo{}, ErrUploadSessionExpired
	}
	// full 会话必须走整文件接口；混用会让 ActualSize 的含义变得不明确。
	if session.Mode != model.UploadModeChunked {
		return UploadSessionInfo{}, ErrUploadModeMismatch
	}
	if session.Status != model.UploadStatusPending {
		return UploadSessionInfo{}, ErrUploadSessionConflict
	}

	// 片数、偏移和本片长度全部由纯计算函数给出。
	// 已确认的 ActualSize 必须正好是这一片的起点，否则就是跳片或重复上传。
	offset, chunkBytes, err := upload.ChunkAt(
		session.ExpectedSize, session.ChunkSize, index,
	)
	if err != nil {
		if errors.Is(err, upload.ErrChunkIndexOutOfRange) {
			return UploadSessionInfo{}, ErrInvalidChunkIndex
		}
		return UploadSessionInfo{}, fmt.Errorf(
			"compute upload chunk geometry for session %s: %w", session.ID, err,
		)
	}
	if offset != session.ActualSize {
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

	store, err := s.stores.Get(entity.StoragePolicyID)
	if err != nil {
		return UploadSessionInfo{}, fmt.Errorf(
			"resolve storage policy %d of upload session %s: %w",
			entity.StoragePolicyID, session.ID, err,
		)
	}

	// 和整文件上传一样：先登记活跃写入，再抢占数据库状态。
	uploadCtx, endUpload, err := s.uploads.begin(ctx, sessionID)
	if err != nil {
		return UploadSessionInfo{}, err
	}
	defer func() {
		endUpload()
		if handoffErr := s.handoffTerminatedSession(ctx, ownerID, sessionID); handoffErr != nil {
			s.logger.Warn("terminated upload session cleanup deferred",
				"session_id", sessionID,
				"error", handoffErr,
			)
		}
	}()

	claimed, err := s.claimUploadSession(uploadCtx, ownerID, sessionID)
	if err != nil {
		return UploadSessionInfo{}, err
	}
	if !claimed {
		current, findErr := s.findOwnedUploadSession(ctx, ownerID, sessionID)
		if findErr == nil && isUploadSessionInvalid(*current) {
			return UploadSessionInfo{}, ErrUploadSessionExpired
		}
		return UploadSessionInfo{}, ErrUploadSessionConflict
	}

	size, err := store.Append(
		uploadCtx, entity.StorageKey, offset, r, chunkBytes,
	)
	if err != nil {
		cause := err
		if errors.Is(err, storage.ErrTooLarge) {
			cause = ErrUploadSizeMismatch
		}
		return UploadSessionInfo{}, s.failUploadChunk(ctx, ownerID, session.ID, cause)
	}
	if size != chunkBytes {
		return UploadSessionInfo{}, s.failUploadChunk(
			ctx, ownerID, session.ID, ErrUploadSizeMismatch,
		)
	}

	info, err := s.finalizeUploadChunk(
		ctx, ownerID, session, offset+size,
	)
	if err != nil {
		return UploadSessionInfo{}, err
	}
	return info, nil
}

// finalizeUploadChunk 在一个短事务里推进会话进度。
//
// newSize 只由服务端已经确认的旧大小加上本次实际写入字节数得到，
// 不接受客户端传的任何偏移或总大小。最后一片写入后直接变成 ready，
// 后续仍然由确认完成接口发布正式 File。
func (s *Service) finalizeUploadChunk(
	ctx context.Context,
	ownerID int,
	session *model.UploadSession,
	newSize int64,
) (UploadSessionInfo, error) {
	status := model.UploadStatusPending
	if newSize >= session.ExpectedSize {
		status = model.UploadStatusReady
	}

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// actual_size 作为条件：即使有意外并发，也不能把进度覆盖回旧值。
		updated := tx.Model(&model.UploadSession{}).
			Where(
				"id = ? AND owner_id = ? AND status = ? AND expires_at > ? AND actual_size = ?",
				session.ID, ownerID, model.UploadStatusReceiving, time.Now(), session.ActualSize,
			).
			UpdateColumns(map[string]any{
				"actual_size": newSize,
				"status":      status,
			})
		if updated.Error != nil {
			return fmt.Errorf("update upload chunk progress: %w", updated.Error)
		}
		if updated.RowsAffected != 1 {
			var current model.UploadSession
			if err := tx.Where("id = ? AND owner_id = ?", session.ID, ownerID).
				First(&current).Error; err == nil && isUploadSessionInvalid(current) {
				return ErrUploadSessionExpired
			}
			return ErrUploadSessionConflict
		}

		// 占位实体的大小跟着实际收到的字节数走，确认完成时会再核对一次。
		entityUpdated := tx.Model(&model.Entity{}).
			Where("id = ? AND status = ? AND ref_count = 0",
				session.EntityID, model.EntityStatusUploading).
			UpdateColumn("size", newSize)
		if entityUpdated.Error != nil {
			return fmt.Errorf("update upload placeholder entity: %w", entityUpdated.Error)
		}
		if entityUpdated.RowsAffected != 1 {
			return ErrUploadSessionConflict
		}
		return nil
	})
	if err != nil {
		return UploadSessionInfo{}, err
	}

	info := toUploadSessionInfo(*session)
	info.ActualSize = newSize
	info.Status = status
	return info, nil
}

// failUploadChunk 把 receiving 放回 pending，允许客户端重试同一片。
//
// 它刻意不删除存储内容：前面的分片已经确认过，删掉会让客户端必须从头再传。
// 本次写了一半的尾巴不会被算进 ActualSize，下一次 Append 会在同一偏移
// 重新写入并截断它。
func (s *Service) failUploadChunk(
	ctx context.Context,
	ownerID int,
	sessionID string,
	cause error,
) error {
	revertCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), orphanCleanupTimeout,
	)
	defer cancel()

	result := s.db.WithContext(revertCtx).
		Model(&model.UploadSession{}).
		Where("id = ? AND owner_id = ? AND status = ?",
			sessionID, ownerID, model.UploadStatusReceiving).
		UpdateColumn("status", model.UploadStatusPending)
	if result.Error != nil {
		s.logger.Warn("upload chunk failed and session revert failed",
			"session_id", sessionID,
			"error", cause,
			"revert_error", result.Error,
		)
		return fmt.Errorf("upload chunk failed (%v); revert session: %w", cause, result.Error)
	}
	if result.RowsAffected != 1 {
		s.logger.Warn("upload chunk failed but session was no longer receiving",
			"session_id", sessionID,
			"error", cause,
		)
	}
	return cause
}
