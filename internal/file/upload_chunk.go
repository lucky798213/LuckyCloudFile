package file

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
	"luckyCloudFile/internal/upload"
)

// UploadSessionChunk 接收 chunked 会话的一个分片。
//
// 分片可以乱序到达，但同一编号只接受一份内容：
// 账本按 index 记录已经成功的片，重复编号再次到达时比较大小和 SHA-256，
// 一致就幂等返回，不一致就按冲突拒绝。分片进度跨请求保留在 Service 的
// 内存账本里，和“当前有没有请求在写”的 uploadController 完全分开。
//
// 一次请求的顺序：
//  1. 查会话，确认属于当前用户、是 chunked、未失效且状态允许；
//  2. 找到占位实体和创建时选定的存储位置；
//  3. 登记活跃写入，把 pending 抢占成 receiving，防止并发写同一会话；
//  4. 计算这一片的偏移和准确长度；
//  5. 最多读取“本片长度 + 1”字节，检查少传、多传和读取中断；
//  6. 服务端计算 SHA-256，检查账本里是不是已经收过这个编号；
//  7. 首次写入：首次 Prepare 临时文件，然后 WriteAt 到指定偏移；
//  8. 写入成功后才登记 receipt、累计有效字节数；
//  9. 所有片到齐后 Finalize 临时文件，再把进度写回数据库；
//  10. 注销活跃请求，必要时移交终态清理。
//
// 读取和校验都在内存里完成，最多 1 MiB + 1 字节，不把整个文件读进来，
// 也不会在长度或摘要不合法时修改目标文件。
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
	switch session.Status {
	case model.UploadStatusReady:
		// 所有分片已经收齐，但还没有正式完成：允许客户端重传同一片。
		// 这是一条只读的幂等路径，不写盘、不抢占 receiving、不改状态。
		return s.replayReadyUploadChunk(ctx, session, index, r)
	case model.UploadStatusPending:
		// 正常的分片写入路径。
	default:
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
	// 分片写入需要预分配 + WriteAt + Finalize 的可选能力。
	chunkStore, ok := store.(storage.ChunkStore)
	if !ok {
		return UploadSessionInfo{}, fmt.Errorf(
			"storage policy %d does not support chunked uploads",
			entity.StoragePolicyID,
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

	// 片数、偏移和本片长度全部由纯计算函数给出。
	offset, chunkBytes, err := upload.ChunkAt(
		session.ExpectedSize, session.ChunkSize, index,
	)
	if err != nil {
		cause := fmt.Errorf(
			"compute upload chunk geometry for session %s: %w", session.ID, err,
		)
		if errors.Is(err, upload.ErrChunkIndexOutOfRange) {
			cause = ErrInvalidChunkIndex
		}
		return UploadSessionInfo{}, s.failUploadChunk(ctx, ownerID, sessionID, cause)
	}
	if chunkBytes <= 0 || chunkBytes > MaxUploadBytes {
		return UploadSessionInfo{}, s.failUploadChunk(
			ctx, ownerID, sessionID,
			fmt.Errorf(
				"upload session %s has invalid chunk length %d",
				session.ID, chunkBytes,
			),
		)
	}

	// 账本正常在创建会话时建立；如果是手工插入的旧数据，只在 DB 进度为 0
	// 时补一个空账本。DB 已经有进度却没有账本说明状态不一致，不能继续。
	received, ok := s.chunks.received(sessionID)
	if !ok {
		if session.ActualSize != 0 {
			return UploadSessionInfo{}, s.failUploadChunk(
				ctx, ownerID, sessionID,
				fmt.Errorf("chunk ledger missing for upload session %s", sessionID),
			)
		}
		s.chunks.create(sessionID)
		received = 0
	}

	// 有界读取：多读 1 个字节用于区分“刚好一片”和“多传”。
	data, err := io.ReadAll(io.LimitReader(r, chunkBytes+1))
	if err != nil {
		return UploadSessionInfo{}, s.failUploadChunk(
			ctx, ownerID, sessionID,
			fmt.Errorf("read upload chunk: %w", err),
		)
	}
	if int64(len(data)) != chunkBytes {
		return UploadSessionInfo{}, s.failUploadChunk(
			ctx, ownerID, sessionID, ErrUploadSizeMismatch,
		)
	}

	receipt := ChunkReceipt{
		Size:   int64(len(data)),
		SHA256: sha256.Sum256(data),
	}

	// 账本里已经有这个编号：内容一致就幂等返回，不一致就拒绝。
	// 这里不再 WriteAt，避免重复写入或覆盖已经确认过的分片。
	if existing, exists := s.chunks.lookup(sessionID, index); exists {
		if existing != receipt {
			return UploadSessionInfo{}, s.failUploadChunk(
				ctx, ownerID, sessionID, ErrUploadSessionConflict,
			)
		}
		return s.finishUploadChunk(
			ctx, uploadCtx, ownerID, session, chunkStore,
			entity.StorageKey, received,
		)
	}

	// 第一次写入前才准备临时文件。PrepareChunks 是幂等的，
	// 初始化重试不会清空已经写过的分片。
	if received == 0 {
		if err := chunkStore.PrepareChunks(
			uploadCtx, entity.StorageKey, session.ExpectedSize,
		); err != nil {
			return UploadSessionInfo{}, s.failUploadChunk(
				ctx, ownerID, sessionID,
				fmt.Errorf("prepare upload chunks: %w", err),
			)
		}
	}

	if err := chunkStore.WriteChunkAt(
		uploadCtx, entity.StorageKey, offset, data,
	); err != nil {
		return UploadSessionInfo{}, s.failUploadChunk(
			ctx, ownerID, sessionID,
			fmt.Errorf("write upload chunk: %w", err),
		)
	}

	// 写入成功后才登记；登记失败说明同编号已经被别的请求抢先确认，
	// 不能把这次写入重复计入进度。
	received, recorded := s.chunks.record(sessionID, index, receipt)
	if !recorded {
		return UploadSessionInfo{}, s.failUploadChunk(
			ctx, ownerID, sessionID, ErrUploadSessionConflict,
		)
	}

	return s.finishUploadChunk(
		ctx, uploadCtx, ownerID, session, chunkStore,
		entity.StorageKey, received,
	)
}

// replayReadyUploadChunk 处理“所有分片已经收齐、但还没有正式完成”时的重复分片。
//
// 这是成功响应丢失后的正常重试：内容必须和账本里的 receipt 完全一致。
// 一致就返回当前 ready 进度；长度或摘要不一致返回冲突。整条路径只读，
// 不 Prepare、不 WriteAt、不抢占 receiving，也不会把 ready 退回 pending。
func (s *Service) replayReadyUploadChunk(
	ctx context.Context,
	session *model.UploadSession,
	index int,
	r io.Reader,
) (UploadSessionInfo, error) {
	if err := ctx.Err(); err != nil {
		return UploadSessionInfo{}, err
	}

	_, chunkBytes, err := upload.ChunkAt(
		session.ExpectedSize, session.ChunkSize, index,
	)
	if err != nil {
		if errors.Is(err, upload.ErrChunkIndexOutOfRange) {
			return UploadSessionInfo{}, ErrInvalidChunkIndex
		}
		return UploadSessionInfo{}, fmt.Errorf(
			"compute replayed upload chunk geometry for session %s: %w",
			session.ID, err,
		)
	}
	if chunkBytes <= 0 || chunkBytes > MaxUploadBytes {
		return UploadSessionInfo{}, fmt.Errorf(
			"upload session %s has invalid chunk length %d",
			session.ID, chunkBytes,
		)
	}

	data, err := io.ReadAll(io.LimitReader(r, chunkBytes+1))
	if err != nil {
		return UploadSessionInfo{}, fmt.Errorf("read replayed upload chunk: %w", err)
	}
	// 这一片已经确认过，内容（包括长度）必须和账本完全一致。
	// 不一致按冲突处理，不再区分少传/多传。
	if int64(len(data)) != chunkBytes {
		return UploadSessionInfo{}, ErrUploadSessionConflict
	}

	receipt := ChunkReceipt{
		Size:   int64(len(data)),
		SHA256: sha256.Sum256(data),
	}
	received, ok := s.chunks.received(session.ID)
	if !ok || received != session.ExpectedSize {
		return UploadSessionInfo{}, ErrUploadSessionConflict
	}
	existing, ok := s.chunks.lookup(session.ID, index)
	if !ok || existing != receipt {
		return UploadSessionInfo{}, ErrUploadSessionConflict
	}

	return toUploadSessionInfo(*session), nil
}

// finishUploadChunk 在所有有效分片都写完后收尾。
//
// 只有账本累计字节数等于声明大小时才 Finalize 临时文件，并让会话变成 ready。
// 所有分片乱序到达也没关系：每片都写在自己的准确区间，收齐后临时文件就是
// 完整内容。Finalize 是幂等的，重复编号的幂等重试可以安全地再调用一次。
func (s *Service) finishUploadChunk(
	ctx context.Context,
	uploadCtx context.Context,
	ownerID int,
	session *model.UploadSession,
	chunkStore storage.ChunkStore,
	storageKey string,
	received int64,
) (UploadSessionInfo, error) {
	if received > session.ExpectedSize {
		return UploadSessionInfo{}, s.failUploadChunk(
			ctx, ownerID, session.ID,
			fmt.Errorf(
				"upload session %s received %d bytes, exceeds expected %d",
				session.ID, received, session.ExpectedSize,
			),
		)
	}

	if received == session.ExpectedSize {
		if err := chunkStore.FinalizeChunks(
			uploadCtx, storageKey, session.ExpectedSize,
		); err != nil {
			return UploadSessionInfo{}, s.failUploadChunk(
				ctx, ownerID, session.ID,
				fmt.Errorf("finalize upload chunks: %w", err),
			)
		}
	}

	info, err := s.finalizeUploadChunk(ctx, ownerID, session, received)
	if err != nil {
		return UploadSessionInfo{}, s.failUploadChunk(ctx, ownerID, session.ID, err)
	}
	return info, nil
}

// finalizeUploadChunk 在一个短事务里推进会话进度。
//
// received 是账本里所有不重复分片的累计字节数，不接受客户端传的任何
// 偏移或总大小。所有片到齐后会话变成 ready，后续仍然由确认完成接口
// 发布正式 File。
func (s *Service) finalizeUploadChunk(
	ctx context.Context,
	ownerID int,
	session *model.UploadSession,
	received int64,
) (UploadSessionInfo, error) {
	// 账本只能单调增长；DB 里的旧进度不能比它更靠前，否则状态已经不一致。
	if received < session.ActualSize || received > session.ExpectedSize {
		return UploadSessionInfo{}, fmt.Errorf(
			"upload session %s has inconsistent chunk progress: received=%d actual=%d expected=%d",
			session.ID, received, session.ActualSize, session.ExpectedSize,
		)
	}

	status := model.UploadStatusPending
	if received == session.ExpectedSize {
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
				"actual_size": received,
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

		// 锁住占位实体再更新大小；只剩最后一片时它会变成声明大小，
		// 确认完成接口会再核对一次。
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
		if entity.Size != received {
			if err := tx.Model(&model.Entity{}).
				Where("id = ?", entity.ID).
				UpdateColumn("size", received).Error; err != nil {
				return fmt.Errorf("update upload placeholder entity: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return UploadSessionInfo{}, err
	}

	info := toUploadSessionInfo(*session)
	info.ActualSize = received
	info.Status = status
	return info, nil
}

// failUploadChunk 把 receiving 放回 pending，允许客户端重试同一片。
//
// 它刻意不删除存储内容：已经确认的分片必须保留，未确认的写入也不会
// 计入账本。客户端用同一编号重试时会覆盖同一段区间；只有真正写入成功
// 并登记 receipt 之后，字节数才会被累计。
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
