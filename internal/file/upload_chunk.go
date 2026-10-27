package file

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
	"luckyCloudFile/internal/upload"
)

// UploadSessionChunk 接收 chunked 会话的一个分片。
//
// 分片可以乱序到达，同一个会话允许多个不同编号并行处理。同一编号只接受
// 一份内容：账本按 index 记录已经成功的片，重复编号再次到达时比较大小和
// SHA-256，一致就幂等返回，不一致就按冲突拒绝。
//
// 一次请求的顺序：
//  1. 轻量校验：会话属于当前用户、是 chunked、未失效且状态允许；
//  2. 计算这一片的偏移和准确长度（纯计算，不碰磁盘）；
//  3. beginChunk：原子地占用编号和并行名额；同一编号、超上限、收尾中
//     都在读取请求体之前被拒绝；
//  4. 用会话创建时留下的绑定令牌 Confirm 会话资源锁；绑定缺失或令牌
//     失效时直接拒绝，不重新申请新锁；
//  5. 重新查会话状态，挡住预检查之后的取消/过期；
//  6. 读取请求体（最多“本片长度 + 1”字节），检查少传、多传和读取中断；
//  7. 服务端计算 SHA-256，检查账本里是不是已经收过这个编号；
//  8. 首次编号才写盘：Prepare 临时文件，然后 WriteAt 到指定偏移；
//  9. 写入成功后才登记 receipt；
//  10. 同步累计进度（保持 receiving，不在这里自动 Finalize）；
//  11. 先退出本片保护，再 end：释放编号和名额；会话级资源锁仍然保留；
//  12. 所有片到齐时再取得收尾门禁，重新 Confirm 一次资源锁，
//     核对整个账本，收尾成 ready。
//
// 第 12 步必须在 end 之后：否则最后一片会等待自己退出，形成自等待死锁。
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
	case model.UploadStatusPending,
		model.UploadStatusReceiving,
		model.UploadStatusReady:
		// 三种状态都可以进入下面的登记流程：
		//   - pending：第一片会把它推进成 receiving；
		//   - receiving：正常的分片写入状态，允许多个编号同时处理；
		//   - ready：所有片已收齐，走只读幂等重放。
	default:
		return UploadSessionInfo{}, ErrUploadSessionConflict
	}

	// 片数、偏移和本片长度全部由纯计算函数给出。放在占名额之前，
	// 非法编号不会占用并行名额，也不会读取请求体。
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
	if chunkBytes <= 0 || chunkBytes > MaxUploadBytes {
		return UploadSessionInfo{}, fmt.Errorf(
			"upload session %s has invalid chunk length %d",
			session.ID, chunkBytes,
		)
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

	// 名额必须在读取请求体之前取得：超上限的请求不读 body，直接 409。
	uploadCtx, endUpload, err := s.uploads.beginChunk(ctx, sessionID, index)
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

	// 用会话创建时留下的绑定令牌进入本次分片请求的保护。
	// 绑定不存在或令牌已经失效时拒绝继续，不能重新申请一把锁让旧会话复活。
	lockRelease, err := s.confirmUploadSessionLock(sessionID)
	if err != nil {
		return UploadSessionInfo{}, err
	}
	lockReleased := false
	defer func() {
		if !lockReleased {
			lockRelease()
		}
	}()

	// 预检查之后会话可能已经被取消/过期，或者被另一个完成请求推进到 ready。
	// 拿到名额并确认资源锁后必须重新读一次，不能相信旧快照。
	current, err := s.findOwnedUploadSession(uploadCtx, ownerID, sessionID)
	if err != nil {
		return UploadSessionInfo{}, err
	}
	if isUploadSessionInvalid(*current) {
		return UploadSessionInfo{}, ErrUploadSessionExpired
	}
	switch current.Status {
	case model.UploadStatusPending:
		// 第一片把 pending 推进成 receiving。并发的第二片可能在这里看到
		// 抢占失败：重新读一次，只要已经是 receiving 就继续（receiving
		// 是正常状态，不是“需要恢复的卡死状态”）。
		claimed, claimErr := s.claimUploadSession(uploadCtx, ownerID, sessionID)
		if claimErr != nil {
			return UploadSessionInfo{}, claimErr
		}
		if !claimed {
			current, err = s.findOwnedUploadSession(uploadCtx, ownerID, sessionID)
			if err != nil {
				return UploadSessionInfo{}, err
			}
			if isUploadSessionInvalid(*current) {
				return UploadSessionInfo{}, ErrUploadSessionExpired
			}
			switch current.Status {
			case model.UploadStatusReceiving:
				// 另一个分片已经抢先把会话推进成 receiving，正常继续。
			case model.UploadStatusReady:
				// 所有片已经收齐，走只读幂等重放。
				return s.replayReadyUploadChunk(uploadCtx, current, index, r)
			default:
				return UploadSessionInfo{}, ErrUploadSessionConflict
			}
		}
	case model.UploadStatusReceiving:
		// 正常的分片写入路径：允许多个编号同时处于 receiving。
	case model.UploadStatusReady:
		// 所有分片已经收齐，但还没有正式完成：允许客户端重传同一片。
		// 这是一条只读的幂等路径，不写盘、不改状态。
		return s.replayReadyUploadChunk(uploadCtx, current, index, r)
	default:
		return UploadSessionInfo{}, ErrUploadSessionConflict
	}
	session = current

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
	// 每次 Read 前检查取消信号；已经进入内核的阻塞读仍然要靠 HTTP 读超时
	// 或连接关闭来唤醒。
	data, err := io.ReadAll(io.LimitReader(
		&chunkContextReader{ctx: uploadCtx, r: r}, chunkBytes+1,
	))
	if err != nil {
		return UploadSessionInfo{}, s.failUploadChunk(
			ctx, ownerID, sessionID,
			fmt.Errorf("read upload chunk: %w", err),
		)
	}

	// 账本里已经有这个编号：长度或摘要不一致都属于“已确认内容被改写”，
	// 必须返回冲突（409），不能落到下面未确认编号的“长度不匹配”（400）。
	// 这里在判断长度之前先查账本，而且两种情况都不写盘。
	if existing, exists := s.chunks.lookup(sessionID, index); exists {
		if int64(len(data)) != existing.Size {
			return UploadSessionInfo{}, s.failUploadChunk(
				ctx, ownerID, sessionID, ErrUploadSessionConflict,
			)
		}
		receipt := ChunkReceipt{
			Size:   int64(len(data)),
			SHA256: sha256.Sum256(data),
		}
		if existing != receipt {
			return UploadSessionInfo{}, s.failUploadChunk(
				ctx, ownerID, sessionID, ErrUploadSessionConflict,
			)
		}

		// 上一次进度落库可能失败。重试已经确认的编号时顺手把进度对齐，
		// 但绝不删除 receipt，也不重新写盘。
		info, syncErr := s.syncChunkProgress(uploadCtx, ownerID, sessionID)
		if syncErr != nil {
			return UploadSessionInfo{}, syncErr
		}
		lockRelease()
		lockReleased = true
		endUpload()
		if info.ActualSize < info.ExpectedSize {
			return info, nil
		}
		return s.finishUploadChunk(ctx, ownerID, sessionID)
	}

	// 未确认编号：长度必须正好等于这一片应该收到的字节数，否则 400。
	if int64(len(data)) != chunkBytes {
		return UploadSessionInfo{}, s.failUploadChunk(
			ctx, ownerID, sessionID, ErrUploadSizeMismatch,
		)
	}

	receipt := ChunkReceipt{
		Size:   int64(len(data)),
		SHA256: sha256.Sum256(data),
	}

	// 第一次写入前才准备临时文件。PrepareChunks 是幂等的，
	// 两个并行的“第一片”同时准备也不会清空对方已经写过的分片。
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
	if _, recorded := s.chunks.record(sessionID, index, receipt); !recorded {
		return UploadSessionInfo{}, s.failUploadChunk(
			ctx, ownerID, sessionID, ErrUploadSessionConflict,
		)
	}

	info, err := s.syncChunkProgress(uploadCtx, ownerID, sessionID)
	if err != nil {
		return UploadSessionInfo{}, err
	}

	// 分片的读取、写盘、登记和进度同步都结束了：
	// 先退出本片保护，再释放编号和名额。会话级资源锁仍然保留。
	lockRelease()
	lockReleased = true
	endUpload()

	// 全部收齐才尝试收尾。这里用原始 ctx：endUpload 已经取消 uploadCtx。
	if info.ActualSize < info.ExpectedSize {
		return info, nil
	}
	return s.finishUploadChunk(ctx, ownerID, sessionID)
}

// replayReadyUploadChunk 处理“所有分片已经收齐、但还没有正式完成”时的重复分片。
//
// 调用方已经取得该编号的活跃名额，所以同一编号的重放不会并发写盘。
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

	data, err := io.ReadAll(io.LimitReader(
		&chunkContextReader{ctx: ctx, r: r}, chunkBytes+1,
	))
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

// finishUploadChunk 在所有分片都登记完之后尝试把会话推进到 ready。
//
// 调用方必须已经执行 endUpload：当前请求不能再算作在途写入，否则这里
// 等待 drained 时会等待自己。
//
// 取得门禁后重新核对整个账本，而不是相信调用方手里的旧累计值；并发完成
// 请求会在门禁上排队，最终都读到同一个 ready/completed 状态。
func (s *Service) finishUploadChunk(
	ctx context.Context,
	ownerID int,
	sessionID string,
) (UploadSessionInfo, error) {
	release, err := s.uploads.beginFinalize(ctx, sessionID)
	if err != nil {
		return UploadSessionInfo{}, err
	}
	defer release()

	// 进入收尾门禁后再次确认资源锁：上一片的保护已经退出，但会话级锁
	// 仍然存在，核对账本、Finalize 和推进 ready 也要受它保护。
	lockRelease, err := s.confirmUploadSessionLock(sessionID)
	if err != nil {
		// 等待门禁期间会话可能已经被并发完成、取消或过期。
		current, findErr := s.findOwnedUploadSession(ctx, ownerID, sessionID)
		if findErr == nil {
			switch current.Status {
			case model.UploadStatusCompleted:
				return toUploadSessionInfo(*current), nil
			case model.UploadStatusCanceled, model.UploadStatusExpired:
				return UploadSessionInfo{}, ErrUploadSessionNotCompletable
			}
			if isUploadSessionInvalid(*current) {
				return UploadSessionInfo{}, ErrUploadSessionExpired
			}
		}
		return UploadSessionInfo{}, err
	}
	defer lockRelease()

	// 重新核对每个编号、Finalize 临时文件、把会话推进到 ready。
	// 已经 completed 时这里是空操作，下面的查询会返回原结果。
	if err := s.prepareChunkedCompletion(ctx, ownerID, sessionID); err != nil {
		return UploadSessionInfo{}, err
	}

	current, err := s.findOwnedUploadSession(ctx, ownerID, sessionID)
	if err != nil {
		return UploadSessionInfo{}, err
	}
	return toUploadSessionInfo(*current), nil
}

// syncChunkProgress 把账本里的最新累计进度同步到数据库。
//
// 关键点：
//   - 先取得本会话的 progressMu，再读取账本最新值；这样两个并行分片不会
//     用“请求开始时的旧 received”倒序覆盖数据库进度；
//   - 数据库事务按 User → Session → Entity 的固定顺序加锁，和完成、取消、
//     过期保持一致；
//   - 只保持 receiving，不在这里自动 Finalize；收尾由 finishUploadChunk
//     在 end 之后统一做。
//
// 落库失败不会删除已经成功的 receipt；下一次同编号重试或 /complete 会
// 重新对齐进度。
func (s *Service) syncChunkProgress(
	ctx context.Context,
	ownerID int,
	sessionID string,
) (UploadSessionInfo, error) {
	unlock := s.uploads.lockChunkProgress(sessionID)
	defer unlock()

	received, ok := s.chunks.received(sessionID)
	if !ok {
		return UploadSessionInfo{}, fmt.Errorf(
			"chunk ledger missing for upload session %s", sessionID,
		)
	}

	var info UploadSessionInfo
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		session, err := lockUserAndUploadSession(ctx, tx, ownerID, sessionID)
		if err != nil {
			return err
		}
		if isUploadSessionInvalid(*session) {
			return ErrUploadSessionExpired
		}
		if session.Mode != model.UploadModeChunked {
			return ErrUploadModeMismatch
		}
		switch session.Status {
		case model.UploadStatusCompleted:
			return ErrUploadSessionConflict
		case model.UploadStatusCanceled, model.UploadStatusExpired:
			return ErrUploadSessionConflict
		}

		// 账本只能单调增长；DB 里的旧进度不能比它更靠前，否则状态已经不一致。
		if received < session.ActualSize || received > session.ExpectedSize {
			return fmt.Errorf(
				"upload session %s has inconsistent chunk progress: received=%d actual=%d expected=%d",
				session.ID, received, session.ActualSize, session.ExpectedSize,
			)
		}

		// 锁住占位实体再更新大小，和完成、取消保持同一加锁顺序。
		var entity model.Entity
		err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
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

		// 第一片到达时把 pending 推进成 receiving；之后一直保持 receiving。
		// 不在这里写成 ready：ready 必须等所有编号都核对通过后再由收尾推进。
		status := session.Status
		if status == model.UploadStatusPending {
			status = model.UploadStatusReceiving
		}
		if session.ActualSize != received || session.Status != status {
			if err := tx.Model(&model.UploadSession{}).
				Where("id = ? AND owner_id = ?", session.ID, ownerID).
				UpdateColumns(map[string]any{
					"actual_size": received,
					"status":      status,
				}).Error; err != nil {
				return fmt.Errorf("update upload chunk progress: %w", err)
			}
		}

		info = toUploadSessionInfo(*session)
		info.ActualSize = received
		info.Status = status
		return nil
	})
	if err != nil {
		return UploadSessionInfo{}, err
	}
	return info, nil
}

// chunkContextReader 在每次 Read 前检查取消信号。
//
// 它只能保证“两次 Read 之间”能及时停止；一次已经进入内核的阻塞读
// 仍然要靠 HTTP 读超时或连接关闭来唤醒。
type chunkContextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *chunkContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// failUploadChunk 返回本次分片请求的错误。
//
// 分片模式下 receiving 是正常状态：一个分片失败不能让整个会话退回 pending，
// 否则会踩到其他并行分片的状态。失败只表示这一片没有登记；已经成功写入的
// receipt 必须保留，客户端用同一编号重试时会覆盖同一段区间。
func (s *Service) failUploadChunk(
	ctx context.Context,
	ownerID int,
	sessionID string,
	cause error,
) error {
	return cause
}
