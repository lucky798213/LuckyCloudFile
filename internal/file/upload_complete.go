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
	"luckyCloudFile/internal/storage"
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

// CompleteUploadSession 把已经收齐的上传内容正式发布到用户目录。
//
// 它不接收也不搬运文件：内容已经在内容上传阶段写进了会话占位 Entity，
// 这里只创建正式 File、激活 Entity、把会话标成 completed。
//
// 分片会话在进入发布事务之前多一个准备阶段（见 prepareChunkedCompletion）：
// 先取得会话级收尾门禁、等在途写入退出，再按内存账本核对每个分片，
// 然后 FinalizeChunks 并把 Session/Entity 推进到 ready。
// 因此 /complete 每次都会重新核对和推进，而不是只在“最后一片到达”时
// 尝试一次；缺任何一片都不会因为文件长度已经等于总大小而发布。
//
// 顺序：
//  1. 先读会话；completed 直接返回原 ResultFileID（先于锁和过期检查，支持幂等重放）；
//  2. canceled / expired 直接拒绝；首次完成必须在有效期内且绑定令牌有效；
//  3. 用会话创建时留下的令牌进入本次请求保护，令牌失效时拒绝发布；
//  4. 分片会话先做收尾准备：门禁 → 再确认锁 → 核对账本 → FinalizeChunks → ready；
//  5. 在事务里先锁用户行，再按 sessionID + ownerID 锁会话；
//  6. ready 且未过期才继续，其余状态拒绝；
//  7. 重新校验目标目录和占位 Entity（uploading / 0，三处大小一致）；
//  8. 把会话预留量转成已用量（reserved → used），
//     创建 File → 激活 Entity（active / 1）→ 会话标记 completed、预留清零；
//  9. 提交成功后关闭会话资源锁；失败保留锁，允许在有效期内重试。
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
	current, err := s.findOwnedUploadSession(ctx, ownerID, sessionID)
	if err != nil {
		return UploadSessionInfo{}, err
	}

	// 已经完成：无条件幂等返回原结果。会话锁可能已经释放或过期，
	// 不能因为令牌不可用而拒绝重放。
	if current.Status == model.UploadStatusCompleted {
		// completed 是终态：无论结果是否完整，都不再需要会话锁。
		s.finishCompletedUpload(sessionID)
		if current.ResultFileID == nil || *current.ResultFileID == "" {
			return UploadSessionInfo{}, ErrUploadSessionInconsistent
		}
		return toUploadSessionInfo(*current), nil
	}
	// canceled / expired 已经不可能再发布，也不需要确认资源锁。
	if current.Status == model.UploadStatusCanceled ||
		current.Status == model.UploadStatusExpired {
		return UploadSessionInfo{}, ErrUploadSessionNotCompletable
	}
	// 首次完成必须在会话有效期内，并且绑定令牌仍然有效。
	if !current.ExpiresAt.After(time.Now()) {
		return UploadSessionInfo{}, ErrUploadSessionExpired
	}

	lockRelease, err := s.confirmUploadSessionLock(sessionID)
	if err != nil {
		// 并发完成可能已经发布并关闭了会话锁；completed 仍然幂等返回。
		if latest, findErr := s.findOwnedUploadSession(ctx, ownerID, sessionID); findErr == nil &&
			latest.Status == model.UploadStatusCompleted {
			s.finishCompletedUpload(sessionID)
			if latest.ResultFileID == nil || *latest.ResultFileID == "" {
				return UploadSessionInfo{}, ErrUploadSessionInconsistent
			}
			return toUploadSessionInfo(*latest), nil
		}
		return UploadSessionInfo{}, err
	}
	defer lockRelease()

	// 分片会话：先确认内容真的收齐，再进入发布事务。
	if current.Mode == model.UploadModeChunked {
		release, gateErr := s.uploads.beginFinalize(ctx, sessionID)
		if gateErr != nil {
			return UploadSessionInfo{}, gateErr
		}
		defer func() {
			release()
			// 收尾期间会话可能被并发取消/过期。handoff 之前因为门禁
			// 把会话当成“仍活跃”而跳过，现在补一次；成功完成时它是空操作。
			if handoffErr := s.handoffTerminatedSession(ctx, ownerID, sessionID); handoffErr != nil {
				s.logger.Warn("terminated upload session cleanup deferred",
					"session_id", sessionID,
					"error", handoffErr,
				)
			}
		}()

		if err := s.prepareChunkedCompletion(ctx, ownerID, sessionID); err != nil {
			return UploadSessionInfo{}, err
		}
	}

	var (
		completed UploadSessionInfo
		fileID    string
	)

	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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

		// 任务发布会话：task_results 必须和 File、Entity、容量结算在同一个
		// 事务里提交。提交成功后，即使任务检查点还没保存，重启也能查到
		// 这份结果并收敛，不会再发布第二份文件。
		if session.TaskID != nil && *session.TaskID != "" {
			result := model.TaskResult{
				TaskID:  *session.TaskID,
				OwnerID: ownerID,
				FileID:  fileID,
			}
			if err := tx.Create(&result).Error; err != nil {
				if errors.Is(err, gorm.ErrDuplicatedKey) {
					// 另一个执行者已经发布了结果：整笔新发布回滚，
					// 交给调用方查询已有结果并取消本次会话。
					return ErrTaskResultExists
				}
				return fmt.Errorf("record task result: %w", err)
			}
		}

		session.Status = model.UploadStatusCompleted
		session.ResultFileID = &fileID
		session.ReservedBytes = 0
		completed = toUploadSessionInfo(session)
		return nil
	})
	if err == nil {
		// 会话已经发布：关闭资源锁，分片账本和运行状态都不再需要。
		// 本次请求的短时保护退出后，锁才真正从管理器移除。
		s.finishCompletedUpload(sessionID)
		return completed, nil
	}

	// 事务可能已经提交但返回了错误（例如提交响应丢失）。这时不能删除内容，
	// 而是用独立 context 加锁重新核对；确认 completed 就返回已有结果。
	verified, verifyErr := s.verifyCompletedUploadSession(ctx, ownerID, sessionID)
	if verifyErr == nil && verified != nil {
		// 提交结果已经确认是 completed：可以结束锁生命周期。
		s.finishCompletedUpload(sessionID)
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

// finishCompletedUpload 收掉已经发布会话的运行期资源。
//
// 完成、幂等重放和“提交结果不确定但核对到 completed”都走这里：
// 关闭资源锁、删除分片账本、清除运行状态。这些操作都是幂等的；
// 关闭锁只标记 closing，等当前请求（以及其它在途保护区）退出后才真正移除。
func (s *Service) finishCompletedUpload(sessionID string) {
	s.closeUploadSessionLock(sessionID)
	s.chunks.remove(sessionID)
	s.uploads.forget(sessionID)
}

// prepareChunkedCompletion 是分片会话的收尾准备阶段。
//
// 调用方必须已经取得会话级收尾门禁，因此这里不会再有新的分片写入。
// 它依次做三件事：
//  1. 按内存账本核对每个合法编号都有成功记录，累计字节数等于 ExpectedSize；
//  2. 调用 FinalizeChunks 把临时对象发布成完整内容（空文件没有临时对象，跳过）；
//  3. 把 Session 推进到 ready、ActualSize/Entity.Size 对齐 ExpectedSize。
//
// 这里只确认“内容已经完整”并完成物理收尾，不创建 File、不结算容量：
// 那些仍然由后面的发布事务完成，避免另开一套发布逻辑。
//
// 任何一步失败都不删除已确认的分片、物理内容或预留容量；用户再次调用
// /complete 会重新核对并继续，即使上一次已经走到了 Finalize 之后。
func (s *Service) prepareChunkedCompletion(
	ctx context.Context,
	ownerID int,
	sessionID string,
) error {
	session, err := s.findOwnedUploadSession(ctx, ownerID, sessionID)
	if err != nil {
		return err
	}
	if session.Mode != model.UploadModeChunked {
		return nil
	}
	switch session.Status {
	case model.UploadStatusCompleted:
		return nil
	case model.UploadStatusCanceled, model.UploadStatusExpired:
		return ErrUploadSessionNotCompletable
	}
	// 到期和重启失效都不能再推进成 ready。
	if !session.ExpiresAt.After(time.Now()) {
		return ErrUploadSessionExpired
	}

	// 账本缺失时不能凭 ActualSize 或文件长度就发布：分片进度只认账本。
	// 空文件没有分片，账本即使不存在也没有任何需要核对的编号。
	uploaded, received, ok := s.chunks.snapshot(session.ID)
	if !ok {
		if session.ExpectedSize != 0 {
			return fmt.Errorf(
				"%w: chunk ledger missing for session %s",
				ErrUploadSessionNotCompletable, session.ID,
			)
		}
		uploaded, received = map[int]ChunkReceipt{}, 0
	}
	if err := verifyChunkProgress(session, uploaded, received); err != nil {
		return err
	}

	var entity model.Entity
	err = s.db.WithContext(ctx).First(&entity, session.EntityID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrUploadSessionConflict
		}
		return fmt.Errorf("query upload placeholder entity: %w", err)
	}
	if entity.Status != model.EntityStatusUploading || entity.RefCount != 0 {
		return ErrUploadSessionConflict
	}

	store, err := s.stores.Get(entity.StoragePolicyID)
	if err != nil {
		return fmt.Errorf(
			"resolve storage policy %d of upload session %s: %w",
			entity.StoragePolicyID, session.ID, err,
		)
	}
	chunkStore, ok := store.(storage.ChunkStore)
	if !ok {
		return fmt.Errorf(
			"storage policy %d does not support chunked uploads",
			entity.StoragePolicyID,
		)
	}

	// 空文件没有 PrepareChunks 产生的临时对象，发布事务会补一个 0 字节对象。
	if session.ExpectedSize > 0 {
		if err := chunkStore.FinalizeChunks(
			ctx, entity.StorageKey, session.ExpectedSize,
		); err != nil {
			return fmt.Errorf("finalize upload chunks: %w", err)
		}
	}

	return s.markChunkedUploadReady(ctx, ownerID, session, entity.ID)
}

// verifyChunkProgress 用账本确认内容完整：编号必须恰好覆盖 [0, chunkCount)，
// 每片长度必须等于几何计算值，累计字节数必须等于声明大小。
//
// 这里刻意不看文件长度：预分配的文件可能已经是总大小，但中间仍可能是空洞。
// 缺一片时返回 ErrUploadSessionNotCompletable（HTTP 409），不发布文件。
func verifyChunkProgress(
	session *model.UploadSession,
	uploaded map[int]ChunkReceipt,
	received int64,
) error {
	count, err := upload.ChunkCount(session.ExpectedSize, session.ChunkSize)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUploadSessionConflict, err)
	}
	if len(uploaded) != count {
		return fmt.Errorf(
			"%w: session %s has %d chunk receipts, want %d",
			ErrUploadSessionNotCompletable, session.ID, len(uploaded), count,
		)
	}

	var total int64
	for index := 0; index < count; index++ {
		receipt, ok := uploaded[index]
		if !ok {
			return fmt.Errorf(
				"%w: session %s is missing chunk %d/%d",
				ErrUploadSessionNotCompletable, session.ID, index, count,
			)
		}
		_, want, err := upload.ChunkAt(
			session.ExpectedSize, session.ChunkSize, index,
		)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrUploadSessionConflict, err)
		}
		if receipt.Size != want {
			return fmt.Errorf(
				"%w: session %s chunk %d has %d bytes, want %d",
				ErrUploadSessionNotCompletable, session.ID, index, receipt.Size, want,
			)
		}
		total += receipt.Size
	}
	if total != received || total != session.ExpectedSize {
		return fmt.Errorf(
			"%w: session %s has %d chunk bytes (ledger %d), want %d",
			ErrUploadSessionNotCompletable, session.ID, total, received, session.ExpectedSize,
		)
	}
	return nil
}

// markChunkedUploadReady 在短事务里把分片会话推进到发布事务要求的 ready：
// status=ready、ActualSize=ExpectedSize、Entity.Size=ExpectedSize。
//
// 它同时作为“上次收尾只完成了一半”的恢复入口：分片已经全部登记、临时对象
// 也已经 Finalize，但数据库更新失败时，Session 可能还是 pending/receiving，
// Entity.Size 也可能落后。这里按 ExpectedSize 重新对齐，重复执行是幂等的。
func (s *Service) markChunkedUploadReady(
	ctx context.Context,
	ownerID int,
	session *model.UploadSession,
	entityID int64,
) error {
	now := time.Now()

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 加锁顺序仍然和完成/取消/过期一致：User → Session → Entity。
		current, err := lockUserAndUploadSession(ctx, tx, ownerID, session.ID)
		if err != nil {
			return err
		}
		switch current.Status {
		case model.UploadStatusCompleted:
			return nil
		case model.UploadStatusCanceled, model.UploadStatusExpired:
			return ErrUploadSessionNotCompletable
		}
		if !current.ExpiresAt.After(now) {
			return ErrUploadSessionExpired
		}
		if current.Mode != model.UploadModeChunked ||
			current.ExpectedSize != session.ExpectedSize ||
			current.EntityID != entityID {
			return ErrUploadSessionConflict
		}

		var entity model.Entity
		err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND status = ? AND ref_count = 0",
				entityID, model.EntityStatusUploading).
			First(&entity).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrUploadSessionConflict
			}
			return fmt.Errorf("lock upload placeholder entity: %w", err)
		}
		if entity.Size != current.ExpectedSize {
			if err := tx.Model(&model.Entity{}).
				Where("id = ?", entity.ID).
				UpdateColumn("size", current.ExpectedSize).Error; err != nil {
				return fmt.Errorf("update upload placeholder entity: %w", err)
			}
		}

		// 已经是 ready 且大小对齐时不需要 UPDATE：MySQL 会把“值没有变化”
		// 报告成 0 行受影响，不能拿 RowsAffected 判断这次操作是否成功。
		if current.Status == model.UploadStatusReady &&
			current.ActualSize == current.ExpectedSize {
			return nil
		}

		// 分片已经全部核对通过，pending/receiving/ready 都允许推进到 ready。
		// 这是恢复路径：正常情况下最后一片到达时已经走过 receiving → ready。
		updated := tx.Model(&model.UploadSession{}).
			Where("id = ? AND owner_id = ? AND status IN ?",
				current.ID, ownerID, unfinishedUploadStatuses).
			UpdateColumns(map[string]any{
				"status":      model.UploadStatusReady,
				"actual_size": current.ExpectedSize,
			})
		if updated.Error != nil {
			return fmt.Errorf("mark upload session ready: %w", updated.Error)
		}
		if updated.RowsAffected != 1 {
			return ErrUploadSessionConflict
		}
		return nil
	})
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
