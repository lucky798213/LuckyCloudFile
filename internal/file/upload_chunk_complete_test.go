package file

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
)

// TestFinalizeGateBlocksWritesAndWaitsForInFlight 验证会话级收尾门禁：
// 取得门禁后新写入被拒绝，并且在途写入真正退出之前不会放行。
func TestFinalizeGateBlocksWritesAndWaitsForInFlight(t *testing.T) {
	controller := newUploadController()
	ctx := context.Background()

	uploadCtx, endUpload, err := controller.begin(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}

	gateReady := make(chan func(), 1)
	gateErr := make(chan error, 1)
	go func() {
		release, err := controller.beginFinalize(ctx, "s1")
		if err != nil {
			gateErr <- err
			return
		}
		gateReady <- release
	}()

	// 门禁会先取消在途写入，然后停在等待写入退出的地方。
	select {
	case <-uploadCtx.Done():
	case err := <-gateErr:
		t.Fatalf("beginFinalize: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("finalize gate did not cancel the in-flight upload")
	}

	if _, _, err := controller.begin(ctx, "s1"); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("begin during finalize error = %v, want ErrUploadSessionConflict", err)
	}
	select {
	case release := <-gateReady:
		release()
		t.Fatal("finalize gate was released before the in-flight upload exited")
	default:
	}
	if !controller.isActive("s1") {
		t.Fatal("finalizing session must still count as active for cleanup")
	}

	// 写入退出之后门禁才拿到；此时新写入仍然被挡住。
	endUpload()
	var release func()
	select {
	case release = <-gateReady:
	case err := <-gateErr:
		t.Fatalf("beginFinalize: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("finalize gate did not proceed after the upload exited")
	}
	if _, _, err := controller.begin(ctx, "s1"); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("begin while holding finalize gate error = %v, want ErrUploadSessionConflict", err)
	}

	release()
	if controller.isActive("s1") {
		t.Fatal("session still active after releasing the finalize gate")
	}
	_, endAgain, err := controller.begin(ctx, "s1")
	if err != nil {
		t.Fatalf("begin after releasing finalize gate: %v", err)
	}
	endAgain()
}

// TestChunkedCompleteAdvancesPendingSession 覆盖“所有分片已经登记，但上一次
// 收尾只完成了一半”的恢复：Session 还是 pending，ActualSize/Entity.Size 落后。
// 再次调用 complete 必须重新核对账本、推进到 ready 并发布。
func TestChunkedCompleteAdvancesPendingSession(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, chunks := chunkedContent()

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "resume.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	for index, chunk := range chunks {
		if _, err := env.service.UploadSessionChunk(
			ctx, env.ownerA.ID, info.ID, index, bytes.NewReader(chunk),
		); err != nil {
			t.Fatalf("upload chunk %d: %v", index, err)
		}
	}

	// 模拟最后一片收尾事务失败后的数据库状态：内容已经全部写入并登记，
	// 但状态退回 pending，进度和 Entity 大小停在倒数第二片。
	session := reloadSession(t, env, info.ID)
	regressed := total - int64(len(chunks[len(chunks)-1]))
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", info.ID).
		UpdateColumns(map[string]any{
			"status":      model.UploadStatusPending,
			"actual_size": regressed,
		}).Error; err != nil {
		t.Fatal(err)
	}
	if err := env.db.Model(&model.Entity{}).
		Where("id = ?", session.EntityID).
		UpdateColumn("size", regressed).Error; err != nil {
		t.Fatal(err)
	}

	completed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatalf("complete pending chunked session: %v", err)
	}
	if completed.Status != model.UploadStatusCompleted || completed.ResultFileID == nil {
		t.Fatalf("completed session = %+v", completed)
	}

	// 发布事务读取的是推进后的 Session/Entity，必须恢复成完整大小。
	stored, entity := loadUploadSessionAndEntity(t, env, info.ID)
	if stored.Status != model.UploadStatusCompleted ||
		stored.ActualSize != total ||
		stored.ReservedBytes != 0 {
		t.Fatalf("stored session after completion = %+v", stored)
	}
	if entity.Status != model.EntityStatusActive ||
		entity.RefCount != 1 || entity.Size != total {
		t.Fatalf("stored entity after completion = %+v", entity)
	}

	_, reader, err := env.service.Open(ctx, env.ownerA.ID, *completed.ResultFileID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	if !bytes.Equal(got, bytes.Join(chunks, nil)) {
		t.Fatal("completed content does not match uploaded chunks")
	}

	user := reloadUser(t, env, env.ownerA.ID)
	if user.ReservedBytes != 0 || user.UsedBytes != total {
		t.Fatalf("quota after completion: reserved=%d used=%d, want 0/%d",
			user.ReservedBytes, user.UsedBytes, total)
	}
	if _, ok := env.service.chunks.received(info.ID); ok {
		t.Fatal("chunk ledger retained after completion")
	}
}

// TestChunkedCompleteRejectsMissingChunkEvenWhenSizesMatch 验证“缺一片就不能完成”：
// 即使 Session/Entity 的大小已经被写成总大小，只要账本缺一个编号，complete
// 也必须返回冲突，并且不删除已经确认的分片、内容或预留容量。
func TestChunkedCompleteRejectsMissingChunkEvenWhenSizesMatch(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, chunks := chunkedContent()

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "hole.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	// 故意跳过中间一片：首片和尾片都成功。
	for _, index := range []int{0, 2} {
		if _, err := env.service.UploadSessionChunk(
			ctx, env.ownerA.ID, info.ID, index, bytes.NewReader(chunks[index]),
		); err != nil {
			t.Fatalf("upload chunk %d: %v", index, err)
		}
	}
	received, ok := env.service.chunks.received(info.ID)
	if !ok || received != total-int64(len(chunks[1])) {
		t.Fatalf("ledger after skipping chunk 1 = (%d, %v)", received, ok)
	}

	// 模拟异常数据：长度看起来已经够了，但账本里没有编号 1。
	session := reloadSession(t, env, info.ID)
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", info.ID).
		UpdateColumns(map[string]any{
			"status":      model.UploadStatusReady,
			"actual_size": total,
		}).Error; err != nil {
		t.Fatal(err)
	}
	if err := env.db.Model(&model.Entity{}).
		Where("id = ?", session.EntityID).
		UpdateColumn("size", total).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := env.service.CompleteUploadSession(
		ctx, env.ownerA.ID, info.ID,
	); !errors.Is(err, ErrUploadSessionNotCompletable) {
		t.Fatalf("completion with missing chunk error = %v, want ErrUploadSessionNotCompletable", err)
	}

	// 没有发布文件，会话和占位内容保持原样，预留容量没有释放。
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("missing chunk completion created %d file(s)", got)
	}
	stored, entity := loadUploadSessionAndEntity(t, env, info.ID)
	if stored.Status != model.UploadStatusReady ||
		stored.ResultFileID != nil ||
		stored.ReservedBytes != total {
		t.Fatalf("missing chunk completion changed session: %+v", stored)
	}
	if entity.Status != model.EntityStatusUploading ||
		entity.RefCount != 0 || entity.Size != total {
		t.Fatalf("missing chunk completion changed entity: %+v", entity)
	}
	if got, ok := env.service.chunks.received(info.ID); !ok || got != received {
		t.Fatalf("missing chunk completion changed ledger: (%d, %v), want (%d, true)",
			got, ok, received)
	}
	user := reloadUser(t, env, env.ownerA.ID)
	if user.ReservedBytes != total || user.UsedBytes != 0 {
		t.Fatalf("quota after rejected completion: reserved=%d used=%d",
			user.ReservedBytes, user.UsedBytes)
	}
}

// failOnceFinalizeStore 让第一次 FinalizeChunks 失败，之后恢复正常。
// 用来复现“最后一片已经写入并登记，但物理收尾失败”的场景。
type failOnceFinalizeStore struct {
	storage.ChunkStore
	fail bool
}

func (s *failOnceFinalizeStore) FinalizeChunks(
	ctx context.Context,
	key string,
	totalSize int64,
) error {
	if s.fail {
		s.fail = false
		return errors.New("forced finalize failure")
	}
	return s.ChunkStore.FinalizeChunks(ctx, key, totalSize)
}

// TestChunkedCompleteRetriesAfterFinalizeFailure 验证物理收尾失败后：
// 成功分片、磁盘内容和预留容量全部保留；再次调用 complete 会重新核对账本、
// 重新 FinalizeChunks，并完成发布。
func TestChunkedCompleteRetriesAfterFinalizeFailure(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, chunks := chunkedContent()

	store := &failOnceFinalizeStore{
		ChunkStore: storage.NewLocal(env.rootA),
		fail:       true,
	}
	registry := storage.NewRegistry()
	if err := registry.Register(env.policyA.ID, store); err != nil {
		t.Fatal(err)
	}
	svc := NewService(env.db, registry)

	info, err := svc.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "retry.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	for index, chunk := range chunks {
		_, err := svc.UploadSessionChunk(
			ctx, env.ownerA.ID, info.ID, index, bytes.NewReader(chunk),
		)
		if index < len(chunks)-1 {
			if err != nil {
				t.Fatalf("upload chunk %d: %v", index, err)
			}
			continue
		}
		// 最后一片：内容已经写入并登记，但 FinalizeChunks 失败。
		if err == nil {
			t.Fatal("last chunk succeeded despite forced finalize failure")
		}
	}

	// 失败只影响本次收尾，不删除分片或容量；状态保持 receiving，
	// 下一次 /complete 会重新核对账本并继续。
	if got, ok := svc.chunks.received(info.ID); !ok || got != total {
		t.Fatalf("ledger after finalize failure = (%d, %v), want (%d, true)", got, ok, total)
	}
	failed, entity := loadUploadSessionAndEntity(t, env, info.ID)
	if failed.Status != model.UploadStatusReceiving || failed.ResultFileID != nil {
		t.Fatalf("session after finalize failure = %+v", failed)
	}
	// 进度在收尾之前已经同步到账本累计值；Finalize 失败只影响 ready，
	// 占位实体仍然是 uploading/0，还没有被发布。
	if entity.Status != model.EntityStatusUploading ||
		entity.RefCount != 0 || entity.Size != total {
		t.Fatalf("entity after finalize failure = %+v", entity)
	}
	user := reloadUser(t, env, env.ownerA.ID)
	if user.ReservedBytes != total || user.UsedBytes != 0 {
		t.Fatalf("quota after finalize failure: reserved=%d used=%d",
			user.ReservedBytes, user.UsedBytes)
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("finalize failure created %d file(s)", got)
	}

	// 同一个会话再调用 complete：应当从账本重新核对、重新 Finalize 并发布。
	completed, err := svc.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatalf("retry complete after finalize failure: %v", err)
	}
	if completed.Status != model.UploadStatusCompleted || completed.ResultFileID == nil {
		t.Fatalf("completed session = %+v", completed)
	}

	_, reader, err := svc.Open(ctx, env.ownerA.ID, *completed.ResultFileID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	if !bytes.Equal(got, bytes.Join(chunks, nil)) {
		t.Fatal("content after retry does not match uploaded chunks")
	}
	user = reloadUser(t, env, env.ownerA.ID)
	if user.ReservedBytes != 0 || user.UsedBytes != total {
		t.Fatalf("quota after retry completion: reserved=%d used=%d, want 0/%d",
			user.ReservedBytes, user.UsedBytes, total)
	}
}

// TestChunkedCompleteConcurrent 验证并发 /complete 在收尾门禁上排队：
// 只发布一份文件，所有调用都返回同一个结果文件 ID。
func TestChunkedCompleteConcurrent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total := UploadChunkSize
	data := bytes.Repeat([]byte("a"), int(total))

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "race.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(data),
	); err != nil {
		t.Fatal(err)
	}

	const callers = 4
	results := make([]UploadSessionInfo, callers)
	errs := make([]error, callers)
	start := make(chan struct{})
	done := make(chan struct{}, callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			<-start
			results[i], errs[i] = env.service.CompleteUploadSession(
				ctx, env.ownerA.ID, info.ID,
			)
		}(i)
	}
	close(start)
	for i := 0; i < callers; i++ {
		<-done
	}

	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d failed: %v", i, err)
		}
		if results[i].ResultFileID == nil ||
			*results[i].ResultFileID != *results[0].ResultFileID {
			t.Fatalf("caller %d returned %+v, want file %q",
				i, results[i], *results[0].ResultFileID)
		}
	}
	if got := countKind(t, env.db, model.KindFile); got != 1 {
		t.Fatalf("concurrent completions created %d files, want 1", got)
	}
}

// TestChunkedCompleteEmptySessionWithoutLedger 验证空文件没有分片可核对：
// 即使内存账本不在了，/complete 仍然可以走发布事务补一个 0 字节对象。
func TestChunkedCompleteEmptySessionWithoutLedger(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "empty.bin", 0, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	env.service.chunks.remove(info.ID)

	completed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatalf("complete empty chunked session without ledger: %v", err)
	}
	if completed.Status != model.UploadStatusCompleted || completed.ResultFileID == nil {
		t.Fatalf("completed session = %+v", completed)
	}

	record, reader, err := env.service.Open(ctx, env.ownerA.ID, *completed.ResultFileID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	if record.Size != 0 || len(got) != 0 {
		t.Fatalf("completed empty file size=%d content_length=%d, want 0/0",
			record.Size, len(got))
	}
}

// TestChunkedChunkRetryWhileReceiving 验证 receiving 是正常状态：
// 某个分片失败后会话保持 receiving；没有活跃写入时，下一次请求直接继续
// 写入即可，不需要先把状态“恢复”成 pending。
func TestChunkedChunkRetryWhileReceiving(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total := UploadChunkSize
	data := bytes.Repeat([]byte("a"), int(total))

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "stuck.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}

	broken := &readThenError{data: bytes.Repeat([]byte("x"), 1024)}
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, broken,
	); err == nil {
		t.Fatal("chunk with interrupted read succeeded")
	}

	// 失败不退回 pending：receiving 就是正常的分片写入状态。
	stuck := reloadSession(t, env, info.ID)
	if stuck.Status != model.UploadStatusReceiving || stuck.ActualSize != 0 {
		t.Fatalf("session after read error = %+v, want receiving/0", stuck)
	}
	if got, ok := env.service.chunks.received(info.ID); !ok || got != 0 {
		t.Fatalf("ledger after read error = (%d, %v)", got, ok)
	}

	// 没有活跃写入，重传同一片直接写入。
	progress, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(data),
	)
	if err != nil {
		t.Fatalf("retry chunk while receiving: %v", err)
	}
	if progress.Status != model.UploadStatusReady || progress.ActualSize != total {
		t.Fatalf("retry progress = %+v", progress)
	}
	completed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, reader, err := env.service.Open(ctx, env.ownerA.ID, *completed.ResultFileID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("retried content does not match")
	}
}

// TestChunkedConfirmedChunkMismatchIsConflict 区分两种长度错误：
// 已确认编号的长度/摘要不一致是 409，未确认编号的长度错误才是 400。
func TestChunkedConfirmedChunkMismatchIsConflict(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, chunks := chunkedContent()

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "mismatch.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(chunks[0]),
	); err != nil {
		t.Fatal(err)
	}

	// 已确认编号短传：不能因为长度不对就返回 400，必须是冲突。
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(chunks[0][:100]),
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("confirmed short retry error = %v, want ErrUploadSessionConflict", err)
	}
	// 已确认编号同长度不同摘要：冲突。
	different := bytes.Repeat([]byte("z"), len(chunks[0]))
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(different),
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("confirmed digest mismatch error = %v, want ErrUploadSessionConflict", err)
	}
	// 未确认编号短传：才是长度不匹配（400）。
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 1, bytes.NewReader(chunks[1][:100]),
	); !errors.Is(err, ErrUploadSizeMismatch) {
		t.Fatalf("unconfirmed short retry error = %v, want ErrUploadSizeMismatch", err)
	}

	if got, ok := env.service.chunks.received(info.ID); !ok || got != int64(len(chunks[0])) {
		t.Fatalf("ledger changed by conflicting retries: (%d, %v)", got, ok)
	}
	if got := reloadSession(t, env, info.ID).Status; got != model.UploadStatusReceiving {
		t.Fatalf("session status after conflicting retries = %q, want receiving", got)
	}

	// 继续上传其余分片，最终内容必须仍然是原始顺序。
	for _, index := range []int{1, 2} {
		if _, err := env.service.UploadSessionChunk(
			ctx, env.ownerA.ID, info.ID, index, bytes.NewReader(chunks[index]),
		); err != nil {
			t.Fatalf("upload chunk %d: %v", index, err)
		}
	}
	completed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, reader, err := env.service.Open(ctx, env.ownerA.ID, *completed.ResultFileID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	if !bytes.Equal(got, bytes.Join(chunks, nil)) {
		t.Fatal("content changed by conflicting retries")
	}
}

// failOnceChunkWriteStore 在指定偏移写入半片后失败一次，之后恢复正常。
type failOnceChunkWriteStore struct {
	storage.ChunkStore
	failOffset int64
	fail       bool
}

func (s *failOnceChunkWriteStore) WriteChunkAt(
	ctx context.Context,
	key string,
	offset int64,
	data []byte,
) error {
	if s.fail && offset == s.failOffset {
		s.fail = false
		half := len(data) / 2
		if err := s.ChunkStore.WriteChunkAt(ctx, key, offset, data[:half]); err != nil {
			return err
		}
		return errors.New("forced partial chunk write failure")
	}
	return s.ChunkStore.WriteChunkAt(ctx, key, offset, data)
}

// TestChunkedRetryAfterPartialWriteFailure 覆盖某片写到一半失败：
// 只重传失败片，之前成功的分片和内容必须保留，最终内容正确。
func TestChunkedRetryAfterPartialWriteFailure(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, chunks := chunkedContent()

	store := &failOnceChunkWriteStore{
		ChunkStore: storage.NewLocal(env.rootA),
		failOffset: UploadChunkSize,
		fail:       true,
	}
	registry := storage.NewRegistry()
	if err := registry.Register(env.policyA.ID, store); err != nil {
		t.Fatal(err)
	}
	svc := NewService(env.db, registry)

	info, err := svc.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "partial.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(chunks[0]),
	); err != nil {
		t.Fatalf("upload chunk 0: %v", err)
	}

	// 第 1 片写到一半失败：不登记，之前成功的第 0 片不能被清掉。
	if _, err := svc.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 1, bytes.NewReader(chunks[1]),
	); err == nil {
		t.Fatal("partial chunk write succeeded")
	}
	if got, ok := svc.chunks.received(info.ID); !ok || got != int64(len(chunks[0])) {
		t.Fatalf("ledger after partial write = (%d, %v), want (%d, true)",
			got, ok, len(chunks[0]))
	}
	if got := reloadSession(t, env, info.ID).Status; got != model.UploadStatusReceiving {
		t.Fatalf("status after partial write = %q, want receiving", got)
	}

	// 只重传第 1 片，再补第 2 片。
	for _, index := range []int{1, 2} {
		if _, err := svc.UploadSessionChunk(
			ctx, env.ownerA.ID, info.ID, index, bytes.NewReader(chunks[index]),
		); err != nil {
			t.Fatalf("retry chunk %d: %v", index, err)
		}
	}
	completed, err := svc.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, reader, err := svc.Open(ctx, env.ownerA.ID, *completed.ResultFileID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	if !bytes.Equal(got, bytes.Join(chunks, nil)) {
		t.Fatal("content after partial write retry does not match")
	}
}

// TestChunkedCompleteRetriesPublishTransaction 覆盖最终发布事务首次失败：
// 事务回滚后再次 complete 只能生成一份 File，容量只结算一次。
func TestChunkedCompleteRetriesPublishTransaction(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, chunks := chunkedContent()

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "publish-retry.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	for index, chunk := range chunks {
		if _, err := env.service.UploadSessionChunk(
			ctx, env.ownerA.ID, info.ID, index, bytes.NewReader(chunk),
		); err != nil {
			t.Fatalf("upload chunk %d: %v", index, err)
		}
	}

	// 分片已经 ready，准备阶段不会再更新 upload_sessions；
	// 因此这里第一次命中的就是发布事务里的 completed 更新。
	const callbackName = "test:fail_publish_once"
	failed := false
	if err := env.db.Callback().Update().Before("gorm:update").Register(
		callbackName,
		func(tx *gorm.DB) {
			if tx.Statement.Schema == nil ||
				tx.Statement.Schema.Table != (model.UploadSession{}).TableName() {
				return
			}
			if failed {
				return
			}
			failed = true
			_ = tx.AddError(errors.New("forced publish failure"))
		},
	); err != nil {
		t.Fatal(err)
	}

	if _, err := env.service.CompleteUploadSession(
		ctx, env.ownerA.ID, info.ID,
	); err == nil {
		t.Fatal("wanted the first publish transaction to fail")
	}
	if !failed {
		t.Fatal("publish callback was not reached")
	}
	if err := env.db.Callback().Update().Remove(callbackName); err != nil {
		t.Fatal(err)
	}

	// 事务整体回滚：没有 File，容量没有结算，会话仍然是 ready。
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("failed publish left %d file(s)", got)
	}
	user := reloadUser(t, env, env.ownerA.ID)
	if user.ReservedBytes != total || user.UsedBytes != 0 {
		t.Fatalf("quota after failed publish: reserved=%d used=%d",
			user.ReservedBytes, user.UsedBytes)
	}
	if got := reloadSession(t, env, info.ID).Status; got != model.UploadStatusReady {
		t.Fatalf("session after failed publish = %q, want ready", got)
	}

	completed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatalf("retry complete: %v", err)
	}
	if completed.ResultFileID == nil {
		t.Fatalf("retry complete returned no file: %+v", completed)
	}
	fileID := *completed.ResultFileID

	// 重复完成仍然是幂等重放，不能再次结算或再建 File。
	again, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.ResultFileID == nil || *again.ResultFileID != fileID {
		t.Fatalf("repeat complete returned %+v, want %q", again, fileID)
	}
	if got := countKind(t, env.db, model.KindFile); got != 1 {
		t.Fatalf("completion retry created %d files, want 1", got)
	}
	user = reloadUser(t, env, env.ownerA.ID)
	if user.ReservedBytes != 0 || user.UsedBytes != total {
		t.Fatalf("quota after retry: reserved=%d used=%d, want 0/%d",
			user.ReservedBytes, user.UsedBytes, total)
	}
}

// TestCancelChunkedUploadDuringWriteCleansAfterExit 覆盖取消与实际分片写入并发：
// 取消先落库，但内容必须等写入退出后才清理。
func TestCancelChunkedUploadDuringWriteCleansAfterExit(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total := UploadChunkSize
	data := bytes.Repeat([]byte("a"), int(total))

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "cancel-writing.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, placeholder := loadUploadSessionAndEntity(t, env, info.ID)
	contentPath := filepath.Join(env.rootA, placeholder.StorageKey)

	reader := &blockingUploadReader{
		data:    data,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(reader.release) }) }
	t.Cleanup(release)

	uploadDone := make(chan error, 1)
	go func() {
		_, err := env.service.UploadSessionChunk(
			ctx, env.ownerA.ID, info.ID, 0, reader,
		)
		uploadDone <- err
	}()

	select {
	case <-reader.started:
	case <-time.After(5 * time.Second):
		t.Fatal("chunk upload did not start reading")
	}
	if got := reloadSession(t, env, info.ID).Status; got != model.UploadStatusReceiving {
		t.Fatalf("session status while writing = %q, want receiving", got)
	}

	canceled, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status != model.UploadStatusCanceled {
		t.Fatalf("canceled info = %+v", canceled)
	}
	// 写入还阻塞在 Read 中：内容不能被提前删除，账本也不能先丢。
	if entity := mustEntity(t, env, placeholder.ID); entity.Status != model.EntityStatusUploading {
		t.Fatalf("entity was cleaned before the chunk writer exited: %+v", entity)
	}
	if _, ok := env.service.chunks.received(info.ID); !ok {
		t.Fatal("chunk ledger removed before the writer exited")
	}

	release()
	select {
	case err := <-uploadDone:
		if err == nil {
			t.Fatal("canceled chunk write succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled chunk write did not exit")
	}

	assertEntityGone(t, env, placeholder.ID)
	assertNoUploadContent(t, contentPath)
	if entries := diskEntries(t, env.rootA); len(entries) != 0 {
		t.Fatalf("canceled chunked write left content on disk: %v", entries)
	}
	if _, ok := env.service.chunks.received(info.ID); ok {
		t.Fatal("chunk ledger retained after canceled write exited")
	}
	if got := loadSessionOnly(t, env, info.ID).Status; got != model.UploadStatusCanceled {
		t.Fatalf("final session status = %q, want canceled", got)
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("canceled chunked write created %d file(s)", got)
	}
}

// TestExpireChunkedUploadDuringWriteCleansAfterExit 覆盖过期与实际分片写入并发：
// 过期先落库，等写入退出后才清理内容。
func TestExpireChunkedUploadDuringWriteCleansAfterExit(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total := UploadChunkSize
	data := bytes.Repeat([]byte("b"), int(total))

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "expire-writing.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	session := reloadSession(t, env, info.ID)
	_, placeholder := loadUploadSessionAndEntity(t, env, info.ID)
	contentPath := filepath.Join(env.rootA, placeholder.StorageKey)

	reader := &blockingUploadReader{
		data:    data,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(reader.release) }) }
	t.Cleanup(release)

	uploadDone := make(chan error, 1)
	go func() {
		_, err := env.service.UploadSessionChunk(
			ctx, env.ownerA.ID, info.ID, 0, reader,
		)
		uploadDone <- err
	}()

	select {
	case <-reader.started:
	case <-time.After(5 * time.Second):
		t.Fatal("chunk upload did not start reading")
	}

	if err := env.service.ExpireUploadSessions(
		ctx, expiryTestTimeAfter(session), 10,
	); err != nil {
		t.Fatal(err)
	}
	if got := loadSessionOnly(t, env, info.ID).Status; got != model.UploadStatusExpired {
		t.Fatalf("session status = %q, want expired", got)
	}
	if entity := mustEntity(t, env, placeholder.ID); entity.Status != model.EntityStatusUploading {
		t.Fatalf("entity was cleaned before the chunk writer exited: %+v", entity)
	}
	if _, ok := env.service.chunks.received(info.ID); !ok {
		t.Fatal("chunk ledger removed before the writer exited")
	}

	release()
	select {
	case err := <-uploadDone:
		if err == nil {
			t.Fatal("expired chunk write succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("expired chunk write did not exit")
	}

	assertEntityGone(t, env, placeholder.ID)
	assertNoUploadContent(t, contentPath)
	if entries := diskEntries(t, env.rootA); len(entries) != 0 {
		t.Fatalf("expired chunked write left content on disk: %v", entries)
	}
	if _, ok := env.service.chunks.received(info.ID); ok {
		t.Fatal("chunk ledger retained after expired write exited")
	}
	if got := loadSessionOnly(t, env, info.ID).Status; got != model.UploadStatusExpired {
		t.Fatalf("final session status = %q, want expired", got)
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("expired chunked write created %d file(s)", got)
	}
}

// pauseSecondFinalizeStore 放行分片写入时的第一次 Finalize，
// 暂停 /complete 准备阶段的第二次 Finalize，用来制造“收尾进行中”的窗口。
type pauseSecondFinalizeStore struct {
	storage.ChunkStore
	calls   int
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *pauseSecondFinalizeStore) FinalizeChunks(
	ctx context.Context,
	key string,
	totalSize int64,
) error {
	s.calls++
	if s.calls == 2 {
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.ChunkStore.FinalizeChunks(ctx, key, totalSize)
}

// TestCancelChunkedCompleteDuringFinalizeCleansAfterExit 覆盖取消与 Finalize 并发：
// 取消先落库，但不能在 Finalize 退出前清理内容；完成请求退出后再收尾。
func TestCancelChunkedCompleteDuringFinalizeCleansAfterExit(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total := UploadChunkSize
	data := bytes.Repeat([]byte("c"), int(total))

	store := &pauseSecondFinalizeStore{
		ChunkStore: storage.NewLocal(env.rootA),
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	registry := storage.NewRegistry()
	if err := registry.Register(env.policyA.ID, store); err != nil {
		t.Fatal(err)
	}
	svc := NewService(env.db, registry)

	info, err := svc.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "cancel-finalize.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(data),
	); err != nil {
		t.Fatal(err)
	}
	_, placeholder := loadUploadSessionAndEntity(t, env, info.ID)

	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(store.release) }) }
	t.Cleanup(release)

	completeDone := make(chan error, 1)
	go func() {
		_, err := svc.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
		completeDone <- err
	}()

	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("complete did not reach FinalizeChunks")
	}

	canceled, err := svc.CancelUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status != model.UploadStatusCanceled {
		t.Fatalf("canceled info = %+v", canceled)
	}
	// Finalize 还在进行：Entity 和账本都不能先被清掉。
	if entity := mustEntity(t, env, placeholder.ID); entity.Status != model.EntityStatusUploading {
		t.Fatalf("entity was cleaned while Finalize was running: %+v", entity)
	}
	if _, ok := svc.chunks.received(info.ID); !ok {
		t.Fatal("chunk ledger removed while Finalize was running")
	}

	release()
	select {
	case err := <-completeDone:
		if err == nil {
			t.Fatal("complete succeeded after concurrent cancel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("complete did not exit after Finalize was released")
	}

	assertEntityGone(t, env, placeholder.ID)
	if entries := diskEntries(t, env.rootA); len(entries) != 0 {
		t.Fatalf("canceled finalize left content on disk: %v", entries)
	}
	if _, ok := svc.chunks.received(info.ID); ok {
		t.Fatal("chunk ledger retained after canceled finalize")
	}
	if got := loadSessionOnly(t, env, info.ID).Status; got != model.UploadStatusCanceled {
		t.Fatalf("final session status = %q, want canceled", got)
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("canceled finalize created %d file(s)", got)
	}
}

// TestChunkedUploadRestartInvalidatesUnfinishedSession 覆盖分片会话的重启失效：
// 新 Service 的账本是空的；旧会话仍必须被作废，不能继续上传或完成。
func TestChunkedUploadRestartInvalidatesUnfinishedSession(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, chunks := chunkedContent()

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "restart.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(chunks[0]),
	); err != nil {
		t.Fatal(err)
	}

	// 模拟进程重启：新 Service 使用空的内存账本。
	registry := storage.NewRegistry()
	if err := registry.Register(env.policyA.ID, storage.NewLocal(env.rootA)); err != nil {
		t.Fatal(err)
	}
	restarted := NewService(env.db, registry)

	if err := restarted.InvalidateUnfinishedUploads(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if _, ok := restarted.chunks.received(info.ID); ok {
		t.Fatal("restarted service inherited the old chunk ledger")
	}
	session := reloadSession(t, env, info.ID)
	if session.Status != model.UploadStatusExpired || session.ReservedBytes != 0 {
		t.Fatalf("session after restart invalidation = %+v", session)
	}

	// 未完成的分片仍然可以“上传”：必须被 410 拒绝，而不是因为账本为空就报冲突。
	if _, err := restarted.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 1, bytes.NewReader(chunks[1]),
	); !errors.Is(err, ErrUploadSessionExpired) {
		t.Fatalf("upload after restart error = %v, want ErrUploadSessionExpired", err)
	}
	if _, err := restarted.CompleteUploadSession(
		ctx, env.ownerA.ID, info.ID,
	); err == nil {
		t.Fatal("complete after restart invalidation succeeded")
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("restart invalidation published %d file(s)", got)
	}

	// 清理链回收占位实体和临时内容。
	if _, err := restarted.CleanupPending(ctx, 10); err != nil {
		t.Fatal(err)
	}
	assertEntityGone(t, env, session.EntityID)
	if entries := diskEntries(t, env.rootA); len(entries) != 0 {
		t.Fatalf("restart cleanup left content on disk: %v", entries)
	}
}

// TestExpireChunkedCompleteDuringFinalizeCleansAfterExit 覆盖过期与 Finalize 并发：
// 过期先落库，但不能在 Finalize 退出前清理；完成请求退出后再收尾。
func TestExpireChunkedCompleteDuringFinalizeCleansAfterExit(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total := UploadChunkSize
	data := bytes.Repeat([]byte("d"), int(total))

	store := &pauseSecondFinalizeStore{
		ChunkStore: storage.NewLocal(env.rootA),
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	registry := storage.NewRegistry()
	if err := registry.Register(env.policyA.ID, store); err != nil {
		t.Fatal(err)
	}
	svc := NewService(env.db, registry)

	info, err := svc.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "expire-finalize.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(data),
	); err != nil {
		t.Fatal(err)
	}
	session := reloadSession(t, env, info.ID)
	_, placeholder := loadUploadSessionAndEntity(t, env, info.ID)

	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(store.release) }) }
	t.Cleanup(release)

	completeDone := make(chan error, 1)
	go func() {
		_, err := svc.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
		completeDone <- err
	}()

	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("complete did not reach FinalizeChunks")
	}

	if err := svc.ExpireUploadSessions(
		ctx, expiryTestTimeAfter(session), 10,
	); err != nil {
		t.Fatal(err)
	}
	if got := loadSessionOnly(t, env, info.ID).Status; got != model.UploadStatusExpired {
		t.Fatalf("session status = %q, want expired", got)
	}
	// Finalize 还在进行：Entity 和账本都不能先被清掉。
	if entity := mustEntity(t, env, placeholder.ID); entity.Status != model.EntityStatusUploading {
		t.Fatalf("entity was cleaned while Finalize was running: %+v", entity)
	}
	if _, ok := svc.chunks.received(info.ID); !ok {
		t.Fatal("chunk ledger removed while Finalize was running")
	}

	release()
	select {
	case err := <-completeDone:
		if err == nil {
			t.Fatal("complete succeeded after concurrent expiry")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("complete did not exit after Finalize was released")
	}

	assertEntityGone(t, env, placeholder.ID)
	if entries := diskEntries(t, env.rootA); len(entries) != 0 {
		t.Fatalf("expired finalize left content on disk: %v", entries)
	}
	if _, ok := svc.chunks.received(info.ID); ok {
		t.Fatal("chunk ledger retained after expired finalize")
	}
	if got := loadSessionOnly(t, env, info.ID).Status; got != model.UploadStatusExpired {
		t.Fatalf("final session status = %q, want expired", got)
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("expired finalize created %d file(s)", got)
	}
}
