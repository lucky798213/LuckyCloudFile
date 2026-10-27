package file

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
)

// notReadReader 记录是否有人读过请求体。用于证明冲突请求在读取 body 之前
// 就被拒绝。
type notReadReader struct {
	read chan struct{}
}

func newNotReadReader() *notReadReader {
	return &notReadReader{read: make(chan struct{}, 1)}
}

func (r *notReadReader) Read(p []byte) (int, error) {
	select {
	case r.read <- struct{}{}:
	default:
	}
	return 0, io.EOF
}

func (r *notReadReader) wasRead() bool {
	select {
	case <-r.read:
		return true
	default:
		return false
	}
}

type chunkUploadResult struct {
	info UploadSessionInfo
	err  error
}

func startChunkUpload(
	service *Service,
	ownerID int,
	sessionID string,
	index int,
	r io.Reader,
) <-chan chunkUploadResult {
	done := make(chan chunkUploadResult, 1)
	go func() {
		info, err := service.UploadSessionChunk(
			context.Background(), ownerID, sessionID, index, r,
		)
		done <- chunkUploadResult{info: info, err: err}
	}()
	return done
}

func waitStarted(t *testing.T, readers ...*blockingUploadReader) {
	t.Helper()

	for _, reader := range readers {
		select {
		case <-reader.started:
		case <-time.After(5 * time.Second):
			t.Fatal("chunk upload did not start reading")
		}
	}
}

func waitChunkResult(t *testing.T, done <-chan chunkUploadResult) chunkUploadResult {
	t.Helper()

	select {
	case result := <-done:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("chunk upload did not exit")
		return chunkUploadResult{}
	}
}

// TestChunkedUploadParallelChunksAndLimit 覆盖并行分片的完整链路：
//   - 不同编号可以同时进入读取阶段；
//   - 同编号的第二个请求、超过并行度的请求都在读取 body 之前被拒绝；
//   - 并行写不同偏移后，整文件内容和 SHA-256 正确；
//   - 最后一片仍然自动推进到 ready。
func TestChunkedUploadParallelChunksAndLimit(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, chunks := chunkedContent()

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "parallel.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}

	reader0 := &blockingUploadReader{
		data:    chunks[0],
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	reader1 := &blockingUploadReader{
		data:    chunks[1],
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(reader0.release)
			close(reader1.release)
		})
	}
	t.Cleanup(release)

	done0 := startChunkUpload(env.service, env.ownerA.ID, info.ID, 0, reader0)
	done1 := startChunkUpload(env.service, env.ownerA.ID, info.ID, 1, reader1)
	waitStarted(t, reader0, reader1)

	// 两个不同编号已经占满并行名额：同一编号的第二个请求被拒绝。
	sameIndex := newNotReadReader()
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, sameIndex,
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("same-index request error = %v, want ErrUploadSessionConflict", err)
	}
	if sameIndex.wasRead() {
		t.Fatal("same-index request read the body before being rejected")
	}

	// 第 N+1 个请求也被拒绝，而且不读 body。
	overLimit := newNotReadReader()
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 2, overLimit,
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("over-limit request error = %v, want ErrUploadSessionConflict", err)
	}
	if overLimit.wasRead() {
		t.Fatal("over-limit request read the body before being rejected")
	}

	release()
	result0 := waitChunkResult(t, done0)
	result1 := waitChunkResult(t, done1)
	if result0.err != nil {
		t.Fatalf("parallel chunk 0: %v", result0.err)
	}
	if result1.err != nil {
		t.Fatalf("parallel chunk 1: %v", result1.err)
	}

	// 两片都已登记，但还差最后一片：会话保持 receiving，不能提前 ready。
	progress := reloadSession(t, env, info.ID)
	if progress.Status != model.UploadStatusReceiving {
		t.Fatalf("status after two parallel chunks = %q, want receiving", progress.Status)
	}
	if progress.ActualSize != 2*UploadChunkSize {
		t.Fatalf("actual_size after two parallel chunks = %d, want %d",
			progress.ActualSize, 2*UploadChunkSize)
	}

	// 最后一片到达后自动收尾。这里同时验证不会等待自己而卡死。
	final, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 2, bytes.NewReader(chunks[2]),
	)
	if err != nil {
		t.Fatalf("upload final chunk: %v", err)
	}
	if final.Status != model.UploadStatusReady || final.ActualSize != total {
		t.Fatalf("final progress = %+v", final)
	}

	// 不同偏移并行写入后，整文件内容必须和原始顺序完全一致。
	session := reloadSession(t, env, info.ID)
	var entity model.Entity
	if err := env.db.First(&entity, session.EntityID).Error; err != nil {
		t.Fatal(err)
	}
	disk, err := os.ReadFile(filepath.Join(env.rootA, entity.StorageKey))
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Join(chunks, nil)
	if !bytes.Equal(disk, want) {
		t.Fatal("parallel chunk content does not match the original bytes")
	}

	completed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatalf("complete parallel session: %v", err)
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
	if !bytes.Equal(got, want) {
		t.Fatal("published parallel content does not match the original bytes")
	}
}

// TestCancelChunkedUploadWithTwoInFlight 验证取消会通知全部在途分片，
// 但物理清理必须等最后一个请求真正退出。
func TestCancelChunkedUploadWithTwoInFlight(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, chunks := chunkedContent()

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "cancel-parallel.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, placeholder := loadUploadSessionAndEntity(t, env, info.ID)
	contentPath := filepath.Join(env.rootA, placeholder.StorageKey)

	reader0 := &blockingUploadReader{
		data:    chunks[0],
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	reader1 := &blockingUploadReader{
		data:    chunks[1],
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	var release0Once, release1Once sync.Once
	release0 := func() { release0Once.Do(func() { close(reader0.release) }) }
	release1 := func() { release1Once.Do(func() { close(reader1.release) }) }
	t.Cleanup(func() {
		release0()
		release1()
	})

	done0 := startChunkUpload(env.service, env.ownerA.ID, info.ID, 0, reader0)
	done1 := startChunkUpload(env.service, env.ownerA.ID, info.ID, 1, reader1)
	waitStarted(t, reader0, reader1)

	canceled, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status != model.UploadStatusCanceled {
		t.Fatalf("canceled info = %+v", canceled)
	}
	// 两个请求都还阻塞在 Read 里：内容、实体和账本都不能提前清理。
	if entity := mustEntity(t, env, placeholder.ID); entity.Status != model.EntityStatusUploading {
		t.Fatalf("entity was cleaned before the chunk writers exited: %+v", entity)
	}
	if _, ok := env.service.chunks.received(info.ID); !ok {
		t.Fatal("chunk ledger removed before the writers exited")
	}

	// 先只放行第一个请求：它退出后第二个仍然阻塞在 Read 里，
	// 所以物理清理必须继续等待。
	release0()
	select {
	case result := <-done0:
		if result.err == nil {
			t.Fatal("canceled chunk 0 succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("chunk 0 did not exit")
	}
	// 第一个退出后还有一个在途请求：仍然不能清理。
	if entity := mustEntity(t, env, placeholder.ID); entity.Status != model.EntityStatusUploading {
		t.Fatalf("entity was cleaned before the last writer exited: %+v", entity)
	}

	release1()
	select {
	case result := <-done1:
		if result.err == nil {
			t.Fatal("canceled chunk 1 succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("chunk 1 did not exit")
	}

	assertEntityGone(t, env, placeholder.ID)
	assertNoUploadContent(t, contentPath)
	if entries := diskEntries(t, env.rootA); len(entries) != 0 {
		t.Fatalf("canceled parallel write left content on disk: %v", entries)
	}
	if _, ok := env.service.chunks.received(info.ID); ok {
		t.Fatal("chunk ledger retained after canceled writes exited")
	}
	if got := loadSessionOnly(t, env, info.ID).Status; got != model.UploadStatusCanceled {
		t.Fatalf("final session status = %q, want canceled", got)
	}
}

// TestSyncChunkProgressUsesLatestLedgerAndNeverMovesBackward 验证进度同步
// 始终读取账本最新值，并且数据库进度比账本更靠前时拒绝回退。
func TestSyncChunkProgressUsesLatestLedgerAndNeverMovesBackward(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, chunks := chunkedContent()

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "progress.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}

	// 先占一个真实的分片名额，保证 chunkRun 在同步期间不会被删除。
	uploadCtx, endUpload, err := env.service.uploads.beginChunk(ctx, info.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer endUpload()

	// 账本里登记前两片：同步必须使用“现在的最新累计值”，而不是调用方
	// 在请求开始时的旧快照。
	for _, index := range []int{0, 1} {
		if _, ok := env.service.chunks.record(info.ID, index, ChunkReceipt{
			Size:   int64(len(chunks[index])),
			SHA256: sha256.Sum256(chunks[index]),
		}); !ok {
			t.Fatalf("record chunk %d", index)
		}
	}

	progress, err := env.service.syncChunkProgress(uploadCtx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatalf("sync chunk progress: %v", err)
	}
	want := 2 * UploadChunkSize
	if progress.ActualSize != want || progress.Status != model.UploadStatusReceiving {
		t.Fatalf("progress = %+v, want actual=%d receiving", progress, want)
	}

	// 模拟一个“数据库进度已经更靠前”的异常状态：再次同步不能把
	// ActualSize 覆盖回旧值，只能报不一致。
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", info.ID).
		UpdateColumn("actual_size", total).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.syncChunkProgress(
		uploadCtx, env.ownerA.ID, info.ID,
	); err == nil {
		t.Fatal("sync moved actual_size backward")
	}
	if got := reloadSession(t, env, info.ID).ActualSize; got != total {
		t.Fatalf("actual_size after rejected sync = %d, want %d", got, total)
	}
}

// TestChunkProgressWriteFailureKeepsReceiptAndRealigns 验证进度落库失败时：
// 已经成功的 receipt 不能被删除；同编号重试时先对齐进度，再正常收尾。
func TestChunkProgressWriteFailureKeepsReceiptAndRealigns(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total := UploadChunkSize
	data := bytes.Repeat([]byte("p"), int(total))

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "progress-fail.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}

	// 第一次 UPDATE 是 pending → receiving 的抢占，第二次是进度同步。
	// 让第二次失败，构造“内容已经写入并登记，但进度没落库”的状态。
	const callbackName = "test:fail_chunk_progress_once"
	updates := 0
	if err := env.db.Callback().Update().Before("gorm:update").Register(
		callbackName,
		func(tx *gorm.DB) {
			if tx.Statement.Schema == nil ||
				tx.Statement.Schema.Table != (model.UploadSession{}).TableName() {
				return
			}
			updates++
			if updates == 2 {
				_ = tx.AddError(errors.New("forced chunk progress failure"))
			}
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(data),
	); err == nil {
		t.Fatal("chunk succeeded despite forced progress failure")
	}
	if err := env.db.Callback().Update().Remove(callbackName); err != nil {
		t.Fatal(err)
	}

	// 内容写入成功、receipt 也已经登记；只有数据库进度落后。
	if got, ok := env.service.chunks.received(info.ID); !ok || got != total {
		t.Fatalf("ledger after progress failure = (%d, %v), want (%d, true)",
			got, ok, total)
	}
	stored := reloadSession(t, env, info.ID)
	if stored.Status != model.UploadStatusReceiving || stored.ActualSize != 0 {
		t.Fatalf("session after progress failure = %+v, want receiving/0", stored)
	}

	// 同一个编号重试：先同步进度，再走正常收尾。
	retried, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(data),
	)
	if err != nil {
		t.Fatalf("retry chunk after progress failure: %v", err)
	}
	if retried.Status != model.UploadStatusReady || retried.ActualSize != total {
		t.Fatalf("retried progress = %+v", retried)
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
		t.Fatal("content after progress retry does not match")
	}
}

// TestChunkedUploadRespectsConfiguredParallelLimit 验证 WithChunkParallel
// 真正改变分片会话的并行度：配置为 1 时，第二个分片在读 body 前被拒绝。
func TestChunkedUploadRespectsConfiguredParallelLimit(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, chunks := chunkedContent()

	svc := env.service.WithChunkParallel(1)
	info, err := svc.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "serial-per-session.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}

	reader0 := &blockingUploadReader{
		data:    chunks[0],
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(reader0.release) }) }
	t.Cleanup(release)

	done0 := startChunkUpload(svc, env.ownerA.ID, info.ID, 0, reader0)
	waitStarted(t, reader0)

	overLimit := newNotReadReader()
	if _, err := svc.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 1, overLimit,
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("second chunk with parallel=1 error = %v, want ErrUploadSessionConflict", err)
	}
	if overLimit.wasRead() {
		t.Fatal("over-limit request read the body with parallel=1")
	}

	release()
	if result := waitChunkResult(t, done0); result.err != nil {
		t.Fatalf("chunk 0: %v", result.err)
	}
	for _, index := range []int{1, 2} {
		if _, err := svc.UploadSessionChunk(
			ctx, env.ownerA.ID, info.ID, index, bytes.NewReader(chunks[index]),
		); err != nil {
			t.Fatalf("chunk %d: %v", index, err)
		}
	}
}
