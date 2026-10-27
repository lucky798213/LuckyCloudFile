package file

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"luckyCloudFile/internal/model"
)

// chunkedContent 按真实分片大小构造测试内容：两整片加一个尾片。
func chunkedContent() (total int64, chunks [][]byte) {
	full := bytes.Repeat([]byte("a"), int(UploadChunkSize))
	second := bytes.Repeat([]byte("b"), int(UploadChunkSize))
	tail := bytes.Repeat([]byte("c"), 100)
	chunks = [][]byte{full, second, tail}
	total = int64(len(full) + len(second) + len(tail))
	return total, chunks
}

func reloadUser(t *testing.T, env *testEnv, userID int) model.User {
	t.Helper()
	var user model.User
	if err := env.db.First(&user, userID).Error; err != nil {
		t.Fatal(err)
	}
	return user
}

func reloadSession(t *testing.T, env *testEnv, sessionID string) model.UploadSession {
	t.Helper()
	var session model.UploadSession
	if err := env.db.First(&session, "id = ?", sessionID).Error; err != nil {
		t.Fatal(err)
	}
	return session
}

// TestCreateChunkedUploadSessionReservesWholeFile 验证：
// chunked 会话的分片参数由服务端给出，容量仍按整个文件只预留一次。
func TestCreateChunkedUploadSessionReservesWholeFile(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, _ := chunkedContent()

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "big.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}

	if info.Mode != model.UploadModeChunked {
		t.Fatalf("mode = %q, want %q", info.Mode, model.UploadModeChunked)
	}
	if info.ChunkSize != UploadChunkSize {
		t.Fatalf("chunk_size = %d, want %d", info.ChunkSize, UploadChunkSize)
	}
	if info.ChunkCount != 3 {
		t.Fatalf("chunk_count = %d, want 3", info.ChunkCount)
	}
	if info.Status != model.UploadStatusPending || info.ActualSize != 0 {
		t.Fatalf("new chunked session = %+v", info)
	}

	session := reloadSession(t, env, info.ID)
	if session.Mode != model.UploadModeChunked ||
		session.ChunkSize != UploadChunkSize ||
		session.ExpectedSize != total ||
		session.ReservedBytes != total {
		t.Fatalf("stored session = %+v", session)
	}

	user := reloadUser(t, env, env.ownerA.ID)
	if user.ReservedBytes != total || user.UsedBytes != 0 {
		t.Fatalf("quota after creating chunked session: reserved=%d used=%d",
			user.ReservedBytes, user.UsedBytes)
	}
	if entries := diskEntries(t, env.rootA); len(entries) != 0 {
		t.Fatalf("creating a chunked session wrote content to disk: %v", entries)
	}
}

// TestChunkedUploadSessionUploadsAndCompletes 走完整链路：
// 逐片上传 → 最后一片 ready → 确认完成 → 内容、大小和容量都正确。
func TestChunkedUploadSessionUploadsAndCompletes(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, chunks := chunkedContent()

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "big.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if received, ok := env.service.chunks.received(info.ID); !ok || received != 0 {
		t.Fatalf("new chunked session ledger = (%d, %v), want (0, true)", received, ok)
	}

	var want int64
	for index, chunk := range chunks {
		progress, err := env.service.UploadSessionChunk(
			ctx, env.ownerA.ID, info.ID, index, bytes.NewReader(chunk),
		)
		if err != nil {
			t.Fatalf("upload chunk %d: %v", index, err)
		}
		want += int64(len(chunk))
		if progress.ActualSize != want {
			t.Fatalf("chunk %d: actual_size = %d, want %d",
				index, progress.ActualSize, want)
		}
		wantStatus := model.UploadStatusReceiving
		if index == len(chunks)-1 {
			wantStatus = model.UploadStatusReady
		}
		if progress.Status != wantStatus {
			t.Fatalf("chunk %d: status = %q, want %q",
				index, progress.Status, wantStatus)
		}

		// 每传一片，容量预留都不变：不会按分片重复预留。
		user := reloadUser(t, env, env.ownerA.ID)
		if user.ReservedBytes != total {
			t.Fatalf("after chunk %d: reserved_bytes = %d, want %d",
				index, user.ReservedBytes, total)
		}
	}

	// 磁盘上应该只有一份完整内容，而且和客户端上传的字节一致。
	session := reloadSession(t, env, info.ID)
	var entity model.Entity
	if err := env.db.First(&entity, session.EntityID).Error; err != nil {
		t.Fatal(err)
	}
	if entity.Size != total {
		t.Fatalf("placeholder entity size = %d, want %d", entity.Size, total)
	}
	disk, err := os.ReadFile(filepath.Join(env.rootA, entity.StorageKey))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(disk, bytes.Join(chunks, nil)) {
		t.Fatalf("disk content does not match uploaded chunks")
	}

	completed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != model.UploadStatusCompleted || completed.ResultFileID == nil {
		t.Fatalf("completed session = %+v", completed)
	}
	if _, ok := env.service.chunks.received(info.ID); ok {
		t.Fatal("chunk ledger retained after completion")
	}

	record, reader, err := env.service.Open(ctx, env.ownerA.ID, *completed.ResultFileID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if record.Size != total || !bytes.Equal(got, bytes.Join(chunks, nil)) {
		t.Fatalf("published file size=%d, content matches=%v", record.Size,
			bytes.Equal(got, bytes.Join(chunks, nil)))
	}

	user := reloadUser(t, env, env.ownerA.ID)
	if user.ReservedBytes != 0 || user.UsedBytes != total {
		t.Fatalf("quota after completion: reserved=%d used=%d, want 0/%d",
			user.ReservedBytes, user.UsedBytes, total)
	}
}

// TestUploadModesRejectMismatchedEndpoints 验证两种模式不能混用接口。
func TestUploadModesRejectMismatchedEndpoints(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, chunks := chunkedContent()

	chunked, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "chunked.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, chunked.ID, bytes.NewReader(chunks[0]),
	); !errors.Is(err, ErrUploadModeMismatch) {
		t.Fatalf("chunked session via content endpoint: wanted ErrUploadModeMismatch, got %v", err)
	}

	full, err := env.service.CreateUploadSession(
		ctx, env.ownerA.ID, "", "full.bin", 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if full.Mode != model.UploadModeFull || full.ChunkSize != 0 || full.ChunkCount != 0 {
		t.Fatalf("full session returned chunk fields: %+v", full)
	}
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, full.ID, 0, bytes.NewReader([]byte("0123456789")),
	); !errors.Is(err, ErrUploadModeMismatch) {
		t.Fatalf("full session via chunk endpoint: wanted ErrUploadModeMismatch, got %v", err)
	}
}

// TestCreateChunkedUploadSessionRejectsInvalidMode 覆盖创建参数。
func TestCreateChunkedUploadSessionRejectsInvalidMode(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	if _, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "x.bin", 10, "weird",
	); !errors.Is(err, ErrInvalidUploadMode) {
		t.Fatalf("invalid mode: wanted ErrInvalidUploadMode, got %v", err)
	}
	if got := countRows(t, env.db, &model.UploadSession{}); got != 0 {
		t.Fatalf("invalid request left %d sessions", got)
	}
}

// TestEmptyChunkedUploadSessionCompletesWithoutChunks 验证空文件：
// chunkCount=0，不发送任何分片，创建后直接 ready，可以直接走完成接口；
// 完成后必须能正常下载到一个 0 字节文件。
func TestEmptyChunkedUploadSessionCompletesWithoutChunks(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "empty.bin", 0, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode != model.UploadModeChunked ||
		info.ChunkSize != UploadChunkSize ||
		info.ChunkCount != 0 {
		t.Fatalf("empty chunked session = %+v", info)
	}
	if info.Status != model.UploadStatusReady || info.ActualSize != 0 {
		t.Fatalf("empty chunked session is not ready: %+v", info)
	}
	if received, ok := env.service.chunks.received(info.ID); !ok || received != 0 {
		t.Fatalf("empty chunked session ledger = (%d, %v), want (0, true)", received, ok)
	}
	if entries := diskEntries(t, env.rootA); len(entries) != 0 {
		t.Fatalf("empty chunked session wrote content to disk: %v", entries)
	}

	completed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != model.UploadStatusCompleted || completed.ResultFileID == nil {
		t.Fatalf("completed empty session = %+v", completed)
	}
	if _, ok := env.service.chunks.received(info.ID); ok {
		t.Fatal("empty chunked session ledger retained after completion")
	}

	// 关键回归：完成后的文件必须能从存储层打开，而且内容长度为 0。
	// 先确认磁盘上真的存在这个对象，再看下载路径读到的是空内容。
	session := reloadSession(t, env, info.ID)
	var entity model.Entity
	if err := env.db.First(&entity, session.EntityID).Error; err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(filepath.Join(env.rootA, entity.StorageKey))
	if err != nil {
		t.Fatalf("completed empty file has no physical object: %v", err)
	}
	if stat.Size() != 0 {
		t.Fatalf("physical empty file size = %d, want 0", stat.Size())
	}

	record, reader, err := env.service.Open(ctx, env.ownerA.ID, *completed.ResultFileID)
	if err != nil {
		t.Fatalf("open completed empty file: %v", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read completed empty file: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close completed empty file: %v", err)
	}
	if record.Size != 0 || len(got) != 0 {
		t.Fatalf("completed empty file size=%d content_length=%d, want 0/0",
			record.Size, len(got))
	}

	// 重复确认完成是幂等的，仍然能下载同一个空文件。
	again, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.ResultFileID == nil || *again.ResultFileID != *completed.ResultFileID {
		t.Fatalf("repeated completion returned %v, want %q",
			again.ResultFileID, *completed.ResultFileID)
	}

	// 任何分片编号都没有意义。
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(nil),
	); err == nil {
		t.Fatal("empty chunked session accepted chunk index 0")
	}

	user := reloadUser(t, env, env.ownerA.ID)
	if user.ReservedBytes != 0 || user.UsedBytes != 0 {
		t.Fatalf("quota after empty upload: reserved=%d used=%d",
			user.ReservedBytes, user.UsedBytes)
	}
}

// TestChunkedUploadAcceptsAnyOrderAndDeduplicates 验证：
// 分片可以乱序到达；同一编号同内容幂等成功，同编号不同内容按冲突拒绝；
// 少传、多传和越界编号都在写盘前被拒绝。
func TestChunkedUploadAcceptsAnyOrderAndDeduplicates(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, chunks := chunkedContent()

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "big.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}

	// 越界编号、少传、多传都必须在修改目标文件之前失败。
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 99, bytes.NewReader(chunks[0]),
	); !errors.Is(err, ErrInvalidChunkIndex) {
		t.Fatalf("invalid index: wanted ErrInvalidChunkIndex, got %v", err)
	}
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(chunks[0][:100]),
	); !errors.Is(err, ErrUploadSizeMismatch) {
		t.Fatalf("short chunk: wanted size mismatch, got %v", err)
	}
	oversized := append(append([]byte(nil), chunks[0]...), 'x')
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(oversized),
	); !errors.Is(err, ErrUploadSizeMismatch) {
		t.Fatalf("oversized chunk: wanted size mismatch, got %v", err)
	}

	// 先上传第 1 片：乱序允许，进度按有效字节累计。
	progress, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 1, bytes.NewReader(chunks[1]),
	)
	if err != nil {
		t.Fatalf("upload out-of-order chunk 1: %v", err)
	}
	if progress.ActualSize != UploadChunkSize ||
		progress.Status != model.UploadStatusReceiving {
		t.Fatalf("progress after chunk 1 = %+v", progress)
	}

	// 同一编号、同一内容：幂等成功，不重复累计。
	again, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 1, bytes.NewReader(chunks[1]),
	)
	if err != nil {
		t.Fatalf("idempotent retry of chunk 1: %v", err)
	}
	if again.ActualSize != UploadChunkSize ||
		again.Status != model.UploadStatusReceiving {
		t.Fatalf("progress after idempotent retry = %+v", again)
	}

	// 同一编号、不同内容：冲突，进度不变。
	different := bytes.Repeat([]byte("z"), len(chunks[1]))
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 1, bytes.NewReader(different),
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("conflicting duplicate: wanted ErrUploadSessionConflict, got %v", err)
	}

	// 补齐其余两片后变成 ready。
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(chunks[0]),
	); err != nil {
		t.Fatalf("upload chunk 0: %v", err)
	}
	final, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 2, bytes.NewReader(chunks[2]),
	)
	if err != nil {
		t.Fatalf("upload chunk 2: %v", err)
	}
	if final.ActualSize != total || final.Status != model.UploadStatusReady {
		t.Fatalf("final progress = %+v", final)
	}

	// 乱序写入的最终内容必须和原始顺序一致。
	session := reloadSession(t, env, info.ID)
	var entity model.Entity
	if err := env.db.First(&entity, session.EntityID).Error; err != nil {
		t.Fatal(err)
	}
	disk, err := os.ReadFile(filepath.Join(env.rootA, entity.StorageKey))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(disk, bytes.Join(chunks, nil)) {
		t.Fatalf("assembled chunk file does not match original content")
	}

	// 完成之后账本应删除，下载内容和大小仍然正确。
	completed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	record, reader, err := env.service.Open(ctx, env.ownerA.ID, *completed.ResultFileID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if record.Size != total || !bytes.Equal(got, bytes.Join(chunks, nil)) {
		t.Fatalf("downloaded content size=%d matches=%v", record.Size,
			bytes.Equal(got, bytes.Join(chunks, nil)))
	}
	if _, ok := env.service.chunks.received(info.ID); ok {
		t.Fatal("chunk ledger retained after completion")
	}
}

// TestChunkedUploadReadyReplayIsIdempotent 覆盖“最后一片成功但响应丢失”的场景：
// 会话已经是 ready、但还没完成时，重传同一片同内容必须成功，且不能写盘、
// 不能把状态退回 receiving；同编号不同内容仍然返回冲突。
func TestChunkedUploadReadyReplayIsIdempotent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total := UploadChunkSize
	data := bytes.Repeat([]byte("a"), int(total))

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "one-chunk.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}

	first, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(data),
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != model.UploadStatusReady || first.ActualSize != total {
		t.Fatalf("first upload progress = %+v", first)
	}

	replayed, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(data),
	)
	if err != nil {
		t.Fatalf("replay same chunk after ready: %v", err)
	}
	if replayed.Status != model.UploadStatusReady || replayed.ActualSize != total {
		t.Fatalf("replayed progress = %+v", replayed)
	}

	// 重复路径不能修改状态或账本。
	session := reloadSession(t, env, info.ID)
	if session.Status != model.UploadStatusReady || session.ActualSize != total {
		t.Fatalf("session changed by ready replay: %+v", session)
	}
	if received, ok := env.service.chunks.received(info.ID); !ok || received != total {
		t.Fatalf("ledger changed by ready replay: (%d, %v)", received, ok)
	}

	// 同编号不同内容必须冲突，而且不能覆盖已经写好的内容。
	different := bytes.Repeat([]byte("b"), int(total))
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(different),
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("conflicting replay after ready: wanted conflict, got %v", err)
	}
	// 长度不同也属于“和已确认内容不一致”。
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(data[:100]),
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("short replay after ready: wanted conflict, got %v", err)
	}
	session = reloadSession(t, env, info.ID)
	if session.Status != model.UploadStatusReady || session.ActualSize != total {
		t.Fatalf("session changed by conflicting replay: %+v", session)
	}
	var entity model.Entity
	if err := env.db.First(&entity, session.EntityID).Error; err != nil {
		t.Fatal(err)
	}
	disk, err := os.ReadFile(filepath.Join(env.rootA, entity.StorageKey))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(disk, data) {
		t.Fatal("conflicting replay overwrote finalized chunk content")
	}

	// 完成之后仍然能下载到原来的内容。
	completed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	record, reader, err := env.service.Open(ctx, env.ownerA.ID, *completed.ResultFileID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if record.Size != total || !bytes.Equal(got, data) {
		t.Fatalf("downloaded content size=%d matches=%v", record.Size, bytes.Equal(got, data))
	}

	// completed 是终态，重复分片不能再被当作 ready 重放。
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(data),
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("chunk after completion: wanted conflict, got %v", err)
	}
}

// TestChunkedUploadReadErrorDoesNotRecordChunk 验证读取中途失败时：
// 不写盘、不登记 receipt、会话保持 receiving（分片模式下这是正常状态），
// 之后可以正常重试同一片。
func TestChunkedUploadReadErrorDoesNotRecordChunk(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	const total = UploadChunkSize

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "one-chunk.bin", total, model.UploadModeChunked,
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

	session := reloadSession(t, env, info.ID)
	if session.Status != model.UploadStatusReceiving || session.ActualSize != 0 {
		t.Fatalf("session after read error = %+v", session)
	}
	if received, ok := env.service.chunks.received(info.ID); !ok || received != 0 {
		t.Fatalf("ledger after read error = (%d, %v), want (0, true)", received, ok)
	}
	if entries := diskEntries(t, env.rootA); len(entries) != 0 {
		t.Fatalf("read error wrote content to disk: %v", entries)
	}

	data := bytes.Repeat([]byte("ok"), int(UploadChunkSize/2))
	retried, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(data),
	)
	if err != nil {
		t.Fatalf("retry chunk after read error: %v", err)
	}
	if retried.Status != model.UploadStatusReady || retried.ActualSize != total {
		t.Fatalf("retried progress = %+v", retried)
	}
}

// readThenError 先返回一段数据，再返回错误，模拟客户端传到一半断开。
type readThenError struct {
	data []byte
	read bool
}

func (r *readThenError) Read(p []byte) (int, error) {
	if r.read {
		return 0, errors.New("connection interrupted")
	}
	r.read = true
	n := copy(p, r.data)
	return n, errors.New("connection interrupted")
}

// TestCancelChunkedUploadSessionCleansContentAndReservation 验证取消分片会话
// 仍然走原来的容量释放和物理清理，不需要为分片单独记账。
func TestCancelChunkedUploadSessionCleansContentAndReservation(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	total, chunks := chunkedContent()

	info, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "big.bin", total, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(chunks[0]),
	); err != nil {
		t.Fatal(err)
	}

	canceled, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status != model.UploadStatusCanceled {
		t.Fatalf("status = %q, want canceled", canceled.Status)
	}
	if _, ok := env.service.chunks.received(info.ID); ok {
		t.Fatal("canceled chunked session ledger was not removed")
	}

	user := reloadUser(t, env, env.ownerA.ID)
	if user.ReservedBytes != 0 || user.UsedBytes != 0 {
		t.Fatalf("quota after cancel: reserved=%d used=%d", user.ReservedBytes, user.UsedBytes)
	}
	if entries := diskEntries(t, env.rootA); len(entries) != 0 {
		t.Fatalf("canceled chunked session left content on disk: %v", entries)
	}
}
