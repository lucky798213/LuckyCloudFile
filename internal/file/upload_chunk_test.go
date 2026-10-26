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
		wantStatus := model.UploadStatusPending
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

// TestCreateChunkedUploadSessionRejectsInvalidModeAndEmptyFile 覆盖创建参数。
func TestCreateChunkedUploadSessionRejectsInvalidModeAndEmptyFile(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	if _, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "x.bin", 10, "weird",
	); !errors.Is(err, ErrInvalidUploadMode) {
		t.Fatalf("invalid mode: wanted ErrInvalidUploadMode, got %v", err)
	}
	if _, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "empty.bin", 0, model.UploadModeChunked,
	); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("empty chunked file: wanted ErrInvalidSize, got %v", err)
	}
	if got := countRows(t, env.db, &model.UploadSession{}); got != 0 {
		t.Fatalf("invalid requests left %d sessions", got)
	}
}

// TestChunkedUploadRejectsWrongOrderAndSize 验证序号、顺序和每片大小都由服务端把关。
func TestChunkedUploadRejectsWrongOrderAndSize(t *testing.T) {
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
		ctx, env.ownerA.ID, info.ID, 99, bytes.NewReader(chunks[0]),
	); !errors.Is(err, ErrInvalidChunkIndex) {
		t.Fatalf("invalid index: wanted ErrInvalidChunkIndex, got %v", err)
	}
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 1, bytes.NewReader(chunks[1]),
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("out-of-order chunk: wanted conflict, got %v", err)
	}
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(chunks[0][:100]),
	); !errors.Is(err, ErrUploadSizeMismatch) {
		t.Fatalf("short chunk: wanted size mismatch, got %v", err)
	}
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(chunks[0][:len(chunks[0])]),
	); err != nil {
		t.Fatalf("retry chunk 0 after failure: %v", err)
	}
	// 已经确认过的片不能重复上传。
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, info.ID, 0, bytes.NewReader(chunks[0]),
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("repeated chunk: wanted conflict, got %v", err)
	}
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

	user := reloadUser(t, env, env.ownerA.ID)
	if user.ReservedBytes != 0 || user.UsedBytes != 0 {
		t.Fatalf("quota after cancel: reserved=%d used=%d", user.ReservedBytes, user.UsedBytes)
	}
	if entries := diskEntries(t, env.rootA); len(entries) != 0 {
		t.Fatalf("canceled chunked session left content on disk: %v", entries)
	}
}
