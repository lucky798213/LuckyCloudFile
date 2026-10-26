package storage

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"testing"
)

// TestPrepareChunksPreallocatesOnceAndKeepsChunks 验证：
// 首次 Prepare 把临时文件长度设成 totalSize；重复 Prepare 不会清空已有分片；
// 后续 WriteChunkAt 也不会重新 Truncate。
func TestPrepareChunksPreallocatesOnceAndKeepsChunks(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())
	const key = "chunked-key"
	const total = int64(10)

	if err := local.PrepareChunks(ctx, key, total); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(local.tempPath(key))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != total {
		t.Fatalf("temp size = %d, want %d", info.Size(), total)
	}
	if _, err := os.Stat(local.Path(key)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("final content appeared before finalize: %v", err)
	}

	if err := local.WriteChunkAt(ctx, key, 2, []byte("xy")); err != nil {
		t.Fatal(err)
	}
	// 初始化重试：不能把刚写进去的分片清掉。
	if err := local.PrepareChunks(ctx, key, total); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(local.tempPath(key))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != int(total) {
		t.Fatalf("content length = %d, want %d", len(data), total)
	}
	if !bytes.Equal(data[2:4], []byte("xy")) {
		t.Fatalf("retry prepare cleared chunk data: %q", data)
	}
	// 预分配留下的是空洞：没写过的位置读出来是 0。
	if data[0] != 0 || data[4] != 0 {
		t.Fatalf("unexpected non-zero hole bytes: %v", data)
	}

	// 再写另一片，前面的分片必须仍然存在，文件长度也不应变化。
	if err := local.WriteChunkAt(ctx, key, 8, []byte("zz")); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(local.tempPath(key))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != int(total) || !bytes.Equal(data[2:4], []byte("xy")) ||
		!bytes.Equal(data[8:10], []byte("zz")) {
		t.Fatalf("content after second chunk = %q", data)
	}
}

// TestPrepareChunksRefusesDifferentSize 验证重试时大小不一致会拒绝，
// 而不是把已有分片按新长度截断。
func TestPrepareChunksRefusesDifferentSize(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())
	const key = "size-mismatch"

	if err := local.PrepareChunks(ctx, key, 5); err != nil {
		t.Fatal(err)
	}
	if err := local.WriteChunkAt(ctx, key, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := local.PrepareChunks(ctx, key, 6); err == nil {
		t.Fatal("prepare with a different total size succeeded")
	}

	data, err := os.ReadFile(local.tempPath(key))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("existing chunks changed after rejected prepare: %q", data)
	}
}

// TestWriteChunkAtUsesOffsetAndRejectsOutOfRange 验证 WriteAt 使用指定偏移、
// 越界写入被拒绝且不破坏已有内容，未 Prepare 时返回 fs.ErrNotExist。
func TestWriteChunkAtUsesOffsetAndRejectsOutOfRange(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())
	const key = "offset-key"

	if err := local.PrepareChunks(ctx, key, 10); err != nil {
		t.Fatal(err)
	}
	if err := local.WriteChunkAt(ctx, key, 2, []byte("xy")); err != nil {
		t.Fatal(err)
	}

	if err := local.WriteChunkAt(ctx, key, 9, []byte("toolong")); !errors.Is(err, ErrChunkOutOfRange) {
		t.Fatalf("out-of-range write: wanted ErrChunkOutOfRange, got %v", err)
	}
	data, err := os.ReadFile(local.tempPath(key))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 10 || !bytes.Equal(data[2:4], []byte("xy")) || data[8] != 0 {
		t.Fatalf("out-of-range write changed content: %q", data)
	}

	if err := local.WriteChunkAt(ctx, "never-prepared", 0, []byte("x")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("write without prepare: wanted fs.ErrNotExist, got %v", err)
	}
	if err := local.WriteChunkAt(ctx, key, -1, []byte("x")); err == nil {
		t.Fatal("negative offset was accepted")
	}
}

// TestFinalizeChunksPublishesAndIsRetryable 验证正式发布、重复发布和发布后拒绝写入。
func TestFinalizeChunksPublishesAndIsRetryable(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())
	const key = "finalize-key"
	const total = int64(5)

	if err := local.PrepareChunks(ctx, key, total); err != nil {
		t.Fatal(err)
	}
	if err := local.WriteChunkAt(ctx, key, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := local.FinalizeChunks(ctx, key, total); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(local.Path(key))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("published content = %q, want %q", data, "hello")
	}
	if _, err := os.Stat(local.tempPath(key)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("temp content still exists after finalize: %v", err)
	}

	// 模拟 Rename 成功但后续数据库更新失败：重试必须直接成功。
	if err := local.FinalizeChunks(ctx, key, total); err != nil {
		t.Fatalf("retry finalize: %v", err)
	}
	if err := local.WriteChunkAt(ctx, key, 0, []byte("xxxxx")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("write after finalize: wanted fs.ErrNotExist, got %v", err)
	}
}

// TestFinalizeChunksEmptyFile 验证 totalSize=0 也能发布成 0 字节正式对象。
func TestFinalizeChunksEmptyFile(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())
	const key = "empty-key"

	if err := local.PrepareChunks(ctx, key, 0); err != nil {
		t.Fatal(err)
	}
	if err := local.FinalizeChunks(ctx, key, 0); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(local.Path(key))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("empty finalized content length = %d, want 0", len(data))
	}
	if err := local.FinalizeChunks(ctx, key, 0); err != nil {
		t.Fatalf("retry finalize empty file: %v", err)
	}
}

// TestFinalizeChunksWithoutContent 验证完全没 Prepare 时返回 fs.ErrNotExist。
func TestFinalizeChunksWithoutContent(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())

	if err := local.FinalizeChunks(ctx, "missing-key", 1); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("finalize missing content: wanted fs.ErrNotExist, got %v", err)
	}
}

// TestRemoveCleansPreparedChunkContent 验证现有的 Local.Remove 仍然能清理
// .upload-<key> 临时对象和已经发布的对象。
func TestRemoveCleansPreparedChunkContent(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())

	if err := local.PrepareChunks(ctx, "prepared", 3); err != nil {
		t.Fatal(err)
	}
	if err := local.WriteChunkAt(ctx, "prepared", 0, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := local.Remove(ctx, "prepared"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(local.tempPath("prepared")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("temp content not removed: %v", err)
	}

	if err := local.PrepareChunks(ctx, "finalized", 3); err != nil {
		t.Fatal(err)
	}
	if err := local.WriteChunkAt(ctx, "finalized", 0, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := local.FinalizeChunks(ctx, "finalized", 3); err != nil {
		t.Fatal(err)
	}
	if err := local.Remove(ctx, "finalized"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(local.Path("finalized")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("final content not removed: %v", err)
	}
}

// TestChunkFilesUseOwnerOnlyPermissions 验证临时内容、发布后的内容都是 0600。
func TestChunkFilesUseOwnerOnlyPermissions(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())
	const key = "private-chunked"

	if err := local.PrepareChunks(ctx, key, 4); err != nil {
		t.Fatal(err)
	}
	if err := local.WriteChunkAt(ctx, key, 0, []byte("data")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(local.tempPath(key))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("temp permission = %04o, want 0600", got)
	}

	if err := local.FinalizeChunks(ctx, key, 4); err != nil {
		t.Fatal(err)
	}
	info, err = os.Stat(local.Path(key))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("final permission = %04o, want 0600", got)
	}
}
