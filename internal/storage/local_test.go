package storage

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveWritesCompleteFile(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())

	size, err := local.Save(ctx, "abc123", strings.NewReader("hello"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if size != 5 {
		t.Fatalf("size = %d, want 5", size)
	}

	data, err := os.ReadFile(local.Path("abc123"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("saved content = %q, want %q", data, "hello")
	}
}

// TestSaveCreatesOwnerOnlyContent 验证上传内容不会被同组/其他用户读取。
//
// 临时文件会用 0600 创建，重命名后正式文件沿用同一权限。
// 这里直接检查最终文件，避免依赖进程 umask 的具体取值。
func TestSaveCreatesOwnerOnlyContent(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())

	if _, err := local.Save(ctx, "private", strings.NewReader("secret"), 1<<20); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(local.Path("private"))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("saved content permission = %04o, want 0600", got)
	}
}

// TestSaveTightensLeftoverTempPermission 覆盖旧版本留下的 0644 临时文件：
// O_CREATE 不会改已存在文件的权限，Save 必须显式收紧后再发布。
func TestSaveTightensLeftoverTempPermission(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())

	if err := os.WriteFile(local.tempPath("leftover"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := local.Save(ctx, "leftover", strings.NewReader("new"), 1<<20); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(local.Path("leftover"))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("published content permission = %04o, want 0600", got)
	}
}

func TestSaveAllowsExactlyMaxBytes(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())

	size, err := local.Save(ctx, "exact", strings.NewReader("12345"), 5)
	if err != nil {
		t.Fatalf("content with exactly maxBytes must be accepted: %v", err)
	}
	if size != 5 {
		t.Fatalf("size = %d, want 5", size)
	}
}

func TestSaveRejectsOverMaxBytes(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	local := NewLocal(root)

	_, err := local.Save(ctx, "too-big", strings.NewReader("123456"), 5)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("wanted ErrTooLarge, got %v", err)
	}

	// 被拒绝的内容不能发布成正式文件，也不能留下临时文件。
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("rejected upload left files behind: %v", entries)
	}
}

// brokenReader 模拟“读到一半网络断了”的情况。
type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) {
	return 0, errors.New("network broke")
}

func TestSaveRemovesTempFileOnFailure(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	local := NewLocal(root)

	if _, err := local.Save(ctx, "id", brokenReader{}, 1<<20); err == nil {
		t.Fatal("wanted an error when the reader fails")
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed save left files behind: %v", entries)
	}
}

// cancelOnReadReader 第一次 Read 返回内容并取消 ctx，下一次 Read 会看到取消。
type cancelOnReadReader struct {
	cancel context.CancelFunc
	data   []byte
	sent   bool
}

func (r *cancelOnReadReader) Read(p []byte) (int, error) {
	if r.sent {
		return 0, io.EOF
	}
	r.sent = true
	r.cancel()
	return copy(p, r.data), nil
}

// TestSaveStopsWhenContextCanceledDuringRead 确认复制过程中会检查 ctx：
// 取消后不能再继续读，更不能把内容发布成正式文件。
func TestSaveStopsWhenContextCanceledDuringRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	local := NewLocal(root)

	_, err := local.Save(ctx, "canceled", &cancelOnReadReader{
		cancel: cancel,
		data:   []byte("hello"),
	}, 1<<20)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wanted context.Canceled, got %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("canceled save left files behind: %v", entries)
	}
}

// cancelOnEOFReader 在返回最后一块内容和 io.EOF 的同时取消 ctx，
// 覆盖“内容刚好读完、但请求已经取消”的窗口。
type cancelOnEOFReader struct {
	cancel context.CancelFunc
	data   []byte
	sent   bool
}

func (r *cancelOnEOFReader) Read(p []byte) (int, error) {
	if r.sent {
		return 0, io.EOF
	}
	r.sent = true
	r.cancel()
	return copy(p, r.data), io.EOF
}

func TestSaveDoesNotPublishContentAfterCanceledAtEOF(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	local := NewLocal(root)

	_, err := local.Save(ctx, "canceled-at-eof", &cancelOnEOFReader{
		cancel: cancel,
		data:   []byte("hello"),
	}, 1<<20)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wanted context.Canceled, got %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("canceled save left files behind: %v", entries)
	}
}

func TestSaveFailsWhenRootDoesNotExist(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir() + "/missing")

	if _, err := local.Save(ctx, "id", strings.NewReader("x"), 1<<20); err == nil {
		t.Fatal("wanted an error when the storage root is missing")
	}
}

func TestOpenAndRemove(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())
	if _, err := local.Save(ctx, "id1", strings.NewReader("hello"), 1<<20); err != nil {
		t.Fatal(err)
	}

	f, err := local.Open(ctx, "id1")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("read %q, want %q", data, "hello")
	}

	if err := local.Remove(ctx, "id1"); err != nil {
		t.Fatal(err)
	}
	if _, err := local.Open(ctx, "id1"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("wanted not-exist error after remove, got %v", err)
	}
}

// TestRemoveDeletesTempContent 覆盖“进程在 Save 中途被强杀”留下的临时文件：
// Remove(key) 必须同时删除正式对象和 .upload-<key>。
func TestRemoveDeletesTempContent(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	local := NewLocal(root)

	tmpPath := filepath.Join(root, ".upload-crashed-key")
	if err := os.WriteFile(tmpPath, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := local.Remove(ctx, "crashed-key"); err != nil {
		t.Fatalf("remove should treat the leftover temp file as cleanable: %v", err)
	}
	if _, err := os.Stat(tmpPath); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("temp content was not removed: %v", err)
	}
}

// TestRemoveReportsRealErrors 确认清理不会把真实删除错误吞掉。
func TestRemoveReportsRealErrors(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	local := NewLocal(root)

	// 在目标位置放一个非空目录，os.Remove 会返回 ENOTEMPTY 之类的真实错误。
	finalPath := filepath.Join(root, "not-a-file")
	if err := os.MkdirAll(filepath.Join(finalPath, "child"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := local.Remove(ctx, "not-a-file"); err == nil {
		t.Fatal("wanted a real remove error")
	}
}
