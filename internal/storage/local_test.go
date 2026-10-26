package storage

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
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
