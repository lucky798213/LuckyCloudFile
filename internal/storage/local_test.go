package storage

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"
)

func TestSaveWritesCompleteFile(t *testing.T) {
	local := NewLocal(t.TempDir())

	size, err := local.Save("abc123", strings.NewReader("hello"), 1<<20)
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
	local := NewLocal(t.TempDir())

	size, err := local.Save("exact", strings.NewReader("12345"), 5)
	if err != nil {
		t.Fatalf("content with exactly maxBytes must be accepted: %v", err)
	}
	if size != 5 {
		t.Fatalf("size = %d, want 5", size)
	}
}

func TestSaveRejectsOverMaxBytes(t *testing.T) {
	root := t.TempDir()
	local := NewLocal(root)

	_, err := local.Save("too-big", strings.NewReader("123456"), 5)
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
	root := t.TempDir()
	local := NewLocal(root)

	if _, err := local.Save("id", brokenReader{}, 1<<20); err == nil {
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

func TestSaveFailsWhenRootDoesNotExist(t *testing.T) {
	local := NewLocal(t.TempDir() + "/missing")

	if _, err := local.Save("id", strings.NewReader("x"), 1<<20); err == nil {
		t.Fatal("wanted an error when the storage root is missing")
	}
}

func TestOpenAndRemove(t *testing.T) {
	local := NewLocal(t.TempDir())
	if _, err := local.Save("id1", strings.NewReader("hello"), 1<<20); err != nil {
		t.Fatal(err)
	}

	f, err := local.Open("id1")
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

	if err := local.Remove("id1"); err != nil {
		t.Fatal(err)
	}
	if _, err := local.Open("id1"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("wanted not-exist error after remove, got %v", err)
	}
}
