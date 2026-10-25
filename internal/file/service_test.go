package file

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"luckyCloudFile/internal/storage"
)

func TestUploadRegistersFile(t *testing.T) {
	service := NewService(storage.NewLocal(t.TempDir()))

	record, err := service.Upload("a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if record.ID == "" || record.Name != "a.txt" || record.Size != 5 {
		t.Fatalf("unexpected record: %+v", record)
	}

	got, ok := service.Get(record.ID)
	if !ok {
		t.Fatal("uploaded file is not registered")
	}
	if got != record {
		t.Fatalf("Get returned %+v, want %+v", got, record)
	}
}

func TestUploadGeneratesDifferentIDs(t *testing.T) {
	service := NewService(storage.NewLocal(t.TempDir()))

	first, err := service.Upload("a.txt", strings.NewReader("one"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Upload("b.txt", strings.NewReader("two"))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Fatalf("two uploads got the same ID: %s", first.ID)
	}
}

func TestGetUnknownID(t *testing.T) {
	service := NewService(storage.NewLocal(t.TempDir()))

	if _, ok := service.Get("not-exist"); ok {
		t.Fatal("unknown ID must not be found")
	}
}

func TestOpenAndDelete(t *testing.T) {
	root := t.TempDir()
	service := NewService(storage.NewLocal(root))

	record, err := service.Upload("a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}

	// 打开并读出内容。
	_, f, err := service.Open(record.ID)
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

	// 删除之后，索引和磁盘文件都应该没有了。
	if err := service.Delete(record.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := service.Get(record.ID); ok {
		t.Fatal("deleted file is still registered")
	}
	if _, _, err := service.Open(record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wanted ErrNotFound after delete, got %v", err)
	}
	if err := service.Delete(record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleting twice should return ErrNotFound, got %v", err)
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("deleted file is still on disk: %v", entries)
	}
}

func TestOpenFileMissingFromDisk(t *testing.T) {
	root := t.TempDir()
	service := NewService(storage.NewLocal(root))

	record, err := service.Upload("a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	// 模拟磁盘文件被外部删掉。
	if err := os.Remove(storage.NewLocal(root).Path(record.ID)); err != nil {
		t.Fatal(err)
	}

	if _, _, err := service.Open(record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wanted ErrNotFound when disk file is missing, got %v", err)
	}
}
