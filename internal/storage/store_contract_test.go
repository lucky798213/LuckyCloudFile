package storage

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
)

// testStoreContract 是所有 Store 实现都该满足的行为：写入 → 读取 → 删除。
//
// 以后新增 S3 实现时，只要用同一个函数跑一遍，就能确认替换存储后
// 业务层看到的行为没有变化。这是双驱动契约测试的起点。
func testStoreContract(t *testing.T, newStore func(t *testing.T) Store) {
	t.Helper()

	t.Run("save read remove", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)

		size, err := store.Save(ctx, "contract-key", strings.NewReader("hello"), 1<<20)
		if err != nil {
			t.Fatalf("save: %v", err)
		}
		if size != 5 {
			t.Fatalf("saved size = %d, want 5", size)
		}

		reader, err := store.Open(ctx, "contract-key")
		if err != nil {
			t.Fatalf("open saved content: %v", err)
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if err := reader.Close(); err != nil {
			t.Fatalf("close reader: %v", err)
		}
		if string(data) != "hello" {
			t.Fatalf("read content = %q, want %q", data, "hello")
		}

		if err := store.Remove(ctx, "contract-key"); err != nil {
			t.Fatalf("remove: %v", err)
		}
		if _, err := store.Open(ctx, "contract-key"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("open removed content: wanted fs.ErrNotExist, got %v", err)
		}
	})

	t.Run("remove missing content", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)

		// 清理逻辑靠 fs.ErrNotExist 判断“对象本来就不存在”，这里锁定这个约定。
		if err := store.Remove(ctx, "never-created"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("remove missing content: wanted fs.ErrNotExist, got %v", err)
		}
	})

	t.Run("reject content over max bytes", func(t *testing.T) {
		ctx := context.Background()
		store := newStore(t)

		if _, err := store.Save(ctx, "too-large", strings.NewReader("123456"), 5); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("save oversized content: wanted ErrTooLarge, got %v", err)
		}
		if _, err := store.Open(ctx, "too-large"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("rejected content must not be published, got %v", err)
		}
	})
}

// TestLocalStoreContract 让 Local 通过 Store 的通用契约。
func TestLocalStoreContract(t *testing.T) {
	testStoreContract(t, func(t *testing.T) Store {
		return NewLocal(t.TempDir())
	})
}
