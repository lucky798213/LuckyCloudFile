package storage

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

// TestAppendWritesSequentialChunks 验证分片按偏移顺序写入同一个文件。
func TestAppendWritesSequentialChunks(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())

	if n, err := local.Append(ctx, "chunked", 0, strings.NewReader("hello"), 5); err != nil || n != 5 {
		t.Fatalf("append first chunk: n=%d err=%v", n, err)
	}
	if n, err := local.Append(ctx, "chunked", 5, strings.NewReader(" world"), 6); err != nil || n != 6 {
		t.Fatalf("append second chunk: n=%d err=%v", n, err)
	}

	data, err := os.ReadFile(local.Path("chunked"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); got != "hello world" {
		t.Fatalf("content = %q, want %q", got, "hello world")
	}
}

// TestAppendRetryReplacesUnconfirmedTail 模拟“上一片写了一半但数据库没记上”：
// 重试同一偏移时必须覆盖旧尾巴，而不是接在后面重复追加。
func TestAppendRetryReplacesUnconfirmedTail(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())

	if _, err := local.Append(ctx, "retry", 0, strings.NewReader("hello"), 5); err != nil {
		t.Fatal(err)
	}
	// 一次没有确认的写入，留下了比 offset 更长的内容。
	if _, err := local.Append(ctx, "retry", 5, strings.NewReader("STALE-TAIL"), 64); err != nil {
		t.Fatal(err)
	}
	if _, err := local.Append(ctx, "retry", 5, strings.NewReader(" world"), 6); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(local.Path("retry"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); got != "hello world" {
		t.Fatalf("content = %q, want %q", got, "hello world")
	}
}

// TestAppendOverMaxRestoresConfirmedContent 验证多传一片时不会破坏前面的分片。
func TestAppendOverMaxRestoresConfirmedContent(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())

	if _, err := local.Append(ctx, "limit", 0, strings.NewReader("hello"), 5); err != nil {
		t.Fatal(err)
	}
	if _, err := local.Append(ctx, "limit", 5, strings.NewReader("toolong"), 3); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("append oversized chunk: wanted ErrTooLarge, got %v", err)
	}

	data, err := os.ReadFile(local.Path("limit"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); got != "hello" {
		t.Fatalf("content after rejected chunk = %q, want %q", got, "hello")
	}
}

// TestAppendCreatesOwnerOnlyContent 确认分片创建的文件和 Save 一样是 0600。
func TestAppendCreatesOwnerOnlyContent(t *testing.T) {
	ctx := context.Background()
	local := NewLocal(t.TempDir())

	if _, err := local.Append(ctx, "private-chunked", 0, strings.NewReader("secret"), 1<<20); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(local.Path("private-chunked"))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("chunk content permission = %04o, want 0600", got)
	}
}
