package file

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/ent"
	"luckyCloudFile/internal/storage"
)

// 这些测试需要真实的 MySQL。没设置 DSN 时会跳过，
// 这样在没有数据库的机器上 `go test ./...` 仍然能通过。
//
// 本地运行方式（compose 已经建好 luckycloudfile_test 库）：
//
//	CLOUDREVE_TEST_MYSQL_DSN='lucky:lucky@tcp(127.0.0.1:3306)/luckycloudfile_test?parseTime=true' go test ./internal/file
func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run MySQL integration tests")
	}
	return dsn
}

// newTestService 每个测试用一个干净的库表 + 临时存储目录。
func newTestService(t *testing.T) (*Service, *ent.Client, string) {
	t.Helper()

	client, err := database.OpenDSN(context.Background(), testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	clearTables(t, client)

	root := t.TempDir()
	return NewService(client, storage.NewLocal(root)), client, root
}

func clearTables(t *testing.T, client *ent.Client) {
	t.Helper()
	ctx := context.Background()

	// 先删 files，再删 entities：files.file_entity 是指向 entities 的外键。
	if _, err := client.File.Delete().Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Entity.Delete().Exec(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestUploadAndOpen(t *testing.T) {
	service, client, _ := newTestService(t)
	ctx := context.Background()

	record, err := service.Upload(ctx, "a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if record.ID == "" || record.Name != "a.txt" || record.Size != 5 {
		t.Fatalf("unexpected record: %+v", record)
	}

	// 数据库里应该正好一条 File、一条 Entity。
	fileCount, err := client.File.Query().Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	entityCount, err := client.Entity.Query().Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fileCount != 1 || entityCount != 1 {
		t.Fatalf("wanted 1 file + 1 entity, got %d + %d", fileCount, entityCount)
	}

	storedFile, err := client.File.Query().Only(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if storedFile.Parent != "/" || storedFile.Owner != "local" {
		t.Fatalf("unexpected defaults: parent=%q owner=%q", storedFile.Parent, storedFile.Owner)
	}

	entity, err := client.Entity.Query().Only(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if entity.Policy != "local" || entity.RefCount != 1 || entity.Size != 5 {
		t.Fatalf("unexpected entity: %+v", entity)
	}
	// File 的公开 ID 和磁盘 key 是两个独立随机值。
	if entity.StorageKey == record.ID {
		t.Fatal("storage key must be different from the public file ID")
	}

	// 按 ID 能打开并读出原内容。
	info, rc, err := service.Open(ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" || info.Name != "a.txt" || info.Size != 5 {
		t.Fatalf("unexpected download: info=%+v data=%q", info, data)
	}
}

// TestUploadPersistsAcrossRestart 就是 P2 的验收：
// 上传 → 关掉客户端（模拟进程退出）→ 重新连库 → 仍能用原 ID 下载，内容不变。
func TestUploadPersistsAcrossRestart(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()
	root := t.TempDir()
	payload := []byte("survives a restart\n")

	first, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	clearTables(t, first)
	service1 := NewService(first, storage.NewLocal(root))
	record, err := service1.Upload(ctx, "persist.bin", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新建立连接和 Service，相当于进程重启。
	second, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	service2 := NewService(second, storage.NewLocal(root))

	info, rc, err := service2.Open(ctx, record.ID)
	if err != nil {
		t.Fatalf("download after restart failed: %v", err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, payload) || info.ID != record.ID || info.Size != int64(len(payload)) {
		t.Fatalf("content changed after restart: info=%+v data=%q", info, data)
	}
}

func TestDeleteRemovesRecordsAndContent(t *testing.T) {
	service, client, root := newTestService(t)
	ctx := context.Background()

	record, err := service.Upload(ctx, "a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}

	if err := service.Delete(ctx, record.ID); err != nil {
		t.Fatal(err)
	}

	if _, _, err := service.Open(ctx, record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wanted ErrNotFound after delete, got %v", err)
	}

	if err := service.Delete(ctx, record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleting twice should return ErrNotFound, got %v", err)
	}

	fileCount, err := client.File.Query().Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	entityCount, err := client.Entity.Query().Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fileCount != 0 || entityCount != 0 {
		t.Fatalf("wanted empty tables, got %d files + %d entities", fileCount, entityCount)
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("deleted content still on disk: %v", entries)
	}
}

func TestOpenUnknownID(t *testing.T) {
	service, _, _ := newTestService(t)

	if _, _, err := service.Open(context.Background(), "not-exist"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wanted ErrNotFound, got %v", err)
	}
}

func TestUploadTooLarge(t *testing.T) {
	service, client, _ := newTestService(t)
	ctx := context.Background()

	content := make([]byte, MaxUploadBytes+1)
	if _, err := service.Upload(ctx, "big.bin", bytes.NewReader(content)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("wanted ErrTooLarge, got %v", err)
	}

	// 超限的上传不能留下数据库记录。
	fileCount, err := client.File.Query().Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fileCount != 0 {
		t.Fatalf("rejected upload left %d file rows", fileCount)
	}
}
