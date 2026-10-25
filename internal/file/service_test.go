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

// newTestService 每个测试用一个干净的库表 + 临时存储目录，
// 并创建两个不同 ID 的测试用户，用来验证文件归属。
func newTestService(t *testing.T) (*Service, *ent.Client, string, int, int) {
	t.Helper()

	client, err := database.OpenDSN(context.Background(), testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	clearTables(t, client)
	ownerA := createTestUser(t, client, "A")
	ownerB := createTestUser(t, client, "B")

	root := t.TempDir()
	return NewService(client, storage.NewLocal(root)), client, root, ownerA, ownerB
}

func createTestUser(t *testing.T, client *ent.Client, username string) int {
	t.Helper()
	user, err := client.User.Create().
		SetUsername(username).
		// 文件业务测试不验证密码，这里只需要一个非空值。
		SetPasswordHash("not-used-in-this-test").
		Save(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return user.ID
}

func clearTables(t *testing.T, client *ent.Client) {
	t.Helper()
	ctx := context.Background()

	// 删除顺序必须满足外键：会话和文件指向用户，文件还指向 entity。
	if _, err := client.AuthSession.Delete().Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.File.Delete().Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Entity.Delete().Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.User.Delete().Exec(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestUploadAndOpen(t *testing.T) {
	service, client, _, ownerA, ownerB := newTestService(t)
	ctx := context.Background()

	record, err := service.Upload(ctx, ownerA, "a.txt", strings.NewReader("hello"))
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
	if storedFile.Parent != "/" || storedFile.OwnerID != ownerA {
		t.Fatalf("unexpected file record: parent=%q owner_id=%d", storedFile.Parent, storedFile.OwnerID)
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
	info, rc, err := service.Open(ctx, ownerA, record.ID)
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

	// B 知道 ID 也不能打开 A 的文件，且 A 的文件仍然可以正常下载。
	if _, _, err := service.Open(ctx, ownerB, record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another user must get ErrNotFound, got %v", err)
	}
	_, rc, err = service.Open(ctx, ownerA, record.ID)
	if err != nil {
		t.Fatalf("owner cannot open file after another user failed: %v", err)
	}
	_ = rc.Close()
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
	ownerID := createTestUser(t, first, "A")
	service1 := NewService(first, storage.NewLocal(root))
	record, err := service1.Upload(ctx, ownerID, "persist.bin", bytes.NewReader(payload))
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

	info, rc, err := service2.Open(ctx, ownerID, record.ID)
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
	service, client, root, ownerA, _ := newTestService(t)
	ctx := context.Background()

	record, err := service.Upload(ctx, ownerA, "a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}

	if err := service.Delete(ctx, ownerA, record.ID); err != nil {
		t.Fatal(err)
	}

	if _, _, err := service.Open(ctx, ownerA, record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wanted ErrNotFound after delete, got %v", err)
	}

	if err := service.Delete(ctx, ownerA, record.ID); !errors.Is(err, ErrNotFound) {
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

func TestDeleteOnlyOwnedFile(t *testing.T) {
	service, _, _, ownerA, ownerB := newTestService(t)
	ctx := context.Background()

	record, err := service.Upload(ctx, ownerA, "a.txt", strings.NewReader("keep me"))
	if err != nil {
		t.Fatal(err)
	}

	// B 即使知道准确的 ID，删除也必须表现为“文件不存在”。
	if err := service.Delete(ctx, ownerB, record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another user must get ErrNotFound on delete, got %v", err)
	}

	// B 的失败不能影响 A：内容仍然可以完整下载。
	_, rc, err := service.Open(ctx, ownerA, record.ID)
	if err != nil {
		t.Fatalf("file was damaged by another user's failed delete: %v", err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	if string(data) != "keep me" {
		t.Fatalf("content changed after failed delete: %q", data)
	}

	// 真正的所有者仍可删除。
	if err := service.Delete(ctx, ownerA, record.ID); err != nil {
		t.Fatal(err)
	}
}

func TestOpenUnknownID(t *testing.T) {
	service, _, _, ownerA, _ := newTestService(t)

	if _, _, err := service.Open(context.Background(), ownerA, "not-exist"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wanted ErrNotFound, got %v", err)
	}
}

func TestUploadTooLarge(t *testing.T) {
	service, client, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	content := make([]byte, MaxUploadBytes+1)
	if _, err := service.Upload(ctx, ownerA, "big.bin", bytes.NewReader(content)); !errors.Is(err, ErrTooLarge) {
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
