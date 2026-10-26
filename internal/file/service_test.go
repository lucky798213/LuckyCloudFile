package file

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"gorm.io/gorm"

	"luckyCloudFile/internal/auth"
	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/model"
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
//
// 用户通过 auth.CreateUser 创建，所以每个用户都带一个根目录。
func newTestService(t *testing.T) (*Service, *gorm.DB, string, *model.User, *model.User) {
	t.Helper()

	db, err := database.OpenDSN(context.Background(), testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close(db) })

	clearTables(t, db)
	ownerA := createTestUser(t, db, "A")
	ownerB := createTestUser(t, db, "B")

	root := t.TempDir()
	return NewService(db, storage.NewLocal(root)), db, root, ownerA, ownerB
}

func createTestUser(t *testing.T, db *gorm.DB, username string) *model.User {
	t.Helper()
	user, err := auth.CreateUser(context.Background(), db, username, "test-password")
	if err != nil {
		t.Fatal(err)
	}
	if user.RootFileID == nil {
		t.Fatalf("user %q was created without a root directory", username)
	}
	return user
}

func clearTables(t *testing.T, db *gorm.DB) {
	t.Helper()

	// files.parent_id 是自引用外键，逐行按顺序删很容易出现“父节点先被删”。
	// 测试里在同一个连接上临时关掉外键检查，清空后立刻打开。
	err := db.Connection(func(conn *gorm.DB) error {
		if err := conn.Exec("SET FOREIGN_KEY_CHECKS = 0").Error; err != nil {
			return err
		}
		for _, table := range []string{"auth_sessions", "files", "entities", "users"} {
			if err := conn.Exec("DELETE FROM " + table).Error; err != nil {
				_ = conn.Exec("SET FOREIGN_KEY_CHECKS = 1").Error
				return err
			}
		}
		return conn.Exec("SET FOREIGN_KEY_CHECKS = 1").Error
	})
	if err != nil {
		t.Fatalf("clear tables: %v", err)
	}
}

// countRows 数一张表有多少行。传 &model.X{} 给 GORM 决定表名。
func countRows(t *testing.T, db *gorm.DB, value any) int64 {
	t.Helper()
	var count int64
	if err := db.Model(value).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

// countKind 数 files 表里某一类节点（文件或目录）有多少行。
func countKind(t *testing.T, db *gorm.DB, kind string) int64 {
	t.Helper()
	var count int64
	if err := db.Model(&model.File{}).Where("kind = ?", kind).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

func TestUploadAndOpen(t *testing.T) {
	service, db, _, ownerA, ownerB := newTestService(t)
	ctx := context.Background()

	record, err := service.Upload(ctx, ownerA.ID, "", "a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if record.ID == "" || record.Name != "a.txt" || record.Size != 5 {
		t.Fatalf("unexpected record: %+v", record)
	}
	if record.Kind != model.KindFile {
		t.Fatalf("kind = %q, want %q", record.Kind, model.KindFile)
	}
	// 不传 parent_id 时必须落在当前用户的根目录。
	if record.ParentID == nil || *record.ParentID != *ownerA.RootFileID {
		t.Fatalf("parent_id = %v, want root %q", record.ParentID, *ownerA.RootFileID)
	}

	// 数据库里：一个普通文件 + 两个用户的根目录，正好一条 Entity。
	if got := countKind(t, db, model.KindFile); got != 1 {
		t.Fatalf("wanted 1 file, got %d", got)
	}
	if got := countKind(t, db, model.KindDirectory); got != 2 {
		t.Fatalf("wanted 2 root directories, got %d", got)
	}
	if got := countRows(t, db, &model.Entity{}); got != 1 {
		t.Fatalf("wanted 1 entity, got %d", got)
	}

	var storedFile model.File
	if err := db.WithContext(ctx).Where("kind = ?", model.KindFile).First(&storedFile).Error; err != nil {
		t.Fatal(err)
	}
	if storedFile.OwnerID != ownerA.ID || storedFile.ParentID == nil {
		t.Fatalf("unexpected file record: owner=%d parent=%v", storedFile.OwnerID, storedFile.ParentID)
	}

	var entity model.Entity
	if err := db.WithContext(ctx).First(&entity).Error; err != nil {
		t.Fatal(err)
	}
	if entity.Policy != "local" || entity.RefCount != 1 || entity.Size != 5 {
		t.Fatalf("unexpected entity: %+v", entity)
	}
	if entity.Status != model.EntityStatusActive {
		t.Fatalf("new entity status = %q, want %q", entity.Status, model.EntityStatusActive)
	}
	// File 的公开 ID 和磁盘 key 是两个独立随机值。
	if entity.StorageKey == record.ID {
		t.Fatal("storage key must be different from the public file ID")
	}

	// 按 ID 能打开并读出原内容。
	info, rc, err := service.Open(ctx, ownerA.ID, record.ID)
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
	if string(data) != "hello" || info.Name != "a.txt" || info.Size != 5 || info.Kind != model.KindFile {
		t.Fatalf("unexpected download: info=%+v data=%q", info, data)
	}

	// B 知道 ID 也不能打开 A 的文件，且 A 的文件仍然可以正常下载。
	if _, _, err := service.Open(ctx, ownerB.ID, record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another user must get ErrNotFound, got %v", err)
	}
	_, rc, err = service.Open(ctx, ownerA.ID, record.ID)
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
	owner := createTestUser(t, first, "A")
	service1 := NewService(first, storage.NewLocal(root))
	record, err := service1.Upload(ctx, owner.ID, "", "persist.bin", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(first); err != nil {
		t.Fatal(err)
	}

	// 重新建立连接和 Service，相当于进程重启。
	second, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(second)
	service2 := NewService(second, storage.NewLocal(root))

	info, rc, err := service2.Open(ctx, owner.ID, record.ID)
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
	service, db, root, ownerA, _ := newTestService(t)
	ctx := context.Background()

	record, err := service.Upload(ctx, ownerA.ID, "", "a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}

	if err := service.Delete(ctx, ownerA.ID, record.ID); err != nil {
		t.Fatal(err)
	}

	if _, _, err := service.Open(ctx, ownerA.ID, record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wanted ErrNotFound after delete, got %v", err)
	}

	if err := service.Delete(ctx, ownerA.ID, record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleting twice should return ErrNotFound, got %v", err)
	}

	if got := countKind(t, db, model.KindFile); got != 0 {
		t.Fatalf("wanted no files left, got %d", got)
	}
	if got := countRows(t, db, &model.Entity{}); got != 0 {
		t.Fatalf("wanted no entities left, got %d", got)
	}
	// 用户的根目录不属于文件，删除文件时不能连根目录一起删。
	if got := countKind(t, db, model.KindDirectory); got != 2 {
		t.Fatalf("root directories were removed: %d left", got)
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

	record, err := service.Upload(ctx, ownerA.ID, "", "a.txt", strings.NewReader("keep me"))
	if err != nil {
		t.Fatal(err)
	}

	// B 即使知道准确的 ID，删除也必须表现为“文件不存在”。
	if err := service.Delete(ctx, ownerB.ID, record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another user must get ErrNotFound on delete, got %v", err)
	}

	// B 的失败不能影响 A：内容仍然可以完整下载。
	_, rc, err := service.Open(ctx, ownerA.ID, record.ID)
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
	if err := service.Delete(ctx, ownerA.ID, record.ID); err != nil {
		t.Fatal(err)
	}
}

func TestOpenUnknownID(t *testing.T) {
	service, _, _, ownerA, _ := newTestService(t)

	if _, _, err := service.Open(context.Background(), ownerA.ID, "not-exist"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wanted ErrNotFound, got %v", err)
	}
}

func TestUploadTooLarge(t *testing.T) {
	service, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	content := make([]byte, MaxUploadBytes+1)
	if _, err := service.Upload(ctx, ownerA.ID, "", "big.bin", bytes.NewReader(content)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("wanted ErrTooLarge, got %v", err)
	}

	// 超限的上传不能留下数据库记录。
	if got := countKind(t, db, model.KindFile); got != 0 {
		t.Fatalf("rejected upload left %d file rows", got)
	}
}

func TestCreateDirectoryAndList(t *testing.T) {
	service, db, diskRoot, ownerA, _ := newTestService(t)
	ctx := context.Background()

	// ① 建目录：只多一行 File，磁盘不变。
	dir, err := service.CreateDirectory(ctx, ownerA.ID, "", "学习资料")
	if err != nil {
		t.Fatal(err)
	}
	if dir.Kind != model.KindDirectory || dir.Size != 0 {
		t.Fatalf("unexpected directory: %+v", dir)
	}
	if dir.ParentID == nil || *dir.ParentID != *ownerA.RootFileID {
		t.Fatalf("directory parent = %v, want root %q", dir.ParentID, *ownerA.RootFileID)
	}
	if got := countRows(t, db, &model.Entity{}); got != 0 {
		t.Fatalf("creating a directory must not create an entity, got %d", got)
	}
	entries, err := os.ReadDir(diskRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("creating a directory must not touch disk, found %v", entries)
	}

	// ② 往目录里传文件，parent_id 指向目录，Entity 照常创建。
	note, err := service.Upload(ctx, ownerA.ID, dir.ID, "笔记.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if note.ParentID == nil || *note.ParentID != dir.ID {
		t.Fatalf("uploaded file parent = %v, want %q", note.ParentID, dir.ID)
	}

	// ③ 根目录只看得到“学习资料”这一层。
	rootList, err := service.ListChildren(ctx, ownerA.ID, "", 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if rootList.Total != 1 || len(rootList.Items) != 1 || rootList.Items[0].ID != dir.ID {
		t.Fatalf("unexpected root listing: %+v", rootList)
	}

	// ④ 目录里只看得到“笔记.txt”，大小来自 Entity。
	dirList, err := service.ListChildren(ctx, ownerA.ID, dir.ID, 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if dirList.Total != 1 || len(dirList.Items) != 1 {
		t.Fatalf("unexpected directory listing: %+v", dirList)
	}
	item := dirList.Items[0]
	if item.ID != note.ID || item.Name != "笔记.txt" || item.Size != 5 || item.Kind != model.KindFile {
		t.Fatalf("unexpected child: %+v", item)
	}

	// ⑤ 分页：page 从 1 开始，超出范围返回空列表但 total 不变。
	page2, err := service.ListChildren(ctx, ownerA.ID, dir.ID, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if page2.Total != 1 || len(page2.Items) != 0 {
		t.Fatalf("unexpected second page: %+v", page2)
	}
	// pageSize 超过 100 会被收敛到 100。
	clamped, err := service.ListChildren(ctx, ownerA.ID, dir.ID, 0, 500)
	if err != nil {
		t.Fatal(err)
	}
	if clamped.Page != 1 || clamped.PageSize != maxPageSize {
		t.Fatalf("pagination was not clamped: %+v", clamped)
	}
}

func TestListDoesNotRecurse(t *testing.T) {
	service, _, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	outer, err := service.CreateDirectory(ctx, ownerA.ID, "", "outer")
	if err != nil {
		t.Fatal(err)
	}
	inner, err := service.CreateDirectory(ctx, ownerA.ID, outer.ID, "inner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Upload(ctx, ownerA.ID, inner.ID, "deep.txt", strings.NewReader("deep")); err != nil {
		t.Fatal(err)
	}

	// 根目录只有 outer；outer 只有 inner；inner 里才有 deep.txt。
	rootList, err := service.ListChildren(ctx, ownerA.ID, "", 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if rootList.Total != 1 || rootList.Items[0].ID != outer.ID {
		t.Fatalf("root listing recursed: %+v", rootList)
	}
	outerList, err := service.ListChildren(ctx, ownerA.ID, outer.ID, 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if outerList.Total != 1 || outerList.Items[0].ID != inner.ID {
		t.Fatalf("outer listing recursed: %+v", outerList)
	}
}

func TestCreateDirectoryDuplicateName(t *testing.T) {
	service, _, _, ownerA, ownerB := newTestService(t)
	ctx := context.Background()

	if _, err := service.CreateDirectory(ctx, ownerA.ID, "", "docs"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDirectory(ctx, ownerA.ID, "", "docs"); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate name: wanted ErrAlreadyExists, got %v", err)
	}
	// 不同用户可以重名：唯一约束是 (owner_id, parent_id, name)。
	if _, err := service.CreateDirectory(ctx, ownerB.ID, "", "docs"); err != nil {
		t.Fatalf("another user must be able to reuse the name: %v", err)
	}
}

func TestDirectoryAccessIsolation(t *testing.T) {
	service, db, _, ownerA, ownerB := newTestService(t)
	ctx := context.Background()

	dir, err := service.CreateDirectory(ctx, ownerA.ID, "", "private")
	if err != nil {
		t.Fatal(err)
	}
	file, err := service.Upload(ctx, ownerA.ID, "", "a.txt", strings.NewReader("secret"))
	if err != nil {
		t.Fatal(err)
	}

	// B 拿着 A 的目录 ID：创建、上传、列表都必须表现为“不存在”。
	if _, err := service.CreateDirectory(ctx, ownerB.ID, dir.ID, "hack"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("B create in A's directory: wanted ErrNotFound, got %v", err)
	}
	if _, err := service.Upload(ctx, ownerB.ID, dir.ID, "hack.txt", strings.NewReader("x")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("B upload into A's directory: wanted ErrNotFound, got %v", err)
	}
	if _, err := service.ListChildren(ctx, ownerB.ID, dir.ID, 1, 20); !errors.Is(err, ErrNotFound) {
		t.Fatalf("B list A's directory: wanted ErrNotFound, got %v", err)
	}

	// A 的目录里不能凭空出现东西；文件数也没有增加。
	list, err := service.ListChildren(ctx, ownerA.ID, dir.ID, 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if list.Total != 0 {
		t.Fatalf("A's directory was modified by B: %+v", list)
	}
	if got := countKind(t, db, model.KindFile); got != 1 {
		t.Fatalf("file count changed to %d", got)
	}

	// parent_id 指向普通文件时必须明确报“不是目录”，而不是当成不存在。
	if _, err := service.CreateDirectory(ctx, ownerA.ID, file.ID, "child"); !errors.Is(err, ErrNotDirectory) {
		t.Fatalf("create inside a file: wanted ErrNotDirectory, got %v", err)
	}
	// 别人的文件同样先按“不存在”处理。
	if _, err := service.CreateDirectory(ctx, ownerB.ID, file.ID, "child"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("B create inside A's file: wanted ErrNotFound, got %v", err)
	}
}

func TestDirectoryDownloadAndDeleteRejected(t *testing.T) {
	service, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	dir, err := service.CreateDirectory(ctx, ownerA.ID, "", "docs")
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := service.Open(ctx, ownerA.ID, dir.ID); !errors.Is(err, ErrIsDirectory) {
		t.Fatalf("open directory: wanted ErrIsDirectory, got %v", err)
	}
	if err := service.Delete(ctx, ownerA.ID, dir.ID); !errors.Is(err, ErrIsDirectory) {
		t.Fatalf("delete directory: wanted ErrIsDirectory, got %v", err)
	}

	// 拒绝之后目录还在。
	if got := countKind(t, db, model.KindDirectory); got != 3 {
		t.Fatalf("directory disappeared after rejected operations: %d", got)
	}
}

func TestInvalidNames(t *testing.T) {
	service, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	badNames := []string{"", ".", "..", "a/b", `a\b`, "a\x00b", strings.Repeat("x", 256)}
	for _, name := range badNames {
		if _, err := service.CreateDirectory(ctx, ownerA.ID, "", name); !errors.Is(err, ErrInvalidName) {
			t.Fatalf("create directory %q: wanted ErrInvalidName, got %v", name, err)
		}
		if _, err := service.Upload(ctx, ownerA.ID, "", name, strings.NewReader("x")); !errors.Is(err, ErrInvalidName) {
			t.Fatalf("upload %q: wanted ErrInvalidName, got %v", name, err)
		}
	}
	if got := countKind(t, db, model.KindFile); got != 0 {
		t.Fatalf("invalid uploads left %d file rows", got)
	}

	// 中文名和 255 个字符都是允许的。
	if _, err := service.CreateDirectory(ctx, ownerA.ID, "", "学习资料"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateDirectory(ctx, ownerA.ID, "", strings.Repeat("x", 255)); err != nil {
		t.Fatal(err)
	}
}

// TestUserRootDirectory 验证建用户时根目录和 root_file_id 是一起写入的。
func TestUserRootDirectory(t *testing.T) {
	_, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	var root model.File
	if err := db.WithContext(ctx).First(&root, "id = ?", *ownerA.RootFileID).Error; err != nil {
		t.Fatal(err)
	}
	if root.Kind != model.KindDirectory || root.Name != "/" || root.ParentID != nil {
		t.Fatalf("unexpected root directory: %+v", root)
	}
	if root.OwnerID != ownerA.ID {
		t.Fatalf("root owner = %d, want %d", root.OwnerID, ownerA.ID)
	}
}

func TestUploadDuplicateName(t *testing.T) {
	service, db, diskRoot, ownerA, ownerB := newTestService(t)
	ctx := context.Background()

	first, err := service.Upload(ctx, ownerA.ID, "", "dup.txt", strings.NewReader("one"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Upload(ctx, ownerA.ID, "", "dup.txt", strings.NewReader("two")); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate upload: wanted ErrAlreadyExists, got %v", err)
	}

	// 第二条被拒绝后，数据库只有第一条，磁盘上也只留第一份内容。
	if got := countKind(t, db, model.KindFile); got != 1 {
		t.Fatalf("wanted 1 file, got %d", got)
	}
	entries, err := os.ReadDir(diskRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("rejected upload left an orphan on disk: %v", entries)
	}
	_, rc, err := service.Open(ctx, ownerA.ID, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	if string(data) != "one" {
		t.Fatalf("first file was damaged: %q", data)
	}

	// 不同用户在自己的根目录下可以同名。
	if _, err := service.Upload(ctx, ownerB.ID, "", "dup.txt", strings.NewReader("other")); err != nil {
		t.Fatalf("another user must be able to reuse the name: %v", err)
	}
}

// TestCopyFileReusesEntity 是复制的核心验收：
// 新 File 指向原 Entity，引用数加一；删掉任意一边，另一边仍能下载。
func TestCopyFileReusesEntity(t *testing.T) {
	service, db, diskRoot, ownerA, _ := newTestService(t)
	ctx := context.Background()

	sourceDir, err := service.CreateDirectory(ctx, ownerA.ID, "", "学习资料")
	if err != nil {
		t.Fatal(err)
	}
	targetDir, err := service.CreateDirectory(ctx, ownerA.ID, "", "备份资料")
	if err != nil {
		t.Fatal(err)
	}
	source, err := service.Upload(ctx, ownerA.ID, sourceDir.ID, "笔记.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}

	copied, err := service.Copy(ctx, ownerA.ID, source.ID, targetDir.ID, "笔记-副本.txt")
	if err != nil {
		t.Fatal(err)
	}
	if copied.ID == "" || copied.ID == source.ID {
		t.Fatalf("copy must have its own id, got %q (source %q)", copied.ID, source.ID)
	}
	if copied.Name != "笔记-副本.txt" || copied.Size != 5 || copied.Kind != model.KindFile {
		t.Fatalf("unexpected copy: %+v", copied)
	}
	if copied.ParentID == nil || *copied.ParentID != targetDir.ID {
		t.Fatalf("copy parent = %v, want %q", copied.ParentID, targetDir.ID)
	}

	// 两条 File 记录，但只有一份 Entity，引用数为 2；磁盘也只有一份内容。
	var entity model.Entity
	if err := db.WithContext(ctx).First(&entity).Error; err != nil {
		t.Fatal(err)
	}
	if entity.RefCount != 2 {
		t.Fatalf("entity ref_count = %d, want 2", entity.RefCount)
	}
	if entity.Status != model.EntityStatusActive {
		t.Fatalf("entity status = %q, want %q", entity.Status, model.EntityStatusActive)
	}
	entries, err := os.ReadDir(diskRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("copy must not duplicate content on disk, found %d files", len(entries))
	}

	// 源文件和副本都能读到同样的内容。
	for _, record := range []File{source, copied} {
		_, rc, err := service.Open(ctx, ownerA.ID, record.ID)
		if err != nil {
			t.Fatalf("open %s: %v", record.ID, err)
		}
		data, err := io.ReadAll(rc)
		if err != nil {
			t.Fatal(err)
		}
		_ = rc.Close()
		if string(data) != "hello" {
			t.Fatalf("content of %s = %q, want hello", record.ID, data)
		}
	}

	// 删掉源文件：Entity 仍被副本引用，不能进入待删除状态。
	if err := service.Delete(ctx, ownerA.ID, source.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.WithContext(ctx).First(&entity, entity.ID).Error; err != nil {
		t.Fatal(err)
	}
	if entity.RefCount != 1 || entity.Status != model.EntityStatusActive {
		t.Fatalf("after deleting source: ref_count=%d status=%q", entity.RefCount, entity.Status)
	}
	_, rc, err := service.Open(ctx, ownerA.ID, copied.ID)
	if err != nil {
		t.Fatalf("copy was damaged by deleting source: %v", err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	if string(data) != "hello" {
		t.Fatalf("copy content after deleting source = %q", data)
	}

	// 删掉副本后，最后一个引用消失，Entity 被清理，磁盘也空出来。
	if err := service.Delete(ctx, ownerA.ID, copied.ID); err != nil {
		t.Fatal(err)
	}
	if got := countRows(t, db, &model.Entity{}); got != 0 {
		t.Fatalf("wanted no entities left, got %d", got)
	}
	entries, err = os.ReadDir(diskRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("content still on disk after deleting both files: %v", entries)
	}
}

func TestCopyFileValidation(t *testing.T) {
	service, db, _, ownerA, ownerB := newTestService(t)
	ctx := context.Background()

	sourceDir, err := service.CreateDirectory(ctx, ownerA.ID, "", "source")
	if err != nil {
		t.Fatal(err)
	}
	source, err := service.Upload(ctx, ownerA.ID, sourceDir.ID, "a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	otherDir, err := service.CreateDirectory(ctx, ownerB.ID, "", "other")
	if err != nil {
		t.Fatal(err)
	}

	// 源文件不存在 / 属于别人：都表现为不存在。
	if _, err := service.Copy(ctx, ownerA.ID, "does-not-exist", sourceDir.ID, "copy.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing source: wanted ErrNotFound, got %v", err)
	}
	if _, err := service.Copy(ctx, ownerB.ID, source.ID, otherDir.ID, "copy.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another user's source: wanted ErrNotFound, got %v", err)
	}

	// 目标目录不存在 / 属于别人：都表现为不存在。
	if _, err := service.Copy(ctx, ownerA.ID, source.ID, "does-not-exist", "copy.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing target: wanted ErrNotFound, got %v", err)
	}
	if _, err := service.Copy(ctx, ownerA.ID, source.ID, otherDir.ID, "copy.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another user's target: wanted ErrNotFound, got %v", err)
	}

	// 源是目录、目标是普通文件：错误要能区分。
	if _, err := service.Copy(ctx, ownerA.ID, sourceDir.ID, sourceDir.ID, "dir-copy"); !errors.Is(err, ErrIsDirectory) {
		t.Fatalf("copy directory: wanted ErrIsDirectory, got %v", err)
	}
	if _, err := service.Copy(ctx, ownerA.ID, source.ID, source.ID, "copy.txt"); !errors.Is(err, ErrNotDirectory) {
		t.Fatalf("copy into a file: wanted ErrNotDirectory, got %v", err)
	}

	// 名字非法：在查询数据库之前就拒绝。
	if _, err := service.Copy(ctx, ownerA.ID, source.ID, sourceDir.ID, "a/b"); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("invalid copy name: wanted ErrInvalidName, got %v", err)
	}

	// 同名副本：409 的底层错误；失败后不能多出 File，引用数也不能变。
	if _, err := service.Copy(ctx, ownerA.ID, source.ID, sourceDir.ID, "a.txt"); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate copy name: wanted ErrAlreadyExists, got %v", err)
	}
	if got := countKind(t, db, model.KindFile); got != 1 {
		t.Fatalf("failed copies left %d file rows", got)
	}
	var entity model.Entity
	if err := db.WithContext(ctx).First(&entity).Error; err != nil {
		t.Fatal(err)
	}
	if entity.RefCount != 1 {
		t.Fatalf("failed copy changed ref_count to %d", entity.RefCount)
	}
}

// TestCopyRejectsInconsistentEntity 验证复制不会在已有异常数据上继续扩大不一致：
// 源 File 还在引用 Entity，但 ref_count 已经是 0，或者 Entity 已经是 pending_delete。
func TestCopyRejectsInconsistentEntity(t *testing.T) {
	service, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	source, err := service.Upload(ctx, ownerA.ID, "", "a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}

	var entity model.Entity
	if err := db.WithContext(ctx).First(&entity).Error; err != nil {
		t.Fatal(err)
	}
	if entity.RefCount != 1 || entity.Status != model.EntityStatusActive {
		t.Fatalf("unexpected fresh entity: %+v", entity)
	}

	// 异常 1：源 File 仍然引用 Entity，但 ref_count 已经是 0。
	if err := db.WithContext(ctx).
		Model(&model.Entity{}).
		Where("id = ?", entity.ID).
		UpdateColumn("ref_count", 0).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := service.Copy(ctx, ownerA.ID, source.ID, "", "copy-refcount.txt"); err == nil {
		t.Fatal("copy with ref_count = 0 must fail")
	}
	if got := countKind(t, db, model.KindFile); got != 1 {
		t.Fatalf("failed copy left %d file rows", got)
	}
	if err := db.WithContext(ctx).Where("id = ?", entity.ID).First(&entity).Error; err != nil {
		t.Fatal(err)
	}
	if entity.RefCount != 0 {
		t.Fatalf("failed copy changed ref_count to %d", entity.RefCount)
	}

	// 异常 2：Entity 已经是 pending_delete，却仍然被 File 引用。
	if err := db.WithContext(ctx).
		Model(&model.Entity{}).
		Where("id = ?", entity.ID).
		Updates(map[string]any{
			"ref_count": 1,
			"status":    model.EntityStatusPendingDelete,
		}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := service.Copy(ctx, ownerA.ID, source.ID, "", "copy-pending.txt"); err == nil {
		t.Fatal("copy from a pending_delete entity must fail")
	}
	if got := countKind(t, db, model.KindFile); got != 1 {
		t.Fatalf("failed copy left %d file rows", got)
	}

	// 数据恢复正常后，复制必须仍然可用，避免检查过严。
	if err := db.WithContext(ctx).
		Model(&model.Entity{}).
		Where("id = ?", entity.ID).
		Updates(map[string]any{
			"ref_count": 1,
			"status":    model.EntityStatusActive,
		}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := service.Copy(ctx, ownerA.ID, source.ID, "", "copy-ok.txt"); err != nil {
		t.Fatalf("copy should succeed after data is consistent again: %v", err)
	}
}
