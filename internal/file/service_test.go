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

// testPolicyBConfigKey 是测试用第二条策略的固定配置键。
// app 启动测试也会给这个键准备临时目录，避免多个测试包共用数据库时
// 出现一条“存在但没配置目录”的策略。
const testPolicyBConfigKey = "test-policy-b"

// testEnv 是一次集成测试的完整环境：真实 MySQL、两个本地目录，
// 以及分别指向它们的策略 A/B。用户默认策略由服务端分配，测试也可以直接改。
type testEnv struct {
	service *Service
	db      *gorm.DB
	rootA   string
	rootB   string
	policyA *model.StoragePolicy
	policyB *model.StoragePolicy
	ownerA  *model.User
	ownerB  *model.User
}

// newTestEnv 每个测试用一个干净的库表 + 两个临时存储目录，
// 并创建两个不同 ID 的测试用户，用来验证文件归属。
//
// 用户通过 auth.CreateUser 创建，所以每个用户都带一个根目录，
// 默认策略是数据库初始化出来的本地策略 A。
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	db, err := database.OpenDSN(context.Background(), testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close(db) })

	clearTables(t, db)

	rootA := t.TempDir()
	rootB := t.TempDir()
	policyA := localStoragePolicy(t, db)
	policyB := ensureTestPolicy(t, db, testPolicyBConfigKey, "B")

	registry := storage.NewRegistry()
	if err := registry.Register(policyA.ID, storage.NewLocal(rootA)); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(policyB.ID, storage.NewLocal(rootB)); err != nil {
		t.Fatal(err)
	}

	ownerA := createTestUser(t, db, "A")
	ownerB := createTestUser(t, db, "B")

	// 其它测试包（例如 app 启动测试）可能同时连同一个测试库。
	// 这里在测试结束后把临时策略 B 收掉，避免它留在库里让别的包
	// 以为存在一条没有配置目录的策略。
	t.Cleanup(func() {
		if err := db.Model(&model.Entity{}).
			Where("storage_policy_id = ?", policyB.ID).
			UpdateColumn("storage_policy_id", policyA.ID).Error; err != nil {
			t.Logf("cleanup policy B entities: %v", err)
		}
		if err := db.Model(&model.User{}).
			Where("default_storage_policy_id = ?", policyB.ID).
			UpdateColumn("default_storage_policy_id", policyA.ID).Error; err != nil {
			t.Logf("cleanup policy B users: %v", err)
		}
		if err := db.Delete(&model.StoragePolicy{}, policyB.ID).Error; err != nil {
			t.Logf("cleanup policy B: %v", err)
		}
	})

	return &testEnv{
		service: NewService(db, registry),
		db:      db,
		rootA:   rootA,
		rootB:   rootB,
		policyA: policyA,
		policyB: policyB,
		ownerA:  ownerA,
		ownerB:  ownerB,
	}
}

// newTestService 保留原有测试的返回形状：默认策略 A 的目录就是 root。
func newTestService(t *testing.T) (*Service, *gorm.DB, string, *model.User, *model.User) {
	t.Helper()
	env := newTestEnv(t)
	return env.service, env.db, env.rootA, env.ownerA, env.ownerB
}

// localStoragePolicy 取出数据库初始化时创建的本地策略。
// 万一别的测试清掉了策略表，这里补建一条，保证测试仍然从真实策略 ID 出发。
func localStoragePolicy(t *testing.T, db *gorm.DB) *model.StoragePolicy {
	t.Helper()

	var policy model.StoragePolicy
	err := db.Where("config_key = ?", model.LocalStorageConfigKey).First(&policy).Error
	if err == nil {
		return &policy
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal(err)
	}

	policy = model.StoragePolicy{
		Name:      "本地存储",
		Type:      model.StorageTypeLocal,
		ConfigKey: model.LocalStorageConfigKey,
	}
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	return &policy
}

// ensureTestPolicy 按 ConfigKey 取测试策略，没有才创建。
// 这样即使上一次测试异常退出留下了这一行，下一次也能直接复用。
func ensureTestPolicy(t *testing.T, db *gorm.DB, configKey, name string) *model.StoragePolicy {
	t.Helper()

	var policy model.StoragePolicy
	err := db.Where("config_key = ?", configKey).First(&policy).Error
	if err == nil {
		return &policy
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal(err)
	}

	policy = model.StoragePolicy{
		Name:      name,
		Type:      model.StorageTypeLocal,
		ConfigKey: configKey,
	}
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	return &policy
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
		for _, table := range []string{"upload_sessions", "auth_sessions", "files", "entities", "users"} {
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

	// 新账号的默认上传位置由服务端写入，并且是按 ConfigKey 找的策略。
	policy := localStoragePolicy(t, db)
	if ownerA.DefaultStoragePolicyID != policy.ID {
		t.Fatalf("new user default policy = %d, want %d", ownerA.DefaultStoragePolicyID, policy.ID)
	}

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
	if entity.RefCount != 1 || entity.Size != 5 {
		t.Fatalf("unexpected entity: %+v", entity)
	}
	if entity.StoragePolicyID != localStoragePolicy(t, db).ID {
		t.Fatalf("entity storage policy = %d, want the server default", entity.StoragePolicyID)
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
	policy := localStoragePolicy(t, first)
	owner := createTestUser(t, first, "A")
	service1 := NewService(first, testRegistry(t, policy, root))
	record, err := service1.Upload(ctx, owner.ID, "", "persist.bin", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(first); err != nil {
		t.Fatal(err)
	}

	// 重新建立连接和 Service，相当于进程重启。
	// 迁移会重复跑一次，但策略 ID 不变，所以读取仍然指向原来的目录。
	second, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(second)
	service2 := NewService(second, testRegistry(t, policy, root))

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

// testRegistry 只注册一条策略，用来构造重启测试的 Service。
func testRegistry(t *testing.T, policy *model.StoragePolicy, root string) *storage.Registry {
	t.Helper()
	registry := storage.NewRegistry()
	if err := registry.Register(policy.ID, storage.NewLocal(root)); err != nil {
		t.Fatal(err)
	}
	return registry
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

// cancelAfterSaveStore 在内容写入成功后立刻取消请求的 ctx，
// 用来模拟“文件已经存好、写数据库时客户端断开”。
type cancelAfterSaveStore struct {
	storage.Store
	cancel       context.CancelFunc
	onSaved      func()
	removed      int
	removeCtxErr error
}

func (s *cancelAfterSaveStore) Save(ctx context.Context, key string, r io.Reader, maxBytes int64) (int64, error) {
	size, err := s.Store.Save(ctx, key, r, maxBytes)
	if s.onSaved != nil {
		s.onSaved()
	}
	s.cancel()
	return size, err
}

func (s *cancelAfterSaveStore) Remove(ctx context.Context, key string) error {
	s.removed++
	s.removeCtxErr = ctx.Err()
	return s.Store.Remove(ctx, key)
}

// TestUploadCompensatesWithFreshContext 验证数据库写入失败后的补偿删除：
// 请求 ctx 已经取消时，仍然要用独立、未取消的上下文去 Remove，
// 否则删除会立刻被取消，内容永远留在存储里。
//
// 这个测试还在写盘成功后把用户默认策略改成 B：本次上传已经选了 A，
// 补偿删除必须继续操作 A，不能回头按新默认值去 B 删。
func TestUploadCompensatesWithFreshContext(t *testing.T) {
	env := newTestEnv(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &cancelAfterSaveStore{
		Store:  storage.NewLocal(env.rootA),
		cancel: cancel,
		onSaved: func() {
			if err := env.db.Model(&model.User{}).
				Where("id = ?", env.ownerA.ID).
				UpdateColumn("default_storage_policy_id", env.policyB.ID).Error; err != nil {
				t.Errorf("change default policy during upload: %v", err)
			}
		},
	}
	registry := storage.NewRegistry()
	if err := registry.Register(env.policyA.ID, store); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(env.policyB.ID, storage.NewLocal(env.rootB)); err != nil {
		t.Fatal(err)
	}
	service := NewService(env.db, registry)

	_, err := service.Upload(ctx, env.ownerA.ID, "", "canceled.txt", strings.NewReader("hello"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wanted the database step to fail with context.Canceled, got %v", err)
	}

	if store.removed != 1 {
		t.Fatalf("compensation must attempt exactly one remove, got %d", store.removed)
	}
	if store.removeCtxErr != nil {
		t.Fatalf("compensation must not reuse the canceled request context: %v", store.removeCtxErr)
	}

	// 补偿删的是本次实际写入的 A，B 没有被碰过。
	for name, root := range map[string]string{"A": env.rootA, "B": env.rootB} {
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("orphan content left in %s: %v", name, entries)
		}
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("failed upload left %d file rows", got)
	}
	if got := countRows(t, env.db, &model.Entity{}); got != 0 {
		t.Fatalf("failed upload left %d entity rows", got)
	}

	// 默认策略确实已经被改成 B；但本次补偿从未使用它。
	var user model.User
	if err := env.db.First(&user, env.ownerA.ID).Error; err != nil {
		t.Fatal(err)
	}
	if user.DefaultStoragePolicyID != env.policyB.ID {
		t.Fatalf("user default policy = %d, want B=%d", user.DefaultStoragePolicyID, env.policyB.ID)
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

func TestDirectoryDownloadRejected(t *testing.T) {
	service, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	dir, err := service.CreateDirectory(ctx, ownerA.ID, "", "docs")
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := service.Open(ctx, ownerA.ID, dir.ID); !errors.Is(err, ErrIsDirectory) {
		t.Fatalf("open directory: wanted ErrIsDirectory, got %v", err)
	}
	if got := countKind(t, db, model.KindDirectory); got != 3 {
		t.Fatalf("download attempt changed directory rows: %d", got)
	}
}

func TestDeleteEmptyDirectory(t *testing.T) {
	service, db, diskRoot, ownerA, _ := newTestService(t)
	ctx := context.Background()

	dir, err := service.CreateDirectory(ctx, ownerA.ID, "", "docs")
	if err != nil {
		t.Fatal(err)
	}

	if err := service.Delete(ctx, ownerA.ID, dir.ID); err != nil {
		t.Fatalf("delete empty directory: %v", err)
	}

	// 目录行没了，父目录列表也看不到它；Entity 和磁盘完全没被碰过。
	if got := countKind(t, db, model.KindDirectory); got != 2 {
		t.Fatalf("wanted only the two root directories left, got %d", got)
	}
	rootList, err := service.ListChildren(ctx, ownerA.ID, "", 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if rootList.Total != 0 {
		t.Fatalf("deleted directory still listed: %+v", rootList)
	}
	if got := countRows(t, db, &model.Entity{}); got != 0 {
		t.Fatalf("deleting a directory created entities: %d", got)
	}
	entries, err := os.ReadDir(diskRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("deleting a directory touched disk: %v", entries)
	}

	// 再删一次：记录已经不存在。
	if err := service.Delete(ctx, ownerA.ID, dir.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleting twice should return ErrNotFound, got %v", err)
	}
}

func TestDeleteNonEmptyDirectoryRejected(t *testing.T) {
	service, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	dir, err := service.CreateDirectory(ctx, ownerA.ID, "", "docs")
	if err != nil {
		t.Fatal(err)
	}
	child, err := service.Upload(ctx, ownerA.ID, dir.ID, "a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}

	if err := service.Delete(ctx, ownerA.ID, dir.ID); !errors.Is(err, ErrDirectoryNotEmpty) {
		t.Fatalf("delete non-empty directory: wanted ErrDirectoryNotEmpty, got %v", err)
	}

	// 拒绝是干净的：目录、子文件、Entity 和内容都还在。
	if got := countKind(t, db, model.KindDirectory); got != 3 {
		t.Fatalf("non-empty directory was removed: %d left", got)
	}
	if got := countKind(t, db, model.KindFile); got != 1 {
		t.Fatalf("child file was removed: %d left", got)
	}
	list, err := service.ListChildren(ctx, ownerA.ID, dir.ID, 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if list.Total != 1 || list.Items[0].ID != child.ID {
		t.Fatalf("unexpected directory listing after rejected delete: %+v", list)
	}
	_, rc, err := service.Open(ctx, ownerA.ID, child.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	if string(data) != "hello" {
		t.Fatalf("child content changed after rejected delete: %q", data)
	}
}

func TestDeleteRootRejected(t *testing.T) {
	service, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	if err := service.Delete(ctx, ownerA.ID, *ownerA.RootFileID); !errors.Is(err, ErrRootDirectory) {
		t.Fatalf("delete root: wanted ErrRootDirectory, got %v", err)
	}

	var root model.File
	if err := db.WithContext(ctx).First(&root, "id = ?", *ownerA.RootFileID).Error; err != nil {
		t.Fatal(err)
	}
	if root.Name != "/" || root.ParentID != nil {
		t.Fatalf("root directory changed: %+v", root)
	}
	if got := countKind(t, db, model.KindDirectory); got != 2 {
		t.Fatalf("root directories were removed: %d left", got)
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

// TestRenameFileKeepsContentAndEntity 是文件改名的核心验收：
// 只改 files.name，Entity、引用计数、磁盘对象都不动，下载内容不变。
func TestRenameFileKeepsContentAndEntity(t *testing.T) {
	service, db, diskRoot, ownerA, _ := newTestService(t)
	ctx := context.Background()

	record, err := service.Upload(ctx, ownerA.ID, "", "notes.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}

	var before model.Entity
	if err := db.WithContext(ctx).First(&before).Error; err != nil {
		t.Fatal(err)
	}

	renamed, err := service.Rename(ctx, ownerA.ID, record.ID, "新笔记.txt")
	if err != nil {
		t.Fatal(err)
	}
	if renamed.ID != record.ID || renamed.Name != "新笔记.txt" {
		t.Fatalf("unexpected renamed record: %+v", renamed)
	}
	if renamed.Size != 5 || renamed.Kind != model.KindFile {
		t.Fatalf("rename changed size or kind: %+v", renamed)
	}
	if !sameParentID(renamed.ParentID, record.ParentID) {
		t.Fatalf("rename changed parent: %v -> %v", record.ParentID, renamed.ParentID)
	}

	// 数据库里只有 files.name 变了，entity_id、owner、parent 都保持原样。
	var stored model.File
	if err := db.WithContext(ctx).First(&stored, "id = ?", record.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Name != "新笔记.txt" || stored.OwnerID != ownerA.ID {
		t.Fatalf("unexpected stored file: %+v", stored)
	}
	if stored.EntityID == nil || *stored.EntityID != before.ID {
		t.Fatalf("rename changed entity_id: %v -> %v", before.ID, stored.EntityID)
	}

	// Entity 一个字段都不能变：引用数、状态、磁盘 key 和大小。
	var after model.Entity
	if err := db.WithContext(ctx).First(&after, before.ID).Error; err != nil {
		t.Fatal(err)
	}
	if after.StorageKey != before.StorageKey || after.Size != before.Size ||
		after.RefCount != before.RefCount || after.Status != before.Status {
		t.Fatalf("rename touched the entity: before=%+v after=%+v", before, after)
	}
	if got := countRows(t, db, &model.Entity{}); got != 1 {
		t.Fatalf("rename created or removed entities: %d", got)
	}

	// 磁盘上还是同一个对象，数量和名字都没变。
	entries, err := os.ReadDir(diskRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != before.StorageKey {
		t.Fatalf("disk object changed: %v", entries)
	}

	// 下载内容不变，返回的名字是新的。
	info, rc, err := service.Open(ctx, ownerA.ID, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	if string(data) != "hello" || info.Name != "新笔记.txt" || info.Size != 5 {
		t.Fatalf("download after rename: info=%+v data=%q", info, data)
	}

	// 列表里看到的也是新名字。
	list, err := service.ListChildren(ctx, ownerA.ID, "", 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if list.Total != 1 || list.Items[0].Name != "新笔记.txt" {
		t.Fatalf("unexpected listing after rename: %+v", list)
	}
}

// TestRenameDirectory 目录改名只改这一行，子树和磁盘都不受影响。
func TestRenameDirectory(t *testing.T) {
	service, db, diskRoot, ownerA, _ := newTestService(t)
	ctx := context.Background()

	dir, err := service.CreateDirectory(ctx, ownerA.ID, "", "旧目录")
	if err != nil {
		t.Fatal(err)
	}
	child, err := service.Upload(ctx, ownerA.ID, dir.ID, "笔记.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}

	renamed, err := service.Rename(ctx, ownerA.ID, dir.ID, "新目录")
	if err != nil {
		t.Fatal(err)
	}
	if renamed.ID != dir.ID || renamed.Name != "新目录" ||
		renamed.Kind != model.KindDirectory || renamed.Size != 0 {
		t.Fatalf("unexpected renamed directory: %+v", renamed)
	}

	// 根目录里是改名后的目录。
	rootList, err := service.ListChildren(ctx, ownerA.ID, "", 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if rootList.Total != 1 || rootList.Items[0].ID != dir.ID || rootList.Items[0].Name != "新目录" {
		t.Fatalf("unexpected root listing: %+v", rootList)
	}

	// 子节点仍然挂在原来的目录 ID 下，内容也能正常下载。
	childList, err := service.ListChildren(ctx, ownerA.ID, dir.ID, 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if childList.Total != 1 || childList.Items[0].ID != child.ID || childList.Items[0].Name != "笔记.txt" {
		t.Fatalf("directory children changed after rename: %+v", childList)
	}
	var storedChild model.File
	if err := db.WithContext(ctx).First(&storedChild, "id = ?", child.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedChild.ParentID == nil || *storedChild.ParentID != dir.ID {
		t.Fatalf("child parent changed to %v", storedChild.ParentID)
	}
	_, rc, err := service.Open(ctx, ownerA.ID, child.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	if string(data) != "hello" {
		t.Fatalf("child content changed after directory rename: %q", data)
	}

	// 改名目录不写磁盘、不建 Entity。
	if got := countRows(t, db, &model.Entity{}); got != 1 {
		t.Fatalf("directory rename created entities: %d", got)
	}
	entries, err := os.ReadDir(diskRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("directory rename touched disk: %v", entries)
	}
}

func TestRenameDuplicateName(t *testing.T) {
	service, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	a, err := service.Upload(ctx, ownerA.ID, "", "a.txt", strings.NewReader("one"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := service.Upload(ctx, ownerA.ID, "", "b.txt", strings.NewReader("two"))
	if err != nil {
		t.Fatal(err)
	}
	dir, err := service.CreateDirectory(ctx, ownerA.ID, "", "docs")
	if err != nil {
		t.Fatal(err)
	}

	// 文件撞文件、文件撞目录、目录撞文件，都由唯一索引拒绝。
	if _, err := service.Rename(ctx, ownerA.ID, b.ID, "a.txt"); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("file over file: wanted ErrAlreadyExists, got %v", err)
	}
	if _, err := service.Rename(ctx, ownerA.ID, b.ID, "docs"); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("file over directory: wanted ErrAlreadyExists, got %v", err)
	}
	if _, err := service.Rename(ctx, ownerA.ID, dir.ID, "a.txt"); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("directory over file: wanted ErrAlreadyExists, got %v", err)
	}

	// 冲突是事务回滚：名字、行数、引用数和内容都不变。
	var storedB model.File
	if err := db.WithContext(ctx).First(&storedB, "id = ?", b.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedB.Name != "b.txt" {
		t.Fatalf("failed rename changed the name to %q", storedB.Name)
	}
	if got := countKind(t, db, model.KindFile); got != 2 {
		t.Fatalf("failed renames changed file rows: %d", got)
	}
	var entity model.Entity
	if err := db.WithContext(ctx).First(&entity).Error; err != nil {
		t.Fatal(err)
	}
	if entity.RefCount != 1 {
		t.Fatalf("failed rename changed ref_count to %d", entity.RefCount)
	}

	// 改成现在的名字是幂等的。
	if renamed, err := service.Rename(ctx, ownerA.ID, a.ID, "a.txt"); err != nil || renamed.Name != "a.txt" {
		t.Fatalf("rename to the same name: record=%+v err=%v", renamed, err)
	}

	// 不冲突的新名字仍然可以改，且内容不变。
	renamed, err := service.Rename(ctx, ownerA.ID, b.ID, "c.txt")
	if err != nil {
		t.Fatal(err)
	}
	if renamed.Name != "c.txt" || renamed.Size != 3 {
		t.Fatalf("unexpected renamed file: %+v", renamed)
	}
	_, rc, err := service.Open(ctx, ownerA.ID, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	if string(data) != "two" {
		t.Fatalf("content changed after rename: %q", data)
	}
}

func TestRenameOnlyOwned(t *testing.T) {
	service, _, _, ownerA, ownerB := newTestService(t)
	ctx := context.Background()

	file, err := service.Upload(ctx, ownerA.ID, "", "a.txt", strings.NewReader("secret"))
	if err != nil {
		t.Fatal(err)
	}
	dir, err := service.CreateDirectory(ctx, ownerA.ID, "", "private")
	if err != nil {
		t.Fatal(err)
	}

	// B 知道准确的 ID，也必须表现为“不存在”，文件和目录都一样。
	if _, err := service.Rename(ctx, ownerB.ID, file.ID, "stolen.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("B rename A's file: wanted ErrNotFound, got %v", err)
	}
	if _, err := service.Rename(ctx, ownerB.ID, dir.ID, "stolen-dir"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("B rename A's directory: wanted ErrNotFound, got %v", err)
	}
	if _, err := service.Rename(ctx, ownerA.ID, "does-not-exist", "x.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown id: wanted ErrNotFound, got %v", err)
	}

	// B 的失败没有改动 A 的节点，内容也完好。
	var storedFile model.File
	if err := service.db.WithContext(ctx).First(&storedFile, "id = ?", file.ID).Error; err != nil {
		t.Fatal(err)
	}
	var storedDir model.File
	if err := service.db.WithContext(ctx).First(&storedDir, "id = ?", dir.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedFile.Name != "a.txt" || storedDir.Name != "private" {
		t.Fatalf("B changed A's names: file=%q dir=%q", storedFile.Name, storedDir.Name)
	}
	_, rc, err := service.Open(ctx, ownerA.ID, file.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	if string(data) != "secret" {
		t.Fatalf("content changed after failed cross-user rename: %q", data)
	}

	// 所有者仍然可以改名。
	if _, err := service.Rename(ctx, ownerA.ID, file.ID, "mine.txt"); err != nil {
		t.Fatal(err)
	}
}

func TestRenameInvalidName(t *testing.T) {
	service, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	record, err := service.Upload(ctx, ownerA.ID, "", "ok.txt", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	dir, err := service.CreateDirectory(ctx, ownerA.ID, "", "ok-dir")
	if err != nil {
		t.Fatal(err)
	}

	badNames := []string{"", ".", "..", "a/b", `a\b`, "a\x00b", strings.Repeat("x", 256)}
	for _, name := range badNames {
		if _, err := service.Rename(ctx, ownerA.ID, record.ID, name); !errors.Is(err, ErrInvalidName) {
			t.Fatalf("rename file to %q: wanted ErrInvalidName, got %v", name, err)
		}
		if _, err := service.Rename(ctx, ownerA.ID, dir.ID, name); !errors.Is(err, ErrInvalidName) {
			t.Fatalf("rename directory to %q: wanted ErrInvalidName, got %v", name, err)
		}
	}

	// 非法名字在写库之前就被拒绝，原名保持不变。
	var stored model.File
	if err := db.WithContext(ctx).First(&stored, "id = ?", record.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Name != "ok.txt" {
		t.Fatalf("invalid rename changed the name to %q", stored.Name)
	}

	// 中文名和 255 个字符都是允许的。
	if _, err := service.Rename(ctx, ownerA.ID, record.ID, "学习笔记"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Rename(ctx, ownerA.ID, record.ID, strings.Repeat("y", 255)); err != nil {
		t.Fatal(err)
	}
}

// TestRenameRootRejected 用户根目录的名字由服务端固定为 "/"，不能被改。
func TestRenameRootRejected(t *testing.T) {
	service, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	if _, err := service.Rename(ctx, ownerA.ID, *ownerA.RootFileID, "hacked"); !errors.Is(err, ErrRootDirectory) {
		t.Fatalf("rename root: wanted ErrRootDirectory, got %v", err)
	}

	// 根目录本身和 users.root_file_id 都没有变化。
	var root model.File
	if err := db.WithContext(ctx).First(&root, "id = ?", *ownerA.RootFileID).Error; err != nil {
		t.Fatal(err)
	}
	if root.Name != "/" || root.Kind != model.KindDirectory || root.ParentID != nil {
		t.Fatalf("root directory was modified: %+v", root)
	}
	var user model.User
	if err := db.WithContext(ctx).First(&user, ownerA.ID).Error; err != nil {
		t.Fatal(err)
	}
	if user.RootFileID == nil || *user.RootFileID != *ownerA.RootFileID {
		t.Fatalf("users.root_file_id changed: %v", user.RootFileID)
	}
}

// sameParentID 比较两个可空父目录 ID。
func sameParentID(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// TestMoveFile 是文件移动的核心验收：
// 只改 parent_id，旧目录不再列出、新目录可见，Entity 和内容都不变。
func TestMoveFile(t *testing.T) {
	service, db, diskRoot, ownerA, _ := newTestService(t)
	ctx := context.Background()

	source, err := service.CreateDirectory(ctx, ownerA.ID, "", "source")
	if err != nil {
		t.Fatal(err)
	}
	target, err := service.CreateDirectory(ctx, ownerA.ID, "", "target")
	if err != nil {
		t.Fatal(err)
	}
	record, err := service.Upload(ctx, ownerA.ID, source.ID, "a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}

	var before model.Entity
	if err := db.WithContext(ctx).First(&before).Error; err != nil {
		t.Fatal(err)
	}

	moved, err := service.Move(ctx, ownerA.ID, record.ID, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if moved.ID != record.ID || moved.Name != "a.txt" || moved.Size != 5 || moved.Kind != model.KindFile {
		t.Fatalf("unexpected moved record: %+v", moved)
	}
	if moved.ParentID == nil || *moved.ParentID != target.ID {
		t.Fatalf("moved parent = %v, want %q", moved.ParentID, target.ID)
	}

	// 旧目录看不到它，新目录能看到。
	oldList, err := service.ListChildren(ctx, ownerA.ID, source.ID, 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if oldList.Total != 0 {
		t.Fatalf("old directory still lists the moved file: %+v", oldList)
	}
	newList, err := service.ListChildren(ctx, ownerA.ID, target.ID, 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if newList.Total != 1 || newList.Items[0].ID != record.ID || newList.Items[0].Name != "a.txt" {
		t.Fatalf("new directory does not list the moved file: %+v", newList)
	}

	// 数据库只改了 parent_id；Entity 和磁盘对象都不变。
	var stored model.File
	if err := db.WithContext(ctx).First(&stored, "id = ?", record.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.ParentID == nil || *stored.ParentID != target.ID ||
		stored.Name != "a.txt" || stored.OwnerID != ownerA.ID {
		t.Fatalf("unexpected stored file: %+v", stored)
	}
	var after model.Entity
	if err := db.WithContext(ctx).First(&after, before.ID).Error; err != nil {
		t.Fatal(err)
	}
	if after.StorageKey != before.StorageKey || after.RefCount != before.RefCount ||
		after.Status != before.Status || after.Size != before.Size {
		t.Fatalf("move touched the entity: before=%+v after=%+v", before, after)
	}
	entries, err := os.ReadDir(diskRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != before.StorageKey {
		t.Fatalf("move touched disk: %v", entries)
	}

	// 移动后内容照常下载。
	info, rc, err := service.Open(ctx, ownerA.ID, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	if string(data) != "hello" || info.Size != 5 {
		t.Fatalf("download after move: info=%+v data=%q", info, data)
	}

	// 移回根目录，再移回 target，都只改 parent_id。
	toRoot, err := service.Move(ctx, ownerA.ID, record.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if toRoot.ParentID == nil || *toRoot.ParentID != *ownerA.RootFileID {
		t.Fatalf("move to root: parent = %v, want %q", toRoot.ParentID, *ownerA.RootFileID)
	}
	if list, err := service.ListChildren(ctx, ownerA.ID, target.ID, 1, 20); err != nil || list.Total != 0 {
		t.Fatalf("target should be empty after moving to root: %+v err=%v", list, err)
	}
	// 已经在目标目录里：幂等成功，不存在冲突。
	if _, err := service.Move(ctx, ownerA.ID, record.ID, ""); err != nil {
		t.Fatalf("move to the current parent must succeed: %v", err)
	}
	if _, err := service.Move(ctx, ownerA.ID, record.ID, target.ID); err != nil {
		t.Fatal(err)
	}
	if got := countRows(t, db, &model.Entity{}); got != 1 {
		t.Fatalf("moves created or removed entities: %d", got)
	}
	entries, err = os.ReadDir(diskRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("moves duplicated or removed disk content: %v", entries)
	}
}

// TestMoveDirectory 移动目录带着整棵子树走，子节点和内容都不受影响。
func TestMoveDirectory(t *testing.T) {
	service, db, diskRoot, ownerA, _ := newTestService(t)
	ctx := context.Background()

	outer, err := service.CreateDirectory(ctx, ownerA.ID, "", "outer")
	if err != nil {
		t.Fatal(err)
	}
	inner, err := service.CreateDirectory(ctx, ownerA.ID, outer.ID, "inner")
	if err != nil {
		t.Fatal(err)
	}
	note, err := service.Upload(ctx, ownerA.ID, inner.ID, "a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	target, err := service.CreateDirectory(ctx, ownerA.ID, "", "target")
	if err != nil {
		t.Fatal(err)
	}

	moved, err := service.Move(ctx, ownerA.ID, outer.ID, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if moved.ID != outer.ID || moved.Kind != model.KindDirectory ||
		moved.ParentID == nil || *moved.ParentID != target.ID {
		t.Fatalf("unexpected moved directory: %+v", moved)
	}

	// 根目录只剩 target；target 里是 outer。
	rootList, err := service.ListChildren(ctx, ownerA.ID, "", 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if rootList.Total != 1 || rootList.Items[0].ID != target.ID {
		t.Fatalf("unexpected root listing: %+v", rootList)
	}
	targetList, err := service.ListChildren(ctx, ownerA.ID, target.ID, 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if targetList.Total != 1 || targetList.Items[0].ID != outer.ID {
		t.Fatalf("unexpected target listing: %+v", targetList)
	}

	// 子树没有变化：inner 还在 outer 下，note 还在 inner 下。
	var storedInner model.File
	if err := db.WithContext(ctx).First(&storedInner, "id = ?", inner.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedInner.ParentID == nil || *storedInner.ParentID != outer.ID {
		t.Fatalf("inner parent changed to %v", storedInner.ParentID)
	}
	innerList, err := service.ListChildren(ctx, ownerA.ID, inner.ID, 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if innerList.Total != 1 || innerList.Items[0].ID != note.ID {
		t.Fatalf("unexpected inner listing: %+v", innerList)
	}

	// 内容和磁盘都不变。
	_, rc, err := service.Open(ctx, ownerA.ID, note.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	if string(data) != "hello" {
		t.Fatalf("content changed after moving directory: %q", data)
	}
	if got := countRows(t, db, &model.Entity{}); got != 1 {
		t.Fatalf("moving a directory created entities: %d", got)
	}
	entries, err := os.ReadDir(diskRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("moving a directory touched disk: %v", entries)
	}
}

// TestMoveDirectoryRejectsCycles 目录不能移到自己或自己的后代里。
func TestMoveDirectoryRejectsCycles(t *testing.T) {
	service, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	a, err := service.CreateDirectory(ctx, ownerA.ID, "", "a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := service.CreateDirectory(ctx, ownerA.ID, a.ID, "b")
	if err != nil {
		t.Fatal(err)
	}
	c, err := service.CreateDirectory(ctx, ownerA.ID, b.ID, "c")
	if err != nil {
		t.Fatal(err)
	}
	sibling, err := service.CreateDirectory(ctx, ownerA.ID, "", "sibling")
	if err != nil {
		t.Fatal(err)
	}

	for _, target := range []string{a.ID, b.ID, c.ID} {
		if _, err := service.Move(ctx, ownerA.ID, a.ID, target); !errors.Is(err, ErrInvalidMove) {
			t.Fatalf("move a into %s: wanted ErrInvalidMove, got %v", target, err)
		}
	}
	// 成环检查失败后树原样保留。
	var storedA model.File
	if err := db.WithContext(ctx).First(&storedA, "id = ?", a.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedA.ParentID == nil || *storedA.ParentID != *ownerA.RootFileID {
		t.Fatalf("failed move changed a's parent to %v", storedA.ParentID)
	}

	// 移到无关的兄弟目录是允许的，c 也跟着 b 一起走。
	if _, err := service.Move(ctx, ownerA.ID, b.ID, sibling.ID); err != nil {
		t.Fatalf("move to an unrelated directory: %v", err)
	}
	var storedC model.File
	if err := db.WithContext(ctx).First(&storedC, "id = ?", c.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedC.ParentID == nil || *storedC.ParentID != b.ID {
		t.Fatalf("descendant chain was broken: c parent = %v", storedC.ParentID)
	}
	if _, err := service.Move(ctx, ownerA.ID, a.ID, sibling.ID); err != nil {
		t.Fatalf("move a next to b: %v", err)
	}
}

// TestMoveValidation 覆盖源、目标、根目录和“同目录”的边界情况。
func TestMoveValidation(t *testing.T) {
	service, _, _, ownerA, ownerB := newTestService(t)
	ctx := context.Background()

	source, err := service.CreateDirectory(ctx, ownerA.ID, "", "source")
	if err != nil {
		t.Fatal(err)
	}
	record, err := service.Upload(ctx, ownerA.ID, source.ID, "a.txt", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	other, err := service.CreateDirectory(ctx, ownerB.ID, "", "other")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := service.Move(ctx, ownerA.ID, "does-not-exist", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing source: wanted ErrNotFound, got %v", err)
	}
	if _, err := service.Move(ctx, ownerB.ID, record.ID, other.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another user's source: wanted ErrNotFound, got %v", err)
	}
	if _, err := service.Move(ctx, ownerA.ID, record.ID, "does-not-exist"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing target: wanted ErrNotFound, got %v", err)
	}
	if _, err := service.Move(ctx, ownerA.ID, record.ID, other.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another user's target: wanted ErrNotFound, got %v", err)
	}
	if _, err := service.Move(ctx, ownerA.ID, record.ID, record.ID); !errors.Is(err, ErrNotDirectory) {
		t.Fatalf("target is a file: wanted ErrNotDirectory, got %v", err)
	}
	if _, err := service.Move(ctx, ownerA.ID, *ownerA.RootFileID, source.ID); !errors.Is(err, ErrRootDirectory) {
		t.Fatalf("move root: wanted ErrRootDirectory, got %v", err)
	}

	// 移到当前目录是幂等成功。
	if moved, err := service.Move(ctx, ownerA.ID, record.ID, source.ID); err != nil || moved.Name != "a.txt" {
		t.Fatalf("move to current parent: record=%+v err=%v", moved, err)
	}
}

// TestMoveDuplicateName 目标目录已有同名节点时移动失败，源和目标都不变。
func TestMoveDuplicateName(t *testing.T) {
	service, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	left, err := service.CreateDirectory(ctx, ownerA.ID, "", "left")
	if err != nil {
		t.Fatal(err)
	}
	right, err := service.CreateDirectory(ctx, ownerA.ID, "", "right")
	if err != nil {
		t.Fatal(err)
	}
	movedFile, err := service.Upload(ctx, ownerA.ID, left.ID, "same.txt", strings.NewReader("one"))
	if err != nil {
		t.Fatal(err)
	}
	existing, err := service.Upload(ctx, ownerA.ID, right.ID, "same.txt", strings.NewReader("two"))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := service.Move(ctx, ownerA.ID, movedFile.ID, right.ID); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate name: wanted ErrAlreadyExists, got %v", err)
	}

	// 失败后源还在 left，right 里的还是原来那个文件，内容都没变。
	var stored model.File
	if err := db.WithContext(ctx).First(&stored, "id = ?", movedFile.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.ParentID == nil || *stored.ParentID != left.ID {
		t.Fatalf("failed move changed the parent to %v", stored.ParentID)
	}
	rightList, err := service.ListChildren(ctx, ownerA.ID, right.ID, 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if rightList.Total != 1 || rightList.Items[0].ID != existing.ID {
		t.Fatalf("target directory changed after failed move: %+v", rightList)
	}
	for id, want := range map[string]string{movedFile.ID: "one", existing.ID: "two"} {
		_, rc, err := service.Open(ctx, ownerA.ID, id)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(rc)
		if err != nil {
			t.Fatal(err)
		}
		_ = rc.Close()
		if string(data) != want {
			t.Fatalf("content of %s = %q, want %q", id, data, want)
		}
	}
}
