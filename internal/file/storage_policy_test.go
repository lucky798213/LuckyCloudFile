package file

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
)

// entityForFile 取出文件记录绑定的 Entity。
func entityForFile(t *testing.T, env *testEnv, fileID string) model.Entity {
	t.Helper()

	var record model.File
	if err := env.db.First(&record, "id = ?", fileID).Error; err != nil {
		t.Fatal(err)
	}
	if record.EntityID == nil {
		t.Fatalf("file %s has no entity", fileID)
	}
	var entity model.Entity
	if err := env.db.First(&entity, *record.EntityID).Error; err != nil {
		t.Fatal(err)
	}
	return entity
}

// diskEntries 返回一个目录下的文件名，用来确认内容落在哪个位置。
func diskEntries(t *testing.T, root string) []string {
	t.Helper()

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func readFile(t *testing.T, service *Service, ownerID int, id string) string {
	t.Helper()

	_, rc, err := service.Open(context.Background(), ownerID, id)
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
	return string(data)
}

// TestUploadRoutesByServerAssignedPolicy 是本步的核心验收：
// 服务端把新上传位置从 A 调到 B 后，新内容进入 B，旧内容仍从 A 读取和删除。
func TestUploadRoutesByServerAssignedPolicy(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// 默认 A：X 写入 A，Entity 记录 A。
	x, err := env.service.Upload(ctx, env.ownerA.ID, "", "x.txt", strings.NewReader("content-x"))
	if err != nil {
		t.Fatal(err)
	}
	entityX := entityForFile(t, env, x.ID)
	if entityX.StoragePolicyID != env.policyA.ID {
		t.Fatalf("X storage policy = %d, want A=%d", entityX.StoragePolicyID, env.policyA.ID)
	}
	if _, err := os.Stat(filepath.Join(env.rootA, entityX.StorageKey)); err != nil {
		t.Fatalf("X content is not in A: %v", err)
	}
	if names := diskEntries(t, env.rootB); len(names) != 0 {
		t.Fatalf("B must stay empty before the server changes the default: %v", names)
	}

	// 由测试模拟服务端：把账号的新上传位置改成 B。
	if err := env.db.Model(&model.User{}).
		Where("id = ?", env.ownerA.ID).
		UpdateColumn("default_storage_policy_id", env.policyB.ID).Error; err != nil {
		t.Fatal(err)
	}

	// 之后的 Y 写入 B，Entity 记录 B。
	y, err := env.service.Upload(ctx, env.ownerA.ID, "", "y.txt", strings.NewReader("content-y"))
	if err != nil {
		t.Fatal(err)
	}
	entityY := entityForFile(t, env, y.ID)
	if entityY.StoragePolicyID != env.policyB.ID {
		t.Fatalf("Y storage policy = %d, want B=%d", entityY.StoragePolicyID, env.policyB.ID)
	}
	if _, err := os.Stat(filepath.Join(env.rootB, entityY.StorageKey)); err != nil {
		t.Fatalf("Y content is not in B: %v", err)
	}
	// X 仍然只在 A，没有被搬走。
	if _, err := os.Stat(filepath.Join(env.rootA, entityX.StorageKey)); err != nil {
		t.Fatalf("X content disappeared from A: %v", err)
	}

	// 下载各自从记录的位置读取。
	if got := readFile(t, env.service, env.ownerA.ID, x.ID); got != "content-x" {
		t.Fatalf("download X = %q", got)
	}
	if got := readFile(t, env.service, env.ownerA.ID, y.ID); got != "content-y" {
		t.Fatalf("download Y = %q", got)
	}

	// 删除 X 只清理 A，不影响 B 里的 Y。
	if err := env.service.Delete(ctx, env.ownerA.ID, x.ID); err != nil {
		t.Fatal(err)
	}
	if names := diskEntries(t, env.rootA); len(names) != 0 {
		t.Fatalf("deleting X did not clean A: %v", names)
	}
	if _, err := os.Stat(filepath.Join(env.rootB, entityY.StorageKey)); err != nil {
		t.Fatalf("deleting X damaged Y in B: %v", err)
	}
	if got := readFile(t, env.service, env.ownerA.ID, y.ID); got != "content-y" {
		t.Fatalf("download Y after deleting X = %q", got)
	}
	if got := countRows(t, env.db, &model.Entity{}); got != 1 {
		t.Fatalf("wanted only Y's entity left, got %d", got)
	}
}

// callbackReader 在第一次 Read 时执行回调，用来在“选择策略之后、
// 写数据库之前”模拟服务端修改默认策略。
type callbackReader struct {
	reader   io.Reader
	once     sync.Once
	callback func()
}

func (r *callbackReader) Read(p []byte) (int, error) {
	r.once.Do(r.callback)
	return r.reader.Read(p)
}

// TestUploadPolicyIsChosenOnce 验证一次上传只选一次策略：
// 即使中途默认策略从 A 变成 B，本次内容和 Entity 仍然属于 A。
func TestUploadPolicyIsChosenOnce(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	reader := &callbackReader{
		reader: strings.NewReader("snapshot"),
		callback: func() {
			if err := env.db.Model(&model.User{}).
				Where("id = ?", env.ownerA.ID).
				UpdateColumn("default_storage_policy_id", env.policyB.ID).Error; err != nil {
				t.Errorf("change default policy mid-upload: %v", err)
			}
		},
	}

	record, err := env.service.Upload(ctx, env.ownerA.ID, "", "snapshot.txt", reader)
	if err != nil {
		t.Fatal(err)
	}
	entity := entityForFile(t, env, record.ID)
	if entity.StoragePolicyID != env.policyA.ID {
		t.Fatalf("entity policy = %d, want A=%d (the policy chosen before writing)", entity.StoragePolicyID, env.policyA.ID)
	}
	if _, err := os.Stat(filepath.Join(env.rootA, entity.StorageKey)); err != nil {
		t.Fatalf("content was not written to A: %v", err)
	}
	if names := diskEntries(t, env.rootB); len(names) != 0 {
		t.Fatalf("content leaked into B: %v", names)
	}

	// 前提是默认策略真的已经改变，否则这个测试没有意义。
	var user model.User
	if err := env.db.First(&user, env.ownerA.ID).Error; err != nil {
		t.Fatal(err)
	}
	if user.DefaultStoragePolicyID != env.policyB.ID {
		t.Fatalf("default policy = %d, want B=%d", user.DefaultStoragePolicyID, env.policyB.ID)
	}

	// 下一次上传才开始使用 B。
	next, err := env.service.Upload(ctx, env.ownerA.ID, "", "next.txt", strings.NewReader("next"))
	if err != nil {
		t.Fatal(err)
	}
	if got := entityForFile(t, env, next.ID).StoragePolicyID; got != env.policyB.ID {
		t.Fatalf("next upload policy = %d, want B=%d", got, env.policyB.ID)
	}
}

// TestUploadFailsWhenPolicyNotRegistered 验证位置缺失时明确失败：
// 不能写进任意一个目录，也不能留下数据库记录。
func TestUploadFailsWhenPolicyNotRegistered(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// 只注册 B，而用户默认策略还是 A。
	registry := storage.NewRegistry()
	if err := registry.Register(env.policyB.ID, storage.NewLocal(env.rootB)); err != nil {
		t.Fatal(err)
	}
	service := NewService(env.db, registry)

	_, err := service.Upload(ctx, env.ownerA.ID, "", "lost.txt", strings.NewReader("lost"))
	if !errors.Is(err, storage.ErrPolicyNotFound) {
		t.Fatalf("wanted ErrPolicyNotFound, got %v", err)
	}
	if names := diskEntries(t, env.rootA); len(names) != 0 {
		t.Fatalf("A was written despite missing policy: %v", names)
	}
	if names := diskEntries(t, env.rootB); len(names) != 0 {
		t.Fatalf("B was written as a fallback: %v", names)
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("failed upload left %d file rows", got)
	}
	if got := countRows(t, env.db, &model.Entity{}); got != 0 {
		t.Fatalf("failed upload left %d entity rows", got)
	}
}

// TestCleanupUsesEntityPolicyAfterDefaultChanges 验证后台清理也以 Entity 记录为准：
// 默认策略已经改成 B，删除 A 里的旧内容仍然去 A 删除；A 暂时删不掉时保留重试记录。
func TestCleanupUsesEntityPolicyAfterDefaultChanges(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	x, err := env.service.Upload(ctx, env.ownerA.ID, "", "x.txt", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	entityX := entityForFile(t, env, x.ID)
	if entityX.StoragePolicyID != env.policyA.ID {
		t.Fatalf("X policy = %d, want A", entityX.StoragePolicyID)
	}
	if err := env.db.Model(&model.User{}).
		Where("id = ?", env.ownerA.ID).
		UpdateColumn("default_storage_policy_id", env.policyB.ID).Error; err != nil {
		t.Fatal(err)
	}

	y, err := env.service.Upload(ctx, env.ownerA.ID, "", "y.txt", strings.NewReader("y"))
	if err != nil {
		t.Fatal(err)
	}
	entityY := entityForFile(t, env, y.ID)

	// 让 A 的物理删除暂时失败：把对象换成非空目录。
	pathX := filepath.Join(env.rootA, entityX.StorageKey)
	if err := os.Remove(pathX); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(pathX, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pathX, "blocker"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := env.service.Delete(ctx, env.ownerA.ID, x.ID); err != nil {
		t.Fatalf("logical delete must succeed: %v", err)
	}
	var pending model.Entity
	if err := env.db.First(&pending, entityX.ID).Error; err != nil {
		t.Fatalf("failed cleanup must keep the entity for retry: %v", err)
	}
	if pending.Status != model.EntityStatusPendingDelete ||
		pending.StoragePolicyID != env.policyA.ID ||
		pending.RefCount != 0 {
		t.Fatalf("unexpected pending entity: %+v", pending)
	}
	if _, err := os.Stat(filepath.Join(env.rootB, entityY.StorageKey)); err != nil {
		t.Fatalf("B content was touched by failed A cleanup: %v", err)
	}

	// 排掉故障后重试：只清理 A 的对象和记录。
	if err := os.RemoveAll(pathX); err != nil {
		t.Fatal(err)
	}
	result, err := env.service.CleanupPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != 1 || result.Removed != 1 || result.Deferred != 0 {
		t.Fatalf("unexpected cleanup result: %+v", result)
	}
	if names := diskEntries(t, env.rootA); len(names) != 0 {
		t.Fatalf("A still has content after retry: %v", names)
	}
	if got := readFile(t, env.service, env.ownerA.ID, y.ID); got != "y" {
		t.Fatalf("Y changed after A cleanup: %q", got)
	}
}
