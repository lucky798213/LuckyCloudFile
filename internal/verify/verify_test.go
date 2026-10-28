package verify

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/auth"
	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
	"luckyCloudFile/internal/task"
)

// 这些测试需要真实 MySQL。没有设置 DSN 时跳过。
//
//	CLOUDREVE_TEST_MYSQL_DSN='lucky:lucky@tcp(127.0.0.1:3306)/luckycloudfile_test?parseTime=true' \
//	  go test ./internal/verify -v

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run MySQL integration tests")
	}
	db, err := database.OpenDSN(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close(db) })
	return db
}

// fixture 是一条“已经有内容、但没有可信摘要”的历史文件。
type fixture struct {
	db       *gorm.DB
	svc      *task.Service
	stores   *storage.Registry
	user     *model.User
	file     model.File
	entity   model.Entity
	dir      string
	policyID int
}

// newFixture 写一份真实内容到临时目录，并建立对应的 files/entities 行。
// 摘要保持 NULL，模拟接入指纹体系之前就存在的旧内容。
func newFixture(t *testing.T, content []byte) *fixture {
	t.Helper()
	ctx := context.Background()
	db := openTestDB(t)

	dir := t.TempDir()
	policyID := ensureLocalPolicy(t, db)
	stores := storage.NewRegistry()
	if err := stores.Register(policyID, storage.NewLocal(dir)); err != nil {
		t.Fatal(err)
	}

	user, err := auth.CreateUser(
		ctx, db,
		fmt.Sprintf("verify-test-%d", time.Now().UnixNano()),
		"verify-test-password",
	)
	if err != nil {
		t.Fatal(err)
	}

	storageKey := fmt.Sprintf("verify-%d", time.Now().UnixNano())
	if err := os.WriteFile(
		filepath.Join(dir, storageKey), content, 0o600,
	); err != nil {
		t.Fatal(err)
	}

	entity := model.Entity{
		StorageKey:      storageKey,
		Size:            int64(len(content)),
		StoragePolicyID: policyID,
		RefCount:        1,
		Status:          model.EntityStatusActive,
	}
	if err := db.Create(&entity).Error; err != nil {
		t.Fatal(err)
	}

	record := model.File{
		ID:       fmt.Sprintf("verify-file-%d", time.Now().UnixNano()),
		Name:     "legacy.bin",
		Kind:     model.KindFile,
		ParentID: user.RootFileID,
		EntityID: &entity.ID,
		OwnerID:  user.ID,
	}
	if err := db.Create(&record).Error; err != nil {
		t.Fatal(err)
	}

	f := &fixture{
		db:       db,
		svc:      task.NewService(db),
		stores:   stores,
		user:     user,
		file:     record,
		entity:   entity,
		dir:      dir,
		policyID: policyID,
	}
	t.Cleanup(func() {
		// 先删内容映射和普通文件，再删根目录：files.parent_id 是自引用外键，
		// 一条语句里先删父目录会违反约束。
		db.Where("owner_id = ?", user.ID).Delete(&model.ContentKey{})
		db.Where("owner_id = ? AND kind = ?", user.ID, model.KindFile).Delete(&model.File{})
		db.Where("owner_id = ? AND kind = ?", user.ID, model.KindDirectory).Delete(&model.File{})
		db.Where("owner_id = ?", user.ID).Delete(&model.Task{})
		db.Delete(&model.Entity{}, entity.ID)
		db.Delete(&model.User{}, user.ID)
	})
	return f
}

// ensureLocalPolicy 取迁移时创建的本地策略；测试不需要新建策略。
func ensureLocalPolicy(t *testing.T, db *gorm.DB) int {
	t.Helper()
	var policy model.StoragePolicy
	if err := db.Where("config_key = ?", model.LocalStorageConfigKey).
		First(&policy).Error; err != nil {
		t.Fatal(err)
	}
	return policy.ID
}

// submit 创建一条直接可领取的内容校验任务。
func (f *fixture) submit(t *testing.T) *model.Task {
	t.Helper()
	state, err := NewState(f.file.ID)
	if err != nil {
		t.Fatal(err)
	}
	stateJSON, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	created, err := f.svc.Create(context.Background(), task.CreateParams{
		Type:         TaskType,
		OwnerID:      f.user.ID,
		Phase:        "verify",
		StateVersion: StateVersion,
		StateJSON:    string(stateJSON),
		NextRunAt:    time.Now().Add(-time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	return created
}

// claim 领取刚创建的任务。
func (f *fixture) claim(t *testing.T, workerID string) task.Claim {
	t.Helper()
	claims, err := f.svc.Claim(
		context.Background(), workerID, 1, 30*time.Second, []string{TaskType},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 1 {
		t.Fatalf("claimed %d tasks, want 1", len(claims))
	}
	return claims[0]
}

// run 用给定 claim 重建执行器并执行一次。
func (f *fixture) run(t *testing.T, claim task.Claim, stores *storage.Registry) (string, error) {
	t.Helper()
	factory := NewFactory(Options{DB: f.db, Stores: stores, Tasks: f.svc})
	runner, err := factory.Rebuild(claim.Record)
	if err != nil {
		t.Fatal(err)
	}
	return runner.Run(context.Background(), claim.Record, nil)
}

func TestRunBackfillsDigestAndContentKey(t *testing.T) {
	content := []byte("legacy content that needs a trusted digest")
	f := newFixture(t, content)
	created := f.submit(t)
	claim := f.claim(t, "worker-a")

	resultJSON, err := f.run(t, claim, f.stores)
	if err != nil {
		t.Fatal(err)
	}

	want := sha256.Sum256(content)
	var result Result
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		t.Fatal(err)
	}
	if result.SHA256 != hex.EncodeToString(want[:]) || result.Size != int64(len(content)) {
		t.Fatalf("result = %+v", result)
	}

	var entity model.Entity
	if err := f.db.First(&entity, f.entity.ID).Error; err != nil {
		t.Fatal(err)
	}
	if entity.ContentSHA256 == nil || *entity.ContentSHA256 != result.SHA256 {
		t.Fatalf("entity digest = %v, want %s", entity.ContentSHA256, result.SHA256)
	}

	var key model.ContentKey
	if err := f.db.Where(
		"owner_id = ? AND storage_policy_id = ? AND sha256 = ? AND size = ?",
		f.user.ID, f.policyID, result.SHA256, result.Size,
	).First(&key).Error; err != nil {
		t.Fatalf("content key not registered: %v", err)
	}
	if key.EntityID != f.entity.ID {
		t.Fatalf("content key entity = %d, want %d", key.EntityID, f.entity.ID)
	}

	var done model.Task
	if err := f.db.First(&done, "id = ?", created.ID).Error; err != nil {
		t.Fatal(err)
	}
	if done.Status != model.TaskStatusCompleted ||
		done.WorkerID != "worker-a" || done.Attempts != 1 ||
		done.LeaseUntil != nil || done.ResultJSON == "" {
		t.Fatalf("completed task = %+v", done)
	}
}

func TestRunRefusesDeletedTargetWithoutRegisteringMapping(t *testing.T) {
	f := newFixture(t, []byte("will be deleted"))
	created := f.submit(t)
	claim := f.claim(t, "worker-a")

	// 内容读取之前目标就没了：直接删除文件行和实体。
	if err := f.db.Where("id = ?", f.file.ID).Delete(&model.File{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.db.Delete(&model.Entity{}, f.entity.ID).Error; err != nil {
		t.Fatal(err)
	}

	_, err := f.run(t, claim, f.stores)
	if !errors.Is(err, ErrTargetGone) {
		t.Fatalf("run = %v, want ErrTargetGone", err)
	}

	var keys int64
	if err := f.db.Model(&model.ContentKey{}).
		Where("owner_id = ?", f.user.ID).Count(&keys).Error; err != nil {
		t.Fatal(err)
	}
	if keys != 0 {
		t.Fatalf("registered %d content keys for a deleted target", keys)
	}

	// 任务收敛为失败，错误信息明确说明目标已不可校验。
	if err := f.svc.Fail(context.Background(), claim, err); err != nil {
		t.Fatal(err)
	}
	var done model.Task
	if err := f.db.First(&done, "id = ?", created.ID).Error; err != nil {
		t.Fatal(err)
	}
	if done.Status != model.TaskStatusFailed ||
		done.LastError == "" || done.LeaseUntil != nil {
		t.Fatalf("failed task = %+v", done)
	}
}

// blockingStore 在 Open 上停住，模拟“读取内容期间发生租约接管”。
type blockingStore struct {
	storage.Store
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.Store.Open(ctx, key)
}

func TestTakeoverDuringReadRejectsStaleWriter(t *testing.T) {
	content := []byte("content read while the lease expires")
	f := newFixture(t, content)

	blocking := &blockingStore{
		Store:   storage.NewLocal(f.dir),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	stores := storage.NewRegistry()
	if err := stores.Register(f.policyID, blocking); err != nil {
		t.Fatal(err)
	}

	created := f.submit(t)
	first := f.claim(t, "worker-a")

	type outcome struct {
		result string
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := f.run(t, first, stores)
		done <- outcome{result: result, err: err}
	}()

	// 等 worker-a 开始读内容，然后让它的租约过期。
	<-blocking.started
	if err := f.db.Model(&model.Task{}).Where("id = ?", created.ID).
		UpdateColumn("lease_until", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}

	// worker-b 接管，attempt 递增。
	second := f.claim(t, "worker-b")
	if second.TaskID != created.ID || second.Attempt != 2 {
		t.Fatalf("takeover claim = %+v", second)
	}

	// 放行 worker-a：它读完了内容，但提交结果时已经失去资格。
	close(blocking.release)
	got := <-done
	if !errors.Is(got.err, task.ErrClaimLost) {
		t.Fatalf("stale run = %v, want ErrClaimLost", got.err)
	}

	var entity model.Entity
	if err := f.db.First(&entity, f.entity.ID).Error; err != nil {
		t.Fatal(err)
	}
	if entity.ContentSHA256 != nil {
		t.Fatalf("stale worker wrote digest %q", *entity.ContentSHA256)
	}
	var keys int64
	if err := f.db.Model(&model.ContentKey{}).
		Where("owner_id = ?", f.user.ID).Count(&keys).Error; err != nil {
		t.Fatal(err)
	}
	if keys != 0 {
		t.Fatalf("stale worker registered %d content keys", keys)
	}

	// 当前持有者可以完成回填。
	if _, err := f.run(t, second, stores); err != nil {
		t.Fatal(err)
	}
	var completed model.Task
	if err := f.db.First(&completed, "id = ?", created.ID).Error; err != nil {
		t.Fatal(err)
	}
	if completed.Status != model.TaskStatusCompleted ||
		completed.WorkerID != "worker-b" || completed.Attempts != 2 {
		t.Fatalf("completed task = %+v", completed)
	}
}
