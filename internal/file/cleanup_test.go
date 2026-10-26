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
	"time"

	"luckyCloudFile/internal/model"
)

// TestDeleteDefersCleanupUntilRetry 覆盖第一个崩溃点：
// 逻辑删除已经提交，但物理对象暂时删不掉。
//
// Delete 必须返回成功（HTTP 层就是 204），Entity 保持 pending_delete；
// 排掉故障后，下一次扫描把对象和记录一起清掉。
func TestDeleteDefersCleanupUntilRetry(t *testing.T) {
	service, db, root, ownerA, _ := newTestService(t)
	ctx := context.Background()

	record, err := service.Upload(ctx, ownerA.ID, "", "a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}

	var entity model.Entity
	if err := db.WithContext(ctx).First(&entity).Error; err != nil {
		t.Fatal(err)
	}

	// 把磁盘对象换成一个非空目录：os.Remove 会失败，用来模拟“物理清理暂时失败”。
	// 正常上传只会产生普通文件，这里只是制造故障，不改变业务逻辑。
	//
	// 这里直接用测试拿到的 root 拼路径，而不是业务接口：Store 刻意不暴露本地路径，
	// “知道对象在磁盘上的位置”只是本地测试的实现细节。
	path := filepath.Join(root, entity.StorageKey)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "blocker"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	// 物理清理失败不能变成删除失败：逻辑删除已经提交，接口必须仍然成功。
	if err := service.Delete(ctx, ownerA.ID, record.ID); err != nil {
		t.Fatalf("logical delete must succeed even when physical cleanup fails: %v", err)
	}

	// 逻辑文件已经消失，不能再下载。
	if _, _, err := service.Open(ctx, ownerA.ID, record.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("file should be logically gone, got %v", err)
	}
	if got := countKind(t, db, model.KindFile); got != 0 {
		t.Fatalf("wanted no file rows left, got %d", got)
	}

	// Entity 还在，状态是 pending_delete，引用数为 0；磁盘对象也还在。
	var pending model.Entity
	if err := db.WithContext(ctx).First(&pending, entity.ID).Error; err != nil {
		t.Fatalf("entity should be kept for retry: %v", err)
	}
	if pending.Status != model.EntityStatusPendingDelete || pending.RefCount != 0 {
		t.Fatalf("unexpected pending entity: %+v", pending)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("blocked object should still exist: %v", err)
	}

	// 排掉故障后扫描一次：对象和 Entity 都要被清掉。
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	result, err := service.CleanupPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != 1 || result.Removed != 1 || result.Deferred != 0 {
		t.Fatalf("unexpected cleanup result: %+v", result)
	}
	if got := countRows(t, db, &model.Entity{}); got != 0 {
		t.Fatalf("wanted entity removed after retry, got %d", got)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("content still on disk after retry: %v", entries)
	}

	// 清理是幂等的：再扫一次什么都不做。
	result, err = service.CleanupPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != 0 || result.Removed != 0 || result.Deferred != 0 {
		t.Fatalf("second cleanup should be a no-op, got %+v", result)
	}
}

// TestCleanupTreatsMissingObjectAsDeleted 覆盖第二个崩溃点：
// 磁盘对象已经删掉，但 Entity 记录还没来得及删。
func TestCleanupTreatsMissingObjectAsDeleted(t *testing.T) {
	service, db, root, ownerA, _ := newTestService(t)
	ctx := context.Background()

	record, err := service.Upload(ctx, ownerA.ID, "", "gone.txt", strings.NewReader("gone"))
	if err != nil {
		t.Fatal(err)
	}

	var entity model.Entity
	if err := db.WithContext(ctx).First(&entity).Error; err != nil {
		t.Fatal(err)
	}

	// 手工制造“磁盘已删、记录还在”的状态：对象先删掉，
	// 再把 File 行和 Entity 改成逻辑删除已提交后的样子。
	if err := os.Remove(filepath.Join(root, entity.StorageKey)); err != nil {
		t.Fatal(err)
	}
	if err := db.WithContext(ctx).
		Model(&model.Entity{}).
		Where("id = ?", entity.ID).
		Updates(map[string]any{
			"ref_count": 0,
			"status":    model.EntityStatusPendingDelete,
		}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.WithContext(ctx).Where("id = ?", record.ID).Delete(&model.File{}).Error; err != nil {
		t.Fatal(err)
	}

	result, err := service.CleanupPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != 1 || result.Removed != 1 || result.Deferred != 0 {
		t.Fatalf("unexpected cleanup result: %+v", result)
	}
	if got := countRows(t, db, &model.Entity{}); got != 0 {
		t.Fatalf("missing object should still let us delete the entity row, got %d", got)
	}
}

// TestCleanupKeepsEntityThatStillHasReferences 验证扫描时的二次确认：
// 即使状态是 pending_delete，只要还有 File 引用，就绝不能删对象。
func TestCleanupKeepsEntityThatStillHasReferences(t *testing.T) {
	service, db, root, ownerA, _ := newTestService(t)
	ctx := context.Background()

	// 手工制造一条“状态和引用不一致”的数据。正常流程不会这样，
	// 但清理逻辑必须宁可保留对象，也不能删掉还在用的内容。
	// RefCount 没有 GORM default，零值会原样写入。
	entity := model.Entity{
		StorageKey:      "pending-with-reference",
		Size:            1,
		StoragePolicyID: localStoragePolicy(t, db).ID,
		RefCount:        0,
		Status:          model.EntityStatusPendingDelete,
	}
	if err := db.WithContext(ctx).Create(&entity).Error; err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, entity.StorageKey)
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	record := model.File{
		ID:       "referenced-file",
		Name:     "still-used.txt",
		Kind:     model.KindFile,
		ParentID: ownerA.RootFileID,
		EntityID: &entity.ID,
		OwnerID:  ownerA.ID,
	}
	if err := db.WithContext(ctx).Create(&record).Error; err != nil {
		t.Fatal(err)
	}

	result, err := service.CleanupPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != 1 || result.Removed != 0 || result.Deferred != 1 {
		t.Fatalf("referenced entity must be deferred, got %+v", result)
	}
	if got := countRows(t, db, &model.Entity{}); got != 1 {
		t.Fatalf("referenced entity was removed, got %d", got)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("referenced object was removed: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("unexpected disk entries: %v", entries)
	}
}

// TestDeleteSharedEntityWaitsForLastReference 验证引用计数：
// 多个 File 共享一份内容时，删掉一个只能减引用，不能提前删磁盘。
func TestDeleteSharedEntityWaitsForLastReference(t *testing.T) {
	service, db, root, ownerA, _ := newTestService(t)
	ctx := context.Background()

	first, err := service.Upload(ctx, ownerA.ID, "", "first.txt", strings.NewReader("shared content"))
	if err != nil {
		t.Fatal(err)
	}

	var entity model.Entity
	if err := db.WithContext(ctx).First(&entity).Error; err != nil {
		t.Fatal(err)
	}

	// 再挂一个 File 到同一个 Entity 上，并把引用数改成 2。
	second := model.File{
		ID:       "shared-second",
		Name:     "second.txt",
		Kind:     model.KindFile,
		ParentID: ownerA.RootFileID,
		EntityID: &entity.ID,
		OwnerID:  ownerA.ID,
	}
	if err := db.WithContext(ctx).Create(&second).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.WithContext(ctx).
		Model(&model.Entity{}).
		Where("id = ?", entity.ID).
		Update("ref_count", 2).Error; err != nil {
		t.Fatal(err)
	}

	// 删第一个：引用 2 → 1，Entity 仍 active，磁盘内容必须保留。
	if err := service.Delete(ctx, ownerA.ID, first.ID); err != nil {
		t.Fatal(err)
	}
	var active model.Entity
	if err := db.WithContext(ctx).First(&active, entity.ID).Error; err != nil {
		t.Fatal(err)
	}
	if active.Status != model.EntityStatusActive || active.RefCount != 1 {
		t.Fatalf("entity should still be active with 1 reference, got %+v", active)
	}
	if _, err := os.Stat(filepath.Join(root, entity.StorageKey)); err != nil {
		t.Fatalf("content was removed while still referenced: %v", err)
	}

	// 另一个 File 仍能完整下载。
	_, rc, err := service.Open(ctx, ownerA.ID, second.ID)
	if err != nil {
		t.Fatalf("shared file should still be readable: %v", err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	if string(data) != "shared content" {
		t.Fatalf("shared content changed: %q", data)
	}

	// 删最后一个：引用归零 → pending_delete → Delete 内部立即清理成功。
	if err := service.Delete(ctx, ownerA.ID, second.ID); err != nil {
		t.Fatal(err)
	}
	if got := countRows(t, db, &model.Entity{}); got != 0 {
		t.Fatalf("wanted entity removed after last reference, got %d", got)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("content still on disk after last reference: %v", entries)
	}
}

// TestConcurrentDeleteSameFileDecrementsOnce 验证 RowsAffected 检查：
// 同一个文件被并发删除时，只能有一个事务成功扣引用。
func TestConcurrentDeleteSameFileDecrementsOnce(t *testing.T) {
	service, db, _, ownerA, _ := newTestService(t)
	ctx := context.Background()

	record, err := service.Upload(ctx, ownerA.ID, "", "race.txt", strings.NewReader("race"))
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- service.Delete(ctx, ownerA.ID, record.ID)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	var succeeded, notFound int
	for err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrNotFound):
			notFound++
		default:
			t.Fatalf("unexpected concurrent delete error: %v", err)
		}
	}
	if succeeded != 1 || notFound != 1 {
		t.Fatalf("wanted exactly one success and one ErrNotFound, got success=%d notfound=%d", succeeded, notFound)
	}
	if got := countRows(t, db, &model.Entity{}); got != 0 {
		t.Fatalf("entity should be cleaned exactly once, %d left", got)
	}
}

// TestCleanupLoopRunsImmediatelyAndStops 验证 app 启动时依赖的行为：
// 循环进入后马上扫描一次，不需要等 interval；ctx 取消后立即退出。
func TestCleanupLoopRunsImmediatelyAndStops(t *testing.T) {
	service, db, _, _, _ := newTestService(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 对象本来就不存在：清理应该把这条 pending_delete 记录直接删掉。
	// RefCount 没有 GORM default，零值会原样写入。
	entity := model.Entity{
		StorageKey:      "never-created",
		Size:            0,
		StoragePolicyID: localStoragePolicy(t, db).ID,
		RefCount:        0,
		Status:          model.EntityStatusPendingDelete,
	}
	if err := db.WithContext(ctx).Create(&entity).Error; err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		service.CleanupLoop(ctx, time.Hour, 10)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for countRows(t, db, &model.Entity{}) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("cleanup loop did not run immediately at startup")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup loop did not stop after context cancellation")
	}
}
