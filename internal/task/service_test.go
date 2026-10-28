package task

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/auth"
	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/model"
)

// 这些测试需要真实 MySQL。没有设置 DSN 时跳过，
// 这样在没有数据库的机器上 `go test ./...` 仍然能通过。
//
// 本地运行方式：
//
//	CLOUDREVE_TEST_MYSQL_DSN='lucky:lucky@tcp(127.0.0.1:3306)/luckycloudfile_test?parseTime=true' \
//	  go test ./internal/task -v
func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run MySQL integration tests")
	}
	return dsn
}

// openTestDB 打开真实数据库，测试结束时关闭连接池。
func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := database.OpenDSN(context.Background(), testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close(db) })
	return db
}

// createOwner 创建一个真实账号，避免任务指向不存在的 owner。
//
// 用户名带纳秒后缀：测试库可能和别的包共用，不能假设只有自己在写。
func createOwner(t *testing.T, db *gorm.DB, label string) *model.User {
	t.Helper()
	username := fmt.Sprintf("task-test-%s-%d", label, time.Now().UnixNano())
	user, err := auth.CreateUser(context.Background(), db, username, "task-test-password")
	if err != nil {
		t.Fatal(err)
	}
	return user
}

// cleanupOwners 只删除本测试创建的数据，不清空整库。
// 先删文件（users 的外键是 RESTRICT），再删任务和账号。
func cleanupOwners(t *testing.T, db *gorm.DB, ownerIDs ...int) {
	t.Helper()
	for _, ownerID := range ownerIDs {
		if err := db.Where("owner_id = ?", ownerID).Delete(&model.File{}).Error; err != nil {
			t.Errorf("cleanup files of owner %d: %v", ownerID, err)
		}
		if err := db.Where("owner_id = ?", ownerID).Delete(&model.Task{}).Error; err != nil {
			t.Errorf("cleanup tasks of owner %d: %v", ownerID, err)
		}
		if err := db.Where("owner_id = ?", ownerID).Delete(&model.TaskResult{}).Error; err != nil {
			t.Errorf("cleanup task results of owner %d: %v", ownerID, err)
		}
		if err := db.Delete(&model.User{}, ownerID).Error; err != nil {
			t.Errorf("cleanup owner %d: %v", ownerID, err)
		}
	}
}

// TestTaskSurvivesServiceRebuildAndOwnerIsolation 是 P8.1 的验收：
//
//   - 任务提交到数据库后，销毁连接和服务对象，重建后仍能用原 ID 查询；
//   - B 用户查不到 A 用户的任务，表现和任务不存在完全一致。
func TestTaskSurvivesServiceRebuildAndOwnerIsolation(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()

	db, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}

	ownerA := createOwner(t, db, "rebuild-a")
	ownerB := createOwner(t, db, "rebuild-b")

	nextRunAt := time.Now().Add(time.Hour).Truncate(time.Second)
	created, err := NewService(db).Create(ctx, CreateParams{
		Type:         "fake_echo",
		OwnerID:      ownerA.ID,
		Phase:        "prepare",
		StateVersion: 2,
		StateJSON:    `{"input":"hello"}`,
		MaxAttempts:  4,
		NextRunAt:    nextRunAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID == "" {
		t.Fatal("create returned an empty task id")
	}
	if created.Status != model.TaskStatusQueued {
		t.Fatalf("new task status = %q, want %q", created.Status, model.TaskStatusQueued)
	}

	// 销毁连接：模拟进程退出，内存里的 Service 对象也一起丢弃。
	if err := database.Close(db); err != nil {
		t.Fatal(err)
	}

	reopened, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatalf("reopen database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close(reopened) })
	t.Cleanup(func() { cleanupOwners(t, reopened, ownerA.ID, ownerB.ID) })

	rebuilt := NewService(reopened)

	stored, err := rebuilt.GetOwned(ctx, ownerA.ID, created.ID)
	if err != nil {
		t.Fatalf("query task after rebuild: %v", err)
	}
	if stored.ID != created.ID ||
		stored.Type != "fake_echo" ||
		stored.OwnerID != ownerA.ID ||
		stored.Status != model.TaskStatusQueued ||
		stored.Phase != "prepare" ||
		stored.StateVersion != 2 ||
		stored.StateJSON != `{"input":"hello"}` ||
		stored.Attempts != 0 ||
		stored.MaxAttempts != 4 ||
		stored.CleanupPending {
		t.Fatalf("reloaded task lost fields: %+v", stored)
	}
	if !stored.NextRunAt.Equal(nextRunAt) {
		t.Fatalf("reloaded next_run_at = %s, want %s", stored.NextRunAt, nextRunAt)
	}
	if stored.FinishedAt != nil || stored.ResultFileID != nil {
		t.Fatalf("queued task should have no result/finish time: %+v", stored)
	}

	// B 查询 A 的任务必须和“不存在”一样，不能暴露任务是否存在。
	if _, err := rebuilt.GetOwned(ctx, ownerB.ID, created.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other owner query error = %v, want ErrNotFound", err)
	}
	if _, err := rebuilt.GetOwned(ctx, ownerA.ID, "missing-task"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing task error = %v, want ErrNotFound", err)
	}
}

// dueTasksFor 只保留本测试关心的任务。所有包共用同一个测试库，
// 这里不能假设 ListDue 返回的整个列表只有自己创建的行。
func dueTasksFor(tasks []model.Task, ids ...string) []model.Task {
	wanted := make(map[string]bool, len(ids))
	for _, id := range ids {
		wanted[id] = true
	}
	filtered := make([]model.Task, 0, len(ids))
	for _, task := range tasks {
		if wanted[task.ID] {
			filtered = append(filtered, task)
		}
	}
	return filtered
}

// TestListDueAndTryStart 验证到期筛选和执行资格的唯一性。
func TestListDueAndTryStart(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "due")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })
	service := NewService(db)

	now := time.Now()
	past := now.Add(-time.Minute).Truncate(time.Second)
	future := now.Add(time.Hour).Truncate(time.Second)

	due, err := service.Create(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
		StateJSON: `{"n":1}`, MaxAttempts: 3, NextRunAt: past,
	})
	if err != nil {
		t.Fatal(err)
	}
	notDue, err := service.Create(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
		StateJSON: `{"n":2}`, MaxAttempts: 3, NextRunAt: future,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 一个等待退避的任务：先取得执行资格，再放回 retry_wait。
	retry, err := service.Create(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
		StateJSON: `{"n":3}`, MaxAttempts: 3, NextRunAt: past,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.TryStart(ctx, retry.ID, now); err != nil {
		t.Fatal(err)
	}
	if err := service.MarkRetry(ctx, retry.ID, 1, past.Add(time.Second), errors.New("temporary")); err != nil {
		t.Fatal(err)
	}

	dueTasks, err := service.ListDue(ctx, now, 100)
	if err != nil {
		t.Fatal(err)
	}
	dueTasks = dueTasksFor(dueTasks, due.ID, retry.ID)
	if len(dueTasks) != 2 {
		t.Fatalf("ListDue returned %d matching tasks, want 2: %+v", len(dueTasks), dueTasks)
	}
	// 按 next_run_at 升序：due(-1m) 先于 retry(-1m+1s)。
	if dueTasks[0].ID != due.ID || dueTasks[1].ID != retry.ID {
		t.Fatalf("ListDue order = [%s %s], want [%s %s]",
			dueTasks[0].ID, dueTasks[1].ID, due.ID, retry.ID)
	}
	for _, item := range dueTasks {
		if item.ID == notDue.ID {
			t.Fatal("ListDue returned a task whose next_run_at is in the future")
		}
	}

	// 未到期的任务不能开始。
	if _, err := service.TryStart(ctx, notDue.ID, now); !errors.Is(err, ErrConflict) {
		t.Fatalf("start future task error = %v, want ErrConflict", err)
	}

	started, err := service.TryStart(ctx, due.ID, now)
	if err != nil {
		t.Fatal(err)
	}
	if started.Status != model.TaskStatusRunning || started.Attempts != 1 {
		t.Fatalf("started task = status %q attempts %d, want running/1",
			started.Status, started.Attempts)
	}

	// 同一个任务不能被第二次取得执行资格。
	if _, err := service.TryStart(ctx, due.ID, now); !errors.Is(err, ErrConflict) {
		t.Fatalf("second start error = %v, want ErrConflict", err)
	}
	if _, err := service.TryStart(ctx, "missing-task", now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("start missing task error = %v, want ErrNotFound", err)
	}

	// 检查点保存 next phase 和 state，并核对本次 attempts。
	if err := service.SaveCheckpoint(ctx, due.ID, 1, Checkpoint{
		Phase: "compress", StateVersion: 3, StateJSON: `{"n":1,"step":2}`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.SaveCheckpoint(ctx, due.ID, 0, Checkpoint{
		Phase: "publish", StateVersion: 3, StateJSON: `{}`,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale checkpoint error = %v, want ErrConflict", err)
	}

	checkpointed, err := service.GetOwned(ctx, owner.ID, due.ID)
	if err != nil {
		t.Fatal(err)
	}
	if checkpointed.Phase != "compress" ||
		checkpointed.StateVersion != 3 ||
		checkpointed.StateJSON != `{"n":1,"step":2}` {
		t.Fatalf("checkpoint not saved: %+v", checkpointed)
	}

	// 迟到的终态更新同样被 attempts 围栏挡住。
	if err := service.MarkCompleted(ctx, due.ID, 0, "result-file"); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale complete error = %v, want ErrConflict", err)
	}
	if err := service.MarkCompleted(ctx, due.ID, 1, "result-file"); err != nil {
		t.Fatal(err)
	}
	completed, err := service.GetOwned(ctx, owner.ID, due.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != model.TaskStatusCompleted {
		t.Fatalf("completed status = %q", completed.Status)
	}
	if completed.ResultFileID == nil || *completed.ResultFileID != "result-file" {
		t.Fatalf("completed result_file_id = %v, want result-file", completed.ResultFileID)
	}
	if completed.FinishedAt == nil {
		t.Fatal("completed task has no finished_at")
	}
	// 终态不再被调度，也不能再次开始。
	if _, err := service.TryStart(ctx, due.ID, now); !errors.Is(err, ErrConflict) {
		t.Fatalf("start completed task error = %v, want ErrConflict", err)
	}
	remaining, err := service.ListDue(ctx, now, 100)
	if err != nil {
		t.Fatal(err)
	}
	remaining = dueTasksFor(remaining, retry.ID)
	if len(remaining) != 1 || remaining[0].ID != retry.ID {
		t.Fatalf("ListDue after completion = %+v, want only retry task", remaining)
	}
}

// TestRetryWaitAndFailure 验证退避不占 worker、次数上限和失败终态。
func TestRetryWaitAndFailure(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "retry")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })
	service := NewService(db)

	now := time.Now()
	past := now.Add(-time.Minute).Truncate(time.Second)
	later := now.Add(time.Hour).Truncate(time.Second)

	created, err := service.Create(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
		StateJSON: `{}`, MaxAttempts: 2, NextRunAt: past,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.TryStart(ctx, created.ID, now); err != nil {
		t.Fatal(err)
	}
	if err := service.MarkRetry(ctx, created.ID, 1, later, errors.New("storage busy")); err != nil {
		t.Fatal(err)
	}

	waiting, err := service.GetOwned(ctx, owner.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if waiting.Status != model.TaskStatusRetryWait {
		t.Fatalf("retry status = %q", waiting.Status)
	}
	if waiting.LastError != "storage busy" {
		t.Fatalf("retry last_error = %q", waiting.LastError)
	}
	if waiting.Attempts != 1 {
		t.Fatalf("retry attempts = %d, want 1 (attempts only change on start)", waiting.Attempts)
	}
	if !waiting.NextRunAt.Equal(later) {
		t.Fatalf("retry next_run_at = %s, want %s", waiting.NextRunAt, later)
	}

	// 还没到 next_run_at：扫描不到，也不能提前开始。
	due, err := service.ListDue(ctx, now, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(dueTasksFor(due, created.ID)) != 0 {
		t.Fatalf("retry task became due before next_run_at: %+v", due)
	}
	if _, err := service.TryStart(ctx, created.ID, now); !errors.Is(err, ErrConflict) {
		t.Fatalf("early start error = %v, want ErrConflict", err)
	}

	// 到期后重新取得执行资格，attempts 变成 2（达到上限）。
	due, err = service.ListDue(ctx, later.Add(time.Second), 100)
	if err != nil {
		t.Fatal(err)
	}
	matched := dueTasksFor(due, created.ID)
	if len(matched) != 1 || matched[0].ID != created.ID {
		t.Fatalf("ListDue after backoff = %+v, want the retry task", due)
	}
	started, err := service.TryStart(ctx, created.ID, later.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if started.Attempts != 2 {
		t.Fatalf("second attempt = %d, want 2", started.Attempts)
	}

	if err := service.MarkFailed(ctx, created.ID, 2, errors.New("permanent")); err != nil {
		t.Fatal(err)
	}
	failed, err := service.GetOwned(ctx, owner.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != model.TaskStatusFailed ||
		failed.LastError != "permanent" ||
		failed.FinishedAt == nil {
		t.Fatalf("failed task = %+v", failed)
	}

	// 次数已经用完，任何时间都不会再被调度或开始。
	due, err = service.ListDue(ctx, later.Add(24*time.Hour), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(dueTasksFor(due, created.ID)) != 0 {
		t.Fatalf("failed task is still due: %+v", due)
	}
	if _, err := service.TryStart(ctx, created.ID, later.Add(24*time.Hour)); !errors.Is(err, ErrConflict) {
		t.Fatalf("start failed task error = %v, want ErrConflict", err)
	}
}

// TestCreateDefaultsAndValidation 固定创建路径的默认值和非法输入。
func TestCreateDefaultsAndValidation(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "create")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })
	service := NewService(db)

	for name, params := range map[string]CreateParams{
		"missing type":      {OwnerID: owner.ID},
		"missing owner":     {Type: "fake_echo"},
		"negative attempts": {Type: "fake_echo", OwnerID: owner.ID, MaxAttempts: -1},
		"negative version":  {Type: "fake_echo", OwnerID: owner.ID, StateVersion: -1},
	} {
		if _, err := service.Create(ctx, params); err == nil {
			t.Fatalf("%s: Create succeeded, want an error", name)
		}
	}

	created, err := service.Create(ctx, CreateParams{Type: "fake_echo", OwnerID: owner.ID})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID == "" {
		t.Fatal("Create generated an empty id")
	}
	if created.Status != model.TaskStatusQueued ||
		created.Attempts != 0 ||
		created.MaxAttempts != DefaultMaxAttempts ||
		created.StateVersion != DefaultStateVersion ||
		created.NextRunAt.IsZero() ||
		created.CreatedAt.IsZero() ||
		created.UpdatedAt.IsZero() ||
		created.FinishedAt != nil ||
		created.CleanupPending {
		t.Fatalf("unexpected created task: %+v", created)
	}

	// 显式 ID 必须原样保存；重复使用同一个 ID 会撞主键，而不是悄悄生成第二个任务。
	explicit, err := service.Create(ctx, CreateParams{
		ID: "stable-task-id", Type: "fake_echo", OwnerID: owner.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if explicit.ID != "stable-task-id" {
		t.Fatalf("explicit id = %q", explicit.ID)
	}
	if _, err := service.Create(ctx, CreateParams{
		ID: "stable-task-id", Type: "fake_echo", OwnerID: owner.ID,
	}); !errors.Is(err, gorm.ErrDuplicatedKey) {
		t.Fatalf("duplicate id error = %v, want gorm.ErrDuplicatedKey", err)
	}

	// MarkRetry 必须给出明确的 next_run_at，不能靠零值猜成立即执行。
	if _, err := service.TryStart(ctx, explicit.ID, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := service.MarkRetry(ctx, explicit.ID, 1, time.Time{}, errors.New("later")); err == nil {
		t.Fatal("MarkRetry accepted a zero next_run_at")
	}
}
