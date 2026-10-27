package cleanup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"luckyCloudFile/internal/auth"
	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/task"
)

// TestCleanupTaskReclaimsFailedTaskDirectories 验证没有进入 finalize 的失败
// 任务也能被回收：
//
//   - 发布遇到同名冲突/配额不足，任务 failed，但 cleanup_pending 已置位；
//   - 重试耗尽后任务 failed，工作目录仍在；
//   - 清理任务扫描 terminal 记录，删除目录成功后才清除责任标记。
func TestCleanupTaskReclaimsFailedTaskDirectories(t *testing.T) {
	if os.Getenv("CLOUDREVE_TEST_MYSQL_DSN") == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run cleanup integration tests")
	}

	ctx := context.Background()
	db, err := database.OpenDSN(ctx, os.Getenv("CLOUDREVE_TEST_MYSQL_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close(db) })

	owner, err := auth.CreateUser(
		ctx, db,
		fmt.Sprintf("cleanup-task-%d", time.Now().UnixNano()),
		"cleanup-task-password",
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Where("owner_id = ?", owner.ID).Delete(&model.TaskResult{}).Error
		_ = db.Where("owner_id = ?", owner.ID).Delete(&model.Task{}).Error
		_ = db.Where("owner_id = ?", owner.ID).Delete(&model.File{}).Error
		_ = db.Delete(&model.User{}, owner.ID).Error
	})

	service := task.NewService(db)
	workDir := t.TempDir()

	publishFailed := createFailedTask(t, service, owner.ID, "publish failed")
	exhausted := createFailedTask(t, service, owner.ID, "retry exhausted")
	for _, record := range []model.Task{publishFailed, exhausted} {
		dir := filepath.Join(workDir, "task-"+record.ID)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(
			filepath.Join(dir, "archive.zip"), []byte("zip"), 0o600,
		); err != nil {
			t.Fatal(err)
		}
	}

	// 内容回收本身用假实现；本测试只验证失败任务的目录责任。
	files := &fakeFiles{}
	factory := NewTaskFactory(files, service, Options{WorkDir: workDir})
	runner, err := factory.Rebuild(model.Task{Type: TaskType, StateVersion: StateVersion})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(ctx, model.Task{}, nil); err != nil {
		t.Fatal(err)
	}

	for _, record := range []model.Task{publishFailed, exhausted} {
		dir := filepath.Join(workDir, "task-"+record.ID)
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("task %s directory still exists: %v", record.ID, err)
		}
		var stored model.Task
		if err := db.First(&stored, "id = ?", record.ID).Error; err != nil {
			t.Fatal(err)
		}
		if stored.Status != model.TaskStatusFailed {
			t.Fatalf("task %s status = %q, want failed", record.ID, stored.Status)
		}
		if stored.CleanupPending {
			t.Fatalf("task %s cleanup_pending was not cleared", record.ID)
		}
	}
}

func createFailedTask(
	t *testing.T,
	service *task.Service,
	ownerID int,
	lastError string,
) model.Task {
	t.Helper()

	ctx := context.Background()
	created, err := service.Create(ctx, task.CreateParams{
		Type:        "cleanup_failed_test",
		OwnerID:     ownerID,
		Phase:       "publish",
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	started, err := service.TryStart(ctx, created.ID, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SetCleanupPending(ctx, created.ID, started.Attempts, true); err != nil {
		t.Fatal(err)
	}
	if err := service.MarkFailed(
		ctx, created.ID, started.Attempts, errors.New(lastError),
	); err != nil {
		t.Fatal(err)
	}
	return *created
}
