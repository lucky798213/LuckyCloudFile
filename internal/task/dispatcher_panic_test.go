package task

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/queue"
)

// TestDispatcherPanicFinalizesTaskAndWorkerContinues 是 panic 隔离回归：
//
//   - 任务取得执行资格、数据库已经是 running 后业务 panic；
//   - 持久化执行边界必须把 panic 转成错误并写终态，不能永久停在 running；
//   - worker 被队列 recover 保住后，后续正常任务仍能执行。
func TestDispatcherPanicFinalizesTaskAndWorkerContinues(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "panic-worker")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	service := NewService(db).WithMaxPending(100)
	q := newTaskTestQueue(t, 1, 4)

	var calls atomic.Int32
	runner := RunnerFunc(func(
		ctx context.Context,
		record model.Task,
		report queue.ProgressFunc,
	) (string, error) {
		if calls.Add(1) == 1 {
			panic("boom")
		}
		return "result-file", nil
	})
	startTestDispatcher(t, service, q, runner, 20*time.Millisecond)

	panicked, err := service.Submit(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
	})
	if err != nil {
		t.Fatal(err)
	}

	failed := waitTask(t, service, owner.ID, panicked.ID, func(record model.Task) bool {
		return record.Status == model.TaskStatusFailed
	})
	if failed.Attempts != 1 {
		t.Fatalf("panicked task attempts = %d, want 1", failed.Attempts)
	}
	if !strings.Contains(failed.LastError, "panic") ||
		!strings.Contains(failed.LastError, "boom") {
		t.Fatalf("panicked task error = %q", failed.LastError)
	}
	if failed.FinishedAt == nil {
		t.Fatal("panicked task has no finished_at")
	}

	// worker 必须还能领取下一项；如果队列的 recover 吞掉了进程或 worker
	// 没有继续，这里会超时。
	next, err := service.Submit(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := waitTask(t, service, owner.ID, next.ID, func(record model.Task) bool {
		return record.Status == model.TaskStatusCompleted
	})
	if completed.ResultFileID == nil || *completed.ResultFileID != "result-file" {
		t.Fatalf("task after panic result = %v", completed.ResultFileID)
	}
}

// TestDispatcherPanicAfterPublishedResultConvergesCompleted 验证 panic 发生在
// 结果已经发布之后时，先核对 task_results 并收敛为 completed，而不是 failed
// 或 retry_wait。内存队列快照也应显示同一个结果。
func TestDispatcherPanicAfterPublishedResultConvergesCompleted(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "panic-published")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	service := NewService(db).WithMaxPending(100)
	q := newTaskTestQueue(t, 1, 4)

	runner := RunnerFunc(func(
		ctx context.Context,
		record model.Task,
		report queue.ProgressFunc,
	) (string, error) {
		// 模拟业务已经提交结果关联，随后才 panic。
		if err := db.WithContext(ctx).Create(&model.TaskResult{
			TaskID:  record.ID,
			OwnerID: record.OwnerID,
			FileID:  "published-file",
		}).Error; err != nil {
			return "", err
		}
		panic("after publish")
	})
	startTestDispatcher(t, service, q, runner, 20*time.Millisecond)

	submitted, err := service.Submit(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "publish",
	})
	if err != nil {
		t.Fatal(err)
	}

	completed := waitTask(t, service, owner.ID, submitted.ID, func(record model.Task) bool {
		return record.Status == model.TaskStatusCompleted
	})
	if completed.Attempts != 1 {
		t.Fatalf("converged task attempts = %d, want 1", completed.Attempts)
	}
	if completed.ResultFileID == nil || *completed.ResultFileID != "published-file" {
		t.Fatalf("converged result = %v", completed.ResultFileID)
	}
	if completed.LastError != "" {
		t.Fatalf("converged task kept error %q", completed.LastError)
	}
	if !completed.CleanupPending {
		t.Fatal("converged task did not keep cleanup responsibility")
	}

	var count int64
	if err := db.Model(&model.TaskResult{}).
		Where("task_id = ?", submitted.ID).
		Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("task results = %d, want 1", count)
	}

	snapshot, err := q.Get(owner.ID, submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != queue.Completed || snapshot.Result != "published-file" {
		t.Fatalf("queue snapshot = %+v, want completed/published-file", snapshot)
	}
}
