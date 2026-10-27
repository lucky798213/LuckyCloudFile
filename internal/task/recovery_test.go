package task

import (
	"context"
	"testing"
	"time"

	"luckyCloudFile/internal/model"
)

// TestRecoverRunningConvergesInterruptedTasks 覆盖启动恢复的三条分支：
//
//   - 结果已发布：直接补 completed，不清空 phase/state，也不重跑；
//   - 未发布且还有次数：保留最后检查点，放回 queued；
//   - 未发布且次数用完：明确 failed，不绕过执行上限。
func TestRecoverRunningConvergesInterruptedTasks(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "recover-running")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	service := NewService(db)
	now := time.Now().Truncate(time.Second)

	published, err := service.Create(ctx, CreateParams{
		ID: "recover-published", Type: "fake_echo", OwnerID: owner.ID,
		Phase: "finalize", StateJSON: `{"step":"finalize"}`,
		MaxAttempts: 3, NextRunAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.TryStart(ctx, published.ID, now); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&model.TaskResult{
		TaskID: published.ID, OwnerID: owner.ID, FileID: "result-file",
	}).Error; err != nil {
		t.Fatal(err)
	}

	requeued, err := service.Create(ctx, CreateParams{
		ID: "recover-requeued", Type: "fake_echo", OwnerID: owner.ID,
		Phase: "compress", StateJSON: `{"step":"compress"}`,
		MaxAttempts: 3, NextRunAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.TryStart(ctx, requeued.ID, now); err != nil {
		t.Fatal(err)
	}

	exhausted, err := service.Create(ctx, CreateParams{
		ID: "recover-exhausted", Type: "fake_echo", OwnerID: owner.ID,
		Phase: "prepare", StateJSON: `{}`,
		MaxAttempts: 1, NextRunAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.TryStart(ctx, exhausted.ID, now); err != nil {
		t.Fatal(err)
	}

	stats, err := service.RecoverRunning(ctx, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Completed != 1 || stats.Requeued != 1 || stats.Failed != 1 {
		t.Fatalf("recovery stats = %+v, want 1/1/1", stats)
	}

	completed, err := service.GetOwned(ctx, owner.ID, published.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != model.TaskStatusCompleted ||
		completed.ResultFileID == nil || *completed.ResultFileID != "result-file" ||
		!completed.CleanupPending ||
		completed.Phase != "finalize" ||
		completed.Attempts != 1 {
		t.Fatalf("recovered published task = %+v", completed)
	}
	if completed.FinishedAt == nil {
		t.Fatal("recovered published task has no finished_at")
	}

	stored, err := service.GetOwned(ctx, owner.ID, requeued.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != model.TaskStatusQueued ||
		stored.Phase != "compress" ||
		stored.StateJSON != `{"step":"compress"}` ||
		stored.Attempts != 1 ||
		stored.NextRunAt.After(now.Add(time.Second)) {
		t.Fatalf("requeued task = %+v", stored)
	}

	failed, err := service.GetOwned(ctx, owner.ID, exhausted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != model.TaskStatusFailed || failed.Attempts != 1 {
		t.Fatalf("exhausted task = %+v", failed)
	}
	if failed.LastError == "" {
		t.Fatal("exhausted task has no error")
	}

	// 已完成和失败的任务不再被扫描；只有 requeued 会被重新发现。
	due, err := service.ListDue(ctx, now.Add(time.Second), 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, record := range due {
		if record.ID == requeued.ID {
			found = true
		}
		if record.ID == published.ID || record.ID == exhausted.ID {
			t.Fatalf("terminal task %q was listed as due", record.ID)
		}
	}
	if !found {
		t.Fatal("recovered task was not listed as due")
	}
}
