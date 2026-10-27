package task

import (
	"context"
	"testing"
	"time"

	"luckyCloudFile/internal/model"
)

// TestEnsurePeriodicDeduplicates 验证周期触发去重：
//
//   - 已有未终态任务时只复用，不插入第二行；
//   - 上一条进入终态后才允许创建下一条。
func TestEnsurePeriodicDeduplicates(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "periodic")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	service := NewService(db)
	params := CreateParams{
		Type: "periodic_test_v1", OwnerID: owner.ID, Phase: "scan",
		StateVersion: 1, StateJSON: `{}`,
	}

	first, err := service.EnsurePeriodic(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.EnsurePeriodic(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID {
		t.Fatalf("periodic trigger created a second task: %s vs %s", first.ID, second.ID)
	}

	var pending int64
	if err := db.Model(&model.Task{}).
		Where("type = ? AND status IN ?", params.Type, unfinishedTaskStatuses).
		Count(&pending).Error; err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("pending periodic tasks = %d, want 1", pending)
	}

	started, err := service.TryStart(ctx, first.ID, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.MarkCompleted(ctx, first.ID, started.Attempts, ""); err != nil {
		t.Fatal(err)
	}

	third, err := service.EnsurePeriodic(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	if third.ID == first.ID {
		t.Fatal("periodic trigger reused a terminal task")
	}
}

// TestListCleanupPendingIgnoresNonTerminalTasks 验证清理任务只接管
// 已经结束的任务，不会在业务仍执行时并发删除它的工作目录。
func TestListCleanupPendingIgnoresNonTerminalTasks(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "cleanup-pending")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	service := NewService(db)
	running, err := service.Create(ctx, CreateParams{
		Type: "cleanup_pending_test", OwnerID: owner.ID, Phase: "scan",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.TryStart(ctx, running.ID, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := service.SetCleanupPending(ctx, running.ID, 1, true); err != nil {
		t.Fatal(err)
	}

	terminal, err := service.Create(ctx, CreateParams{
		Type: "cleanup_pending_test", OwnerID: owner.ID, Phase: "scan",
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.TryStart(ctx, terminal.ID, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := service.MarkFailed(ctx, terminal.ID, 1,
		context.DeadlineExceeded); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.Task{}).
		Where("id = ?", terminal.ID).
		UpdateColumn("cleanup_pending", true).Error; err != nil {
		t.Fatal(err)
	}

	tasks, err := service.ListCleanupPending(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	foundTerminal := false
	for _, record := range tasks {
		if record.ID == running.ID {
			t.Fatal("running task was returned as cleanup pending")
		}
		if record.ID == terminal.ID {
			foundTerminal = true
		}
	}
	if !foundTerminal {
		t.Fatal("terminal cleanup-pending task was not listed")
	}

	if err := service.ClearCleanupPending(ctx, terminal.ID); err != nil {
		t.Fatal(err)
	}
	var stored model.Task
	if err := db.First(&stored, "id = ?", terminal.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.CleanupPending {
		t.Fatal("cleanup pending flag was not cleared")
	}
}
