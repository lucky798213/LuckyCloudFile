package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
)

// 这一组测试验证多实例领取的核心约定：同一个任务只会被一个 worker
// 领到；租约过期后可以被接管；被接管的旧 worker 不能再写任何结果。
// 它们需要真实 MySQL（SKIP LOCKED 是数据库能力），没有 DSN 时跳过。

// createLeaseTask 创建一条立即可领取的任务。
func createLeaseTask(t *testing.T, svc *Service, ownerID int, taskType string) *model.Task {
	t.Helper()
	created, err := svc.Create(context.Background(), CreateParams{
		Type:      taskType,
		OwnerID:   ownerID,
		StateJSON: `{}`,
		NextRunAt: time.Now().Add(-time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	return created
}

func TestClaimGivesEachTaskToExactlyOneWorker(t *testing.T) {
	db := openTestDB(t)
	owner := createOwner(t, db, "claim-exclusive")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	ctx := context.Background()
	svc := NewService(db)
	first := createLeaseTask(t, svc, owner.ID, "lease_test")
	second := createLeaseTask(t, svc, owner.ID, "lease_test")

	claimedA, err := svc.Claim(ctx, "worker-a", 1, 30*time.Second, []string{"lease_test"})
	if err != nil {
		t.Fatal(err)
	}
	claimedB, err := svc.Claim(ctx, "worker-b", 1, 30*time.Second, []string{"lease_test"})
	if err != nil {
		t.Fatal(err)
	}
	if len(claimedA) != 1 || len(claimedB) != 1 {
		t.Fatalf("claims = %d and %d, want 1 and 1", len(claimedA), len(claimedB))
	}
	if claimedA[0].TaskID == claimedB[0].TaskID {
		t.Fatalf("both workers claimed task %s", claimedA[0].TaskID)
	}
	for _, claim := range []Claim{claimedA[0], claimedB[0]} {
		if claim.Attempt != 1 {
			t.Fatalf("claim %s attempt = %d, want 1", claim.TaskID, claim.Attempt)
		}
		if claim.WorkerID != "worker-a" && claim.WorkerID != "worker-b" {
			t.Fatalf("claim worker = %q", claim.WorkerID)
		}
		if claim.LeaseUntil.Before(time.Now()) {
			t.Fatalf("claim %s lease already expired", claim.TaskID)
		}
	}

	// 两条任务都已经在 running，重复领取不会再拿到。
	extra, err := svc.Claim(ctx, "worker-c", 5, 30*time.Second, []string{"lease_test"})
	if err != nil {
		t.Fatal(err)
	}
	if len(extra) != 0 {
		t.Fatalf("claimed %d already-running tasks", len(extra))
	}

	if claimedA[0].TaskID != first.ID && claimedA[0].TaskID != second.ID {
		t.Fatalf("claim returned unexpected task %s", claimedA[0].TaskID)
	}
}

func TestClaimOnlyTakesRegisteredTypes(t *testing.T) {
	db := openTestDB(t)
	owner := createOwner(t, db, "claim-types")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	ctx := context.Background()
	svc := NewService(db)
	local := createLeaseTask(t, svc, owner.ID, "local_only")
	remote := createLeaseTask(t, svc, owner.ID, "remote_only")

	claimed, err := svc.Claim(ctx, "worker-a", 5, 30*time.Second, []string{"remote_only"})
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].TaskID != remote.ID {
		t.Fatalf("claimed = %+v, want only task %s", claimed, remote.ID)
	}

	var reloaded model.Task
	if err := db.First(&reloaded, "id = ?", local.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reloaded.Status != model.TaskStatusQueued {
		t.Fatalf("unregistered-type task status = %q, want queued", reloaded.Status)
	}
}

func TestHeartbeatExtendsLease(t *testing.T) {
	db := openTestDB(t)
	owner := createOwner(t, db, "lease-heartbeat")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	ctx := context.Background()
	svc := NewService(db)
	created := createLeaseTask(t, svc, owner.ID, "lease_test")

	claimed, err := svc.Claim(ctx, "worker-a", 1, 30*time.Second, []string{"lease_test"})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %+v, err = %v", claimed, err)
	}

	// 把租约缩短到只剩 1 秒，续租后应该恢复到 30 秒左右。
	almostExpired := time.Now().Add(time.Second)
	if err := db.Model(&model.Task{}).Where("id = ?", created.ID).
		UpdateColumn("lease_until", almostExpired).Error; err != nil {
		t.Fatal(err)
	}
	if err := svc.Heartbeat(ctx, claimed[0]); err != nil {
		t.Fatal(err)
	}

	var reloaded model.Task
	if err := db.First(&reloaded, "id = ?", created.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reloaded.LeaseUntil == nil || reloaded.LeaseUntil.Before(time.Now().Add(20*time.Second)) {
		t.Fatalf("lease_until = %v, want about 30s in the future", reloaded.LeaseUntil)
	}
}

func TestExpiredLeaseIsTakenOverAndOldWorkerIsRejected(t *testing.T) {
	db := openTestDB(t)
	owner := createOwner(t, db, "lease-takeover")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	ctx := context.Background()
	svc := NewService(db)
	created := createLeaseTask(t, svc, owner.ID, "lease_test")

	first, err := svc.Claim(ctx, "worker-a", 1, 30*time.Second, []string{"lease_test"})
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim = %+v, err = %v", first, err)
	}

	// 模拟 worker-a 暂停：心跳停止，租约到期。
	if err := db.Model(&model.Task{}).Where("id = ?", created.ID).
		UpdateColumn("lease_until", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	if err := svc.Heartbeat(ctx, first[0]); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("heartbeat after lease expiry = %v, want ErrClaimLost", err)
	}

	// worker-b 在租约到期后接管同一条任务，attempt 递增。
	second, err := svc.Claim(ctx, "worker-b", 1, 30*time.Second, []string{"lease_test"})
	if err != nil || len(second) != 1 {
		t.Fatalf("second claim = %+v, err = %v", second, err)
	}
	if second[0].TaskID != created.ID {
		t.Fatalf("worker-b claimed %s, want %s", second[0].TaskID, created.ID)
	}
	if second[0].Attempt != 2 || second[0].WorkerID != "worker-b" {
		t.Fatalf("takeover claim = %+v, want attempt 2 by worker-b", second[0])
	}

	// worker-a 恢复后提交结果：业务回调不能执行，任务也不能被改成 completed。
	applied := false
	err = svc.Complete(ctx, first[0], `{"stale":true}`, func(tx *gorm.DB) error {
		applied = true
		return nil
	})
	if !errors.Is(err, ErrClaimLost) {
		t.Fatalf("stale complete = %v, want ErrClaimLost", err)
	}
	if applied {
		t.Fatal("business callback ran for a stale claim")
	}

	var reloaded model.Task
	if err := db.First(&reloaded, "id = ?", created.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reloaded.Status != model.TaskStatusRunning ||
		reloaded.Attempts != 2 || reloaded.WorkerID != "worker-b" ||
		reloaded.ResultJSON != "" {
		t.Fatalf("task after stale complete = %+v", reloaded)
	}

	// 当前持有者可以正常完成，并保存结果投影。
	if err := svc.Complete(ctx, second[0], `{"sha256":"abc"}`, nil); err != nil {
		t.Fatal(err)
	}
	// 用新的结构体读取：GORM 扫描 NULL 到已赋值的指针字段时不会清零，
	// 复用旧变量会把上一轮的 lease_until 带进来。
	var completed model.Task
	if err := db.First(&completed, "id = ?", created.ID).Error; err != nil {
		t.Fatal(err)
	}
	if completed.Status != model.TaskStatusCompleted ||
		completed.ResultJSON != `{"sha256":"abc"}` ||
		completed.LeaseUntil != nil {
		t.Fatalf("completed task = %+v", completed)
	}
}

func TestCompleteRollsBackWhenApplyFails(t *testing.T) {
	db := openTestDB(t)
	owner := createOwner(t, db, "lease-apply")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	ctx := context.Background()
	svc := NewService(db)
	created := createLeaseTask(t, svc, owner.ID, "lease_test")

	claimed, err := svc.Claim(ctx, "worker-a", 1, 30*time.Second, []string{"lease_test"})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %+v, err = %v", claimed, err)
	}

	boom := errors.New("business write failed")
	if err := svc.Complete(ctx, claimed[0], `{"ok":true}`, func(tx *gorm.DB) error {
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("complete with failing apply = %v, want boom", err)
	}

	var reloaded model.Task
	if err := db.First(&reloaded, "id = ?", created.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reloaded.Status != model.TaskStatusRunning || reloaded.ResultJSON != "" {
		t.Fatalf("task after failed apply = %+v", reloaded)
	}

	// 同一个 claim 仍然可以重试收尾。
	if err := svc.Complete(ctx, claimed[0], `{"ok":true}`, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&reloaded, "id = ?", created.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reloaded.Status != model.TaskStatusCompleted {
		t.Fatalf("task status = %q, want completed", reloaded.Status)
	}
}

func TestFailIsFencedByClaim(t *testing.T) {
	db := openTestDB(t)
	owner := createOwner(t, db, "lease-fail")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	ctx := context.Background()
	svc := NewService(db)
	created := createLeaseTask(t, svc, owner.ID, "lease_test")

	first, err := svc.Claim(ctx, "worker-a", 1, 30*time.Second, []string{"lease_test"})
	if err != nil || len(first) != 1 {
		t.Fatalf("claim = %+v, err = %v", first, err)
	}
	if err := db.Model(&model.Task{}).Where("id = ?", created.ID).
		UpdateColumn("lease_until", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	second, err := svc.Claim(ctx, "worker-b", 1, 30*time.Second, []string{"lease_test"})
	if err != nil || len(second) != 1 {
		t.Fatalf("takeover claim = %+v, err = %v", second, err)
	}

	if err := svc.Fail(ctx, first[0], errors.New("stale failure")); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("stale fail = %v, want ErrClaimLost", err)
	}
	if err := svc.Fail(ctx, second[0], errors.New("current failure")); err != nil {
		t.Fatal(err)
	}

	var reloaded model.Task
	if err := db.First(&reloaded, "id = ?", created.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reloaded.Status != model.TaskStatusFailed ||
		reloaded.LastError != "current failure" ||
		reloaded.LeaseUntil != nil {
		t.Fatalf("failed task = %+v", reloaded)
	}
}
