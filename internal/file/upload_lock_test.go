package file

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"luckyCloudFile/internal/idgen"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/pathlock"
)

// newTestUploadSessionRow 构造一条可以独立插入的会话记录，供核对测试使用。
// 它不依赖创建流程，EntityID 也不会被真正引用。
func newTestUploadSessionRow(env *testEnv, sessionID string) model.UploadSession {
	return model.UploadSession{
		ID:           sessionID,
		OwnerID:      env.ownerA.ID,
		ParentID:     *env.ownerA.RootFileID,
		Name:         "lock-test.txt",
		EntityID:     0,
		ExpectedSize: 0,
		ActualSize:   0,
		Mode:         model.UploadModeFull,
		Status:       model.UploadStatusPending,
		ExpiresAt:    time.Now().Add(UploadSessionTTL),
	}
}

// acquireTestSessionLock 模拟创建会话时已经拿到目标锁并建立绑定。
func acquireTestSessionLock(t *testing.T, env *testEnv, sessionID, name string) *pathLockGuard {
	t.Helper()

	ctx := context.Background()
	target, err := env.service.resolveTargetPath(ctx, env.ownerA.ID, "", name)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := env.service.acquirePathLocks(pathLockRequest{
		Snapshot:  target,
		Scope:     pathlock.Node,
		ExpiresAt: time.Now().Add(UploadSessionTTL),
	})
	if err != nil {
		t.Fatal(err)
	}
	env.service.uploadLocks.bind(sessionID, guard.leases[0])
	return guard
}

// TestUploadSessionHoldsTargetLockUntilCancel 验证会话创建时就占住最终目标：
// 只要会话还活着，父目录的改名/删除（子树锁）就会被目标节点锁挡住；
// 取消会话后锁释放，同样的目录操作可以继续。
func TestUploadSessionHoldsTargetLockUntilCancel(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	dir, err := env.service.CreateDirectory(ctx, env.ownerA.ID, "", "docs")
	if err != nil {
		t.Fatal(err)
	}
	info, err := env.service.CreateUploadSession(
		ctx, env.ownerA.ID, dir.ID, "report.txt", 5,
	)
	if err != nil {
		t.Fatal(err)
	}

	// 父目录改名申请的是子树锁 /docs，和会话的节点锁 /docs/report.txt 冲突。
	if _, err := env.service.Rename(ctx, env.ownerA.ID, dir.ID, "archive"); !errors.Is(err, pathlock.ErrConflict) {
		t.Fatalf("rename parent while session is active error = %v, want ErrConflict", err)
	}
	// 删除父目录同样被挡住，即使目录里还没有正式 File。
	if err := env.service.Delete(ctx, env.ownerA.ID, dir.ID); !errors.Is(err, pathlock.ErrConflict) {
		t.Fatalf("delete parent while session is active error = %v, want ErrConflict", err)
	}

	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, info.ID); err != nil {
		t.Fatal(err)
	}

	// 取消提交后锁立即关闭，不再阻塞目录操作。
	if _, err := env.service.Rename(ctx, env.ownerA.ID, dir.ID, "archive"); err != nil {
		t.Fatalf("rename parent after cancel: %v", err)
	}
}

// TestCompletedUploadReplayIgnoresReleasedLock 验证完成发布后的幂等重放：
// 资源锁已经释放、会话也过期，重复完成仍然返回原结果，不重新发布。
func TestCompletedUploadReplayIgnoresReleasedLock(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	created, _, _ := mustReadyUploadSession(t, env, env.ownerA.ID, "done.txt", "hello")
	completed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.ResultFileID == nil {
		t.Fatal("completed session has no result file")
	}

	// 绑定已经删除，目标锁也已经真正释放：删除结果文件不需要等待锁。
	if _, ok := env.service.uploadLocks.lookup(created.ID); ok {
		t.Fatal("completed session still has a resource lock binding")
	}
	if err := env.service.Delete(ctx, env.ownerA.ID, *completed.ResultFileID); err != nil {
		t.Fatalf("delete published file after completion: %v", err)
	}

	// 会话后来过期，completed 仍然先于过期判断。
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", created.ID).
		UpdateColumn("expires_at", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatal(err)
	}
	replayed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatalf("replay completed session: %v", err)
	}
	if replayed.ResultFileID == nil || *replayed.ResultFileID != *completed.ResultFileID {
		t.Fatalf("replay result = %+v, want %q", replayed, *completed.ResultFileID)
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("replay recreated %d file(s)", got)
	}
}

// TestUploadSessionTransferRejectsLostBinding 验证绑定丢失或令牌失效后，
// 传输和发布都必须拒绝，不能悄悄重新申请一把锁让旧会话复活。
func TestUploadSessionTransferRejectsLostBinding(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// 整文件会话：写完内容后关闭会话锁，再完成必须被拒绝。
	full, err := env.service.CreateUploadSession(ctx, env.ownerA.ID, "", "lost.bin", 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, full.ID, strings.NewReader("hello"),
	); err != nil {
		t.Fatal(err)
	}
	env.service.closeUploadSessionLock(full.ID)
	if _, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, full.ID); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("complete after lock closed error = %v, want ErrUploadSessionConflict", err)
	}

	// 另一个整文件会话还没开始写，绑定关闭后内容上传也必须被拒绝。
	pending, err := env.service.CreateUploadSession(ctx, env.ownerA.ID, "", "lost-content.bin", 5)
	if err != nil {
		t.Fatal(err)
	}
	env.service.closeUploadSessionLock(pending.ID)
	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, pending.ID, strings.NewReader("hello"),
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("content upload after lock closed error = %v, want ErrUploadSessionConflict", err)
	}

	// 分片会话：绑定关闭后新的分片也被拒绝。
	chunked, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "lost-chunk.bin", UploadChunkSize, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	env.service.closeUploadSessionLock(chunked.ID)
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, chunked.ID, 0, strings.NewReader("chunk"),
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("chunk upload after lock closed error = %v, want ErrUploadSessionConflict", err)
	}

	// 清掉三个会话，避免占位内容留在测试库里。
	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, full.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, pending.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, chunked.ID); err != nil {
		t.Fatal(err)
	}
}

// TestUploadSessionCreationFailureReleasesLock 验证创建事务失败时锁被释放：
// 容量不足导致创建失败后，补足容量可以立即用同一个目标重新创建。
func TestUploadSessionCreationFailureReleasesLock(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	setUserQuota(t, env.db, env.ownerA.ID, 0)
	if _, err := env.service.CreateUploadSession(
		ctx, env.ownerA.ID, "", "retry.bin", 10,
	); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("create with zero quota error = %v, want ErrQuotaExceeded", err)
	}

	setUserQuota(t, env.db, env.ownerA.ID, 100)
	info, err := env.service.CreateUploadSession(ctx, env.ownerA.ID, "", "retry.bin", 10)
	if err != nil {
		t.Fatalf("create after capacity failure must not be blocked by a leaked lock: %v", err)
	}
	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, info.ID); err != nil {
		t.Fatal(err)
	}
}

// TestCancelAndExpireCloseTargetLock 验证终态路径都会关闭资源锁：
// 取消和到期之后，同一个目标可以立刻创建新会话。
func TestCancelAndExpireCloseTargetLock(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	canceled, err := env.service.CreateUploadSession(ctx, env.ownerA.ID, "", "cancel.bin", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, canceled.ID); err != nil {
		t.Fatal(err)
	}
	afterCancel, err := env.service.CreateUploadSession(ctx, env.ownerA.ID, "", "cancel.bin", 1)
	if err != nil {
		t.Fatalf("create after cancel: %v", err)
	}
	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, afterCancel.ID); err != nil {
		t.Fatal(err)
	}

	expiring, err := env.service.CreateUploadSession(ctx, env.ownerA.ID, "", "expire.bin", 1)
	if err != nil {
		t.Fatal(err)
	}
	session := loadSessionOnly(t, env, expiring.ID)
	if err := env.service.ExpireUploadSessions(ctx, expiryTestTimeAfter(session), 10); err != nil {
		t.Fatal(err)
	}
	afterExpire, err := env.service.CreateUploadSession(ctx, env.ownerA.ID, "", "expire.bin", 1)
	if err != nil {
		t.Fatalf("create after expiry: %v", err)
	}
	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, afterExpire.ID); err != nil {
		t.Fatal(err)
	}
}

// TestFindUploadSessionForVerificationWaitsForUncommittedInsert 验证创建结果核对
// 使用锁定读：原事务已经插入会话但还没有提交时，核对必须等待，不能因为
// 普通查询暂时看不到未提交行就提前判定“不存在”。
func TestFindUploadSessionForVerificationWaitsForUncommittedInsert(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	sessionID, err := idgen.New()
	if err != nil {
		t.Fatal(err)
	}
	session := newTestUploadSessionRow(env, sessionID)

	tx := env.db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback().Error
		}
	}()
	if err := tx.Create(&session).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		env.db.Delete(&model.UploadSession{}, "id = ?", sessionID)
	})

	type verification struct {
		session *model.UploadSession
		found   bool
		err     error
	}
	resultCh := make(chan verification, 1)
	go func() {
		foundSession, found, verifyErr := env.service.findUploadSessionForVerification(
			ctx, env.ownerA.ID, sessionID,
		)
		resultCh <- verification{session: foundSession, found: found, err: verifyErr}
	}()

	// 原事务还没提交：核对必须停在 SELECT ... FOR UPDATE 上。
	select {
	case result := <-resultCh:
		t.Fatalf("verification decided before the inserting transaction finished: found=%v err=%v",
			result.found, result.err)
	case <-time.After(300 * time.Millisecond):
	}

	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	committed = true

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("verification after commit: %v", result.err)
		}
		if !result.found || result.session == nil || result.session.ID != sessionID {
			t.Fatalf("verification after commit = %+v, want session %s", result, sessionID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("verification did not finish after the inserting transaction committed")
	}
}

// TestFindUploadSessionForVerificationAfterRollback 验证原事务回滚后，
// 锁定读确定看不到会话，调用方可以安全释放锁。
func TestFindUploadSessionForVerificationAfterRollback(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	sessionID, err := idgen.New()
	if err != nil {
		t.Fatal(err)
	}
	session := newTestUploadSessionRow(env, sessionID)

	tx := env.db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	if err := tx.Create(&session).Error; err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}

	foundSession, found, err := env.service.findUploadSessionForVerification(
		ctx, env.ownerA.ID, sessionID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if found || foundSession != nil {
		t.Fatalf("verification saw a rolled-back session: %+v", foundSession)
	}
}

// TestSettleFailedCreationKeepsLockAfterCommit 验证原事务提交后，创建失败核对
// 会保留资源锁和绑定，后续请求仍然受同一把锁保护。
func TestSettleFailedCreationKeepsLockAfterCommit(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	sessionID, err := idgen.New()
	if err != nil {
		t.Fatal(err)
	}
	const name = "settle-commit.bin"
	guard := acquireTestSessionLock(t, env, sessionID, name)

	session := newTestUploadSessionRow(env, sessionID)
	if err := env.db.Create(&session).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		env.service.closeUploadSessionLock(sessionID)
		guard.releaseProtection()
		env.db.Delete(&model.UploadSession{}, "id = ?", sessionID)
	})

	verified, keep, err := env.service.settleFailedUploadSessionCreation(
		ctx, env.ownerA.ID, sessionID, guard,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !keep || verified == nil || verified.ID != sessionID {
		t.Fatalf("settle after commit = (%+v, keep=%v), want session %s and keep=true",
			verified, keep, sessionID)
	}

	// 绑定仍然存在，令牌仍然可以 Confirm：锁没有被提前释放。
	binding, ok := env.service.uploadLocks.lookup(sessionID)
	if !ok {
		t.Fatal("resource lock binding removed after committed creation")
	}
	release, err := env.service.pathLocks.Confirm(
		binding.namespace, binding.path, binding.token,
	)
	if err != nil {
		t.Fatalf("resource lock was released after committed creation: %v", err)
	}
	release()
}

// TestSettleFailedCreationReleasesLockAfterRollback 验证确定回滚时，
// 创建失败核对会删除绑定并释放目标锁。
func TestSettleFailedCreationReleasesLockAfterRollback(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	sessionID, err := idgen.New()
	if err != nil {
		t.Fatal(err)
	}
	const name = "settle-rollback.bin"
	guard := acquireTestSessionLock(t, env, sessionID, name)

	verified, keep, err := env.service.settleFailedUploadSessionCreation(
		ctx, env.ownerA.ID, sessionID, guard,
	)
	if err != nil {
		t.Fatal(err)
	}
	if keep || verified != nil {
		t.Fatalf("settle after rollback = (%+v, keep=%v), want nil and keep=false",
			verified, keep)
	}
	if _, ok := env.service.uploadLocks.lookup(sessionID); ok {
		t.Fatal("resource lock binding was not removed after rollback")
	}

	// 目标锁已经释放：同一个路径可以立即重新申请。
	target, err := env.service.resolveTargetPath(ctx, env.ownerA.ID, "", name)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := env.service.acquirePathLocks(pathLockRequest{
		Snapshot:  target,
		Scope:     pathlock.Node,
		ExpiresAt: time.Now().Add(UploadSessionTTL),
	})
	if err != nil {
		t.Fatalf("target lock was not released after rollback: %v", err)
	}
	fresh.release()
}
