package file

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
)

// 本文件是 P4 容量闭环的验收测试：预留、完成结算、取消/过期/重启释放，
// 以及旧上传、复制、删除是否都按“逻辑文件用量”记账。
//
// 所有断言都直接读 users 表上的三个字段，不只看接口返回值：
// 容量账目是持久化事实，不能靠内存状态或日志证明。

// setUserQuota 直接设置测试账号的配额。生产路径由服务端在建号时写入。
func setUserQuota(t *testing.T, db *gorm.DB, userID int, quotaBytes int64) {
	t.Helper()
	if err := db.Model(&model.User{}).
		Where("id = ?", userID).
		UpdateColumn("quota_bytes", quotaBytes).Error; err != nil {
		t.Fatal(err)
	}
}

// userAccounting 从数据库读出最新的容量账目。
func userAccounting(t *testing.T, db *gorm.DB, userID int) model.User {
	t.Helper()
	var user model.User
	if err := db.Select("id", "quota_bytes", "used_bytes", "reserved_bytes").
		First(&user, userID).Error; err != nil {
		t.Fatal(err)
	}
	return user
}

// assertAccounting 校验已用和预留容量，并顺带确认没有越过配额上界。
func assertAccounting(t *testing.T, db *gorm.DB, userID int, wantUsed, wantReserved int64) {
	t.Helper()
	user := userAccounting(t, db, userID)
	if user.UsedBytes != wantUsed || user.ReservedBytes != wantReserved {
		t.Fatalf(
			"user %d accounting: used=%d reserved=%d, want used=%d reserved=%d",
			userID, user.UsedBytes, user.ReservedBytes, wantUsed, wantReserved,
		)
	}
	if user.UsedBytes < 0 || user.ReservedBytes < 0 ||
		user.UsedBytes+user.ReservedBytes > user.QuotaBytes {
		t.Fatalf(
			"user %d broke the quota invariant: used=%d reserved=%d quota=%d",
			userID, user.UsedBytes, user.ReservedBytes, user.QuotaBytes,
		)
	}
}

// assertSessionReserved 校验会话上持久化的预留量。
func assertSessionReserved(t *testing.T, db *gorm.DB, sessionID string, want int64) {
	t.Helper()
	var session model.UploadSession
	if err := db.Select("id", "reserved_bytes").First(&session, "id = ?", sessionID).Error; err != nil {
		t.Fatal(err)
	}
	if session.ReservedBytes != want {
		t.Fatalf("session %s reserved_bytes = %d, want %d", sessionID, session.ReservedBytes, want)
	}
}

// TestCreateUploadSessionConcurrentReservation 对应验收 1：
// 剩余容量只够一个会话时，两个并发创建只能成功一个。
func TestCreateUploadSessionConcurrentReservation(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const size = int64(10)
	setUserQuota(t, env.db, env.ownerA.ID, size)

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			// 两个请求同时抢最后一份容量。
			_, err := env.service.CreateUploadSession(
				ctx, env.ownerA.ID, "", fmt.Sprintf("race-%d.bin", i), size,
			)
			results <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)

	var successes, exceeded int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrQuotaExceeded):
			exceeded++
		default:
			t.Fatalf("unexpected concurrent create error: %v", err)
		}
	}
	if successes != 1 || exceeded != 1 {
		t.Fatalf("concurrent reservation: successes=%d exceeded=%d, want 1/1", successes, exceeded)
	}

	// 失败的请求不能留下任何占位记录或预留。
	assertAccounting(t, env.db, env.ownerA.ID, 0, size)
	if got := countRows(t, env.db, &model.UploadSession{}); got != 1 {
		t.Fatalf("concurrent create left %d sessions, want 1", got)
	}
	if got := countRows(t, env.db, &model.Entity{}); got != 1 {
		t.Fatalf("concurrent create left %d entities, want 1", got)
	}
}

// TestCreateUploadSessionReservesAndRollsBack 对应验收 2：
// 成功后预留；会话插入失败时，预留和占位 Entity 一起回滚。
func TestCreateUploadSessionReservesAndRollsBack(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const size = int64(4096)
	setUserQuota(t, env.db, env.ownerA.ID, size*2)

	created, err := env.service.CreateUploadSession(ctx, env.ownerA.ID, "", "reserve.bin", size)
	if err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, size)
	assertSessionReserved(t, env.db, created.ID, size)

	// 空文件不占容量，也不能被误判成“更新零行 = 容量不足”。
	empty, err := env.service.CreateUploadSession(ctx, env.ownerA.ID, "", "empty.bin", 0)
	if err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, size)
	assertSessionReserved(t, env.db, empty.ID, 0)

	// 再次创建相同大小：剩余容量只够一个，必须被拒绝且不留痕迹。
	if _, err := env.service.CreateUploadSession(
		ctx, env.ownerA.ID, "", "too-much.bin", size*2,
	); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("over-quota create error = %v, want ErrQuotaExceeded", err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, size)
	if got := countRows(t, env.db, &model.UploadSession{}); got != 2 {
		t.Fatalf("rejected create left %d sessions, want 2", got)
	}
}

// TestCompleteUploadSessionSettlesQuotaOnce 对应验收 3：
// 完成把预留转成已用；重复完成只返回原结果，不再次计费。
func TestCompleteUploadSessionSettlesQuotaOnce(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const size = int64(5)
	setUserQuota(t, env.db, env.ownerA.ID, 100)

	created, _, _ := mustReadyUploadSession(t, env, env.ownerA.ID, "settle.txt", "hello")
	assertAccounting(t, env.db, env.ownerA.ID, 0, size)

	first, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, size, 0)
	assertSessionReserved(t, env.db, created.ID, 0)

	again, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.ResultFileID == nil || first.ResultFileID == nil ||
		*again.ResultFileID != *first.ResultFileID {
		t.Fatalf("repeat completion result = %+v, want %q", again, *first.ResultFileID)
	}
	// 重复完成没有二次计费，也没有重新预留。
	assertAccounting(t, env.db, env.ownerA.ID, size, 0)
	assertSessionReserved(t, env.db, created.ID, 0)
}

// TestCompleteUploadSessionFailureKeepsReservation 覆盖“发布失败但预留保留”：
// 事务回滚后预留不能被回滚掉；同一个会话在有效期内可以重试完成。
//
// 失败用一次性数据库错误模拟，而不是真的制造同名冲突：会话目标路径被
// 资源锁占住，同名冲突文件在会话存活期间也无法通过 Service 删除。
func TestCompleteUploadSessionFailureKeepsReservation(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const size = int64(5)
	setUserQuota(t, env.db, env.ownerA.ID, 100)

	created, _, _ := mustReadyUploadSession(t, env, env.ownerA.ID, "retry.bin", "hello")
	assertAccounting(t, env.db, env.ownerA.ID, 0, size)

	const callbackName = "test:fail_completion_update_once"
	forcedErr := errors.New("forced completion update failure")
	failed := false
	if err := env.db.Callback().Update().Before("gorm:update").Register(
		callbackName,
		func(tx *gorm.DB) {
			if failed || tx.Statement.Schema == nil ||
				tx.Statement.Schema.Table != (model.UploadSession{}).TableName() {
				return
			}
			failed = true
			_ = tx.AddError(forcedErr)
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.db.Callback().Update().Remove(callbackName); err != nil {
			t.Logf("remove test callback: %v", err)
		}
	})

	if _, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID); !errors.Is(err, forcedErr) {
		t.Fatalf("failed completion error = %v, want forced failure", err)
	}
	// 事务回滚：会话仍然 ready，预留和已用都没有变化，内容也还在。
	session := loadSessionOnly(t, env, created.ID)
	if session.Status != model.UploadStatusReady {
		t.Fatalf("failed completion status = %q, want ready", session.Status)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, size)
	assertSessionReserved(t, env.db, created.ID, size)
	if _, ok := env.service.uploadLocks.lookup(created.ID); !ok {
		t.Fatal("failed completion must keep the session resource lock for retry")
	}

	// 会话锁没有被失败释放：同一个会话重试完成，只结算这一次。
	if _, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID); err != nil {
		t.Fatalf("retry completion: %v", err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, size, 0)
	assertSessionReserved(t, env.db, created.ID, 0)
	if _, ok := env.service.uploadLocks.lookup(created.ID); ok {
		t.Fatal("completed session must release the resource lock")
	}
}

// TestTerminatedUploadSessionsReleaseReservationOnce 对应验收 4 和 7 的账目部分：
// 取消、过期、重启失效都在终态事务里释放预留，重复执行不再扣减。
func TestTerminatedUploadSessionsReleaseReservationOnce(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const size = int64(10)
	setUserQuota(t, env.db, env.ownerA.ID, 100)

	canceled := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "cancel.bin", size)
	expired := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "expire.bin", size)
	recovered := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "recover.bin", size)
	assertAccounting(t, env.db, env.ownerA.ID, 0, size*3)

	// 1. 主动取消：预留立即释放；重复取消不再扣减。
	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, canceled.ID); err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, size*2)
	assertSessionReserved(t, env.db, canceled.ID, 0)

	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, canceled.ID); err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, size*2)

	// 2. 自动过期：只让第二条到期。
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", expired.ID).
		UpdateColumn("expires_at", time.Now().Add(-time.Minute)).Error; err != nil {
		t.Fatal(err)
	}
	if err := env.service.ExpireUploadSessions(ctx, time.Now(), 10); err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, size)
	assertSessionReserved(t, env.db, expired.ID, 0)

	// 3. 启动恢复：把剩下的未完成会话作废并释放预留。
	if err := env.service.InvalidateUnfinishedUploads(ctx, 10); err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, 0)
	assertSessionReserved(t, env.db, recovered.ID, 0)

	// 再跑一遍所有扫描和取消：预留已经是 0，不能重复释放成负数。
	if err := env.service.ExpireUploadSessions(ctx, time.Now().Add(time.Hour), 10); err != nil {
		t.Fatal(err)
	}
	if err := env.service.InvalidateUnfinishedUploads(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, expired.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, recovered.ID); err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, 0)
}

// TestCompleteAndCancelRaceKeepsAccountingConsistent 对应验收 5：
// 完成与取消竞争时，最终账目必须和胜出的状态一致。
func TestCompleteAndCancelRaceKeepsAccountingConsistent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const size = int64(5)
	const rounds = 5
	setUserQuota(t, env.db, env.ownerA.ID, size*rounds+100)

	var wantUsed int64
	var wantFiles int64
	for round := 0; round < rounds; round++ {
		created, _, _ := mustReadyUploadSession(
			t, env, env.ownerA.ID, fmt.Sprintf("race-complete-%d.txt", round), "hello",
		)

		start := make(chan struct{})
		completeResult := make(chan error, 1)
		cancelResult := make(chan error, 1)

		go func() {
			<-start
			_, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID)
			completeResult <- err
		}()
		go func() {
			<-start
			_, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, created.ID)
			cancelResult <- err
		}()
		close(start)

		completeErr := <-completeResult
		cancelErr := <-cancelResult

		session := loadSessionOnly(t, env, created.ID)
		switch session.Status {
		case model.UploadStatusCompleted:
			if completeErr != nil {
				t.Fatalf("round %d: completed but completion failed: %v", round, completeErr)
			}
			if !errors.Is(cancelErr, ErrUploadSessionCompleted) {
				t.Fatalf("round %d: completed session cancel error = %v, want ErrUploadSessionCompleted", round, cancelErr)
			}
			wantUsed += size
			wantFiles++
			if got := countKind(t, env.db, model.KindFile); got != wantFiles {
				t.Fatalf("round %d: completed round produced %d files, want %d", round, got, wantFiles)
			}
		case model.UploadStatusCanceled:
			if completeErr == nil {
				t.Fatalf("round %d: canceled session was completed", round)
			}
			if cancelErr != nil {
				t.Fatalf("round %d: canceled but cancel failed: %v", round, cancelErr)
			}
			if got := countKind(t, env.db, model.KindFile); got != wantFiles {
				t.Fatalf("round %d: canceled round created a file (%d files, want %d)", round, got, wantFiles)
			}
		default:
			t.Fatalf("round %d: unexpected final status %q", round, session.Status)
		}

		assertAccounting(t, env.db, env.ownerA.ID, wantUsed, 0)
		assertSessionReserved(t, env.db, created.ID, 0)
	}
}

// TestUploadContentFailureKeepsReservation 对应验收 6：
// 内容传输失败退回 pending 时保留预留，重传不重复预留。
func TestUploadContentFailureKeepsReservation(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const size = int64(5)
	setUserQuota(t, env.db, env.ownerA.ID, 100)

	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "retry.txt", size)
	assertAccounting(t, env.db, env.ownerA.ID, 0, size)

	// 多传一个字节：失败，但会话回到 pending，预留必须保留。
	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, created.ID, strings.NewReader("hello!"),
	); !errors.Is(err, ErrUploadSizeMismatch) {
		t.Fatalf("oversized content error = %v, want ErrUploadSizeMismatch", err)
	}
	session, _ := loadUploadSessionAndEntity(t, env, created.ID)
	if session.Status != model.UploadStatusPending {
		t.Fatalf("failed content left status %q, want pending", session.Status)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, size)
	assertSessionReserved(t, env.db, created.ID, size)

	// 重传正确内容：预留不变；完成后只结算一次。
	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, created.ID, strings.NewReader("hello"),
	); err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, size)

	if _, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID); err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, size, 0)
}

// removeFailingStore 让 Save/Open 照常工作，但物理删除总是失败。
// 用来验证“清理反复失败不会重复扣容量”。
type removeFailingStore struct {
	storage.Store
	err error
}

func (s removeFailingStore) Remove(context.Context, string) error {
	return s.err
}

// TestPhysicalCleanupFailureKeepsAccounting 对应验收 7：
// 会话已经在终态事务里释放过预留，物理清理反复失败也不能再扣一次。
func TestPhysicalCleanupFailureKeepsAccounting(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	policyC := ensureTestPolicy(t, env.db, "quota-cleanup-failure", "C")
	if err := env.service.stores.Register(policyC.ID, removeFailingStore{
		Store: storage.NewLocal(t.TempDir()),
		err:   errors.New("simulated physical cleanup failure"),
	}); err != nil {
		t.Fatal(err)
	}
	// 让这个测试的上传走会失败的清理目录。
	if err := env.db.Model(&model.User{}).
		Where("id = ?", env.ownerA.ID).
		UpdateColumn("default_storage_policy_id", policyC.ID).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// 先解除引用，再删策略，避免外键 RESTRICT 挡住清理。
		_ = env.db.Model(&model.Entity{}).
			Where("storage_policy_id = ?", policyC.ID).
			UpdateColumn("storage_policy_id", env.policyA.ID).Error
		_ = env.db.Model(&model.User{}).
			Where("default_storage_policy_id = ?", policyC.ID).
			UpdateColumn("default_storage_policy_id", env.policyA.ID).Error
		_ = env.db.Delete(&model.StoragePolicy{}, policyC.ID).Error
	})

	setUserQuota(t, env.db, env.ownerA.ID, 100)
	created, _, _ := mustReadyUploadSession(t, env, env.ownerA.ID, "stuck.bin", "hello")
	assertAccounting(t, env.db, env.ownerA.ID, 0, 5)

	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, created.ID); err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, 0)
	assertSessionReserved(t, env.db, created.ID, 0)

	// 取消时已经尝试过一次物理删除并失败；多轮清理仍然失败，
	// 但容量账目保持不变（预留和已用都不再变化）。
	for round := 0; round < 3; round++ {
		if _, err := env.service.CleanupPending(ctx, 10); err != nil {
			t.Fatal(err)
		}
		assertAccounting(t, env.db, env.ownerA.ID, 0, 0)
	}

	_, entity := loadUploadSessionAndEntity(t, env, created.ID)
	if entity.Status != model.EntityStatusPendingDelete {
		t.Fatalf("entity status after failed cleanup = %q, want pending_delete", entity.Status)
	}
}

// TestCopyAndDeleteAccountLogicalUsage 对应验收 8：
// 复制增加一份用量；删除共享实体的一个引用只释放一份。
func TestCopyAndDeleteAccountLogicalUsage(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const size = int64(5)
	setUserQuota(t, env.db, env.ownerA.ID, 20)

	original, err := env.service.Upload(ctx, env.ownerA.ID, "", "orig.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, size, 0)

	copied, err := env.service.Copy(ctx, env.ownerA.ID, original.ID, "", "copy.txt")
	if err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, size*2, 0)
	if got := readFile(t, env.service, env.ownerA.ID, copied.ID); got != "hello" {
		t.Fatalf("copied content = %q", got)
	}

	// 删除源文件：Entity 还有副本引用，物理内容保留；逻辑用量只释放一份。
	if err := env.service.Delete(ctx, env.ownerA.ID, original.ID); err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, size, 0)
	if got := readFile(t, env.service, env.ownerA.ID, copied.ID); got != "hello" {
		t.Fatalf("copy lost its content after deleting the original: %q", got)
	}

	// 剩余容量正好等于已用：复制必须被拒绝，不能创建 File 或增加引用数。
	setUserQuota(t, env.db, env.ownerA.ID, size)
	if _, err := env.service.Copy(ctx, env.ownerA.ID, copied.ID, "", "copy-again.txt"); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("over-quota copy error = %v, want ErrQuotaExceeded", err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, size, 0)
	if got := countKind(t, env.db, model.KindFile); got != 1 {
		t.Fatalf("rejected copy left %d files, want 1", got)
	}

	// 删除最后一个引用：释放最后一份用量。
	if err := env.service.Delete(ctx, env.ownerA.ID, copied.ID); err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, 0)
}

// TestLegacyUploadRespectsQuota 对应验收 9：
// 旧完整上传没有提前预留，但最终发布不能绕过配额；失败时补偿删除内容。
func TestLegacyUploadRespectsQuota(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	setUserQuota(t, env.db, env.ownerA.ID, 10)

	if _, err := env.service.Upload(ctx, env.ownerA.ID, "", "five.txt", strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 5, 0)

	// 已经放了 5 字节，再传 6 字节会超配额：拒绝，且不能留下记录或磁盘内容。
	if _, err := env.service.Upload(ctx, env.ownerA.ID, "", "six.txt", strings.NewReader("hello!")); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("over-quota upload error = %v, want ErrQuotaExceeded", err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 5, 0)
	if got := countKind(t, env.db, model.KindFile); got != 1 {
		t.Fatalf("rejected upload left %d files, want 1", got)
	}
	if entries := diskEntries(t, env.rootA); len(entries) != 1 {
		t.Fatalf("rejected upload left %d content object(s), want 1: %v", len(entries), entries)
	}

	// 再传 5 字节正好用满：允许。
	if _, err := env.service.Upload(ctx, env.ownerA.ID, "", "full.txt", strings.NewReader("world")); err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 10, 0)

	// 用满后再传 1 字节：拒绝。
	if _, err := env.service.Upload(ctx, env.ownerA.ID, "", "one.txt", strings.NewReader("x")); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("upload on full quota error = %v, want ErrQuotaExceeded", err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 10, 0)
}

// TestEmptyFilesAndZeroQuota 对应验收 10 的空文件部分：
// 0 表示没有可用容量；空文件不占容量，也不应被容量检查误伤。
func TestEmptyFilesAndZeroQuota(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	setUserQuota(t, env.db, env.ownerA.ID, 0)

	// 配额为 0 时，非空会话直接拒绝。
	if _, err := env.service.CreateUploadSession(
		ctx, env.ownerA.ID, "", "no-room.bin", 1,
	); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("non-empty session with zero quota error = %v, want ErrQuotaExceeded", err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, 0)

	// 空文件会话可以创建、可以完成，始终不占容量。
	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "empty.txt", 0)
	assertSessionReserved(t, env.db, created.ID, 0)
	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, created.ID, strings.NewReader(""),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID); err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, 0)

	// 旧上传接口传空文件同样允许。
	if _, err := env.service.Upload(
		ctx, env.ownerA.ID, "", "empty-legacy.txt", strings.NewReader(""),
	); err != nil {
		t.Fatal(err)
	}
	assertAccounting(t, env.db, env.ownerA.ID, 0, 0)
}
