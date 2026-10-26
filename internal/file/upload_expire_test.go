package file

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
)

// expiryTestTimeAfter 返回一个确定晚于会话 expires_at 的时刻。
// 测试用显式时间制造到期，不真的等待一小时。
func expiryTestTimeAfter(session model.UploadSession) time.Time {
	return session.ExpiresAt.Add(time.Second)
}

// TestExpirePendingUploadSession 覆盖 pending 到期：
// 会话变成 expired，未写入内容的占位 Entity 也被回收。
func TestExpirePendingUploadSession(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "pending-expire.txt", 5)
	session, entity := loadUploadSessionAndEntity(t, env, created.ID)
	if session.Status != model.UploadStatusPending {
		t.Fatalf("test setup status = %q, want pending", session.Status)
	}

	if err := env.service.ExpireUploadSessions(ctx, expiryTestTimeAfter(session), 10); err != nil {
		t.Fatal(err)
	}

	final := loadSessionOnly(t, env, created.ID)
	if final.Status != model.UploadStatusExpired {
		t.Fatalf("session status = %q, want expired", final.Status)
	}
	assertEntityGone(t, env, entity.ID)
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("expiring a pending session published %d file(s)", got)
	}

	// 过期后不能再确认完成。
	if _, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID); !errors.Is(err, ErrUploadSessionNotCompletable) {
		t.Fatalf("complete after expiry error = %v, want ErrUploadSessionNotCompletable", err)
	}

	// 重复扫描是幂等的：不会再次改变状态，也不会报错。
	if err := env.service.ExpireUploadSessions(ctx, expiryTestTimeAfter(session), 10); err != nil {
		t.Fatal(err)
	}
	if again := loadSessionOnly(t, env, created.ID); again.Status != model.UploadStatusExpired {
		t.Fatalf("repeat expiry changed status to %q", again.Status)
	}
}

// TestExpireReadyUploadSessionRemovesContent 覆盖 ready 到期：
// 已经完整写好的占位内容被清理，不会生成正式 File。
func TestExpireReadyUploadSessionRemovesContent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	created, session, entity := mustReadyUploadSession(t, env, env.ownerA.ID, "ready-expire.bin", "hello")
	contentPath := filepath.Join(env.rootA, entity.StorageKey)
	assertUploadContent(t, contentPath, "hello")

	if err := env.service.ExpireUploadSessions(ctx, expiryTestTimeAfter(session), 10); err != nil {
		t.Fatal(err)
	}

	final := loadSessionOnly(t, env, created.ID)
	if final.Status != model.UploadStatusExpired || final.ResultFileID != nil {
		t.Fatalf("expired ready session = %+v", final)
	}
	assertEntityGone(t, env, entity.ID)
	assertNoUploadContent(t, contentPath)
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("expiring a ready session published %d file(s)", got)
	}
	if _, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID); !errors.Is(err, ErrUploadSessionNotCompletable) {
		t.Fatalf("complete after expiry error = %v, want ErrUploadSessionNotCompletable", err)
	}
}

// TestExpireReceivingUploadStopsBeforeCleanup 覆盖 receiving 到期：
// 会话先终止；活跃写入退出前不能移交物理清理，退出后由退出路径收尾。
func TestExpireReceivingUploadStopsBeforeCleanup(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	const payload = "0123456789"

	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "receiving-expire.bin", int64(len(payload)))
	session, placeholder := loadUploadSessionAndEntity(t, env, created.ID)
	contentPath := filepath.Join(env.rootA, placeholder.StorageKey)

	reader := &blockingUploadReader{
		data:    []byte(payload),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(reader.release) }) }
	t.Cleanup(release)

	type uploadResult struct {
		info UploadSessionInfo
		err  error
	}
	uploadDone := make(chan uploadResult, 1)
	go func() {
		info, err := env.service.UploadSessionContent(ctx, env.ownerA.ID, created.ID, reader)
		uploadDone <- uploadResult{info: info, err: err}
	}()

	select {
	case <-reader.started:
	case <-time.After(3 * time.Second):
		t.Fatal("upload did not start reading")
	}

	if err := env.service.ExpireUploadSessions(ctx, expiryTestTimeAfter(session), 10); err != nil {
		t.Fatal(err)
	}
	if got := loadSessionOnly(t, env, created.ID); got.Status != model.UploadStatusExpired {
		t.Fatalf("session status = %q, want expired", got.Status)
	}
	// 写入还阻塞在 Read 中：Entity 必须仍是 uploading，不能被提前标记清理。
	if entity := mustEntity(t, env, placeholder.ID); entity.Status != model.EntityStatusUploading {
		t.Fatalf("entity was handed off before the writer exited: %+v", entity)
	}

	// 已过期会话再被取消也必须保持 expired，不能改写成 canceled。
	info, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != model.UploadStatusExpired {
		t.Fatalf("cancel expired session returned %q, want expired", info.Status)
	}
	if got := loadSessionOnly(t, env, created.ID); got.Status != model.UploadStatusExpired {
		t.Fatalf("cancel rewrote expired session to %q", got.Status)
	}
	if entity := mustEntity(t, env, placeholder.ID); entity.Status != model.EntityStatusUploading {
		t.Fatalf("cancel cleaned entity before the writer exited: %+v", entity)
	}

	// 放行阻塞读，写入看到取消信号后退出；退出路径负责移交清理。
	release()
	select {
	case result := <-uploadDone:
		if result.err == nil {
			t.Fatalf("expired upload should fail, got %+v", result.info)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expired upload did not exit")
	}

	assertEntityGone(t, env, placeholder.ID)
	assertNoUploadContent(t, contentPath)
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("expired receiving upload published %d file(s)", got)
	}
	// 退出路径不能把 expired 恢复成 pending / ready。
	final := loadSessionOnly(t, env, created.ID)
	if final.Status != model.UploadStatusExpired {
		t.Fatalf("final session status = %q, want expired", final.Status)
	}
}

// expiryPauseAfterSaveStore 在内容完整写入后暂停，用来制造
// “内容已经写完、收尾事务还没提交”的过期窗口。
type expiryPauseAfterSaveStore struct {
	storage.Store
	saved   chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *expiryPauseAfterSaveStore) Save(
	ctx context.Context,
	key string,
	r io.Reader,
	maxBytes int64,
) (int64, error) {
	size, err := s.Store.Save(ctx, key, r, maxBytes)
	if err == nil {
		s.once.Do(func() { close(s.saved) })
		<-s.release
	}
	return size, err
}

// TestExpireDuringUploadExitKeepsExpired 覆盖写入已经完成、但退出路径
// 还没收尾时发生的过期：最终必须保持 expired，不能恢复成 pending / ready。
func TestExpireDuringUploadExitKeepsExpired(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	store := &expiryPauseAfterSaveStore{
		Store:   storage.NewLocal(env.rootA),
		saved:   make(chan struct{}),
		release: make(chan struct{}),
	}
	registry := storage.NewRegistry()
	if err := registry.Register(env.policyA.ID, store); err != nil {
		t.Fatal(err)
	}
	svc := NewService(env.db, registry)

	created := mustCreateUploadContentSession(t, svc, env.ownerA.ID, "late-exit.bin", 5)
	session, entity := loadUploadSessionAndEntity(t, env, created.ID)
	contentPath := filepath.Join(env.rootA, entity.StorageKey)

	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(store.release) }) }
	t.Cleanup(release)

	type uploadResult struct {
		info UploadSessionInfo
		err  error
	}
	uploadDone := make(chan uploadResult, 1)
	go func() {
		info, err := svc.UploadSessionContent(ctx, env.ownerA.ID, created.ID, strings.NewReader("hello"))
		uploadDone <- uploadResult{info: info, err: err}
	}()

	select {
	case <-store.saved:
	case <-time.After(3 * time.Second):
		t.Fatal("upload did not finish writing")
	}

	// 内容已经写完，但请求还没退出；此时让会话过期。
	if err := svc.ExpireUploadSessions(ctx, expiryTestTimeAfter(session), 10); err != nil {
		t.Fatal(err)
	}
	if got := loadSessionOnly(t, env, created.ID); got.Status != model.UploadStatusExpired {
		t.Fatalf("session status = %q, want expired", got.Status)
	}
	if got := mustEntity(t, env, entity.ID); got.Status != model.EntityStatusUploading {
		t.Fatalf("entity was cleaned while writer was still active: %+v", got)
	}

	release()
	select {
	case result := <-uploadDone:
		if result.err == nil {
			t.Fatalf("expired upload should fail, got %+v", result.info)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expired upload did not exit")
	}

	final := loadSessionOnly(t, env, created.ID)
	if final.Status != model.UploadStatusExpired {
		t.Fatalf("upload exit rewrote expired session to %q", final.Status)
	}
	assertEntityGone(t, env, entity.ID)
	assertNoUploadContent(t, contentPath)
	assertNoUploadContent(t, filepath.Join(env.rootA, ".upload-"+entity.StorageKey))
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("expired upload published %d file(s)", got)
	}
}

// TestExpireCompletedUploadSessionLeavesFile 覆盖已完成会话超过到期时间：
// 到期扫描必须跳过 completed，正式文件仍可下载。
func TestExpireCompletedUploadSessionLeavesFile(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	created, _, entity := mustReadyUploadSession(t, env, env.ownerA.ID, "published.bin", "hello")
	completed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	fileID := *completed.ResultFileID

	// 把 expires_at 改到过去，再扫描：completed 不在候选范围内。
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", created.ID).
		UpdateColumn("expires_at", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatal(err)
	}
	if err := env.service.ExpireUploadSessions(ctx, time.Now(), 10); err != nil {
		t.Fatal(err)
	}

	final := loadSessionOnly(t, env, created.ID)
	if final.Status != model.UploadStatusCompleted ||
		final.ResultFileID == nil || *final.ResultFileID != fileID {
		t.Fatalf("completed session was changed by expiry: %+v", final)
	}
	currentEntity := mustEntity(t, env, entity.ID)
	if currentEntity.Status != model.EntityStatusActive || currentEntity.RefCount != 1 {
		t.Fatalf("completed entity was changed by expiry: %+v", currentEntity)
	}
	mustOpenContent(t, env.service, env.ownerA.ID, fileID, "hello")

	// 整轮清理也不能误删已经发布的内容。
	if _, err := env.service.CleanupPending(ctx, 10); err != nil {
		t.Fatal(err)
	}
	final = loadSessionOnly(t, env, created.ID)
	if final.Status != model.UploadStatusCompleted {
		t.Fatalf("cleanup changed completed session to %q", final.Status)
	}
	mustOpenContent(t, env.service, env.ownerA.ID, fileID, "hello")
}

// TestExpireCleanupFailureKeepsPendingDelete 覆盖物理删除失败：
// expired 已经落库，Entity 保留 pending_delete，下一轮清理成功回收。
func TestExpireCleanupFailureKeepsPendingDelete(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	store := &flakyRemoveStore{Store: storage.NewLocal(env.rootA), remaining: 1}
	registry := storage.NewRegistry()
	if err := registry.Register(env.policyA.ID, store); err != nil {
		t.Fatal(err)
	}
	svc := NewService(env.db, registry)

	created, session, entity := mustReadyUploadSessionWithService(t, svc, env, "flaky-expire.bin", "hello")
	contentPath := filepath.Join(env.rootA, entity.StorageKey)

	// 清理失败不应让过期本身失败，也不应回滚已经提交的 expired。
	if err := svc.ExpireUploadSessions(ctx, expiryTestTimeAfter(session), 10); err != nil {
		t.Fatal(err)
	}
	if got := loadSessionOnly(t, env, created.ID); got.Status != model.UploadStatusExpired {
		t.Fatalf("session status = %q, want expired", got.Status)
	}
	pending := mustEntity(t, env, entity.ID)
	if pending.Status != model.EntityStatusPendingDelete || pending.RefCount != 0 {
		t.Fatalf("entity after failed cleanup = %+v", pending)
	}
	assertUploadContent(t, contentPath, "hello")

	result, err := svc.CleanupPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Removed == 0 {
		t.Fatalf("cleanup did not remove anything: %+v", result)
	}
	assertEntityGone(t, env, entity.ID)
	assertNoUploadContent(t, contentPath)
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("failed expiry cleanup published %d file(s)", got)
	}
}

// TestCleanupResumesExpiredSessionAfterRestart 模拟“会话已 expired、
// Entity 还是 uploading 时进程崩溃”：新 Service 必须靠数据库记录继续清理。
func TestCleanupResumesExpiredSessionAfterRestart(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	created, _, entity := mustReadyUploadSession(t, env, env.ownerA.ID, "resume-expired.bin", "hello")
	contentPath := filepath.Join(env.rootA, entity.StorageKey)

	// 只写 expired，不做清理，模拟到期扫描提交后立刻崩溃。
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", created.ID).
		UpdateColumn("status", model.UploadStatusExpired).Error; err != nil {
		t.Fatal(err)
	}
	if got := mustEntity(t, env, entity.ID); got.Status != model.EntityStatusUploading {
		t.Fatalf("test setup entity = %+v", got)
	}
	assertUploadContent(t, contentPath, "hello")

	// 全新 Service 的活跃写入登记表是空的，相当于进程重启。
	restarted := NewService(env.db, testRegistry(t, env.policyA, env.rootA))
	if _, err := restarted.CleanupPending(ctx, 10); err != nil {
		t.Fatal(err)
	}

	final := loadSessionOnly(t, env, created.ID)
	if final.Status != model.UploadStatusExpired {
		t.Fatalf("resume changed session status to %q", final.Status)
	}
	assertEntityGone(t, env, entity.ID)
	assertNoUploadContent(t, contentPath)
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("resumed expiry published %d file(s)", got)
	}
}

// TestCleanupPendingExpiresAndCleansInOnePass 验证现有清理循环的接入顺序：
// 同一轮 CleanupPending 里先标记 expired，再完成占位内容回收。
func TestCleanupPendingExpiresAndCleansInOnePass(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	created, _, entity := mustReadyUploadSession(t, env, env.ownerA.ID, "cleanup-expire.bin", "hello")
	contentPath := filepath.Join(env.rootA, entity.StorageKey)

	// 不直接调用 ExpireUploadSessions：把到期会话交给整轮清理入口。
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", created.ID).
		UpdateColumn("expires_at", time.Now().Add(-time.Minute)).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := env.service.CleanupPending(ctx, 10); err != nil {
		t.Fatal(err)
	}

	final := loadSessionOnly(t, env, created.ID)
	if final.Status != model.UploadStatusExpired {
		t.Fatalf("session status = %q, want expired", final.Status)
	}
	assertEntityGone(t, env, entity.ID)
	assertNoUploadContent(t, contentPath)
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("cleanup expiry published %d file(s)", got)
	}
}

// TestExpireBatchesRetryAfterSingleFailure 覆盖多批次、重复扫描和单条失败：
// 失败的会话不阻塞后面的会话，下一轮能重试成功，整体保持幂等。
func TestExpireBatchesRetryAfterSingleFailure(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const count = 5
	created := make([]UploadSessionInfo, 0, count)
	sessions := make(map[string]model.UploadSession, count)
	entities := make(map[string]model.Entity, count)
	var latestExpiry time.Time
	for i := 0; i < count; i++ {
		name := "batch-" + string(rune('a'+i)) + ".bin"
		info, session, entity := mustReadyUploadSession(t, env, env.ownerA.ID, name, "hello")
		created = append(created, info)
		sessions[info.ID] = session
		entities[info.ID] = entity
		if session.ExpiresAt.After(latestExpiry) {
			latestExpiry = session.ExpiresAt
		}
	}

	ids := make([]string, 0, count)
	for _, info := range created {
		ids = append(ids, info.ID)
	}
	sort.Strings(ids)

	// 只让第一条到期 UPDATE 失败一次，验证游标仍然向前推进。
	const callbackName = "test:fail_first_expiry_update"
	var failed bool
	if err := env.db.Callback().Update().Before("gorm:update").Register(
		callbackName,
		func(tx *gorm.DB) {
			if failed || tx.Statement.Schema == nil ||
				tx.Statement.Schema.Table != (model.UploadSession{}).TableName() {
				return
			}
			failed = true
			_ = tx.AddError(errors.New("forced expiry failure"))
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.db.Callback().Update().Remove(callbackName); err != nil {
			t.Logf("remove test callback: %v", err)
		}
	})

	now := latestExpiry.Add(time.Second)

	// batchSize=1 让每条会话单独占一批，能直接检验游标推进。
	if err := env.service.ExpireUploadSessions(ctx, now, 1); err != nil {
		t.Fatal(err)
	}
	if !failed {
		t.Fatal("test callback did not force a failure")
	}

	var notExpired []string
	var expiredCount int
	for _, id := range ids {
		status := loadSessionOnly(t, env, id).Status
		switch status {
		case model.UploadStatusReady:
			// 失败的是 UPDATE，事务回滚，会话保持原来的 ready。
			notExpired = append(notExpired, id)
		case model.UploadStatusExpired:
			expiredCount++
		default:
			t.Fatalf("session %s status = %q", id, status)
		}
	}
	if len(notExpired) != 1 || notExpired[0] != ids[0] {
		t.Fatalf("failed session should be the first candidate, not expired=%v want [%s]", notExpired, ids[0])
	}
	if expiredCount != count-1 {
		t.Fatalf("expired count = %d, want %d", expiredCount, count-1)
	}
	if got := countRows(t, env.db, &model.Entity{}); got != 1 {
		t.Fatalf("entities left = %d, want only the failed session's placeholder", got)
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("expiry created %d formal file(s)", got)
	}

	// 下一轮从 id 开头重新扫描，失败的那条会被重试并成功回收。
	if err := env.service.ExpireUploadSessions(ctx, now, 2); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if status := loadSessionOnly(t, env, id).Status; status != model.UploadStatusExpired {
			t.Fatalf("session %s status after retry = %q, want expired", id, status)
		}
	}
	if got := countRows(t, env.db, &model.Entity{}); got != 0 {
		t.Fatalf("entities left after retry = %d", got)
	}
	for _, entity := range entities {
		assertNoUploadContent(t, filepath.Join(env.rootA, entity.StorageKey))
	}

	// 再次扫描必须完全幂等。
	if err := env.service.ExpireUploadSessions(ctx, now, 2); err != nil {
		t.Fatal(err)
	}
	if got := countRows(t, env.db, &model.Entity{}); got != 0 {
		t.Fatalf("repeated scan removed/created entities: %d", got)
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("repeated scan published %d file(s)", got)
	}
}

// TestCancelExpiredUploadSessionKeepsExpired 覆盖取消语义：
// expired 是终态，取消只能继续推进清理，不能改写成 canceled。
func TestCancelExpiredUploadSessionKeepsExpired(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	created, _, entity := mustReadyUploadSession(t, env, env.ownerA.ID, "cancel-expired.bin", "hello")
	contentPath := filepath.Join(env.rootA, entity.StorageKey)

	// 模拟到期扫描已经提交，但还没来得及移交清理。
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", created.ID).
		UpdateColumn("status", model.UploadStatusExpired).Error; err != nil {
		t.Fatal(err)
	}

	info, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != model.UploadStatusExpired {
		t.Fatalf("cancel expired session returned %q, want expired", info.Status)
	}
	if got := loadSessionOnly(t, env, created.ID); got.Status != model.UploadStatusExpired {
		t.Fatalf("cancel rewrote expired session to %q", got.Status)
	}

	// 取消仍然要推进清理：expired 会话的占位内容被回收。
	assertEntityGone(t, env, entity.ID)
	assertNoUploadContent(t, contentPath)
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("cancel expired session published %d file(s)", got)
	}

	// 再取消一次仍然幂等，状态保持 expired。
	again, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != model.UploadStatusExpired {
		t.Fatalf("repeat cancel returned %q, want expired", again.Status)
	}
}
