package file

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
)

func loadSessionOnly(t *testing.T, env *testEnv, sessionID string) model.UploadSession {
	t.Helper()

	var session model.UploadSession
	if err := env.db.First(&session, "id = ?", sessionID).Error; err != nil {
		t.Fatal(err)
	}
	return session
}

func mustEntity(t *testing.T, env *testEnv, entityID int64) model.Entity {
	t.Helper()

	var entity model.Entity
	if err := env.db.First(&entity, entityID).Error; err != nil {
		t.Fatal(err)
	}
	return entity
}

func assertEntityGone(t *testing.T, env *testEnv, entityID int64) {
	t.Helper()

	var entity model.Entity
	err := env.db.First(&entity, entityID).Error
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("entity %d should be cleaned up, got err=%v entity=%+v", entityID, err, entity)
	}
}

func TestUploadControllerBeginCancelEnd(t *testing.T) {
	controller := newUploadController()
	ctx := context.Background()

	uploadCtx, end, err := controller.begin(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if uploadCtx.Err() != nil {
		t.Fatalf("fresh upload context is already canceled: %v", uploadCtx.Err())
	}
	if !controller.isActive("s1") {
		t.Fatal("upload was not registered")
	}
	if _, _, err := controller.begin(ctx, "s1"); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("duplicate begin error = %v, want ErrUploadSessionConflict", err)
	}

	done := controller.cancel("s1")
	if uploadCtx.Err() == nil {
		t.Fatal("cancel did not cancel the upload context")
	}
	select {
	case <-done:
		t.Fatal("done closed before the upload exited")
	default:
	}

	end()
	end() // end 必须幂等
	if controller.isActive("s1") {
		t.Fatal("upload was not unregistered")
	}
	select {
	case <-done:
	default:
		t.Fatal("done was not closed after End")
	}

	// 没有活跃写入时，cancel 返回已经关闭的 channel。
	select {
	case <-controller.cancel("s1"):
	default:
		t.Fatal("cancel without an active upload should return a closed channel")
	}
}

// TestCancelPendingUploadSession 覆盖 pending 取消：
// 不生成正式文件，占位 Entity 被立即清理，重复取消仍然成功。
func TestCancelPendingUploadSession(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "pending.txt", 5)

	info, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != model.UploadStatusCanceled {
		t.Fatalf("canceled session info = %+v", info)
	}

	session := loadSessionOnly(t, env, created.ID)
	if session.Status != model.UploadStatusCanceled {
		t.Fatalf("stored session = %+v", session)
	}
	assertEntityGone(t, env, session.EntityID)
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("cancel created %d formal file(s)", got)
	}

	// 取消后不能再完成。
	if _, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID); !errors.Is(err, ErrUploadSessionNotCompletable) {
		t.Fatalf("complete after cancel error = %v, want ErrUploadSessionNotCompletable", err)
	}

	// 重复取消：不报错，也不留下新记录。
	again, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != model.UploadStatusCanceled {
		t.Fatalf("repeat cancel status = %q", again.Status)
	}
}

// TestCancelReadyUploadSessionRemovesContent 覆盖 ready 取消：
// 完整内容被清理，之后不能再确认完成。
func TestCancelReadyUploadSessionRemovesContent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created, _, entity := mustReadyUploadSession(t, env, env.ownerA.ID, "ready.txt", "hello")
	contentPath := filepath.Join(env.rootA, entity.StorageKey)
	assertUploadContent(t, contentPath, "hello")

	info, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != model.UploadStatusCanceled {
		t.Fatalf("canceled session info = %+v", info)
	}

	session := loadSessionOnly(t, env, created.ID)
	if session.Status != model.UploadStatusCanceled || session.ResultFileID != nil {
		t.Fatalf("stored session = %+v", session)
	}
	assertEntityGone(t, env, entity.ID)
	assertNoUploadContent(t, contentPath)
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("ready cancel created %d formal file(s)", got)
	}
	if _, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID); !errors.Is(err, ErrUploadSessionNotCompletable) {
		t.Fatalf("complete after ready cancel error = %v, want ErrUploadSessionNotCompletable", err)
	}

	// 重复取消时 Entity 已经不存在，也必须成功。
	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, created.ID); err != nil {
		t.Fatalf("repeat cancel after cleanup: %v", err)
	}
}

// TestCancelReceivingUploadStopsBeforeCleanup 覆盖 receiving 取消：
// 取消先落库，但内容要等写入请求退出后才清理，不能删完又被写回来。
func TestCancelReceivingUploadStopsBeforeCleanup(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	const payload = "0123456789"
	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "receiving.bin", int64(len(payload)))
	_, placeholder := loadUploadSessionAndEntity(t, env, created.ID)
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

	// 写入还阻塞在 Read 中，此时取消：会话立刻 canceled，但内容不能已经删掉。
	canceled, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status != model.UploadStatusCanceled {
		t.Fatalf("canceled info = %+v", canceled)
	}
	session := loadSessionOnly(t, env, created.ID)
	if session.Status != model.UploadStatusCanceled {
		t.Fatalf("session was not canceled: %+v", session)
	}
	// 活跃写入还没退出，Entity 必须仍是 uploading，不能被提前标记清理。
	if entity := mustEntity(t, env, placeholder.ID); entity.Status != model.EntityStatusUploading {
		t.Fatalf("entity was cleaned before the upload exited: %+v", entity)
	}

	// 放行阻塞读，写入看到取消信号后退出；退出路径负责移交清理。
	release()
	select {
	case result := <-uploadDone:
		if result.err == nil {
			t.Fatalf("canceled upload should fail, got %+v", result.info)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled upload did not exit")
	}

	assertEntityGone(t, env, placeholder.ID)
	assertNoUploadContent(t, contentPath)
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("canceled receiving upload created %d formal file(s)", got)
	}
	// 退出路径不能在取消之后把会话恢复成 ready。
	final := loadSessionOnly(t, env, created.ID)
	if final.Status != model.UploadStatusCanceled {
		t.Fatalf("final session status = %q, want canceled", final.Status)
	}
}

// TestHandoffWaitsForActiveUpload 直接验证“有活跃写入时不移交清理”。
func TestHandoffWaitsForActiveUpload(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "guard.txt", 5)
	_, placeholder := loadUploadSessionAndEntity(t, env, created.ID)

	// 模拟取消事务已经提交，但写入仍登记为活跃。
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", created.ID).
		UpdateColumn("status", model.UploadStatusCanceled).Error; err != nil {
		t.Fatal(err)
	}
	_, end, err := env.service.uploads.begin(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}

	if err := env.service.handoffTerminatedSession(ctx, env.ownerA.ID, created.ID); err != nil {
		t.Fatal(err)
	}
	if entity := mustEntity(t, env, placeholder.ID); entity.Status != model.EntityStatusUploading {
		t.Fatalf("handoff cleaned entity while upload was active: %+v", entity)
	}

	end()
	if err := env.service.handoffTerminatedSession(ctx, env.ownerA.ID, created.ID); err != nil {
		t.Fatal(err)
	}
	assertEntityGone(t, env, placeholder.ID)
}

// TestCancelCompletedUploadSession 覆盖 completed 取消：
// 返回冲突，正式文件和内容保持原样。
func TestCancelCompletedUploadSession(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created, _, _ := mustReadyUploadSession(t, env, env.ownerA.ID, "done.txt", "hello")
	completed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	fileID := *completed.ResultFileID

	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, created.ID); !errors.Is(err, ErrUploadSessionCompleted) {
		t.Fatalf("cancel completed error = %v, want ErrUploadSessionCompleted", err)
	}

	session := loadSessionOnly(t, env, created.ID)
	if session.Status != model.UploadStatusCompleted ||
		session.ResultFileID == nil || *session.ResultFileID != fileID {
		t.Fatalf("completed session was changed: %+v", session)
	}
	entity := mustEntity(t, env, session.EntityID)
	if entity.Status != model.EntityStatusActive || entity.RefCount != 1 {
		t.Fatalf("completed entity was changed: %+v", entity)
	}
	mustOpenContent(t, env.service, env.ownerA.ID, fileID, "hello")
	if got := countKind(t, env.db, model.KindFile); got != 1 {
		t.Fatalf("cancel completed changed file count to %d", got)
	}
}

// TestCancelAndCompleteConcurrent 验证取消和完成只能一方成功，最终状态一致。
func TestCancelAndCompleteConcurrent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const rounds = 4
	completedCount := 0
	for i := 0; i < rounds; i++ {
		name := fmt.Sprintf("race-%d.txt", i)
		created, _, _ := mustReadyUploadSession(t, env, env.ownerA.ID, name, "hello")

		var completeInfo UploadSessionInfo
		var completeErr error
		var cancelInfo UploadSessionInfo
		var cancelErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			completeInfo, completeErr = env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID)
		}()
		go func() {
			defer wg.Done()
			cancelInfo, cancelErr = env.service.CancelUploadSession(ctx, env.ownerA.ID, created.ID)
		}()
		wg.Wait()

		session := loadSessionOnly(t, env, created.ID)
		switch {
		case completeErr == nil && cancelErr == nil:
			t.Fatalf("round %d: both complete and cancel succeeded", i)
		case completeErr == nil:
			completedCount++
			if cancelErr == nil || !errors.Is(cancelErr, ErrUploadSessionCompleted) {
				t.Fatalf("round %d: complete won but cancel error = %v", i, cancelErr)
			}
			if session.Status != model.UploadStatusCompleted {
				t.Fatalf("round %d: complete won but session = %+v", i, session)
			}
			if completeInfo.ResultFileID == nil {
				t.Fatalf("round %d: complete returned no file: %+v", i, completeInfo)
			}
			mustOpenContent(t, env.service, env.ownerA.ID, *completeInfo.ResultFileID, "hello")
		case cancelErr == nil:
			if !errors.Is(completeErr, ErrUploadSessionNotCompletable) {
				t.Fatalf("round %d: cancel won but complete error = %v", i, completeErr)
			}
			if cancelInfo.Status != model.UploadStatusCanceled ||
				session.Status != model.UploadStatusCanceled {
				t.Fatalf("round %d: cancel won but session = %+v / %+v", i, cancelInfo, session)
			}
			var files int64
			if err := env.db.Model(&model.File{}).
				Where("owner_id = ? AND parent_id = ? AND name = ?",
					env.ownerA.ID, *env.ownerA.RootFileID, name).
				Count(&files).Error; err != nil {
				t.Fatal(err)
			}
			if files != 0 {
				t.Fatalf("round %d: cancel won but %d file(s) were published", i, files)
			}
		default:
			t.Fatalf("round %d: neither complete nor cancel succeeded: complete=%v cancel=%v",
				i, completeErr, cancelErr)
		}
	}

	if got := countKind(t, env.db, model.KindFile); got != int64(completedCount) {
		t.Fatalf("formal file count = %d, want %d", got, completedCount)
	}
}

// flakyRemoveStore 第一次物理删除失败，之后恢复正常，用来验证清理重试。
type flakyRemoveStore struct {
	storage.Store
	mu        sync.Mutex
	remaining int
}

func (s *flakyRemoveStore) Remove(ctx context.Context, key string) error {
	s.mu.Lock()
	if s.remaining > 0 {
		s.remaining--
		s.mu.Unlock()
		return errors.New("remove blocked")
	}
	s.mu.Unlock()
	return s.Store.Remove(ctx, key)
}

// TestCancelCleanupFailureKeepsPendingDelete 覆盖物理删除失败：
// 取消仍然成功，Entity 保留 pending_delete，后续清理能重试成功。
func TestCancelCleanupFailureKeepsPendingDelete(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	store := &flakyRemoveStore{Store: storage.NewLocal(env.rootA), remaining: 1}
	registry := storage.NewRegistry()
	if err := registry.Register(env.policyA.ID, store); err != nil {
		t.Fatal(err)
	}
	svc := NewService(env.db, registry)
	created, _, _ := mustReadyUploadSessionWithService(t, svc, env, "flaky.bin", "hello")
	_, entity := loadUploadSessionAndEntity(t, env, created.ID)
	contentPath := filepath.Join(env.rootA, entity.StorageKey)

	info, err := svc.CancelUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatalf("cancel must succeed even when physical cleanup fails: %v", err)
	}
	if info.Status != model.UploadStatusCanceled {
		t.Fatalf("canceled info = %+v", info)
	}

	// 删除失败：保留 pending_delete 和物理内容，等下一轮重试。
	if got := mustEntity(t, env, entity.ID); got.Status != model.EntityStatusPendingDelete || got.RefCount != 0 {
		t.Fatalf("entity after failed cleanup = %+v", got)
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
}

// mustReadyUploadSessionWithService 用指定 Service 创建并上传，供自定义存储的测试使用。
func mustReadyUploadSessionWithService(t *testing.T, svc *Service, env *testEnv, name, content string) (UploadSessionInfo, model.UploadSession, model.Entity) {
	t.Helper()

	created := mustCreateUploadContentSession(t, svc, env.ownerA.ID, name, int64(len(content)))
	if _, err := svc.UploadSessionContent(
		context.Background(), env.ownerA.ID, created.ID, strings.NewReader(content),
	); err != nil {
		t.Fatal(err)
	}
	session, entity := loadUploadSessionAndEntity(t, env, created.ID)
	return created, session, entity
}

// TestCleanupPendingResumesCanceledSession 模拟“取消已落库但服务立刻崩溃”：
// 清理循环必须能把仍是 uploading/0 的占位内容继续移交。
func TestCleanupPendingResumesCanceledSession(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created, _, entity := mustReadyUploadSession(t, env, env.ownerA.ID, "resume.bin", "hello")
	contentPath := filepath.Join(env.rootA, entity.StorageKey)

	// 只写 canceled，不做清理，模拟取消事务提交后进程退出。
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", created.ID).
		UpdateColumn("status", model.UploadStatusCanceled).Error; err != nil {
		t.Fatal(err)
	}
	if got := mustEntity(t, env, entity.ID); got.Status != model.EntityStatusUploading {
		t.Fatalf("test setup entity = %+v", got)
	}

	if _, err := env.service.CleanupPending(ctx, 10); err != nil {
		t.Fatal(err)
	}

	assertEntityGone(t, env, entity.ID)
	assertNoUploadContent(t, contentPath)
	if got := loadSessionOnly(t, env, created.ID); got.Status != model.UploadStatusCanceled {
		t.Fatalf("resume changed session status to %q", got.Status)
	}
}

// TestCancelWithCanceledRequestContext 验证请求 context 已经取消时，
// 取消仍能落库并完成清理。
func TestCancelWithCanceledRequestContext(t *testing.T) {
	env := newTestEnv(t)
	created, _, entity := mustReadyUploadSession(t, env, env.ownerA.ID, "ctx-canceled.bin", "hello")
	contentPath := filepath.Join(env.rootA, entity.StorageKey)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	info, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != model.UploadStatusCanceled {
		t.Fatalf("canceled info = %+v", info)
	}
	if got := loadSessionOnly(t, env, created.ID); got.Status != model.UploadStatusCanceled {
		t.Fatalf("session status = %q, want canceled", got.Status)
	}
	assertEntityGone(t, env, entity.ID)
	assertNoUploadContent(t, contentPath)
}

// TestCancelForeignUploadSession 验证越权取消：统一 404，不影响别人的上传。
func TestCancelForeignUploadSession(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created, _, entity := mustReadyUploadSession(t, env, env.ownerA.ID, "mine.txt", "hello")
	contentPath := filepath.Join(env.rootA, entity.StorageKey)

	if _, err := env.service.CancelUploadSession(ctx, env.ownerB.ID, created.ID); !errors.Is(err, ErrUploadSessionNotFound) {
		t.Fatalf("foreign cancel error = %v, want ErrUploadSessionNotFound", err)
	}

	if got := loadSessionOnly(t, env, created.ID); got.Status != model.UploadStatusReady {
		t.Fatalf("foreign cancel changed session: %+v", got)
	}
	if got := mustEntity(t, env, entity.ID); got.Status != model.EntityStatusUploading || got.RefCount != 0 {
		t.Fatalf("foreign cancel changed entity: %+v", got)
	}
	assertUploadContent(t, contentPath, "hello")
}
