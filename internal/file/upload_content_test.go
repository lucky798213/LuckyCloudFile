package file

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
)

func mustCreateUploadContentSession(t *testing.T, svc *Service, ownerID int, name string, size int64) UploadSessionInfo {
	t.Helper()

	info, err := svc.CreateUploadSession(context.Background(), ownerID, "", name, size)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func loadUploadSessionAndEntity(t *testing.T, env *testEnv, sessionID string) (model.UploadSession, model.Entity) {
	t.Helper()

	var session model.UploadSession
	if err := env.db.First(&session, "id = ?", sessionID).Error; err != nil {
		t.Fatal(err)
	}
	var entity model.Entity
	if err := env.db.First(&entity, session.EntityID).Error; err != nil {
		t.Fatal(err)
	}
	return session, entity
}

func assertUploadContent(t *testing.T, path, want string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != want {
		t.Fatalf("stored content = %q, want %q", data, want)
	}
}

func assertNoUploadContent(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("content should not exist at %s, stat err = %v", path, err)
	}
}

// TestUploadSessionContentHappyPath 覆盖正常完整上传：
// 会话 ready、Entity 大小更新但仍是 uploading/0、没有正式 File。
func TestUploadSessionContentHappyPath(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "hello.txt", 5)

	info, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, created.ID, strings.NewReader("hello"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != model.UploadStatusReady {
		t.Fatalf("status = %q, want %q", info.Status, model.UploadStatusReady)
	}
	if info.ActualSize != 5 || info.ExpectedSize != 5 {
		t.Fatalf("unexpected sizes: %+v", info)
	}
	if info.ResultFileID != nil {
		t.Fatalf("content upload must not create a result file: %q", *info.ResultFileID)
	}

	session, entity := loadUploadSessionAndEntity(t, env, created.ID)
	if session.Status != model.UploadStatusReady || session.ActualSize != 5 {
		t.Fatalf("stored session = %+v", session)
	}
	if entity.Status != model.EntityStatusUploading ||
		entity.RefCount != 0 ||
		entity.Size != 5 {
		t.Fatalf("placeholder entity = %+v", entity)
	}
	if entity.StoragePolicyID != env.policyA.ID {
		t.Fatalf("entity policy = %d, want %d", entity.StoragePolicyID, env.policyA.ID)
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("content upload created %d formal file(s)", got)
	}

	// 内容确实写到了会话原先选定的位置。
	assertUploadContent(t, filepath.Join(env.rootA, entity.StorageKey), "hello")
	if entries := diskEntries(t, env.rootB); len(entries) != 0 {
		t.Fatalf("content leaked into policy B: %v", entries)
	}

	// 查询接口能看到 ready，但普通下载接口还找不到这个会话 ID。
	fetched, err := env.service.GetUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.Status != model.UploadStatusReady || fetched.ActualSize != 5 {
		t.Fatalf("fetched session = %+v", fetched)
	}
	if _, _, err := env.service.Open(ctx, env.ownerA.ID, created.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("session content should not be downloadable yet, got %v", err)
	}

	// ready 之后不能再写内容，原内容不能被覆盖。
	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, created.ID, strings.NewReader("again"),
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("second content write: error = %v, want ErrUploadSessionConflict", err)
	}
	assertUploadContent(t, filepath.Join(env.rootA, entity.StorageKey), "hello")
}

// TestUploadSessionContentEmptyFile 验证空文件可以成功，
// 也验证 ExpectedSize=0 时多传一个字节会被拒绝。
func TestUploadSessionContentEmptyFile(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "empty.txt", 0)
	info, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, created.ID, strings.NewReader(""),
	)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != model.UploadStatusReady || info.ActualSize != 0 {
		t.Fatalf("unexpected empty upload info: %+v", info)
	}
	_, entity := loadUploadSessionAndEntity(t, env, created.ID)
	if entity.Size != 0 || entity.Status != model.EntityStatusUploading || entity.RefCount != 0 {
		t.Fatalf("unexpected empty placeholder entity: %+v", entity)
	}
	assertUploadContent(t, filepath.Join(env.rootA, entity.StorageKey), "")

	// 声明 0 字节却发来 1 字节：必须失败，不能留下 ready。
	over := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "zero-but-one.bin", 0)
	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, over.ID, strings.NewReader("x"),
	); !errors.Is(err, ErrUploadSizeMismatch) {
		t.Fatalf("error = %v, want ErrUploadSizeMismatch", err)
	}
	overSession, overEntity := loadUploadSessionAndEntity(t, env, over.ID)
	if overSession.Status != model.UploadStatusPending || overSession.ActualSize != 0 {
		t.Fatalf("failed empty upload changed session: %+v", overSession)
	}
	assertNoUploadContent(t, filepath.Join(env.rootA, overEntity.StorageKey))
}

// TestUploadSessionContentRejectsSizeMismatch 覆盖少传、多传和空内容：
// 三种情况都必须回到 pending，并清掉已经写下的残留。
func TestUploadSessionContentRejectsSizeMismatch(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	tests := []struct {
		name string
		body string
	}{
		{"short", "abc"},
		{"over", "abcdef"},
		{"empty", ""},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			created := mustCreateUploadContentSession(
				t, env.service, env.ownerA.ID, test.name+".bin", 5,
			)
			_, err := env.service.UploadSessionContent(
				ctx, env.ownerA.ID, created.ID, strings.NewReader(test.body),
			)
			if !errors.Is(err, ErrUploadSizeMismatch) {
				t.Fatalf("error = %v, want ErrUploadSizeMismatch", err)
			}

			session, entity := loadUploadSessionAndEntity(t, env, created.ID)
			if session.Status != model.UploadStatusPending || session.ActualSize != 0 {
				t.Fatalf("failed upload changed session: %+v", session)
			}
			if entity.Size != 0 || entity.Status != model.EntityStatusUploading ||
				entity.RefCount != 0 {
				t.Fatalf("failed upload changed entity: %+v", entity)
			}
			assertNoUploadContent(t, filepath.Join(env.rootA, entity.StorageKey))
			if got := countKind(t, env.db, model.KindFile); got != 0 {
				t.Fatalf("failed upload created %d formal file(s)", got)
			}

			// 清理成功后会话回到 pending，可以用正确内容整文件重传。
			retry, err := env.service.UploadSessionContent(
				ctx, env.ownerA.ID, created.ID, strings.NewReader("12345"),
			)
			if err != nil {
				t.Fatal(err)
			}
			if retry.Status != model.UploadStatusReady || retry.ActualSize != 5 {
				t.Fatalf("retry upload info = %+v", retry)
			}
			assertUploadContent(t, filepath.Join(env.rootA, entity.StorageKey), "12345")
		})
	}
}

// interruptedReader 模拟读取过程中连接中断。
type interruptedReader struct{}

func (interruptedReader) Read([]byte) (int, error) {
	return 0, errors.New("read interrupted")
}

// TestUploadSessionContentRejectsReadError 确认读取失败不会留下 ready 或残留内容。
func TestUploadSessionContentRejectsReadError(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "broken.bin", 5)

	body := io.MultiReader(strings.NewReader("he"), interruptedReader{})
	_, err := env.service.UploadSessionContent(ctx, env.ownerA.ID, created.ID, body)
	if err == nil {
		t.Fatal("wanted a read error")
	}
	if errors.Is(err, ErrUploadSizeMismatch) {
		t.Fatalf("read error was misreported as size mismatch: %v", err)
	}

	session, entity := loadUploadSessionAndEntity(t, env, created.ID)
	if session.Status != model.UploadStatusPending || session.ActualSize != 0 {
		t.Fatalf("failed upload changed session: %+v", session)
	}
	if entity.Size != 0 {
		t.Fatalf("failed upload changed entity size to %d", entity.Size)
	}
	assertNoUploadContent(t, filepath.Join(env.rootA, entity.StorageKey))
}

// TestUploadSessionContentOwnerAndExpiryChecks 覆盖归属、过期和不可写状态。
func TestUploadSessionContentOwnerAndExpiryChecks(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "mine.bin", 3)

	// 别人的会话和不存在的会话都返回同一个 404 错误。
	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerB.ID, created.ID, strings.NewReader("abc"),
	); !errors.Is(err, ErrUploadSessionNotFound) {
		t.Fatalf("foreign session: error = %v, want ErrUploadSessionNotFound", err)
	}
	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, "missing", strings.NewReader("abc"),
	); !errors.Is(err, ErrUploadSessionNotFound) {
		t.Fatalf("missing session: error = %v, want ErrUploadSessionNotFound", err)
	}

	// 过期会话在预检查阶段被拒绝，不改状态、不写内容。
	expired := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "expired.bin", 3)
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", expired.ID).
		UpdateColumn("expires_at", time.Now().Add(-time.Minute)).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, expired.ID, strings.NewReader("abc"),
	); !errors.Is(err, ErrUploadSessionExpired) {
		t.Fatalf("expired session: error = %v, want ErrUploadSessionExpired", err)
	}
	expiredSession, expiredEntity := loadUploadSessionAndEntity(t, env, expired.ID)
	if expiredSession.Status != model.UploadStatusPending {
		t.Fatalf("expired upload changed status to %q", expiredSession.Status)
	}
	assertNoUploadContent(t, filepath.Join(env.rootA, expiredEntity.StorageKey))

	// 已经是 receiving 的会话不能再写。
	receiving := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "receiving.bin", 3)
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", receiving.ID).
		UpdateColumn("status", model.UploadStatusReceiving).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, receiving.ID, strings.NewReader("abc"),
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("receiving session: error = %v, want ErrUploadSessionConflict", err)
	}

	// 占位实体不再是 uploading/0 时也必须拒绝。
	badEntity := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "bad-entity.bin", 3)
	if err := env.db.Model(&model.Entity{}).
		Where("id = (SELECT entity_id FROM upload_sessions WHERE id = ?)", badEntity.ID).
		Updates(map[string]any{"status": model.EntityStatusActive, "ref_count": 1}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, badEntity.ID, strings.NewReader("abc"),
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("invalid entity: error = %v, want ErrUploadSessionConflict", err)
	}
}

// TestUploadSessionContentUsesSessionStoragePolicy 确认写内容时使用 Entity
// 记录的策略，而不是用户后来修改过的默认策略。
func TestUploadSessionContentUsesSessionStoragePolicy(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "pinned.bin", 5)

	// 创建会话后服务端把 A 的默认位置改到 B。
	if err := env.db.Model(&model.User{}).
		Where("id = ?", env.ownerA.ID).
		UpdateColumn("default_storage_policy_id", env.policyB.ID).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, created.ID, strings.NewReader("hello"),
	); err != nil {
		t.Fatal(err)
	}

	_, entity := loadUploadSessionAndEntity(t, env, created.ID)
	if entity.StoragePolicyID != env.policyA.ID {
		t.Fatalf("entity policy = %d, want session policy A=%d", entity.StoragePolicyID, env.policyA.ID)
	}
	assertUploadContent(t, filepath.Join(env.rootA, entity.StorageKey), "hello")
	if entries := diskEntries(t, env.rootB); len(entries) != 0 {
		t.Fatalf("content followed the new default policy B: %v", entries)
	}
}

// blockingUploadReader 在第一次 Read 时通知测试并阻塞，用来制造并发写窗口。
type blockingUploadReader struct {
	data        []byte
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	offset      int
}

func (r *blockingUploadReader) Read(p []byte) (int, error) {
	if r.offset == 0 {
		r.startedOnce.Do(func() { close(r.started) })
		<-r.release
	}
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

// TestUploadSessionContentConcurrentClaims 验证两个请求同时上传时，
// 只有一个能抢到 receiving，另一个直接返回冲突。
func TestUploadSessionContentConcurrentClaims(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	const payload = "0123456789"
	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "concurrent.bin", int64(len(payload)))

	reader := &blockingUploadReader{
		data:    []byte(payload),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	released := false
	release := func() {
		if !released {
			released = true
			close(reader.release)
		}
	}
	t.Cleanup(release)

	type uploadResult struct {
		info UploadSessionInfo
		err  error
	}
	firstDone := make(chan uploadResult, 1)
	go func() {
		info, err := env.service.UploadSessionContent(ctx, env.ownerA.ID, created.ID, reader)
		firstDone <- uploadResult{info: info, err: err}
	}()

	select {
	case <-reader.started:
	case <-time.After(3 * time.Second):
		t.Fatal("first upload did not start reading")
	}

	// 第一个请求已经处于 receiving 并阻塞在读取，第二个必须被拒绝。
	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, created.ID, strings.NewReader(payload),
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("concurrent upload: error = %v, want ErrUploadSessionConflict", err)
	}

	release()
	var first uploadResult
	select {
	case first = <-firstDone:
	case <-time.After(3 * time.Second):
		t.Fatal("first upload did not finish")
	}
	if first.err != nil {
		t.Fatal(first.err)
	}
	if first.info.Status != model.UploadStatusReady || first.info.ActualSize != int64(len(payload)) {
		t.Fatalf("first upload info = %+v", first.info)
	}
	_, entity := loadUploadSessionAndEntity(t, env, created.ID)
	assertUploadContent(t, filepath.Join(env.rootA, entity.StorageKey), payload)
}

// removeFailStore 让补偿删除固定失败，用来覆盖“清理失败不能回到 pending”。
type removeFailStore struct {
	storage.Store
	err error
}

func (s *removeFailStore) Remove(context.Context, string) error {
	return s.err
}

func TestUploadSessionContentCleanupFailureKeepsReceiving(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	registry := storage.NewRegistry()
	if err := registry.Register(env.policyA.ID, &removeFailStore{
		Store: storage.NewLocal(env.rootA),
		err:   errors.New("remove blocked"),
	}); err != nil {
		t.Fatal(err)
	}
	svc := NewService(env.db, registry)
	created := mustCreateUploadContentSession(t, svc, env.ownerA.ID, "cleanup-fail.bin", 5)

	_, err := svc.UploadSessionContent(ctx, env.ownerA.ID, created.ID, strings.NewReader("abc"))
	if err == nil {
		t.Fatal("wanted a cleanup error")
	}

	session, entity := loadUploadSessionAndEntity(t, env, created.ID)
	if session.Status != model.UploadStatusReceiving {
		t.Fatalf("cleanup failure must keep receiving, got %q", session.Status)
	}
	if session.ActualSize != 0 || entity.Size != 0 {
		t.Fatalf("failed upload changed sizes: session=%+v entity=%+v", session, entity)
	}
	// 内容已经发布，但补偿删除失败，所以对象还在，供后续重试清理。
	assertUploadContent(t, filepath.Join(env.rootA, entity.StorageKey), "abc")

	// receiving 期间不能再写，避免覆盖残留内容。
	if _, err := svc.UploadSessionContent(
		ctx, env.ownerA.ID, created.ID, strings.NewReader("hello"),
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("upload after cleanup failure: error = %v, want ErrUploadSessionConflict", err)
	}
	assertUploadContent(t, filepath.Join(env.rootA, entity.StorageKey), "abc")
}

// TestUploadSessionContentFinalizeFailureCleansUp 用 GORM 回调让 ready 收尾失败，
// 验证会核对数据库、清理内容并回到 pending。
func TestUploadSessionContentFinalizeFailureCleansUp(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "finalize-fail.bin", 5)

	const callbackName = "test:fail_upload_session_ready_update"
	forcedErr := errors.New("forced ready update failure")
	updates := 0
	if err := env.db.Callback().Update().Before("gorm:update").Register(
		callbackName,
		func(tx *gorm.DB) {
			if tx.Statement.Schema == nil ||
				tx.Statement.Schema.Table != (model.UploadSession{}).TableName() {
				return
			}
			updates++
			// 第 1 次是 pending→receiving，第 2 次是 receiving→ready。
			if updates == 2 {
				_ = tx.AddError(forcedErr)
			}
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.db.Callback().Update().Remove(callbackName); err != nil {
			t.Logf("remove test callback: %v", err)
		}
	})

	_, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, created.ID, strings.NewReader("hello"),
	)
	if err == nil {
		t.Fatal("wanted a finalize error")
	}
	if updates < 2 {
		t.Fatalf("ready update was not attempted, updates=%d", updates)
	}

	session, entity := loadUploadSessionAndEntity(t, env, created.ID)
	if session.Status != model.UploadStatusPending || session.ActualSize != 0 {
		t.Fatalf("finalize failure did not revert to pending: %+v", session)
	}
	if entity.Size != 0 || entity.Status != model.EntityStatusUploading || entity.RefCount != 0 {
		t.Fatalf("finalize failure changed entity: %+v", entity)
	}
	assertNoUploadContent(t, filepath.Join(env.rootA, entity.StorageKey))
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("failed finalize created %d formal file(s)", got)
	}
}

// TestVerifyReadyUploadSessionRecognizesCommittedFinalize 模拟“事务其实已经提交，
// 但调用方收到了错误”：核对逻辑必须把 ready 当成成功，而不是删掉内容。
func TestVerifyReadyUploadSessionRecognizesCommittedFinalize(t *testing.T) {
	env := newTestEnv(t)
	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "verified.bin", 5)
	_, entity := loadUploadSessionAndEntity(t, env, created.ID)

	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", created.ID).
		Updates(map[string]any{
			"status":      model.UploadStatusReady,
			"actual_size": int64(5),
		}).Error; err != nil {
		t.Fatal(err)
	}
	if err := env.db.Model(&model.Entity{}).
		Where("id = ?", entity.ID).
		UpdateColumn("size", 5).Error; err != nil {
		t.Fatal(err)
	}

	// 请求 ctx 已经取消，核对必须用独立 context 仍然成功。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	info, err := env.service.verifyReadyUploadSession(ctx, env.ownerA.ID, created.ID, 5)
	if err != nil {
		t.Fatal(err)
	}
	if info == nil || info.Status != model.UploadStatusReady || info.ActualSize != 5 {
		t.Fatalf("verify returned %+v, want ready session", info)
	}

	// 大小对不上说明数据库结果不一致，不能当成成功。
	if _, err := env.service.verifyReadyUploadSession(
		ctx, env.ownerA.ID, created.ID, 4,
	); err == nil {
		t.Fatal("size mismatch must not be treated as committed")
	}

	// 确认还在 receiving：返回 (nil, nil)，调用方可以安全清理。
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", created.ID).
		Updates(map[string]any{
			"status":      model.UploadStatusReceiving,
			"actual_size": int64(0),
		}).Error; err != nil {
		t.Fatal(err)
	}
	info, err = env.service.verifyReadyUploadSession(ctx, env.ownerA.ID, created.ID, 5)
	if err != nil {
		t.Fatal(err)
	}
	if info != nil {
		t.Fatalf("receiving session should not verify as committed, got %+v", info)
	}
}

// TestUploadSessionContentVerifyFailureKeepsContent 让 ready 收尾失败，同时让
// 核对查询也失败。此时结果不确定，必须保留内容并停在 receiving，不能删除。
func TestUploadSessionContentVerifyFailureKeepsContent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "verify-fail.bin", 5)

	const updateCallback = "test:fail_upload_session_ready_update_for_verify"
	const queryCallback = "test:fail_upload_session_verify_query"
	forcedUpdateErr := errors.New("forced ready update failure")
	forcedQueryErr := errors.New("forced verify query failure")
	updates := 0
	queries := 0
	if err := env.db.Callback().Update().Before("gorm:update").Register(
		updateCallback,
		func(tx *gorm.DB) {
			if tx.Statement.Schema == nil ||
				tx.Statement.Schema.Table != (model.UploadSession{}).TableName() {
				return
			}
			updates++
			if updates == 2 {
				_ = tx.AddError(forcedUpdateErr)
			}
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.db.Callback().Update().Remove(updateCallback); err != nil {
			t.Logf("remove test update callback: %v", err)
		}
	})
	if err := env.db.Callback().Query().Before("gorm:query").Register(
		queryCallback,
		func(tx *gorm.DB) {
			if tx.Statement.Schema == nil ||
				tx.Statement.Schema.Table != (model.UploadSession{}).TableName() {
				return
			}
			queries++
			// 第 1 次是预检查，第 2 次是收尾失败后的核对。
			if queries == 2 {
				_ = tx.AddError(forcedQueryErr)
			}
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.db.Callback().Query().Remove(queryCallback); err != nil {
			t.Logf("remove test query callback: %v", err)
		}
	})

	_, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, created.ID, strings.NewReader("hello"),
	)
	if err == nil {
		t.Fatal("wanted a finalize/verify error")
	}
	if updates < 2 || queries < 2 {
		t.Fatalf("callbacks did not reach finalize/verify: updates=%d queries=%d", updates, queries)
	}

	session, entity := loadUploadSessionAndEntity(t, env, created.ID)
	if session.Status != model.UploadStatusReceiving || session.ActualSize != 0 {
		t.Fatalf("uncertain finalize must stay receiving: %+v", session)
	}
	if entity.Size != 0 || entity.Status != model.EntityStatusUploading || entity.RefCount != 0 {
		t.Fatalf("uncertain finalize changed entity: %+v", entity)
	}
	// 结果不确定时绝不删除内容。
	assertUploadContent(t, filepath.Join(env.rootA, entity.StorageKey), "hello")
}

// TestVerifyReadyUploadSessionWaitsForInFlightFinalize 复现“收尾事务还没提交
// 就开始核对”的窗口：普通 SELECT 会读到旧 receiving，SELECT ... FOR UPDATE
// 必须等待原事务提交，然后看到 ready。
func TestVerifyReadyUploadSessionWaitsForInFlightFinalize(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "late-commit.bin", 5)
	_, entity := loadUploadSessionAndEntity(t, env, created.ID)

	local := storage.NewLocal(env.rootA)
	if _, err := local.Save(ctx, entity.StorageKey, strings.NewReader("hello"), 5); err != nil {
		t.Fatal(err)
	}
	claimed, err := env.service.claimUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil || !claimed {
		t.Fatalf("claim: claimed=%v err=%v", claimed, err)
	}

	// 模拟收尾事务：会话已经改成 ready、实体大小也更新，但尚未提交。
	tx := env.db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	if err := tx.Model(&model.UploadSession{}).
		Where("id = ?", created.ID).
		Updates(map[string]any{
			"status":      model.UploadStatusReady,
			"actual_size": int64(5),
		}).Error; err != nil {
		t.Fatal(err)
	}
	if err := tx.Model(&model.Entity{}).
		Where("id = ?", entity.ID).
		UpdateColumn("size", 5).Error; err != nil {
		t.Fatal(err)
	}

	// 用一个 Query 回调确认核对查询已经开始（随后会阻塞在行锁上）。
	queryStarted := make(chan struct{})
	var startedOnce sync.Once
	const callbackName = "test:signal_verify_lock_query"
	if err := env.db.Callback().Query().Before("gorm:query").Register(
		callbackName,
		func(tx *gorm.DB) {
			if tx.Statement.Schema != nil &&
				tx.Statement.Schema.Table == (model.UploadSession{}).TableName() {
				startedOnce.Do(func() { close(queryStarted) })
			}
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.db.Callback().Query().Remove(callbackName); err != nil {
			t.Logf("remove test callback: %v", err)
		}
	})

	type verifyResult struct {
		info *UploadSessionInfo
		err  error
	}
	done := make(chan verifyResult, 1)
	go func() {
		info, err := env.service.verifyReadyUploadSession(
			ctx, env.ownerA.ID, created.ID, 5,
		)
		done <- verifyResult{info: info, err: err}
	}()

	select {
	case <-queryStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("verification query did not start")
	}
	// 原事务还没提交，核对必须被行锁挡住，不能返回 receiving。
	select {
	case result := <-done:
		t.Fatalf("verification returned before finalize transaction ended: info=%+v err=%v",
			result.info, result.err)
	case <-time.After(150 * time.Millisecond):
	}

	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}

	var result verifyResult
	select {
	case result = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("verification did not finish after finalize transaction committed")
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.info == nil ||
		result.info.Status != model.UploadStatusReady ||
		result.info.ActualSize != 5 {
		t.Fatalf("verification result = %+v, want ready session", result.info)
	}
	// 关键：看到 ready 后不能删除内容。
	assertUploadContent(t, filepath.Join(env.rootA, entity.StorageKey), "hello")
}

// cancelAfterContentSaveStore 在内容完整写入后取消请求 ctx，模拟“写完后客户端断开”。
type cancelAfterContentSaveStore struct {
	storage.Store
	cancel context.CancelFunc
}

func (s *cancelAfterContentSaveStore) Save(
	ctx context.Context,
	key string,
	r io.Reader,
	maxBytes int64,
) (int64, error) {
	size, err := s.Store.Save(ctx, key, r, maxBytes)
	if err == nil {
		s.cancel()
	}
	return size, err
}

func TestUploadSessionContentCanceledAfterSaveCleansUp(t *testing.T) {
	env := newTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry := storage.NewRegistry()
	if err := registry.Register(env.policyA.ID, &cancelAfterContentSaveStore{
		Store:  storage.NewLocal(env.rootA),
		cancel: cancel,
	}); err != nil {
		t.Fatal(err)
	}
	svc := NewService(env.db, registry)
	created := mustCreateUploadContentSession(t, svc, env.ownerA.ID, "canceled.bin", 5)

	_, err := svc.UploadSessionContent(ctx, env.ownerA.ID, created.ID, strings.NewReader("hello"))
	if err == nil {
		t.Fatal("wanted an error after the request was canceled")
	}

	session, entity := loadUploadSessionAndEntity(t, env, created.ID)
	if session.Status != model.UploadStatusPending || session.ActualSize != 0 {
		t.Fatalf("canceled upload did not revert to pending: %+v", session)
	}
	if entity.Size != 0 || entity.Status != model.EntityStatusUploading || entity.RefCount != 0 {
		t.Fatalf("canceled upload changed entity: %+v", entity)
	}
	assertNoUploadContent(t, filepath.Join(env.rootA, entity.StorageKey))
}
