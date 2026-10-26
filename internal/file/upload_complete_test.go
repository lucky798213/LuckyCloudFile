package file

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
)

// mustReadyUploadSession 创建会话并写完内容，返回 ready 会话和它的占位 Entity。
func mustReadyUploadSession(t *testing.T, env *testEnv, ownerID int, name, content string) (UploadSessionInfo, model.UploadSession, model.Entity) {
	t.Helper()

	created := mustCreateUploadContentSession(t, env.service, ownerID, name, int64(len(content)))
	if _, err := env.service.UploadSessionContent(
		context.Background(), ownerID, created.ID, strings.NewReader(content),
	); err != nil {
		t.Fatal(err)
	}
	session, entity := loadUploadSessionAndEntity(t, env, created.ID)
	return created, session, entity
}

func mustOpenContent(t *testing.T, svc *Service, ownerID int, fileID, want string) {
	t.Helper()

	_, rc, err := svc.Open(context.Background(), ownerID, fileID)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != want {
		t.Fatalf("downloaded content = %q, want %q", data, want)
	}
}

// TestCompleteUploadSessionPublishesFile 覆盖正常完成：
// 新增一条 File、Entity 变成 active/1、Session 变成 completed，
// 随后目录可见、下载内容与上传内容一致。
func TestCompleteUploadSessionPublishesFile(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created, readySession, placeholder := mustReadyUploadSession(t, env, env.ownerA.ID, "hello.txt", "hello")

	info, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.ID != created.ID || info.Status != model.UploadStatusCompleted {
		t.Fatalf("completed session info = %+v", info)
	}
	if info.ResultFileID == nil || *info.ResultFileID == "" {
		t.Fatalf("completed session has no result file: %+v", info)
	}
	fileID := *info.ResultFileID
	if info.ExpectedSize != 5 || info.ActualSize != 5 {
		t.Fatalf("unexpected sizes: %+v", info)
	}

	session, entity := loadUploadSessionAndEntity(t, env, created.ID)
	if session.Status != model.UploadStatusCompleted ||
		session.ResultFileID == nil || *session.ResultFileID != fileID {
		t.Fatalf("stored session = %+v", session)
	}
	if entity.ID != placeholder.ID {
		t.Fatalf("completion created a new entity %d, want existing %d", entity.ID, placeholder.ID)
	}
	if entity.Status != model.EntityStatusActive || entity.RefCount != 1 || entity.Size != 5 {
		t.Fatalf("activated entity = %+v", entity)
	}
	if readySession.EntityID != placeholder.ID {
		t.Fatalf("test setup used wrong entity: %+v", readySession)
	}

	// 正式 File 的字段全部来自会话，客户端没有重新指定。
	var record model.File
	if err := env.db.Preload("Entity").First(&record, "id = ?", fileID).Error; err != nil {
		t.Fatal(err)
	}
	if record.Name != "hello.txt" || record.OwnerID != env.ownerA.ID ||
		record.Kind != model.KindFile ||
		record.ParentID == nil || *record.ParentID != *env.ownerA.RootFileID ||
		record.EntityID == nil || *record.EntityID != entity.ID {
		t.Fatalf("published file = %+v", record)
	}
	if got := countKind(t, env.db, model.KindFile); got != 1 {
		t.Fatalf("wanted 1 formal file, got %d", got)
	}

	// 目录列表能看到它，下载内容和源内容一致。
	listing, err := env.service.ListChildren(ctx, env.ownerA.ID, "", 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if listing.Total != 1 || len(listing.Items) != 1 || listing.Items[0].ID != fileID {
		t.Fatalf("root listing = %+v", listing)
	}
	mustOpenContent(t, env.service, env.ownerA.ID, fileID, "hello")
}

// TestCompleteUploadSessionIsIdempotent 覆盖重复完成：
// 即使会话后来过期、正式文件后来被删除，也只能返回原文件 ID，不能重建。
func TestCompleteUploadSessionIsIdempotent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created, _, _ := mustReadyUploadSession(t, env, env.ownerA.ID, "idem.txt", "hello")

	first, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	fileID := *first.ResultFileID

	// 会话过期之后重复确认：completed 先于过期判断，仍然返回原结果。
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", created.ID).
		UpdateColumn("expires_at", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatal(err)
	}
	again, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.ResultFileID == nil || *again.ResultFileID != fileID {
		t.Fatalf("repeat completion returned %+v, want %q", again, fileID)
	}
	if again.Status != model.UploadStatusCompleted {
		t.Fatalf("repeat completion status = %q", again.Status)
	}
	if got := countKind(t, env.db, model.KindFile); got != 1 {
		t.Fatalf("repeat completion created files, count = %d", got)
	}
	_, entity := loadUploadSessionAndEntity(t, env, created.ID)
	if entity.RefCount != 1 || entity.Status != model.EntityStatusActive {
		t.Fatalf("repeat completion changed entity: %+v", entity)
	}

	// 正式文件后来被删除：重复确认不能把它“补”回来。
	if err := env.service.Delete(ctx, env.ownerA.ID, fileID); err != nil {
		t.Fatal(err)
	}
	replayed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ResultFileID == nil || *replayed.ResultFileID != fileID {
		t.Fatalf("replay after delete returned %+v, want %q", replayed, fileID)
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("replay recreated the deleted file, count = %d", got)
	}
	var recreated int64
	if err := env.db.Model(&model.File{}).Where("id = ?", fileID).Count(&recreated).Error; err != nil {
		t.Fatal(err)
	}
	if recreated != 0 {
		t.Fatalf("deleted file %s was recreated", fileID)
	}
}

// TestCompleteUploadSessionRejectsNotReady 覆盖未就绪、过期和越权：
// 三种情况都不能发布文件。
func TestCompleteUploadSessionRejectsNotReady(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// pending：还没有上传内容。
	pending := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "pending.txt", 5)
	if _, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, pending.ID); !errors.Is(err, ErrUploadSessionNotCompletable) {
		t.Fatalf("pending completion error = %v, want ErrUploadSessionNotCompletable", err)
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("pending completion created %d file(s)", got)
	}

	// ready 但已过期：拒绝，保留 ready 和内容。
	created, _, entity := mustReadyUploadSession(t, env, env.ownerA.ID, "expired.txt", "hello")
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", created.ID).
		UpdateColumn("expires_at", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID); !errors.Is(err, ErrUploadSessionExpired) {
		t.Fatalf("expired completion error = %v, want ErrUploadSessionExpired", err)
	}
	session, currentEntity := loadUploadSessionAndEntity(t, env, created.ID)
	if session.Status != model.UploadStatusReady || session.ResultFileID != nil {
		t.Fatalf("expired completion changed session: %+v", session)
	}
	if currentEntity.Status != model.EntityStatusUploading || currentEntity.RefCount != 0 {
		t.Fatalf("expired completion changed entity: %+v", currentEntity)
	}
	assertUploadContent(t, filepath.Join(env.rootA, entity.StorageKey), "hello")

	// 越权：别人的会话按不存在处理。
	if _, err := env.service.CompleteUploadSession(ctx, env.ownerB.ID, created.ID); !errors.Is(err, ErrUploadSessionNotFound) {
		t.Fatalf("foreign completion error = %v, want ErrUploadSessionNotFound", err)
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("rejected completions created %d file(s)", got)
	}
}

// TestCompleteUploadSessionNameConflictKeepsContent 覆盖同名冲突：
// 事务回滚，会话仍是 ready，已经传好的内容保留。
func TestCompleteUploadSessionNameConflictKeepsContent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created, _, entity := mustReadyUploadSession(t, env, env.ownerA.ID, "same.txt", "hello")

	// 先在同一个目录放一个同名文件。
	if _, err := env.service.Upload(ctx, env.ownerA.ID, "", "same.txt", strings.NewReader("other")); err != nil {
		t.Fatal(err)
	}
	before := countKind(t, env.db, model.KindFile)

	if _, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("conflicting completion error = %v, want ErrAlreadyExists", err)
	}

	session, currentEntity := loadUploadSessionAndEntity(t, env, created.ID)
	if session.Status != model.UploadStatusReady || session.ResultFileID != nil {
		t.Fatalf("conflict changed session: %+v", session)
	}
	if currentEntity.Status != model.EntityStatusUploading ||
		currentEntity.RefCount != 0 || currentEntity.Size != 5 {
		t.Fatalf("conflict changed entity: %+v", currentEntity)
	}
	assertUploadContent(t, filepath.Join(env.rootA, entity.StorageKey), "hello")
	if got := countKind(t, env.db, model.KindFile); got != before {
		t.Fatalf("conflict changed file count from %d to %d", before, got)
	}
}

// TestCompleteUploadSessionMissingDirectoryKeepsContent 覆盖目录失效：
// 事务回滚，会话仍是 ready，内容保留。
func TestCompleteUploadSessionMissingDirectoryKeepsContent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	dir, err := env.service.CreateDirectory(ctx, env.ownerA.ID, "", "target")
	if err != nil {
		t.Fatal(err)
	}
	created := mustCreateUploadContentSessionInParent(t, env, env.ownerA.ID, dir.ID, "gone.txt", "hello")
	_, entity := loadUploadSessionAndEntity(t, env, created.ID)

	// 上传期间目录被删除。
	if err := env.service.Delete(ctx, env.ownerA.ID, dir.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing directory completion error = %v, want ErrNotFound", err)
	}

	session, currentEntity := loadUploadSessionAndEntity(t, env, created.ID)
	if session.Status != model.UploadStatusReady || session.ResultFileID != nil {
		t.Fatalf("missing directory changed session: %+v", session)
	}
	if currentEntity.Status != model.EntityStatusUploading || currentEntity.RefCount != 0 {
		t.Fatalf("missing directory changed entity: %+v", currentEntity)
	}
	assertUploadContent(t, filepath.Join(env.rootA, entity.StorageKey), "hello")
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("missing directory completion created %d file(s)", got)
	}
}

// mustCreateUploadContentSessionInParent 与 mustCreateUploadContentSession 类似，
// 但可以指定目标目录。
func mustCreateUploadContentSessionInParent(t *testing.T, env *testEnv, ownerID int, parentID, name, content string) UploadSessionInfo {
	t.Helper()

	created, err := env.service.CreateUploadSession(context.Background(), ownerID, parentID, name, int64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.UploadSessionContent(
		context.Background(), ownerID, created.ID, strings.NewReader(content),
	); err != nil {
		t.Fatal(err)
	}
	return created
}

// TestCompleteUploadSessionTransactionRollsBack 让最后一步 Session 更新失败，
// 验证 File 创建和 Entity 激活也一起回滚，会话仍是 ready。
func TestCompleteUploadSessionTransactionRollsBack(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created, _, entity := mustReadyUploadSession(t, env, env.ownerA.ID, "rollback.txt", "hello")

	const callbackName = "test:fail_upload_session_complete_update"
	forcedErr := errors.New("forced completion update failure")
	if err := env.db.Callback().Update().Before("gorm:update").Register(
		callbackName,
		func(tx *gorm.DB) {
			if tx.Statement.Schema == nil ||
				tx.Statement.Schema.Table != (model.UploadSession{}).TableName() {
				return
			}
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

	if _, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, created.ID); err == nil {
		t.Fatal("wanted a completion error")
	}

	// File、Entity、Session 三个变化全部回滚。
	session, currentEntity := loadUploadSessionAndEntity(t, env, created.ID)
	if session.Status != model.UploadStatusReady || session.ResultFileID != nil {
		t.Fatalf("failed completion changed session: %+v", session)
	}
	if currentEntity.Status != model.EntityStatusUploading ||
		currentEntity.RefCount != 0 || currentEntity.Size != 5 {
		t.Fatalf("failed completion changed entity: %+v", currentEntity)
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("failed completion left %d file(s)", got)
	}
	assertUploadContent(t, filepath.Join(env.rootA, entity.StorageKey), "hello")
}

// TestCompleteUploadSessionConcurrent 验证多个完成请求同时到达时，
// 只创建一份正式文件，所有调用返回同一个文件 ID。
func TestCompleteUploadSessionConcurrent(t *testing.T) {
	env := newTestEnv(t)
	created, _, _ := mustReadyUploadSession(t, env, env.ownerA.ID, "race.txt", "hello")

	const callers = 6
	results := make([]UploadSessionInfo, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = env.service.CompleteUploadSession(
				context.Background(), env.ownerA.ID, created.ID,
			)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d failed: %v", i, err)
		}
		if results[i].ResultFileID == nil || *results[i].ResultFileID == "" {
			t.Fatalf("caller %d returned no result file: %+v", i, results[i])
		}
		if *results[i].ResultFileID != *results[0].ResultFileID {
			t.Fatalf("caller %d got %q, want %q", i, *results[i].ResultFileID, *results[0].ResultFileID)
		}
	}

	if got := countKind(t, env.db, model.KindFile); got != 1 {
		t.Fatalf("concurrent completions created %d files, want 1", got)
	}
	session, entity := loadUploadSessionAndEntity(t, env, created.ID)
	if session.Status != model.UploadStatusCompleted ||
		session.ResultFileID == nil || *session.ResultFileID != *results[0].ResultFileID {
		t.Fatalf("stored session = %+v", session)
	}
	if entity.Status != model.EntityStatusActive || entity.RefCount != 1 {
		t.Fatalf("stored entity = %+v", entity)
	}
	mustOpenContent(t, env.service, env.ownerA.ID, *results[0].ResultFileID, "hello")
}

// TestVerifyCompletedUploadSession 覆盖“提交结果不确定”时的核对：
// completed 返回原结果；ready 返回 (nil, nil)，调用方保留内容。
func TestVerifyCompletedUploadSession(t *testing.T) {
	env := newTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	created, _, _ := mustReadyUploadSession(t, env, env.ownerA.ID, "verify.txt", "hello")

	// 仍是 ready：核对返回 nil，表示事务确实没有提交。
	info, err := env.service.verifyCompletedUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info != nil {
		t.Fatalf("ready session should not verify as completed: %+v", info)
	}

	// 模拟“事务已经提交，但调用方收到了错误”。
	fileID := "committed-file-id"
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", created.ID).
		Updates(map[string]any{
			"status":         model.UploadStatusCompleted,
			"result_file_id": fileID,
		}).Error; err != nil {
		t.Fatal(err)
	}
	info, err = env.service.verifyCompletedUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info == nil || info.Status != model.UploadStatusCompleted ||
		info.ResultFileID == nil || *info.ResultFileID != fileID {
		t.Fatalf("verify returned %+v, want completed with %q", info, fileID)
	}

	// completed 却缺少 result_file_id：数据异常，不能当成成功。
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", created.ID).
		UpdateColumn("result_file_id", nil).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.verifyCompletedUploadSession(ctx, env.ownerA.ID, created.ID); !errors.Is(err, ErrUploadSessionInconsistent) {
		t.Fatalf("missing result file verify error = %v, want ErrUploadSessionInconsistent", err)
	}
}
