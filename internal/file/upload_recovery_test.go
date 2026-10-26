package file

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
)

// setUploadSessionStatus 直接把会话改成某个状态，用来构造测试场景。
// 正常业务不应该这样改写状态，这里只是模拟“重启前正处于 receiving”等情况。
func setUploadSessionStatus(t *testing.T, env *testEnv, sessionID, status string) {
	t.Helper()

	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", sessionID).
		UpdateColumn("status", status).Error; err != nil {
		t.Fatal(err)
	}
}

// TestInvalidateUnfinishedUploadsExpiresEveryUnfinishedStatus 覆盖启动恢复的状态边界：
// 未到期的 pending / receiving / ready 全部变成 expired，completed / canceled 不动。
//
// 关键点是“未到期”：恢复不能依赖 expires_at，否则重启前刚创建、还有一小时
// 有效期的会话仍然能用。这里同时验证恢复只改状态，不提前删除占位内容。
func TestInvalidateUnfinishedUploadsExpiresEveryUnfinishedStatus(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	pendingInfo := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "pending.bin", 5)
	_, pendingEntity := loadUploadSessionAndEntity(t, env, pendingInfo.ID)

	receivingInfo := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "receiving.bin", 5)
	setUploadSessionStatus(t, env, receivingInfo.ID, model.UploadStatusReceiving)
	_, receivingEntity := loadUploadSessionAndEntity(t, env, receivingInfo.ID)

	readyInfo, readySession, readyEntity := mustReadyUploadSession(t, env, env.ownerA.ID, "ready.bin", "hello")
	readyPath := filepath.Join(env.rootA, readyEntity.StorageKey)

	finishedInfo, _, finishedEntity := mustReadyUploadSession(t, env, env.ownerA.ID, "completed.bin", "done")
	completedInfo, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, finishedInfo.ID)
	if err != nil {
		t.Fatal(err)
	}
	completedFileID := *completedInfo.ResultFileID

	canceledInfo := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "canceled.bin", 5)
	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, canceledInfo.ID); err != nil {
		t.Fatal(err)
	}

	unfinished := []struct {
		info   UploadSessionInfo
		entity model.Entity
	}{
		{info: pendingInfo, entity: pendingEntity},
		{info: receivingInfo, entity: receivingEntity},
		{info: readyInfo, entity: readyEntity},
	}
	expiresAt := make(map[string]time.Time, len(unfinished))
	for _, item := range unfinished {
		session := loadSessionOnly(t, env, item.info.ID)
		if !session.ExpiresAt.After(time.Now()) {
			t.Fatalf("test setup session %s is already expired", session.ID)
		}
		expiresAt[session.ID] = session.ExpiresAt
	}
	// readySession 只用来确认构造场景状态正确。
	if readySession.Status != model.UploadStatusReady {
		t.Fatalf("test setup ready session status = %q", readySession.Status)
	}

	if err := env.service.InvalidateUnfinishedUploads(ctx, 2); err != nil {
		t.Fatal(err)
	}

	for _, item := range unfinished {
		session := loadSessionOnly(t, env, item.info.ID)
		if session.Status != model.UploadStatusExpired {
			t.Fatalf("session %s status = %q, want expired", session.ID, session.Status)
		}
		// expires_at 必须原封不动：失效原因不是时间，而是服务重启。
		if !session.ExpiresAt.Equal(expiresAt[session.ID]) {
			t.Fatalf("session %s expires_at changed: %v -> %v",
				session.ID, expiresAt[session.ID], session.ExpiresAt)
		}
		if session.ResultFileID != nil {
			t.Fatalf("invalidated session %s got result file %q", session.ID, *session.ResultFileID)
		}

		// 恢复只切断资格，不删 Entity、不删磁盘内容；物理清理交给 CleanupLoop。
		entity := mustEntity(t, env, item.entity.ID)
		if entity.Status != model.EntityStatusUploading || entity.RefCount != 0 {
			t.Fatalf("invalidated session entity = %+v, want uploading/0", entity)
		}
	}
	assertUploadContent(t, readyPath, "hello")

	// completed 不受影响：状态、结果文件和 Entity 都保持原样。
	completed := loadSessionOnly(t, env, completedInfo.ID)
	if completed.Status != model.UploadStatusCompleted ||
		completed.ResultFileID == nil || *completed.ResultFileID != completedFileID {
		t.Fatalf("completed session was changed by recovery: %+v", completed)
	}
	active := mustEntity(t, env, finishedEntity.ID)
	if active.Status != model.EntityStatusActive || active.RefCount != 1 {
		t.Fatalf("completed entity was changed by recovery: %+v", active)
	}
	mustOpenContent(t, env.service, env.ownerA.ID, completedFileID, "done")

	// canceled 同样是终态，保持自己的结束原因。
	if canceled := loadSessionOnly(t, env, canceledInfo.ID); canceled.Status != model.UploadStatusCanceled {
		t.Fatalf("canceled session status = %q, want canceled", canceled.Status)
	}

	// 三个未完成会话都没有生成正式文件。
	if got := countKind(t, env.db, model.KindFile); got != 1 {
		t.Fatalf("formal file count = %d, want only the completed one", got)
	}
}

// TestInvalidatedUploadCannotContinueOrComplete 覆盖重启后的旧会话：
// 内容上传返回 410 对应的 ErrUploadSessionExpired，确认完成被拒绝，
// 并且不会生成正式文件。
func TestInvalidatedUploadCannotContinueOrComplete(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	type staleSession struct {
		info UploadSessionInfo
	}
	stale := make([]staleSession, 0, 3)

	pending := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "stale-pending.bin", 5)
	stale = append(stale, staleSession{info: pending})

	receiving := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "stale-receiving.bin", 5)
	setUploadSessionStatus(t, env, receiving.ID, model.UploadStatusReceiving)
	stale = append(stale, staleSession{info: receiving})

	ready, _, readyEntity := mustReadyUploadSession(t, env, env.ownerA.ID, "stale-ready.bin", "hello")
	readyPath := filepath.Join(env.rootA, readyEntity.StorageKey)
	stale = append(stale, staleSession{info: ready})

	if err := env.service.InvalidateUnfinishedUploads(ctx, 10); err != nil {
		t.Fatal(err)
	}

	for _, item := range stale {
		session := loadSessionOnly(t, env, item.info.ID)
		if session.Status != model.UploadStatusExpired || !session.ExpiresAt.After(time.Now()) {
			t.Fatalf("stale session = %+v, want expired with future expires_at", session)
		}

		// 查询接口返回 stored 状态，客户端能直接看到“已失效”这个原因。
		info, err := env.service.GetUploadSession(ctx, env.ownerA.ID, item.info.ID)
		if err != nil {
			t.Fatal(err)
		}
		if info.Status != model.UploadStatusExpired {
			t.Fatalf("get invalidated session %s status = %q, want expired",
				item.info.ID, info.Status)
		}

		// 即使还没到原定到期时间，也必须拒绝继续上传。
		_, err = env.service.UploadSessionContent(
			ctx, env.ownerA.ID, item.info.ID, strings.NewReader("again"),
		)
		if !errors.Is(err, ErrUploadSessionExpired) {
			t.Fatalf("upload on invalidated session %s: error = %v, want ErrUploadSessionExpired",
				item.info.ID, err)
		}
	}

	// 恢复只切资格：ready 会话已经收到的内容不能被上传请求覆盖或删除。
	assertUploadContent(t, readyPath, "hello")

	// 确认完成同样被拒绝，而且不能产生 File。
	for _, item := range stale {
		if _, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, item.info.ID); err == nil {
			t.Fatalf("complete on invalidated session %s succeeded", item.info.ID)
		}
		final := loadSessionOnly(t, env, item.info.ID)
		if final.Status != model.UploadStatusExpired || final.ResultFileID != nil {
			t.Fatalf("invalidated session %s changed by completion: %+v", item.info.ID, final)
		}
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("invalidated sessions published %d formal file(s)", got)
	}

	// 取消已失效会话：保持 expired（不能改写成 canceled），并继续推进清理。
	canceled, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, ready.ID)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status != model.UploadStatusExpired {
		t.Fatalf("cancel invalidated session returned %q, want expired", canceled.Status)
	}
	if got := loadSessionOnly(t, env, ready.ID); got.Status != model.UploadStatusExpired {
		t.Fatalf("cancel rewrote expired session to %q", got.Status)
	}
	assertEntityGone(t, env, readyEntity.ID)
	assertNoUploadContent(t, readyPath)
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("cancel published %d formal file(s)", got)
	}
}

// TestInvalidateUnfinishedUploadsKeepsCompletedSession 覆盖幂等边界：
// completed 会话重启后仍然可以下载，重复确认完成返回同一个结果文件。
func TestInvalidateUnfinishedUploadsKeepsCompletedSession(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	readyInfo, _, entity := mustReadyUploadSession(t, env, env.ownerA.ID, "keep-completed.bin", "hello")
	completedInfo, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, readyInfo.ID)
	if err != nil {
		t.Fatal(err)
	}
	fileID := *completedInfo.ResultFileID

	if err := env.service.InvalidateUnfinishedUploads(ctx, 10); err != nil {
		t.Fatal(err)
	}

	final := loadSessionOnly(t, env, readyInfo.ID)
	if final.Status != model.UploadStatusCompleted ||
		final.ResultFileID == nil || *final.ResultFileID != fileID {
		t.Fatalf("completed session changed by recovery: %+v", final)
	}
	active := mustEntity(t, env, entity.ID)
	if active.Status != model.EntityStatusActive || active.RefCount != 1 {
		t.Fatalf("completed entity changed by recovery: %+v", active)
	}
	mustOpenContent(t, env.service, env.ownerA.ID, fileID, "hello")

	again, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, readyInfo.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.ResultFileID == nil || *again.ResultFileID != fileID {
		t.Fatalf("repeat complete returned %+v, want file %s", again, fileID)
	}
	if got := countKind(t, env.db, model.KindFile); got != 1 {
		t.Fatalf("repeat complete created extra files: %d", got)
	}
}

// TestInvalidateUnfinishedUploadsCleanupReclaimsPlaceholders 覆盖接入现有清理链：
// 恢复后旧占位 Entity、正式内容和 .upload- 临时文件都由 CleanupPending 回收。
func TestInvalidateUnfinishedUploadsCleanupReclaimsPlaceholders(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	readyInfo, _, entity := mustReadyUploadSession(t, env, env.ownerA.ID, "reclaim.bin", "hello")
	contentPath := filepath.Join(env.rootA, entity.StorageKey)
	// 模拟上次进程在 Save 中途退出留下的临时文件；Remove 必须同时处理它和正式内容。
	tempPath := filepath.Join(env.rootA, ".upload-"+entity.StorageKey)
	if err := os.WriteFile(tempPath, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}

	pendingInfo := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "reclaim-pending.bin", 5)
	_, pendingEntity := loadUploadSessionAndEntity(t, env, pendingInfo.ID)

	if err := env.service.InvalidateUnfinishedUploads(ctx, 10); err != nil {
		t.Fatal(err)
	}

	// 恢复本身不删任何东西：这两条记录和文件都应该还在。
	if got := loadSessionOnly(t, env, readyInfo.ID); got.Status != model.UploadStatusExpired {
		t.Fatalf("ready session status = %q, want expired", got.Status)
	}
	if got := mustEntity(t, env, entity.ID); got.Status != model.EntityStatusUploading {
		t.Fatalf("recovery removed content before cleanup: %+v", got)
	}
	assertUploadContent(t, contentPath, "hello")
	assertUploadContent(t, tempPath, "partial")

	// 新 Service 的活跃写入登记表是空的，相当于重启后的清理循环。
	restarted := NewService(env.db, testRegistry(t, env.policyA, env.rootA))
	if _, err := restarted.CleanupPending(ctx, 10); err != nil {
		t.Fatal(err)
	}

	assertEntityGone(t, env, entity.ID)
	assertEntityGone(t, env, pendingEntity.ID)
	assertNoUploadContent(t, contentPath)
	assertNoUploadContent(t, tempPath)
	for _, info := range []UploadSessionInfo{readyInfo, pendingInfo} {
		if got := loadSessionOnly(t, env, info.ID); got.Status != model.UploadStatusExpired {
			t.Fatalf("cleanup changed session %s to %q", info.ID, got.Status)
		}
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("cleanup published %d formal file(s)", got)
	}
}

// TestInvalidateUnfinishedUploadsCleanupFailureRetries 覆盖物理删除失败：
// 会话保持 expired，Entity 保留 pending_delete，内容不丢，下一轮清理能成功。
func TestInvalidateUnfinishedUploadsCleanupFailureRetries(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// 第一轮 CleanupPending 会尝试两次删除（恢复扫描一次、pending_delete 扫描一次），
	// 让两次都失败，才能观察到“保留记录等下一轮”的中间状态。
	store := &flakyRemoveStore{Store: storage.NewLocal(env.rootA), remaining: 2}
	registry := storage.NewRegistry()
	if err := registry.Register(env.policyA.ID, store); err != nil {
		t.Fatal(err)
	}
	svc := NewService(env.db, registry)
	created, _, entity := mustReadyUploadSessionWithService(t, svc, env, "recovery-flaky.bin", "hello")
	contentPath := filepath.Join(env.rootA, entity.StorageKey)

	if err := svc.InvalidateUnfinishedUploads(ctx, 10); err != nil {
		t.Fatal(err)
	}
	result, err := svc.CleanupPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Deferred == 0 {
		t.Fatalf("cleanup did not defer any failed removal: %+v", result)
	}

	if got := loadSessionOnly(t, env, created.ID); got.Status != model.UploadStatusExpired {
		t.Fatalf("session status = %q, want expired", got.Status)
	}
	pending := mustEntity(t, env, entity.ID)
	if pending.Status != model.EntityStatusPendingDelete || pending.RefCount != 0 {
		t.Fatalf("entity after failed cleanup = %+v, want pending_delete/0", pending)
	}
	assertUploadContent(t, contentPath, "hello")

	// 存储恢复后，下一轮清理成功回收。
	result, err = svc.CleanupPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Removed == 0 {
		t.Fatalf("retry cleanup removed nothing: %+v", result)
	}
	assertEntityGone(t, env, entity.ID)
	assertNoUploadContent(t, contentPath)
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("cleanup published %d formal file(s)", got)
	}
}

// TestInvalidateUnfinishedUploadsPaginatesAndIsIdempotent 覆盖多批次和重复执行：
// 按 id 游标继续扫描直到清空，不漏记录；第二次执行不改变任何状态。
func TestInvalidateUnfinishedUploadsPaginatesAndIsIdempotent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const count = 5
	infos := make([]UploadSessionInfo, 0, count)
	entities := make([]model.Entity, 0, count)
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("recovery-page-%d.bin", i)
		switch i % 3 {
		case 0:
			info := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, name, 3)
			_, entity := loadUploadSessionAndEntity(t, env, info.ID)
			infos = append(infos, info)
			entities = append(entities, entity)
		case 1:
			info := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, name, 3)
			setUploadSessionStatus(t, env, info.ID, model.UploadStatusReceiving)
			_, entity := loadUploadSessionAndEntity(t, env, info.ID)
			infos = append(infos, info)
			entities = append(entities, entity)
		case 2:
			info, _, entity := mustReadyUploadSession(t, env, env.ownerA.ID, name, "hello")
			infos = append(infos, info)
			entities = append(entities, entity)
		}
	}

	// 让批次之间夹杂一条 completed，验证候选过滤不会把它带进来。
	finishedInfo, _, _ := mustReadyUploadSession(t, env, env.ownerA.ID, "recovery-page-completed.bin", "done")
	completedInfo, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, finishedInfo.ID)
	if err != nil {
		t.Fatal(err)
	}

	// batchSize=2 时 5 条未完成会话至少要分三批，任何漏掉都会被发现。
	if err := env.service.InvalidateUnfinishedUploads(ctx, 2); err != nil {
		t.Fatal(err)
	}
	for _, info := range infos {
		if got := loadSessionOnly(t, env, info.ID); got.Status != model.UploadStatusExpired {
			t.Fatalf("session %s status = %q, want expired", info.ID, got.Status)
		}
	}
	if got := countKind(t, env.db, model.KindFile); got != 1 {
		t.Fatalf("formal file count = %d, want 1", got)
	}

	// 再执行一次：没有候选，状态和占位内容都不应发生变化。
	if err := env.service.InvalidateUnfinishedUploads(ctx, 1); err != nil {
		t.Fatal(err)
	}
	for i, info := range infos {
		if got := loadSessionOnly(t, env, info.ID); got.Status != model.UploadStatusExpired {
			t.Fatalf("repeat recovery changed session %s to %q", info.ID, got.Status)
		}
		if got := mustEntity(t, env, entities[i].ID); got.Status != model.EntityStatusUploading || got.RefCount != 0 {
			t.Fatalf("repeat recovery changed entity %d: %+v", entities[i].ID, got)
		}
	}
	if got := loadSessionOnly(t, env, completedInfo.ID); got.Status != model.UploadStatusCompleted {
		t.Fatalf("repeat recovery changed completed session to %q", got.Status)
	}
	if got := countKind(t, env.db, model.KindFile); got != 1 {
		t.Fatalf("repeat recovery created files: %d", got)
	}
}

// TestInvalidateUnfinishedUploadsReturnsQueryError 覆盖候选查询失败：
// 必须把错误返回给启动流程，而不是当作“没有旧会话”。
func TestInvalidateUnfinishedUploadsReturnsQueryError(t *testing.T) {
	env := newTestEnv(t)
	mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "query-error.bin", 3)

	const callbackName = "test:fail_invalidate_query"
	if err := env.db.Callback().Query().Before("gorm:query").Register(
		callbackName,
		func(tx *gorm.DB) {
			if tx.Statement.Schema == nil ||
				tx.Statement.Schema.Table != (model.UploadSession{}).TableName() {
				return
			}
			_ = tx.AddError(errors.New("forced invalidation query failure"))
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.db.Callback().Query().Remove(callbackName); err != nil {
			t.Logf("remove test callback: %v", err)
		}
	})

	err := env.service.InvalidateUnfinishedUploads(context.Background(), 10)
	if err == nil || !strings.Contains(err.Error(), "query unfinished upload sessions") {
		t.Fatalf("query failure = %v, want a wrapped query error", err)
	}
}

// TestInvalidateUnfinishedUploadsReturnsUpdateError 覆盖批量更新失败：
// 事务回滚，游标不推进，错误返回给启动流程；去掉故障后可以重试成功。
func TestInvalidateUnfinishedUploadsReturnsUpdateError(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	first := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "update-error-a.bin", 3)
	second := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "update-error-b.bin", 3)
	ids := []string{first.ID, second.ID}

	const callbackName = "test:fail_invalidate_update"
	failUpdate := true
	if err := env.db.Callback().Update().Before("gorm:update").Register(
		callbackName,
		func(tx *gorm.DB) {
			if !failUpdate || tx.Statement.Schema == nil ||
				tx.Statement.Schema.Table != (model.UploadSession{}).TableName() {
				return
			}
			_ = tx.AddError(errors.New("forced invalidation update failure"))
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.db.Callback().Update().Remove(callbackName); err != nil {
			t.Logf("remove test callback: %v", err)
		}
	})

	err := env.service.InvalidateUnfinishedUploads(ctx, 2)
	if err == nil || !strings.Contains(err.Error(), "mark unfinished upload sessions expired") {
		t.Fatalf("update failure = %v, want a wrapped update error", err)
	}

	// 整批事务回滚：不能出现“前几条已 expired、后面的没处理”的部分结果。
	for _, id := range ids {
		if got := loadSessionOnly(t, env, id); got.Status != model.UploadStatusPending {
			t.Fatalf("session %s status after failed recovery = %q, want pending", id, got.Status)
		}
	}

	// 故障移除后重试成功，说明失败没有让游标或状态提前推进。
	failUpdate = false
	if err := env.service.InvalidateUnfinishedUploads(ctx, 2); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if got := loadSessionOnly(t, env, id); got.Status != model.UploadStatusExpired {
			t.Fatalf("session %s status after retry = %q, want expired", id, got.Status)
		}
	}
}

// TestUploadSessionCreatedAfterRecoveryWorks 覆盖重启后的新上传：
// 恢复只处理启动时已经存在的会话，之后创建的会话仍然可以上传和发布。
func TestUploadSessionCreatedAfterRecoveryWorks(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	staleInfo := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "before-restart.bin", 5)
	if err := env.service.InvalidateUnfinishedUploads(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if got := loadSessionOnly(t, env, staleInfo.ID); got.Status != model.UploadStatusExpired {
		t.Fatalf("stale session status = %q, want expired", got.Status)
	}

	freshInfo := mustCreateUploadContentSession(t, env.service, env.ownerA.ID, "after-restart.bin", 5)
	if !freshInfo.ExpiresAt.After(time.Now()) {
		t.Fatalf("fresh session has a past expires_at: %+v", freshInfo)
	}

	uploaded, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, freshInfo.ID, strings.NewReader("hello"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if uploaded.Status != model.UploadStatusReady {
		t.Fatalf("fresh session status = %q, want ready", uploaded.Status)
	}

	completed, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, freshInfo.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.ResultFileID == nil {
		t.Fatalf("fresh session completed without result file: %+v", completed)
	}
	mustOpenContent(t, env.service, env.ownerA.ID, *completed.ResultFileID, "hello")

	// 旧会话仍然是 expired，不会被新上传“救活”。
	if got := loadSessionOnly(t, env, staleInfo.ID); got.Status != model.UploadStatusExpired {
		t.Fatalf("stale session status changed to %q", got.Status)
	}
}
