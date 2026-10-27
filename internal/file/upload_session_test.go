package file

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
)

// TestCreateUploadSessionPersistsPlaceholder 验证正常创建：
// 一条 pending 会话 + 一条 uploading/0 占位 Entity，没有 File，也没有磁盘内容。
func TestCreateUploadSessionPersistsPlaceholder(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const expectedSize = int64(10 << 20)
	info, err := env.service.CreateUploadSession(ctx, env.ownerA.ID, "", "照片.jpg", expectedSize)
	if err != nil {
		t.Fatal(err)
	}

	if info.ID == "" || info.Name != "照片.jpg" {
		t.Fatalf("unexpected session info: %+v", info)
	}
	// 不传 parent_id 时必须保存解析后的真实根目录 ID。
	if info.ParentID != *env.ownerA.RootFileID {
		t.Fatalf("parent_id = %q, want root %q", info.ParentID, *env.ownerA.RootFileID)
	}
	if info.ExpectedSize != expectedSize || info.ActualSize != 0 {
		t.Fatalf("unexpected sizes: %+v", info)
	}
	if info.Status != model.UploadStatusPending {
		t.Fatalf("status = %q, want %q", info.Status, model.UploadStatusPending)
	}
	if info.ResultFileID != nil {
		t.Fatalf("new session already has a result file: %q", *info.ResultFileID)
	}
	if !info.ExpiresAt.After(time.Now()) ||
		info.ExpiresAt.After(time.Now().Add(UploadSessionTTL+time.Minute)) {
		t.Fatalf("unexpected expiry: %v", info.ExpiresAt)
	}

	var session model.UploadSession
	if err := env.db.WithContext(ctx).First(&session, "id = ?", info.ID).Error; err != nil {
		t.Fatal(err)
	}
	if session.OwnerID != env.ownerA.ID ||
		session.ParentID != *env.ownerA.RootFileID ||
		session.Name != "照片.jpg" ||
		session.ExpectedSize != expectedSize ||
		session.ActualSize != 0 ||
		session.Status != model.UploadStatusPending ||
		session.ResultFileID != nil {
		t.Fatalf("stored session = %+v", session)
	}

	var entity model.Entity
	if err := env.db.WithContext(ctx).First(&entity, session.EntityID).Error; err != nil {
		t.Fatal(err)
	}
	if entity.Status != model.EntityStatusUploading {
		t.Fatalf("placeholder status = %q, want %q", entity.Status, model.EntityStatusUploading)
	}
	if entity.RefCount != 0 {
		t.Fatalf("placeholder ref_count = %d, want 0", entity.RefCount)
	}
	if entity.Size != 0 {
		t.Fatalf("placeholder size = %d, want 0", entity.Size)
	}
	if entity.StoragePolicyID != env.policyA.ID {
		t.Fatalf("placeholder policy = %d, want %d", entity.StoragePolicyID, env.policyA.ID)
	}
	if entity.StorageKey == "" || entity.StorageKey == info.ID {
		t.Fatalf("unexpected placeholder storage key %q", entity.StorageKey)
	}

	// 占位内容不能被任何正式 File 引用。
	var references int64
	if err := env.db.WithContext(ctx).
		Model(&model.File{}).
		Where("entity_id = ?", entity.ID).
		Count(&references).Error; err != nil {
		t.Fatal(err)
	}
	if references != 0 {
		t.Fatalf("placeholder entity is referenced by %d file(s)", references)
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("creating a session created %d file(s)", got)
	}
	if entries := diskEntries(t, env.rootA); len(entries) != 0 {
		t.Fatalf("creating a session wrote content to disk: %v", entries)
	}
}

// TestCreateUploadSessionAllowsEmptyFile 单独确认 size=0 是合法请求。
func TestCreateUploadSessionAllowsEmptyFile(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	info, err := env.service.CreateUploadSession(ctx, env.ownerA.ID, "", "empty.txt", 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.ExpectedSize != 0 {
		t.Fatalf("expected_size = %d, want 0", info.ExpectedSize)
	}
	if got := countRows(t, env.db, &model.UploadSession{}); got != 1 {
		t.Fatalf("wanted 1 session, got %d", got)
	}
	if got := countRows(t, env.db, &model.Entity{}); got != 1 {
		t.Fatalf("wanted 1 placeholder entity, got %d", got)
	}
}

// TestCreateUploadSessionReservesTargetName 验证三点：
//   - 会话保存显式传入的目录 ID；
//   - 创建会话时占住最终目标，同名会话不能同时存在；
//   - 前一个会话取消后目标锁释放，同名会话可以重新创建。
func TestCreateUploadSessionReservesTargetName(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	dir, err := env.service.CreateDirectory(ctx, env.ownerA.ID, "", "docs")
	if err != nil {
		t.Fatal(err)
	}

	first, err := env.service.CreateUploadSession(ctx, env.ownerA.ID, dir.ID, "same.bin", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.CreateUploadSession(
		ctx, env.ownerA.ID, dir.ID, "same.bin", 2,
	); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("second session with the same name error = %v, want ErrUploadSessionConflict", err)
	}
	if got := countRows(t, env.db, &model.UploadSession{}); got != 1 {
		t.Fatalf("wanted 1 session, got %d", got)
	}
	if got := countRows(t, env.db, &model.Entity{}); got != 1 {
		t.Fatalf("wanted 1 placeholder entity, got %d", got)
	}

	// 取消先结束会话生命周期并释放目标锁，同名会话才能重新开始。
	if _, err := env.service.CancelUploadSession(ctx, env.ownerA.ID, first.ID); err != nil {
		t.Fatal(err)
	}
	second, err := env.service.CreateUploadSession(ctx, env.ownerA.ID, dir.ID, "same.bin", 2)
	if err != nil {
		t.Fatalf("creating a session after cancel must succeed: %v", err)
	}
	if first.ParentID != dir.ID || second.ParentID != dir.ID {
		t.Fatalf("session parents = %q / %q, want %q", first.ParentID, second.ParentID, dir.ID)
	}
	if first.ID == second.ID {
		t.Fatal("two sessions got the same ID")
	}
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("sessions created %d formal file(s)", got)
	}
}

// TestCreateUploadSessionRejectsInvalidInput 覆盖参数错误：
// 所有失败都不能留下 Session 或占位 Entity。
func TestCreateUploadSessionRejectsInvalidInput(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	baselineSessions := countRows(t, env.db, &model.UploadSession{})
	baselineEntities := countRows(t, env.db, &model.Entity{})

	invalid := []struct {
		name     string
		parentID string
		fileName string
		size     int64
		wantErr  error
	}{
		{"negative size", "", "x.bin", -1, ErrInvalidSize},
		{"too large", "", "x.bin", MaxUploadBytes + 1, ErrTooLarge},
		{"empty name", "", "", 0, ErrInvalidName},
		{"dot name", "", ".", 0, ErrInvalidName},
		{"separator in name", "", "a/b", 0, ErrInvalidName},
		{"foreign parent", *env.ownerB.RootFileID, "x.bin", 0, ErrNotFound},
		{"missing parent", "missing-directory", "x.bin", 0, ErrNotFound},
	}

	for _, test := range invalid {
		test := test
		t.Run(test.name, func(t *testing.T) {
			_, err := env.service.CreateUploadSession(
				ctx, env.ownerA.ID, test.parentID, test.fileName, test.size,
			)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			if got := countRows(t, env.db, &model.UploadSession{}); got != baselineSessions {
				t.Fatalf("invalid create left %d session(s)", got-baselineSessions)
			}
			if got := countRows(t, env.db, &model.Entity{}); got != baselineEntities {
				t.Fatalf("invalid create left %d entity/entities", got-baselineEntities)
			}
		})
	}

	// 父节点存在但它是普通文件：必须返回 ErrNotDirectory。
	fileRecord, err := env.service.Upload(ctx, env.ownerA.ID, "", "node.txt", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	baselineSessions = countRows(t, env.db, &model.UploadSession{})
	baselineEntities = countRows(t, env.db, &model.Entity{})

	if _, err := env.service.CreateUploadSession(
		ctx, env.ownerA.ID, fileRecord.ID, "x.bin", 0,
	); !errors.Is(err, ErrNotDirectory) {
		t.Fatalf("file as parent: error = %v, want ErrNotDirectory", err)
	}
	if got := countRows(t, env.db, &model.UploadSession{}); got != baselineSessions {
		t.Fatalf("file-as-parent left %d session(s)", got-baselineSessions)
	}
	if got := countRows(t, env.db, &model.Entity{}); got != baselineEntities {
		t.Fatalf("file-as-parent left %d entity/entities", got-baselineEntities)
	}
}

// TestCreateUploadSessionRollsBackPlaceholderOnSessionFailure 用 GORM 回调强制
// UploadSession 插入失败，确认同一个事务里的占位 Entity 也一起回滚。
//
// 不建触发器、不改表名：回调只挂在这个测试自己的 *gorm.DB 上，
// 不会影响并行运行的其他测试包共用同一个测试库。
func TestCreateUploadSessionRollsBackPlaceholderOnSessionFailure(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	const callbackName = "test:fail_upload_session_insert"
	forcedErr := errors.New("forced upload session insert failure")
	if err := env.db.Callback().Create().Before("gorm:create").Register(
		callbackName,
		func(tx *gorm.DB) {
			if tx.Statement.Schema != nil &&
				tx.Statement.Schema.Table == (model.UploadSession{}).TableName() {
				_ = tx.AddError(forcedErr)
			}
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.db.Callback().Create().Remove(callbackName); err != nil {
			t.Logf("remove test callback: %v", err)
		}
	})

	_, err := env.service.CreateUploadSession(ctx, env.ownerA.ID, "", "fail.bin", 10)
	if !errors.Is(err, forcedErr) {
		t.Fatalf("error = %v, want forced session insert failure", err)
	}
	if got := countRows(t, env.db, &model.UploadSession{}); got != 0 {
		t.Fatalf("failed create left %d session(s)", got)
	}
	if got := countRows(t, env.db, &model.Entity{}); got != 0 {
		t.Fatalf("failed create left %d placeholder entity/entities", got)
	}
	if entries := diskEntries(t, env.rootA); len(entries) != 0 {
		t.Fatalf("failed create wrote content to disk: %v", entries)
	}
	// 容量预留也是同一个事务的一部分：会话插入失败时不能留下预留。
	if got := userAccounting(t, env.db, env.ownerA.ID); got.ReservedBytes != 0 {
		t.Fatalf("failed create left %d reserved bytes", got.ReservedBytes)
	}
}

// TestGetUploadSessionOwnerIsolation 验证查询必须同时匹配 owner ID，
// 并且查询本身不会自动把过期会话改成 expired。
func TestGetUploadSessionOwnerIsolation(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	created, err := env.service.CreateUploadSession(ctx, env.ownerA.ID, "", "mine.bin", 3)
	if err != nil {
		t.Fatal(err)
	}

	got, err := env.service.GetUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != created.ID || got.ParentID != created.ParentID ||
		got.Name != created.Name || got.ExpectedSize != created.ExpectedSize ||
		got.ActualSize != created.ActualSize || got.Status != created.Status {
		t.Fatalf("fetched session = %+v, want %+v", got, created)
	}

	// 别人的会话和不存在的会话必须返回同一个错误。
	if _, err := env.service.GetUploadSession(
		ctx, env.ownerB.ID, created.ID,
	); !errors.Is(err, ErrUploadSessionNotFound) {
		t.Fatalf("foreign session: error = %v, want ErrUploadSessionNotFound", err)
	}
	if _, err := env.service.GetUploadSession(
		ctx, env.ownerA.ID, "does-not-exist",
	); !errors.Is(err, ErrUploadSessionNotFound) {
		t.Fatalf("missing session: error = %v, want ErrUploadSessionNotFound", err)
	}

	// 查询只读：即使已经过期，也不会自动修改状态或清理占位实体。
	if err := env.db.Model(&model.UploadSession{}).
		Where("id = ?", created.ID).
		UpdateColumn("expires_at", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatal(err)
	}
	expired, err := env.service.GetUploadSession(ctx, env.ownerA.ID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if expired.Status != model.UploadStatusPending || !expired.ExpiresAt.Before(time.Now()) {
		t.Fatalf("query changed an expired session: %+v", expired)
	}
	if got := countRows(t, env.db, &model.Entity{}); got != 1 {
		t.Fatalf("query removed the placeholder entity: %d left", got)
	}
}
