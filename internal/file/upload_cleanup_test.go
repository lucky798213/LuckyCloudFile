package file

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"testing"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
)

// TestResumeTerminatedSessionsPaginatesPastFailedHandoff 覆盖恢复扫描的游标分页：
// 第一条移交持续失败时，后面同一批里的正常残留仍然必须被处理。
//
// 用 batchSize=1 放大问题：没有分页时每次只取到第一条，故障条目会永远占住
// 这个位置，后面的会话一直轮不到。固定失败第一条 Entity 的移交更新，
// 验证一次 CleanupPending 就能越过它清理第二条，且下轮还能重试第一条。
func TestResumeTerminatedSessionsPaginatesPastFailedHandoff(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	type terminatedRecord struct {
		session model.UploadSession
		entity  model.Entity
	}
	records := make([]terminatedRecord, 0, 2)
	for _, name := range []string{"resume-page-a.bin", "resume-page-b.bin"} {
		_, session, entity := mustReadyUploadSession(t, env, env.ownerA.ID, name, "hello")
		records = append(records, terminatedRecord{session: session, entity: entity})
	}

	// 按扫描顺序排序，确保我们固定失败的确实是“第一条”。
	sort.Slice(records, func(i, j int) bool {
		return records[i].session.ID < records[j].session.ID
	})

	// 模拟到期扫描已经提交、进程在移交前崩溃：会话 expired，Entity 仍 uploading。
	for _, record := range records {
		if err := env.db.Model(&model.UploadSession{}).
			Where("id = ?", record.session.ID).
			UpdateColumn("status", model.UploadStatusExpired).Error; err != nil {
			t.Fatal(err)
		}
	}

	// 只让本轮第一次 Entity 移交更新失败，模拟一个持续故障的“第一条”。
	// 分页正确时，本轮仍会继续处理第二条；下一轮再把第一条重试成功。
	const callbackName = "test:fail_first_entity_handoff_update"
	failNext := true
	if err := env.db.Callback().Update().Before("gorm:update").Register(
		callbackName,
		func(tx *gorm.DB) {
			if !failNext || tx.Statement.Schema == nil ||
				tx.Statement.Schema.Table != (model.Entity{}).TableName() {
				return
			}
			failNext = false
			_ = tx.AddError(errors.New("forced handoff failure"))
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.db.Callback().Update().Remove(callbackName); err != nil {
			t.Logf("remove test callback: %v", err)
		}
	})

	if _, err := env.service.CleanupPending(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if failNext {
		t.Fatal("test callback did not force the first handoff to fail")
	}

	first, second := records[0], records[1]

	// 第二条不能被第一条的失败挡住：它应该已经完成清理。
	assertEntityGone(t, env, second.entity.ID)
	assertNoUploadContent(t, filepath.Join(env.rootA, second.entity.StorageKey))
	if got := loadSessionOnly(t, env, second.session.ID); got.Status != model.UploadStatusExpired {
		t.Fatalf("second session status = %q, want expired", got.Status)
	}

	// 第一条的移交失败了，必须保留 Entity 和内容等待下一轮，不能误删。
	firstEntity := mustEntity(t, env, first.entity.ID)
	if firstEntity.Status != model.EntityStatusUploading || firstEntity.RefCount != 0 {
		t.Fatalf("failed first entity = %+v, want uploading/0", firstEntity)
	}
	assertUploadContent(t, filepath.Join(env.rootA, first.entity.StorageKey), "hello")
	if got := loadSessionOnly(t, env, first.session.ID); got.Status != model.UploadStatusExpired {
		t.Fatalf("first session status = %q, want expired", got.Status)
	}

	// 下一轮从 id 开头重新扫描：第一条这次能成功移交并回收。
	failNext = false
	if _, err := env.service.CleanupPending(ctx, 1); err != nil {
		t.Fatal(err)
	}
	assertEntityGone(t, env, first.entity.ID)
	assertNoUploadContent(t, filepath.Join(env.rootA, first.entity.StorageKey))
	if got := countKind(t, env.db, model.KindFile); got != 0 {
		t.Fatalf("resume cleanup published %d file(s)", got)
	}
}
