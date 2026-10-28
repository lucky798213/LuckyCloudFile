package file

import (
	"context"
	"errors"
	"strings"
	"testing"

	"luckyCloudFile/internal/model"
)

func TestInstantUploadHitsExistingContentAndIsIdempotent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	first, err := env.service.Upload(ctx, env.ownerA.ID, "", "a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.Upload(ctx, env.ownerA.ID, "", "other.txt", strings.NewReader("world")); err != nil {
		t.Fatal(err)
	}

	result, err := env.service.InstantUpload(
		ctx, env.ownerA.ID, "", "b.txt", 5, digestOf("hello"), "key-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Instant || result.FileID == "" {
		t.Fatalf("result = %+v, want a hit with file ID", result)
	}

	var hit model.File
	if err := env.db.First(&hit, "id = ?", result.FileID).Error; err != nil {
		t.Fatal(err)
	}
	var source model.File
	if err := env.db.First(&source, "id = ?", first.ID).Error; err != nil {
		t.Fatal(err)
	}
	if *hit.EntityID != *source.EntityID {
		t.Fatalf("instant file entity = %d, want %d", *hit.EntityID, *source.EntityID)
	}

	var entity model.Entity
	if err := env.db.First(&entity, *source.EntityID).Error; err != nil {
		t.Fatal(err)
	}
	if entity.RefCount != 2 {
		t.Fatalf("ref_count = %d, want 2", entity.RefCount)
	}

	// 同键同参数重试必须返回同一个文件，不重复建文件或加引用。
	replay, err := env.service.InstantUpload(
		ctx, env.ownerA.ID, "", "b.txt", 5, digestOf("hello"), "key-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if replay.FileID != result.FileID {
		t.Fatalf("replay file = %q, want %q", replay.FileID, result.FileID)
	}
	if err := env.db.First(&entity, entity.ID).Error; err != nil {
		t.Fatal(err)
	}
	if entity.RefCount != 2 {
		t.Fatalf("ref_count after replay = %d, want 2", entity.RefCount)
	}

	// 同键不同参数要拒绝，不能悄悄返回上次的文件。
	_, err = env.service.InstantUpload(
		ctx, env.ownerA.ID, "", "c.txt", 5, digestOf("hello"), "key-1",
	)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("error = %v, want ErrIdempotencyConflict", err)
	}

	var user model.User
	if err := env.db.First(&user, env.ownerA.ID).Error; err != nil {
		t.Fatal(err)
	}
	// a.txt + other.txt + b.txt，每份逻辑文件都计费。
	wantUsed := int64(len("hello") + len("world") + len("hello"))
	if user.UsedBytes != wantUsed {
		t.Fatalf("used_bytes = %d, want %d", user.UsedBytes, wantUsed)
	}
}

func TestInstantUploadMissIsScopedAndRecorded(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	result, err := env.service.InstantUpload(
		ctx, env.ownerA.ID, "", "missing.txt", 5, digestOf("absent"), "miss-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Instant || result.FileID != "" {
		t.Fatalf("result = %+v, want miss", result)
	}

	// 同键同参数重试仍返回未命中，即使此时另一个用户已经有同样的内容。
	if _, err := env.service.Upload(ctx, env.ownerB.ID, "", "owner-b.txt", strings.NewReader("absent")); err != nil {
		t.Fatal(err)
	}
	replay, err := env.service.InstantUpload(
		ctx, env.ownerA.ID, "", "missing.txt", 5, digestOf("absent"), "miss-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Instant || replay.FileID != "" {
		t.Fatalf("replay = %+v, want recorded miss", replay)
	}

	// 不跨用户：B 的内容不能成为 A 的秒传命中。
	cross, err := env.service.InstantUpload(
		ctx, env.ownerA.ID, "", "cross.txt", 5, digestOf("absent"), "miss-2",
	)
	if err != nil {
		t.Fatal(err)
	}
	if cross.Instant {
		t.Fatal("instant upload must not reuse another user's content")
	}

	var files int64
	if err := env.db.Model(&model.File{}).
		Where("owner_id = ? AND kind = ?", env.ownerA.ID, model.KindFile).
		Count(&files).Error; err != nil {
		t.Fatal(err)
	}
	if files != 0 {
		t.Fatalf("owner A files = %d, want 0", files)
	}
}

func TestInstantUploadConsumesLogicalQuota(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	if _, err := env.service.Upload(ctx, env.ownerA.ID, "", "a.txt", strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}
	if err := env.db.Model(&model.User{}).
		Where("id = ?", env.ownerA.ID).
		UpdateColumn("quota_bytes", 5).Error; err != nil {
		t.Fatal(err)
	}

	_, err := env.service.InstantUpload(
		ctx, env.ownerA.ID, "", "b.txt", 5, digestOf("hello"), "quota-1",
	)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("error = %v, want ErrQuotaExceeded", err)
	}

	// 失败的事务不能留下幂等结果，补足容量后同一个键仍然可以正常命中。
	if err := env.db.Model(&model.User{}).
		Where("id = ?", env.ownerA.ID).
		UpdateColumn("quota_bytes", 10).Error; err != nil {
		t.Fatal(err)
	}
	result, err := env.service.InstantUpload(
		ctx, env.ownerA.ID, "", "b.txt", 5, digestOf("hello"), "quota-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Instant {
		t.Fatal("instant upload should hit after quota is restored")
	}
}

func TestInstantUploadRejectsInvalidInput(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	if _, err := env.service.InstantUpload(
		ctx, env.ownerA.ID, "", "a.txt", 5, "not-a-digest", "key",
	); !errors.Is(err, ErrInvalidContentSHA256) {
		t.Fatalf("error = %v, want ErrInvalidContentSHA256", err)
	}
	if _, err := env.service.InstantUpload(
		ctx, env.ownerA.ID, "", "a.txt", 5, digestOf("hello"), "",
	); !errors.Is(err, ErrInvalidIdempotencyKey) {
		t.Fatalf("error = %v, want ErrInvalidIdempotencyKey", err)
	}
	if _, err := env.service.InstantUpload(
		ctx, env.ownerA.ID, "", "a.txt", MaxUploadBytes+1, digestOf("hello"), "key",
	); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("error = %v, want ErrTooLarge", err)
	}
}
