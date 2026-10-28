package file

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"

	"luckyCloudFile/internal/model"
)

func digestOf(value string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}

func TestUploadReusesEntityForSameContent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	first, err := env.service.Upload(ctx, env.ownerA.ID, "", "a.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := env.service.Upload(ctx, env.ownerA.ID, "", "b.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}

	var files []model.File
	if err := env.db.Where("owner_id = ? AND kind = ?", env.ownerA.ID, model.KindFile).
		Order("name ASC").Find(&files).Error; err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("files = %d, want 2", len(files))
	}
	if files[0].EntityID == nil || files[1].EntityID == nil {
		t.Fatal("published files must have entities")
	}
	if *files[0].EntityID != *files[1].EntityID {
		t.Fatalf("same content used different entities: %d vs %d",
			*files[0].EntityID, *files[1].EntityID)
	}
	if first.ID == second.ID {
		t.Fatal("two logical files must have different IDs")
	}

	var entity model.Entity
	if err := env.db.First(&entity, *files[0].EntityID).Error; err != nil {
		t.Fatal(err)
	}
	if entity.RefCount != 2 {
		t.Fatalf("ref_count = %d, want 2", entity.RefCount)
	}
	if entity.ContentSHA256 == nil || *entity.ContentSHA256 != digestOf("hello") {
		t.Fatalf("content_sha256 = %v, want %s", entity.ContentSHA256, digestOf("hello"))
	}

	var keys []model.ContentKey
	if err := env.db.Where("owner_id = ?", env.ownerA.ID).Find(&keys).Error; err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0].EntityID != entity.ID {
		t.Fatalf("content keys = %+v, want one mapping to entity %d", keys, entity.ID)
	}

	// 第二次上传的物理对象应当在事务提交后立即被清掉。
	entries, err := os.ReadDir(env.rootA)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("stored objects = %d, want 1", len(entries))
	}

	var user model.User
	if err := env.db.First(&user, env.ownerA.ID).Error; err != nil {
		t.Fatal(err)
	}
	if user.UsedBytes != 10 {
		t.Fatalf("used_bytes = %d, want 10 (two logical files)", user.UsedBytes)
	}
}

func TestCompleteUploadSessionReusesEntity(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	complete := func(name string) UploadSessionInfo {
		t.Helper()
		session, err := env.service.CreateUploadSession(
			ctx, env.ownerA.ID, "", name, 5,
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := env.service.UploadSessionContent(
			ctx, env.ownerA.ID, session.ID, strings.NewReader("hello"),
		); err != nil {
			t.Fatal(err)
		}
		result, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, session.ID)
		if err != nil {
			t.Fatal(err)
		}
		if result.ResultFileID == nil {
			t.Fatal("completed session has no result file")
		}
		return result
	}

	complete("a.txt")
	complete("b.txt")

	var files []model.File
	if err := env.db.Where("owner_id = ? AND kind = ?", env.ownerA.ID, model.KindFile).
		Order("name ASC").Find(&files).Error; err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("files = %d, want 2", len(files))
	}
	if *files[0].EntityID != *files[1].EntityID {
		t.Fatalf("same content used different entities: %d vs %d",
			*files[0].EntityID, *files[1].EntityID)
	}

	var entities []model.Entity
	if err := env.db.Where("storage_policy_id = ?", env.policyA.ID).
		Find(&entities).Error; err != nil {
		t.Fatal(err)
	}
	if len(entities) != 1 {
		t.Fatalf("entities = %d, want 1 (duplicate placeholder should be cleaned)", len(entities))
	}
	if entities[0].RefCount != 2 {
		t.Fatalf("ref_count = %d, want 2", entities[0].RefCount)
	}
	if entities[0].Status != model.EntityStatusActive {
		t.Fatalf("status = %q, want active", entities[0].Status)
	}
	if entities[0].ContentSHA256 == nil || *entities[0].ContentSHA256 != digestOf("hello") {
		t.Fatalf("content_sha256 = %v, want %s", entities[0].ContentSHA256, digestOf("hello"))
	}
}

func TestChunkedUploadComputesWholeFileDigest(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	session, err := env.service.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, "", "chunked.txt", 5, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.UploadSessionChunk(
		ctx, env.ownerA.ID, session.ID, 0, strings.NewReader("hello"),
	); err != nil {
		t.Fatal(err)
	}
	result, err := env.service.CompleteUploadSession(ctx, env.ownerA.ID, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.ResultFileID == nil {
		t.Fatal("completed session has no result file")
	}

	var record model.File
	if err := env.db.First(&record, "id = ?", *result.ResultFileID).Error; err != nil {
		t.Fatal(err)
	}
	var entity model.Entity
	if err := env.db.First(&entity, *record.EntityID).Error; err != nil {
		t.Fatal(err)
	}
	if entity.ContentSHA256 == nil || *entity.ContentSHA256 != digestOf("hello") {
		t.Fatalf("content_sha256 = %v, want %s", entity.ContentSHA256, digestOf("hello"))
	}
}
