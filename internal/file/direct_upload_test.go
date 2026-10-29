package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
	"luckyCloudFile/internal/task"
)

type multipartFixture struct {
	*storage.Local
	lostResponse bool
}

func (m *multipartFixture) Initiate(context.Context, string) (string, error) {
	return "cloud-upload", nil
}
func (m *multipartFixture) SignPart(context.Context, string, string, int, time.Duration) (storage.SignedPart, error) {
	return storage.SignedPart{}, nil
}
func (m *multipartFixture) ListParts(context.Context, string, string) ([]storage.UploadedPart, error) {
	return []storage.UploadedPart{{Number: 1, Size: 5, ETag: "etag"}}, nil
}
func (m *multipartFixture) Complete(ctx context.Context, key, id string, _ []storage.UploadedPart) error {
	_, e := m.Save(ctx, key, strings.NewReader("hello"), 5)
	if e != nil {
		return e
	}
	if m.lostResponse {
		m.lostResponse = false
		return errors.New("merge response lost")
	}
	return nil
}
func (m *multipartFixture) Abort(context.Context, string, string) error { return nil }

// One integration scenario covers persistent restart, an ambiguous merge,
// verify-before-publish, dedup accounting, ownership and idempotent cancellation.
func TestDirectUploadRecoveryAndPublication(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	env.db.Where("type = ?", DirectVerifyTask).Delete(&model.Task{})
	env.db.Exec("DELETE FROM direct_upload_sessions")
	defer env.db.Where("type = ?", DirectVerifyTask).Delete(&model.Task{})
	defer env.db.Exec("DELETE FROM direct_upload_sessions")
	registry := storage.NewRegistry()
	cloud := &multipartFixture{Local: storage.NewLocal(t.TempDir()), lostResponse: true}
	if e := registry.Register(env.policyA.ID, cloud); e != nil {
		t.Fatal(e)
	}
	svc := NewService(env.db, registry)
	hash := sha256.Sum256([]byte("hello"))
	digest := hex.EncodeToString(hash[:])
	first, e := svc.CreateDirectUpload(ctx, env.ownerA.ID, "", "one.txt", 5, digest, "request-one")
	if e != nil {
		t.Fatal(e)
	}
	again, e := svc.CreateDirectUpload(ctx, env.ownerA.ID, "", "one.txt", 5, digest, "request-one")
	if e != nil || again.ID != first.ID {
		t.Fatalf("idempotency: %+v %v", again, e)
	}
	if _, e = svc.GetDirectUpload(ctx, env.ownerB.ID, first.ID); !errors.Is(e, ErrNotFound) {
		t.Fatalf("ownership: %v", e)
	}
	if _, e = svc.CompleteDirectUpload(ctx, env.ownerA.ID, first.ID); e == nil {
		t.Fatal("expected response loss")
	}
	if e = env.db.Model(&model.DirectUploadSession{}).Where("id = ?", first.ID).Update("operation_until", time.Now().Add(-time.Minute)).Error; e != nil {
		t.Fatal(e)
	}
	svc = NewService(env.db, registry) // New process-local controllers, same durable state.
	if e = svc.ReconcileDirectUploads(ctx, 100); e != nil {
		t.Fatal(e)
	}
	tasks := task.NewService(env.db)
	verify := func() {
		t.Helper()
		claims, e := tasks.Claim(ctx, "direct-test", 1, time.Minute, []string{DirectVerifyTask})
		if e != nil || len(claims) != 1 {
			t.Fatalf("claim: %+v %v", claims, e)
		}
		runner, e := svc.DirectVerifyFactory(tasks).Rebuild(claims[0].Record)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = runner.Run(ctx, claims[0].Record, nil); e != nil {
			t.Fatal(e)
		}
	}
	state, e := svc.GetDirectUpload(ctx, env.ownerA.ID, first.ID)
	if e != nil || state.Status != "verifying" {
		t.Fatalf("recovery: %+v %v", state, e)
	}
	var unpublished int64
	env.db.Model(&model.File{}).Where("owner_id = ? AND name = ?", env.ownerA.ID, "one.txt").Count(&unpublished)
	if unpublished != 0 {
		t.Fatal("published before verification")
	}
	verify()
	state, e = svc.GetDirectUpload(ctx, env.ownerA.ID, first.ID)
	if e != nil || state.Status != "completed" {
		t.Fatalf("publication: %+v %v", state, e)
	}
	second, e := svc.CreateDirectUpload(ctx, env.ownerA.ID, "", "two.txt", 5, digest, "request-two")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = svc.CompleteDirectUpload(ctx, env.ownerA.ID, second.ID); e != nil {
		t.Fatal(e)
	}
	verify()
	var files []model.File
	if e = env.db.Where("owner_id = ? AND kind = ?", env.ownerA.ID, model.KindFile).Find(&files).Error; e != nil {
		t.Fatal(e)
	}
	if len(files) != 2 || *files[0].EntityID != *files[1].EntityID {
		t.Fatalf("dedup: %+v", files)
	}
	third, e := svc.CreateDirectUpload(ctx, env.ownerA.ID, "", "cancel.txt", 5, digest, "request-three")
	if e != nil {
		t.Fatal(e)
	}
	for range 2 {
		if _, e = svc.CancelDirectUpload(ctx, env.ownerA.ID, third.ID); e != nil {
			t.Fatal(e)
		}
	}
	var user model.User
	if e = env.db.First(&user, env.ownerA.ID).Error; e != nil {
		t.Fatal(e)
	}
	if user.ReservedBytes != 0 || user.UsedBytes != 10 {
		t.Fatalf("accounting: reserved=%d used=%d", user.ReservedBytes, user.UsedBytes)
	}
}
