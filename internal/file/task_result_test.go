package file

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
)

// TestTaskUploadSessionRecordsResultAtomically 验证 task_results 和文件发布
// 在同一个事务里提交：重复发布会被唯一键拒绝，文件数量和容量都不增加。
func TestTaskUploadSessionRecordsResultAtomically(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	content := []byte("zip content")

	completed := completeTaskSession(
		t, env.service, env.ownerA.ID, "bundle.zip", "task-1", content,
	)
	if completed.ResultFileID == nil || *completed.ResultFileID == "" {
		t.Fatal("task session completed without result file")
	}
	resultID := *completed.ResultFileID

	found, err := env.service.FindTaskResult(ctx, env.ownerA.ID, "task-1")
	if err != nil {
		t.Fatal(err)
	}
	if found != resultID {
		t.Fatalf("task result = %q, want %q", found, resultID)
	}
	if _, err := env.service.FindTaskResult(
		ctx, env.ownerB.ID, "task-1",
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other owner result error = %v, want ErrNotFound", err)
	}

	filesBefore := countRows(t, env.db, &model.File{})
	userBefore := loadUser(t, env.db, env.ownerA.ID)

	// 同一个任务再发布一份：唯一键让整笔事务回滚，不能新增文件或容量。
	duplicate, err := env.service.CreateTaskUploadSession(
		ctx, env.ownerA.ID, "", "second.zip", int64(len(content)), "task-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.UploadSessionContent(
		ctx, env.ownerA.ID, duplicate.ID, bytes.NewReader(content),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := env.service.CompleteUploadSession(
		ctx, env.ownerA.ID, duplicate.ID,
	); !errors.Is(err, ErrTaskResultExists) {
		t.Fatalf("duplicate completion error = %v, want ErrTaskResultExists", err)
	}

	if got := countRows(t, env.db, &model.File{}); got != filesBefore {
		t.Fatalf("files = %d after rejected duplicate, want %d", got, filesBefore)
	}
	userAfter := loadUser(t, env.db, env.ownerA.ID)
	if userAfter.UsedBytes != userBefore.UsedBytes {
		t.Fatalf("used bytes = %d after rejected duplicate, want %d",
			userAfter.UsedBytes, userBefore.UsedBytes)
	}
	stillReady, err := env.service.GetUploadSession(ctx, env.ownerA.ID, duplicate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stillReady.Status != model.UploadStatusReady {
		t.Fatalf("duplicate session status = %q, want ready", stillReady.Status)
	}

	// 取消多余会话，释放它的预留，验证没有多扣或多留。
	if _, err := env.service.CancelUploadSession(
		ctx, env.ownerA.ID, duplicate.ID,
	); err != nil {
		t.Fatal(err)
	}
	userCanceled := loadUser(t, env.db, env.ownerA.ID)
	if userCanceled.ReservedBytes != 0 {
		t.Fatalf("reserved bytes = %d after cancel, want 0", userCanceled.ReservedBytes)
	}

	// 用户删除结果文件后，凭据仍在：恢复任务只能复用，不能重新发布。
	if err := env.service.Delete(ctx, env.ownerA.ID, resultID); err != nil {
		t.Fatal(err)
	}
	found, err = env.service.FindTaskResult(ctx, env.ownerA.ID, "task-1")
	if err != nil {
		t.Fatal(err)
	}
	if found != resultID {
		t.Fatalf("task result after file deletion = %q, want %q", found, resultID)
	}
}

func completeTaskSession(
	t *testing.T,
	service *Service,
	ownerID int,
	name, taskID string,
	content []byte,
) UploadSessionInfo {
	t.Helper()

	ctx := context.Background()
	session, err := service.CreateTaskUploadSession(
		ctx, ownerID, "", name, int64(len(content)), taskID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.UploadSessionContent(
		ctx, ownerID, session.ID, bytes.NewReader(content),
	); err != nil {
		t.Fatal(err)
	}
	completed, err := service.CompleteUploadSession(ctx, ownerID, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	return completed
}

func loadUser(t *testing.T, db *gorm.DB, id int) model.User {
	t.Helper()

	var user model.User
	if err := db.First(&user, id).Error; err != nil {
		t.Fatal(err)
	}
	return user
}
