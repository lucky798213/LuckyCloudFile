package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/model"
)

func cancelUploadRequest(t *testing.T, sessionID, token string) *http.Request {
	t.Helper()

	req := httptest.NewRequest(
		http.MethodDelete,
		"/api/upload-sessions/"+sessionID,
		nil,
	)
	if token != "" {
		req = authorize(req, token)
	}
	return req
}

func mustReadyFakeSession(t *testing.T, fake *fakeFileService, name, content string) file.UploadSessionInfo {
	t.Helper()

	created, err := fake.CreateUploadSession(
		context.Background(), testUserAID, "dir-a", name, int64(len(content)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fake.UploadSessionContent(
		context.Background(), testUserAID, created.ID, strings.NewReader(content),
	); err != nil {
		t.Fatal(err)
	}
	return created
}

// TestUploadSessionCancelHTTP 验证取消接口：202 + canceled，重复取消幂等，
// 取消后不能完成；别人和不存在都是 404，已完成返回 409。
func TestUploadSessionCancelHTTP(t *testing.T) {
	router, fake := newTestRouter(t)
	addFakeDirectory(fake, "dir-a", testUserAID)
	created := mustReadyFakeSession(t, fake, "hello.bin", "hello")

	first := httptest.NewRecorder()
	router.ServeHTTP(first, cancelUploadRequest(t, created.ID, testTokenA))
	if first.Code != http.StatusAccepted {
		t.Fatalf("cancel: wanted 202, got %d: %s", first.Code, first.Body.String())
	}
	var info file.UploadSessionInfo
	if err := json.Unmarshal(first.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.ID != created.ID || info.Status != model.UploadStatusCanceled {
		t.Fatalf("canceled info = %+v", info)
	}
	if got := fake.sessions[created.ID].content; len(got) != 0 {
		t.Fatalf("canceled session kept content: %q", got)
	}

	// 重复取消仍然 202。
	again := httptest.NewRecorder()
	router.ServeHTTP(again, cancelUploadRequest(t, created.ID, testTokenA))
	if again.Code != http.StatusAccepted {
		t.Fatalf("repeat cancel: wanted 202, got %d: %s", again.Code, again.Body.String())
	}

	// 取消后确认完成必须被拒绝。
	complete := httptest.NewRecorder()
	router.ServeHTTP(complete, completeUploadRequest(t, created.ID, testTokenA))
	if complete.Code != http.StatusConflict {
		t.Fatalf("complete after cancel: wanted 409, got %d: %s", complete.Code, complete.Body.String())
	}
	var body ErrorResponse
	if err := json.Unmarshal(complete.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Code != "upload_session_not_completable" {
		t.Fatalf("complete after cancel code = %q", body.Code)
	}

	// 越权和不存在的会话统一 404。
	for _, check := range []struct {
		name  string
		token string
		id    string
	}{
		{"foreign", testTokenB, created.ID},
		{"missing", testTokenA, "does-not-exist"},
	} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, cancelUploadRequest(t, check.id, check.token))
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("%s: wanted 404, got %d: %s", check.name, recorder.Code, recorder.Body.String())
		}
	}

	// 未登录。
	unauth := httptest.NewRecorder()
	router.ServeHTTP(unauth, cancelUploadRequest(t, created.ID, ""))
	if unauth.Code != http.StatusUnauthorized {
		t.Fatalf("not logged in: wanted 401, got %d: %s", unauth.Code, unauth.Body.String())
	}
}

// TestUploadSessionCancelCompletedHTTP 验证已完成会话不能取消。
func TestUploadSessionCancelCompletedHTTP(t *testing.T) {
	router, fake := newTestRouter(t)
	addFakeDirectory(fake, "dir-a", testUserAID)
	created := mustReadyFakeSession(t, fake, "done.bin", "hello")

	completed, err := fake.CompleteUploadSession(context.Background(), testUserAID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	fileID := *completed.ResultFileID

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, cancelUploadRequest(t, created.ID, testTokenA))
	if recorder.Code != http.StatusConflict {
		t.Fatalf("cancel completed: wanted 409, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var body ErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Code != "upload_session_completed" {
		t.Fatalf("code = %q, want upload_session_completed", body.Code)
	}

	if _, ok := fake.records[fileID]; !ok {
		t.Fatalf("completed file %s was removed", fileID)
	}
	if data, ok := fake.stored(fileID); !ok || string(data) != "hello" {
		t.Fatalf("completed content = %q, ok=%v", data, ok)
	}
}
