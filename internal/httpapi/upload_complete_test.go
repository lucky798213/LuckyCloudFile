package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/model"
)

func completeUploadRequest(t *testing.T, sessionID, token string) *http.Request {
	t.Helper()

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/upload-sessions/"+sessionID+"/complete",
		nil,
	)
	if token != "" {
		req = authorize(req, token)
	}
	return req
}

// TestUploadSessionCompleteHTTP 验证 HTTP 层：首次和重复完成都是 200，
// 响应是 completed + result_file_id，重复调用不会新增正式文件。
func TestUploadSessionCompleteHTTP(t *testing.T) {
	router, fake := newTestRouter(t)
	addFakeDirectory(fake, "dir-a", testUserAID)

	created, err := fake.CreateUploadSession(
		context.Background(), testUserAID, "dir-a", "hello.bin", 5,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fake.UploadSessionContent(
		context.Background(), testUserAID, created.ID, strings.NewReader("hello"),
	); err != nil {
		t.Fatal(err)
	}

	first := httptest.NewRecorder()
	router.ServeHTTP(first, completeUploadRequest(t, created.ID, testTokenA))
	if first.Code != http.StatusOK {
		t.Fatalf("complete: wanted 200, got %d: %s", first.Code, first.Body.String())
	}
	var info file.UploadSessionInfo
	if err := json.Unmarshal(first.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.Status != model.UploadStatusCompleted {
		t.Fatalf("completed status = %q", info.Status)
	}
	if info.ResultFileID == nil || *info.ResultFileID == "" {
		t.Fatalf("completed response has no result_file_id: %s", first.Body.String())
	}
	fileID := *info.ResultFileID

	// 正式文件已经出现在目标目录，内容仍可下载。
	if record, ok := fake.records[fileID]; !ok || record.Name != "hello.bin" || record.Kind != model.KindFile {
		t.Fatalf("published file = %+v, ok=%v", record, ok)
	}
	if data, ok := fake.stored(fileID); !ok || string(data) != "hello" {
		t.Fatalf("published content = %q, ok=%v", data, ok)
	}

	// 重复完成：200、同一个文件 ID、不新增正式文件。
	again := httptest.NewRecorder()
	router.ServeHTTP(again, completeUploadRequest(t, created.ID, testTokenA))
	if again.Code != http.StatusOK {
		t.Fatalf("repeat complete: wanted 200, got %d: %s", again.Code, again.Body.String())
	}
	var repeated file.UploadSessionInfo
	if err := json.Unmarshal(again.Body.Bytes(), &repeated); err != nil {
		t.Fatal(err)
	}
	if repeated.ResultFileID == nil || *repeated.ResultFileID != fileID {
		t.Fatalf("repeat complete returned %+v, want %q", repeated, fileID)
	}
	files := 0
	for _, record := range fake.records {
		if record.Kind == model.KindFile {
			files++
		}
	}
	if files != 1 {
		t.Fatalf("after repeat complete there are %d files, want 1", files)
	}
}

// TestUploadSessionCompleteHTTPErrors 验证完成接口的错误映射：
// 未就绪/过期/越权/同名/目录失效/数据异常都不能发布文件。
func TestUploadSessionCompleteHTTPErrors(t *testing.T) {
	t.Run("not ready", func(t *testing.T) {
		router, fake := newTestRouter(t)
		addFakeDirectory(fake, "dir-a", testUserAID)
		created, err := fake.CreateUploadSession(context.Background(), testUserAID, "dir-a", "x.bin", 5)
		if err != nil {
			t.Fatal(err)
		}

		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, completeUploadRequest(t, created.ID, testTokenA))
		if recorder.Code != http.StatusConflict {
			t.Fatalf("wanted 409, got %d: %s", recorder.Code, recorder.Body.String())
		}
		var body ErrorResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Code != "upload_session_not_completable" {
			t.Fatalf("code = %q, want upload_session_not_completable", body.Code)
		}
	})

	t.Run("expired", func(t *testing.T) {
		router, fake := newTestRouter(t)
		addFakeDirectory(fake, "dir-a", testUserAID)
		created, err := fake.CreateUploadSession(context.Background(), testUserAID, "dir-a", "x.bin", 5)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fake.UploadSessionContent(context.Background(), testUserAID, created.ID, strings.NewReader("hello")); err != nil {
			t.Fatal(err)
		}
		session := fake.sessions[created.ID]
		session.info.ExpiresAt = time.Now().Add(-time.Hour)
		fake.sessions[created.ID] = session

		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, completeUploadRequest(t, created.ID, testTokenA))
		if recorder.Code != http.StatusGone {
			t.Fatalf("wanted 410, got %d: %s", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("foreign and missing", func(t *testing.T) {
		router, fake := newTestRouter(t)
		addFakeDirectory(fake, "dir-a", testUserAID)
		created, err := fake.CreateUploadSession(context.Background(), testUserAID, "dir-a", "x.bin", 5)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fake.UploadSessionContent(context.Background(), testUserAID, created.ID, strings.NewReader("hello")); err != nil {
			t.Fatal(err)
		}

		for _, check := range []struct {
			name  string
			token string
			id    string
		}{
			{"foreign", testTokenB, created.ID},
			{"missing", testTokenA, "does-not-exist"},
		} {
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, completeUploadRequest(t, check.id, check.token))
			if recorder.Code != http.StatusNotFound {
				t.Fatalf("%s: wanted 404, got %d: %s", check.name, recorder.Code, recorder.Body.String())
			}
		}
	})

	t.Run("name conflict", func(t *testing.T) {
		router, fake := newTestRouter(t)
		addFakeDirectory(fake, "dir-a", testUserAID)
		created, err := fake.CreateUploadSession(context.Background(), testUserAID, "dir-a", "same.bin", 5)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fake.UploadSessionContent(context.Background(), testUserAID, created.ID, strings.NewReader("hello")); err != nil {
			t.Fatal(err)
		}
		parentID := "dir-a"
		fake.records["existing"] = file.File{
			ID:       "existing",
			Name:     "same.bin",
			Kind:     model.KindFile,
			ParentID: &parentID,
		}
		fake.owners["existing"] = testUserAID

		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, completeUploadRequest(t, created.ID, testTokenA))
		if recorder.Code != http.StatusConflict {
			t.Fatalf("wanted 409, got %d: %s", recorder.Code, recorder.Body.String())
		}
		var body ErrorResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Code != "already_exists" {
			t.Fatalf("code = %q, want already_exists", body.Code)
		}
	})

	t.Run("missing directory", func(t *testing.T) {
		router, fake := newTestRouter(t)
		addFakeDirectory(fake, "dir-a", testUserAID)
		created, err := fake.CreateUploadSession(context.Background(), testUserAID, "dir-a", "x.bin", 5)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fake.UploadSessionContent(context.Background(), testUserAID, created.ID, strings.NewReader("hello")); err != nil {
			t.Fatal(err)
		}
		delete(fake.records, "dir-a")

		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, completeUploadRequest(t, created.ID, testTokenA))
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("wanted 404, got %d: %s", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("completed without result file", func(t *testing.T) {
		router, fake := newTestRouter(t)
		created, err := fake.CreateUploadSession(context.Background(), testUserAID, "", "x.bin", 5)
		if err != nil {
			t.Fatal(err)
		}
		session := fake.sessions[created.ID]
		session.info.Status = model.UploadStatusCompleted
		fake.sessions[created.ID] = session

		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, completeUploadRequest(t, created.ID, testTokenA))
		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("wanted 500, got %d: %s", recorder.Code, recorder.Body.String())
		}
		var body ErrorResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Code != "upload_session_inconsistent" {
			t.Fatalf("code = %q, want upload_session_inconsistent", body.Code)
		}
	})

	t.Run("not logged in", func(t *testing.T) {
		router, fake := newTestRouter(t)
		created, err := fake.CreateUploadSession(context.Background(), testUserAID, "", "x.bin", 5)
		if err != nil {
			t.Fatal(err)
		}
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, completeUploadRequest(t, created.ID, ""))
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("wanted 401, got %d: %s", recorder.Code, recorder.Body.String())
		}
	})
}
