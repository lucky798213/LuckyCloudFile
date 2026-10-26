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

func uploadContentRequest(t *testing.T, sessionID, body, token string) *http.Request {
	t.Helper()

	req := httptest.NewRequest(
		http.MethodPut,
		"/api/upload-sessions/"+sessionID+"/content",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/octet-stream")
	if token != "" {
		req = authorize(req, token)
	}
	return req
}

func TestUploadSessionContentHTTP(t *testing.T) {
	router, fake := newTestRouter(t)
	addFakeDirectory(fake, "dir-a", testUserAID)

	created, err := fake.CreateUploadSession(
		context.Background(), testUserAID, "dir-a", "hello.bin", 5,
	)
	if err != nil {
		t.Fatal(err)
	}

	// 正常内容：返回 200，会话 ready，假存储里保存的是原始字节。
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, uploadContentRequest(t, created.ID, "hello", testTokenA))
	if resp.Code != http.StatusOK {
		t.Fatalf("upload content: wanted 200, got %d: %s", resp.Code, resp.Body.String())
	}
	var info file.UploadSessionInfo
	if err := json.Unmarshal(resp.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.Status != model.UploadStatusReady || info.ActualSize != 5 {
		t.Fatalf("uploaded session info = %+v", info)
	}
	if got := string(fake.sessions[created.ID].content); got != "hello" {
		t.Fatalf("stored content = %q, want %q", got, "hello")
	}

	// 查询接口可以看到 ready。
	get := httptest.NewRecorder()
	router.ServeHTTP(get, uploadSessionJSONRequest(
		t, http.MethodGet, "/api/upload-sessions/"+created.ID, "", testTokenA,
	))
	if get.Code != http.StatusOK {
		t.Fatalf("get ready session: wanted 200, got %d: %s", get.Code, get.Body.String())
	}

	// 目录里仍然没有正式文件。
	list := httptest.NewRecorder()
	router.ServeHTTP(list, authorize(httptest.NewRequest(
		http.MethodGet, "/api/files?parent_id=dir-a", nil,
	), testTokenA))
	if list.Code != http.StatusOK {
		t.Fatalf("list directory: wanted 200, got %d: %s", list.Code, list.Body.String())
	}
	var listing file.ListResult
	if err := json.Unmarshal(list.Body.Bytes(), &listing); err != nil {
		t.Fatal(err)
	}
	if listing.Total != 0 || len(listing.Items) != 0 {
		t.Fatalf("content upload created formal files: %+v", listing)
	}

	// ready 之后重复写入返回 409，且原内容不被覆盖。
	again := httptest.NewRecorder()
	router.ServeHTTP(again, uploadContentRequest(t, created.ID, "again", testTokenA))
	if again.Code != http.StatusConflict {
		t.Fatalf("second upload: wanted 409, got %d: %s", again.Code, again.Body.String())
	}
	var conflict ErrorResponse
	if err := json.Unmarshal(again.Body.Bytes(), &conflict); err != nil {
		t.Fatal(err)
	}
	if conflict.Code != "upload_session_conflict" {
		t.Fatalf("conflict code = %q", conflict.Code)
	}
	if got := string(fake.sessions[created.ID].content); got != "hello" {
		t.Fatalf("second upload overwrote content: %q", got)
	}
}

func TestUploadSessionContentHTTPEmptyAndMismatch(t *testing.T) {
	router, fake := newTestRouter(t)
	addFakeDirectory(fake, "dir-a", testUserAID)

	// 空文件。
	empty, err := fake.CreateUploadSession(
		context.Background(), testUserAID, "dir-a", "empty.bin", 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, uploadContentRequest(t, empty.ID, "", testTokenA))
	if resp.Code != http.StatusOK {
		t.Fatalf("empty upload: wanted 200, got %d: %s", resp.Code, resp.Body.String())
	}
	var info file.UploadSessionInfo
	if err := json.Unmarshal(resp.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.Status != model.UploadStatusReady || info.ActualSize != 0 {
		t.Fatalf("empty upload info = %+v", info)
	}

	// 少传：400 size_mismatch，会话保持 pending，之后可以整文件重传。
	short, err := fake.CreateUploadSession(
		context.Background(), testUserAID, "dir-a", "short.bin", 5,
	)
	if err != nil {
		t.Fatal(err)
	}
	bad := httptest.NewRecorder()
	router.ServeHTTP(bad, uploadContentRequest(t, short.ID, "abc", testTokenA))
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("short upload: wanted 400, got %d: %s", bad.Code, bad.Body.String())
	}
	var body ErrorResponse
	if err := json.Unmarshal(bad.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Code != "size_mismatch" {
		t.Fatalf("size mismatch code = %q", body.Code)
	}
	if fake.sessions[short.ID].info.Status != model.UploadStatusPending {
		t.Fatalf("failed upload changed status to %q", fake.sessions[short.ID].info.Status)
	}

	retry := httptest.NewRecorder()
	router.ServeHTTP(retry, uploadContentRequest(t, short.ID, "hello", testTokenA))
	if retry.Code != http.StatusOK {
		t.Fatalf("retry upload: wanted 200, got %d: %s", retry.Code, retry.Body.String())
	}
}

func TestUploadSessionContentHTTPNotFoundExpiredAndUnauthorized(t *testing.T) {
	router, fake := newTestRouter(t)
	addFakeDirectory(fake, "dir-a", testUserAID)
	created, err := fake.CreateUploadSession(
		context.Background(), testUserAID, "dir-a", "owned.bin", 3,
	)
	if err != nil {
		t.Fatal(err)
	}

	// 别人的会话和不存在的会话返回同一个 404。
	for _, test := range []struct {
		name  string
		id    string
		token string
	}{
		{"foreign", created.ID, testTokenB},
		{"missing", "does-not-exist", testTokenA},
	} {
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, uploadContentRequest(t, test.id, "abc", test.token))
		if resp.Code != http.StatusNotFound {
			t.Fatalf("%s: wanted 404, got %d: %s", test.name, resp.Code, resp.Body.String())
		}
		var body ErrorResponse
		if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Code != "upload_session_not_found" {
			t.Fatalf("%s: code = %q, want upload_session_not_found", test.name, body.Code)
		}
	}

	// 过期会话返回 410。
	entry := fake.sessions[created.ID]
	entry.info.ExpiresAt = time.Now().Add(-time.Minute)
	fake.sessions[created.ID] = entry
	expired := httptest.NewRecorder()
	router.ServeHTTP(expired, uploadContentRequest(t, created.ID, "abc", testTokenA))
	if expired.Code != http.StatusGone {
		t.Fatalf("expired session: wanted 410, got %d: %s", expired.Code, expired.Body.String())
	}
	var expiredBody ErrorResponse
	if err := json.Unmarshal(expired.Body.Bytes(), &expiredBody); err != nil {
		t.Fatal(err)
	}
	if expiredBody.Code != "upload_session_expired" {
		t.Fatalf("expired code = %q", expiredBody.Code)
	}

	// 未登录返回 401。
	unauthorized := httptest.NewRecorder()
	router.ServeHTTP(unauthorized, uploadContentRequest(t, created.ID, "abc", ""))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated upload: wanted 401, got %d", unauthorized.Code)
	}
}
