package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/model"
)

// TestUploadSessionHTTPChunkedFlow 覆盖分片模式在 HTTP 层的完整链路：
// 创建 → 分片上传 → 确认完成，以及 full/chunked 不能混用接口。
func TestUploadSessionHTTPChunkedFlow(t *testing.T) {
	router, _ := newTestRouter(t)

	created := httptest.NewRecorder()
	router.ServeHTTP(created, uploadSessionJSONRequest(
		t, http.MethodPost, "/api/upload-sessions",
		`{"name":"demo.bin","size":4,"mode":"chunked"}`,
		testTokenA,
	))
	if created.Code != http.StatusCreated {
		t.Fatalf("create chunked session: wanted 201, got %d: %s",
			created.Code, created.Body.String())
	}

	var info file.UploadSessionInfo
	if err := json.Unmarshal(created.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.Mode != model.UploadModeChunked ||
		info.ChunkSize != file.UploadChunkSize ||
		info.ChunkCount != 1 ||
		info.Status != model.UploadStatusPending {
		t.Fatalf("unexpected chunked session: %+v", info)
	}

	// chunked 会话不能走整文件接口。
	wrongEndpoint := httptest.NewRecorder()
	router.ServeHTTP(wrongEndpoint, uploadSessionJSONRequest(
		t, http.MethodPut, "/api/upload-sessions/"+info.ID+"/content", "abcd", testTokenA,
	))
	if wrongEndpoint.Code != http.StatusConflict {
		t.Fatalf("content endpoint on chunked session: wanted 409, got %d: %s",
			wrongEndpoint.Code, wrongEndpoint.Body.String())
	}
	assertErrorCode(t, wrongEndpoint, "upload_mode_mismatch")

	// 唯一一片：长度必须正好是 4 字节。
	wrongSize := httptest.NewRecorder()
	router.ServeHTTP(wrongSize, uploadSessionJSONRequest(
		t, http.MethodPut, "/api/upload-sessions/"+info.ID+"/chunks/0", "abc", testTokenA,
	))
	if wrongSize.Code != http.StatusBadRequest {
		t.Fatalf("short chunk: wanted 400, got %d: %s", wrongSize.Code, wrongSize.Body.String())
	}
	assertErrorCode(t, wrongSize, "size_mismatch")

	uploaded := httptest.NewRecorder()
	router.ServeHTTP(uploaded, uploadSessionJSONRequest(
		t, http.MethodPut, "/api/upload-sessions/"+info.ID+"/chunks/0", "abcd", testTokenA,
	))
	if uploaded.Code != http.StatusOK {
		t.Fatalf("upload chunk: wanted 200, got %d: %s", uploaded.Code, uploaded.Body.String())
	}
	var progress file.UploadSessionInfo
	if err := json.Unmarshal(uploaded.Body.Bytes(), &progress); err != nil {
		t.Fatal(err)
	}
	if progress.ActualSize != 4 || progress.Status != model.UploadStatusReady {
		t.Fatalf("chunk progress = %+v", progress)
	}

	// 最后一片已经收齐、会话 ready：重传同内容必须幂等成功。
	replayed := httptest.NewRecorder()
	router.ServeHTTP(replayed, uploadSessionJSONRequest(
		t, http.MethodPut, "/api/upload-sessions/"+info.ID+"/chunks/0", "abcd", testTokenA,
	))
	if replayed.Code != http.StatusOK {
		t.Fatalf("replay ready chunk: wanted 200, got %d: %s",
			replayed.Code, replayed.Body.String())
	}
	var replayInfo file.UploadSessionInfo
	if err := json.Unmarshal(replayed.Body.Bytes(), &replayInfo); err != nil {
		t.Fatal(err)
	}
	if replayInfo.Status != model.UploadStatusReady || replayInfo.ActualSize != 4 {
		t.Fatalf("replay progress = %+v", replayInfo)
	}

	// ready 状态下同编号不同内容仍然冲突。
	conflicting := httptest.NewRecorder()
	router.ServeHTTP(conflicting, uploadSessionJSONRequest(
		t, http.MethodPut, "/api/upload-sessions/"+info.ID+"/chunks/0", "wxyz", testTokenA,
	))
	if conflicting.Code != http.StatusConflict {
		t.Fatalf("conflicting ready replay: wanted 409, got %d: %s",
			conflicting.Code, conflicting.Body.String())
	}
	assertErrorCode(t, conflicting, "upload_session_conflict")

	completed := httptest.NewRecorder()
	router.ServeHTTP(completed, uploadSessionJSONRequest(
		t, http.MethodPost, "/api/upload-sessions/"+info.ID+"/complete", "", testTokenA,
	))
	if completed.Code != http.StatusOK {
		t.Fatalf("complete chunked session: wanted 200, got %d: %s",
			completed.Code, completed.Body.String())
	}
	var done file.UploadSessionInfo
	if err := json.Unmarshal(completed.Body.Bytes(), &done); err != nil {
		t.Fatal(err)
	}
	if done.Status != model.UploadStatusCompleted || done.ResultFileID == nil {
		t.Fatalf("completed chunked session = %+v", done)
	}
}

// TestUploadSessionHTTPChunkModeGuards 覆盖创建参数和 full 会话的分片接口拦截。
func TestUploadSessionHTTPChunkModeGuards(t *testing.T) {
	router, _ := newTestRouter(t)

	invalid := httptest.NewRecorder()
	router.ServeHTTP(invalid, uploadSessionJSONRequest(
		t, http.MethodPost, "/api/upload-sessions",
		`{"name":"x.bin","size":10,"mode":"weird"}`, testTokenA,
	))
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid mode: wanted 400, got %d: %s", invalid.Code, invalid.Body.String())
	}
	assertErrorCode(t, invalid, "invalid_mode")

	// 不传 mode 必须保持原来的 full 行为。
	created := httptest.NewRecorder()
	router.ServeHTTP(created, uploadSessionJSONRequest(
		t, http.MethodPost, "/api/upload-sessions",
		`{"name":"full.bin","size":10}`, testTokenA,
	))
	if created.Code != http.StatusCreated {
		t.Fatalf("create full session: wanted 201, got %d: %s", created.Code, created.Body.String())
	}
	var info file.UploadSessionInfo
	if err := json.Unmarshal(created.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.Mode != model.UploadModeFull || info.ChunkSize != 0 || info.ChunkCount != 0 {
		t.Fatalf("default session is not full: %+v", info)
	}

	wrongEndpoint := httptest.NewRecorder()
	router.ServeHTTP(wrongEndpoint, uploadSessionJSONRequest(
		t, http.MethodPut, "/api/upload-sessions/"+info.ID+"/chunks/0",
		strings.Repeat("x", 10), testTokenA,
	))
	if wrongEndpoint.Code != http.StatusConflict {
		t.Fatalf("chunk endpoint on full session: wanted 409, got %d: %s",
			wrongEndpoint.Code, wrongEndpoint.Body.String())
	}
	assertErrorCode(t, wrongEndpoint, "upload_mode_mismatch")
}

// TestUploadSessionHTTPEmptyChunkedCompletes 验证空文件没有分片：
// 创建后直接 ready，不需要调用分片接口，直接确认完成。
func TestUploadSessionHTTPEmptyChunkedCompletes(t *testing.T) {
	router, _ := newTestRouter(t)

	created := httptest.NewRecorder()
	router.ServeHTTP(created, uploadSessionJSONRequest(
		t, http.MethodPost, "/api/upload-sessions",
		`{"name":"empty.bin","size":0,"mode":"chunked"}`, testTokenA,
	))
	if created.Code != http.StatusCreated {
		t.Fatalf("create empty chunked session: wanted 201, got %d: %s",
			created.Code, created.Body.String())
	}
	var info file.UploadSessionInfo
	if err := json.Unmarshal(created.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.ChunkCount != 0 || info.Status != model.UploadStatusReady {
		t.Fatalf("empty chunked session = %+v", info)
	}

	completed := httptest.NewRecorder()
	router.ServeHTTP(completed, uploadSessionJSONRequest(
		t, http.MethodPost, "/api/upload-sessions/"+info.ID+"/complete", "", testTokenA,
	))
	if completed.Code != http.StatusOK {
		t.Fatalf("complete empty chunked session: wanted 200, got %d: %s",
			completed.Code, completed.Body.String())
	}
	var done file.UploadSessionInfo
	if err := json.Unmarshal(completed.Body.Bytes(), &done); err != nil {
		t.Fatal(err)
	}
	if done.Status != model.UploadStatusCompleted || done.ResultFileID == nil {
		t.Fatalf("completed empty chunked session = %+v", done)
	}
}

func assertErrorCode(t *testing.T, recorder *httptest.ResponseRecorder, want string) {
	t.Helper()
	var body ErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Code != want {
		t.Fatalf("error code = %q, want %q: %s", body.Code, want, recorder.Body.String())
	}
}
