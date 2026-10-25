package httpapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/storage"
)

// newTestRouter 创建一个用临时目录做存储的路由，测试之间互不影响。
func newTestRouter(t *testing.T) (http.Handler, string) {
	t.Helper()

	root := t.TempDir()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	router := NewRouter(logger, file.NewService(storage.NewLocal(root)))
	return router, root
}

// uploadRequest 构造一个 multipart/form-data 上传请求。
func uploadRequest(t *testing.T, field, filename string, content []byte) *http.Request {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile(field, filename)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req
}

// upload 上传一个文件并返回解析后的记录，省得每个测试都重复一遍。
func upload(t *testing.T, router http.Handler, content []byte) file.File {
	t.Helper()

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, uploadRequest(t, "file", "test.bin", content))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("wanted 201, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var record file.File
	if err := json.Unmarshal(recorder.Body.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	return record
}

func TestUploadFile(t *testing.T) {
	router, root := newTestRouter(t)
	content := []byte("hello luckyCloudFile")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, uploadRequest(t, "file", "greeting.txt", content))

	if recorder.Code != http.StatusCreated {
		t.Fatalf("wanted 201, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var record file.File
	if err := json.Unmarshal(recorder.Body.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record.ID == "" || record.Name != "greeting.txt" || record.Size != int64(len(content)) {
		t.Fatalf("unexpected record: %+v", record)
	}

	// 磁盘上的文件名应该是生成的 ID，内容必须和上传的完全一致。
	saved, err := os.ReadFile(filepath.Join(root, record.ID))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(saved, content) {
		t.Fatalf("saved content = %q, want %q", saved, content)
	}

	// 原始文件名只当元数据，不能真的在磁盘上生成这个文件。
	if _, err := os.Stat(filepath.Join(root, "greeting.txt")); !os.IsNotExist(err) {
		t.Fatal("client file name must not be used as the disk path")
	}
}

func TestUploadDoesNotUseClientNameAsPath(t *testing.T) {
	router, root := newTestRouter(t)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, uploadRequest(t, "file", "../../evil.txt", []byte("x")))

	if recorder.Code != http.StatusCreated {
		t.Fatalf("wanted 201, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var record file.File
	if err := json.Unmarshal(recorder.Body.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record.Name != "evil.txt" {
		t.Fatalf("name = %q, want evil.txt", record.Name)
	}

	// 存储目录里只应该有那一个用 ID 命名的文件。
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != record.ID {
		t.Fatalf("unexpected files in storage: %v", entries)
	}
}

func TestUploadMissingFileField(t *testing.T) {
	router, _ := newTestRouter(t)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("name", "not-a-file"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("wanted 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var response ErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Code != "missing_file" {
		t.Fatalf("code = %q, want missing_file", response.Code)
	}
}

func TestUploadWrongFieldName(t *testing.T) {
	router, root := newTestRouter(t)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, uploadRequest(t, "avatar", "a.txt", []byte("x")))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("wanted 400 for field named avatar, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var response ErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Code != "invalid_field" {
		t.Fatalf("code = %q, want invalid_field", response.Code)
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("rejected upload left files behind: %v", entries)
	}
}

func TestUploadExactlyAtLimit(t *testing.T) {
	router, _ := newTestRouter(t)
	content := bytes.Repeat([]byte("x"), int(file.MaxUploadBytes))

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, uploadRequest(t, "file", "exact.bin", content))

	// multipart 的表单头和边界不应该算进 32 MiB 里。
	if recorder.Code != http.StatusCreated {
		t.Fatalf("exactly %d bytes should be accepted, got %d: %s",
			file.MaxUploadBytes, recorder.Code, recorder.Body.String())
	}
}

func TestUploadFileContentTooLarge(t *testing.T) {
	router, root := newTestRouter(t)
	content := bytes.Repeat([]byte("x"), int(file.MaxUploadBytes)+1)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, uploadRequest(t, "file", "big.bin", content))

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("wanted 413, got %d: %s", recorder.Code, recorder.Body.String())
	}

	// 失败的上传不能留下任何文件（包括临时文件）。
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed upload left files behind: %v", entries)
	}
}

func TestUploadHugeRequestBody(t *testing.T) {
	router, _ := newTestRouter(t)
	// 超过“32 MiB 文件 + 1 MiB 表单余量”，会被 MaxBytesReader 提前拦住。
	content := bytes.Repeat([]byte("x"), int(file.MaxUploadBytes+multipartOverheadBytes)+1)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, uploadRequest(t, "file", "huge.bin", content))

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("wanted 413, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestUploadRejectsNonMultipart(t *testing.T) {
	router, _ := newTestRouter(t)

	req := httptest.NewRequest(http.MethodPost, "/api/files", strings.NewReader("raw body"))
	req.Header.Set("Content-Type", "text/plain")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("wanted 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

// TestUploadDownloadDelete 是 P1 的验收闭环：
// 上传 → 下载 → SHA-256 一致 → 删除 → 再下载 404。
func TestUploadDownloadDelete(t *testing.T) {
	router, root := newTestRouter(t)
	content := []byte("luckyCloudFile P1 closed loop\n")
	wantHash := sha256.Sum256(content)

	record := upload(t, router, content)

	// 下载并校验内容。
	download := httptest.NewRecorder()
	router.ServeHTTP(download, httptest.NewRequest(http.MethodGet, "/api/files/"+record.ID, nil))
	if download.Code != http.StatusOK {
		t.Fatalf("wanted 200, got %d: %s", download.Code, download.Body.String())
	}
	if got := sha256.Sum256(download.Body.Bytes()); got != wantHash {
		t.Fatalf("downloaded SHA-256 = %x, want %x", got, wantHash)
	}
	if disposition := download.Header().Get("Content-Disposition"); !strings.Contains(disposition, record.Name) {
		t.Fatalf("Content-Disposition = %q, want it to contain %q", disposition, record.Name)
	}
	if length := download.Header().Get("Content-Length"); length != strconv.FormatInt(record.Size, 10) {
		t.Fatalf("Content-Length = %q, want %d", length, record.Size)
	}

	// 删除。
	deleted := httptest.NewRecorder()
	router.ServeHTTP(deleted, httptest.NewRequest(http.MethodDelete, "/api/files/"+record.ID, nil))
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("wanted 204, got %d: %s", deleted.Code, deleted.Body.String())
	}

	// 磁盘文件没了，再下载是 404。
	if _, err := os.Stat(filepath.Join(root, record.ID)); !os.IsNotExist(err) {
		t.Fatal("file is still on disk after delete")
	}
	after := httptest.NewRecorder()
	router.ServeHTTP(after, httptest.NewRequest(http.MethodGet, "/api/files/"+record.ID, nil))
	if after.Code != http.StatusNotFound {
		t.Fatalf("wanted 404 after delete, got %d", after.Code)
	}
}

func TestDownloadUnknownID(t *testing.T) {
	router, _ := newTestRouter(t)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/files/does-not-exist", nil))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("wanted 404, got %d", recorder.Code)
	}
	var response ErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Code != "file_not_found" {
		t.Fatalf("code = %q, want file_not_found", response.Code)
	}
}

func TestDeleteUnknownID(t *testing.T) {
	router, _ := newTestRouter(t)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodDelete, "/api/files/does-not-exist", nil))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("wanted 404, got %d", recorder.Code)
	}
}
