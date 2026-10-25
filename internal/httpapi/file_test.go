package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"luckyCloudFile/internal/file"
)

// fakeFileService 实现和 *file.Service 相同的三个方法，但不连数据库、不写磁盘。
//
// HTTP 层的测试只关心“状态码、响应头、JSON 对不对”，
// 真正的数据库和磁盘行为由 internal/file 的集成测试覆盖。
type fakeFileService struct {
	mu       sync.Mutex
	nextID   int
	records  map[string]file.File
	contents map[string][]byte
	owners   map[string]int
}

func newFakeFileService() *fakeFileService {
	return &fakeFileService{
		records:  make(map[string]file.File),
		contents: make(map[string][]byte),
		owners:   make(map[string]int),
	}
}

func (f *fakeFileService) Upload(ctx context.Context, ownerID int, name string, r io.Reader) (file.File, error) {
	// 测试替身把内容读进内存就够了，不用真的写磁盘。
	data, err := io.ReadAll(io.LimitReader(r, file.MaxUploadBytes+1))
	if err != nil {
		return file.File{}, err
	}
	if int64(len(data)) > file.MaxUploadBytes {
		return file.File{}, file.ErrTooLarge
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	id := fmt.Sprintf("fake-%d", f.nextID)
	record := file.File{ID: id, Name: name, Size: int64(len(data))}
	f.records[id] = record
	f.contents[id] = data
	f.owners[id] = ownerID
	return record, nil
}

func (f *fakeFileService) Open(ctx context.Context, ownerID int, id string) (file.File, io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.records[id]
	if !ok || f.owners[id] != ownerID {
		return file.File{}, nil, file.ErrNotFound
	}
	return record, io.NopCloser(bytes.NewReader(f.contents[id])), nil
}

func (f *fakeFileService) Delete(ctx context.Context, ownerID int, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.records[id]; !ok || f.owners[id] != ownerID {
		return file.ErrNotFound
	}
	delete(f.records, id)
	delete(f.contents, id)
	delete(f.owners, id)
	return nil
}

func (f *fakeFileService) stored(id string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.contents[id]
	return data, ok
}

// newTestRouter 创建一个使用假文件/认证服务的路由，测试之间互不影响。
func newTestRouter(t *testing.T) (http.Handler, *fakeFileService) {
	t.Helper()
	fake := newFakeFileService()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewRouter(logger, fake, newFakeAuthService()), fake
}

// authorize 给请求加上 A 或 B 的测试 Token。
func authorize(req *http.Request, token string) *http.Request {
	req.Header.Set("Authorization", "Bearer "+token)
	return req
}

// uploadRequest 构造一个 multipart/form-data 上传请求，默认以 A 登录。
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
	return authorize(req, testTokenA)
}

// upload 上传一个文件并返回解析后的记录。
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
	router, fake := newTestRouter(t)
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

	saved, ok := fake.stored(record.ID)
	if !ok || !bytes.Equal(saved, content) {
		t.Fatalf("stored content = %q, want %q", saved, content)
	}
}

func TestUploadDoesNotUseClientNameAsPath(t *testing.T) {
	router, fake := newTestRouter(t)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, uploadRequest(t, "file", "../../evil.txt", []byte("x")))

	if recorder.Code != http.StatusCreated {
		t.Fatalf("wanted 201, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var record file.File
	if err := json.Unmarshal(recorder.Body.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	// 处理器会把客户端路径压成文件名，并且只把 ID 交给 Service。
	if record.Name != "evil.txt" {
		t.Fatalf("name = %q, want evil.txt", record.Name)
	}
	if record.ID == "evil.txt" {
		t.Fatal("client file name must not be used as the file ID")
	}
	if _, ok := fake.stored(record.ID); !ok {
		t.Fatal("file content was not stored under the generated ID")
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
	authorize(req, testTokenA)

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
	router, fake := newTestRouter(t)

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
	if len(fake.records) != 0 {
		t.Fatal("rejected upload must not be stored")
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
	router, _ := newTestRouter(t)
	content := bytes.Repeat([]byte("x"), int(file.MaxUploadBytes)+1)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, uploadRequest(t, "file", "big.bin", content))

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("wanted 413, got %d: %s", recorder.Code, recorder.Body.String())
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
	authorize(req, testTokenA)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("wanted 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

// TestFileOwnershipIsolation 证明 B 知道 A 的文件 ID 也操作不了，
// 并且 B 的失败不会破坏 A 的文件。
func TestFileOwnershipIsolation(t *testing.T) {
	router, _ := newTestRouter(t)
	content := []byte("only A can read this")

	record := upload(t, router, content)

	// B 知道准确的 ID，但下载必须得到 404。
	byB := httptest.NewRecorder()
	router.ServeHTTP(byB, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+record.ID, nil), testTokenB))
	if byB.Code != http.StatusNotFound {
		t.Fatalf("B download wanted 404, got %d: %s", byB.Code, byB.Body.String())
	}

	// B 删除同样得到 404。
	deleteByB := httptest.NewRecorder()
	router.ServeHTTP(deleteByB, authorize(httptest.NewRequest(http.MethodDelete, "/api/files/"+record.ID, nil), testTokenB))
	if deleteByB.Code != http.StatusNotFound {
		t.Fatalf("B delete wanted 404, got %d: %s", deleteByB.Code, deleteByB.Body.String())
	}

	// A 仍然能完整下载，证明 B 的失败没有影响文件。
	byA := httptest.NewRecorder()
	router.ServeHTTP(byA, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+record.ID, nil), testTokenA))
	if byA.Code != http.StatusOK {
		t.Fatalf("A download after B failed wanted 200, got %d: %s", byA.Code, byA.Body.String())
	}
	if !bytes.Equal(byA.Body.Bytes(), content) {
		t.Fatalf("A content changed after B failed: %q", byA.Body.Bytes())
	}

	// 最后 A 可以删除自己的文件。
	deleteByA := httptest.NewRecorder()
	router.ServeHTTP(deleteByA, authorize(httptest.NewRequest(http.MethodDelete, "/api/files/"+record.ID, nil), testTokenA))
	if deleteByA.Code != http.StatusNoContent {
		t.Fatalf("A delete wanted 204, got %d", deleteByA.Code)
	}
}

// TestUploadDownloadDelete 是 HTTP 层的闭环：
// 上传 → 下载 → SHA-256 一致 → 删除 → 再下载 404。
func TestUploadDownloadDelete(t *testing.T) {
	router, fake := newTestRouter(t)
	content := []byte("luckyCloudFile closed loop\n")
	wantHash := sha256.Sum256(content)

	record := upload(t, router, content)

	download := httptest.NewRecorder()
	router.ServeHTTP(download, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+record.ID, nil), testTokenA))
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

	deleted := httptest.NewRecorder()
	router.ServeHTTP(deleted, authorize(httptest.NewRequest(http.MethodDelete, "/api/files/"+record.ID, nil), testTokenA))
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("wanted 204, got %d: %s", deleted.Code, deleted.Body.String())
	}
	if _, ok := fake.stored(record.ID); ok {
		t.Fatal("file is still stored after delete")
	}

	after := httptest.NewRecorder()
	router.ServeHTTP(after, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+record.ID, nil), testTokenA))
	if after.Code != http.StatusNotFound {
		t.Fatalf("wanted 404 after delete, got %d", after.Code)
	}
}

func TestFileEndpointsRequireLogin(t *testing.T) {
	router, fake := newTestRouter(t)

	requests := []*http.Request{
		uploadRequest(t, "file", "a.txt", []byte("x")),
		httptest.NewRequest(http.MethodGet, "/api/files/whatever", nil),
		httptest.NewRequest(http.MethodDelete, "/api/files/whatever", nil),
	}
	for _, req := range requests {
		// uploadRequest 默认带 A 的 Token；这里统一去掉，验证未登录行为。
		req.Header.Del("Authorization")
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s without token: wanted 401, got %d: %s",
				req.Method, req.URL.Path, recorder.Code, recorder.Body.String())
		}
		var response ErrorResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Code != "unauthorized" {
			t.Fatalf("code = %q, want unauthorized", response.Code)
		}
	}

	// 无效 Token 也不能上传，更不能留下文件。
	req := uploadRequest(t, "file", "a.txt", []byte("x"))
	req.Header.Set("Authorization", "Bearer not-a-real-token")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("invalid token: wanted 401, got %d", recorder.Code)
	}
	if len(fake.records) != 0 {
		t.Fatal("unauthenticated upload must not be stored")
	}
}

func TestDownloadUnknownID(t *testing.T) {
	router, _ := newTestRouter(t)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, authorize(httptest.NewRequest(http.MethodGet, "/api/files/does-not-exist", nil), testTokenA))

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
	router.ServeHTTP(recorder, authorize(httptest.NewRequest(http.MethodDelete, "/api/files/does-not-exist", nil), testTokenA))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("wanted 404, got %d", recorder.Code)
	}
}
