package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/model"
)

// fakeUploadSession 是假服务里的一条上传会话，只用来验证 HTTP 层。
type fakeUploadSession struct {
	info    file.UploadSessionInfo
	ownerID int
	content []byte
}

// fakeFileService 实现和 *file.Service 相同的接口，但不连数据库、不写磁盘。
//
// HTTP 层的测试只关心“状态码、响应头、JSON 对不对”，
// 真正的数据库和磁盘行为由 internal/file 的集成测试覆盖。
type fakeFileService struct {
	mu       sync.Mutex
	nextID   int
	records  map[string]file.File
	contents map[string][]byte
	owners   map[string]int
	// sessions 是上传会话 ID → 会话。和 records 分开，模拟 upload_sessions 表。
	sessions map[string]fakeUploadSession
	// roots 是“用户根目录”的 ID → 用户 ID。假实现不把根目录放进 records，
	// 只用来验证根目录不能被改名。
	roots map[string]int
}

func newFakeFileService() *fakeFileService {
	return &fakeFileService{
		records:  make(map[string]file.File),
		contents: make(map[string][]byte),
		owners:   make(map[string]int),
		sessions: make(map[string]fakeUploadSession),
		roots:    make(map[string]int),
	}
}

// checkParent 模拟 requireDirectory：父目录必须属于当前用户、而且真的是目录。
// 调用时必须已经持有 f.mu。
func (f *fakeFileService) checkParent(ownerID int, parentID string) error {
	if parentID == "" {
		// 假实现里不建根目录节点，空字符串直接当作根。
		return nil
	}
	record, ok := f.records[parentID]
	if !ok || f.owners[parentID] != ownerID {
		return file.ErrNotFound
	}
	if record.Kind != model.KindDirectory {
		return file.ErrNotDirectory
	}
	return nil
}

// nameTaken 模拟 (owner_id, parent_id, name) 唯一约束。
// 调用时必须已经持有 f.mu。
func (f *fakeFileService) nameTaken(ownerID int, parentID, name string) bool {
	for id, record := range f.records {
		if f.owners[id] != ownerID || record.Name != name {
			continue
		}
		if sameParent(record.ParentID, parentID) {
			return true
		}
	}
	return false
}

func sameParent(parent *string, parentID string) bool {
	if parent == nil {
		return parentID == ""
	}
	return *parent == parentID
}

func (f *fakeFileService) Upload(ctx context.Context, ownerID int, parentID, name string, r io.Reader) (file.File, error) {
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

	if err := f.checkParent(ownerID, parentID); err != nil {
		return file.File{}, err
	}
	if f.nameTaken(ownerID, parentID, name) {
		return file.File{}, file.ErrAlreadyExists
	}

	f.nextID++
	id := fmt.Sprintf("fake-%d", f.nextID)
	record := file.File{ID: id, Name: name, Size: int64(len(data)), Kind: model.KindFile}
	if parentID != "" {
		record.ParentID = &parentID
	}
	f.records[id] = record
	f.contents[id] = data
	f.owners[id] = ownerID
	return record, nil
}

func (f *fakeFileService) CreateDirectory(ctx context.Context, ownerID int, parentID, name string) (file.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.checkParent(ownerID, parentID); err != nil {
		return file.File{}, err
	}
	if f.nameTaken(ownerID, parentID, name) {
		return file.File{}, file.ErrAlreadyExists
	}

	f.nextID++
	id := fmt.Sprintf("fake-%d", f.nextID)
	record := file.File{ID: id, Name: name, Kind: model.KindDirectory}
	if parentID != "" {
		record.ParentID = &parentID
	}
	f.records[id] = record
	f.owners[id] = ownerID
	return record, nil
}

func (f *fakeFileService) ListChildren(ctx context.Context, ownerID int, parentID string, page, pageSize int) (file.ListResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.checkParent(ownerID, parentID); err != nil {
		return file.ListResult{}, err
	}

	items := make([]file.File, 0)
	for id, record := range f.records {
		if f.owners[id] == ownerID && sameParent(record.ParentID, parentID) {
			items = append(items, record)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Name == items[j].Name {
			return items[i].ID < items[j].ID
		}
		return items[i].Name < items[j].Name
	})

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	total := int64(len(items))
	start := (page - 1) * pageSize
	if start > len(items) {
		start = len(items)
	}
	end := start + pageSize
	if end > len(items) {
		end = len(items)
	}

	return file.ListResult{
		Items:    items[start:end],
		Total:    total,
		Page:     page,
		PageSize: pageSize,
	}, nil
}

func (f *fakeFileService) Open(ctx context.Context, ownerID int, id string) (file.File, io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	record, ok := f.records[id]
	if !ok || f.owners[id] != ownerID {
		return file.File{}, nil, file.ErrNotFound
	}
	if record.Kind != model.KindFile {
		return file.File{}, nil, file.ErrIsDirectory
	}
	return record, io.NopCloser(bytes.NewReader(f.contents[id])), nil
}

func (f *fakeFileService) Delete(ctx context.Context, ownerID int, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if owner, isRoot := f.roots[id]; isRoot && owner == ownerID {
		return file.ErrRootDirectory
	}
	record, ok := f.records[id]
	if !ok || f.owners[id] != ownerID {
		return file.ErrNotFound
	}
	// 目录只能删空的；非空返回 409 的底层错误。
	if record.Kind == model.KindDirectory {
		for childID, child := range f.records {
			if f.owners[childID] == ownerID && sameParent(child.ParentID, id) {
				return file.ErrDirectoryNotEmpty
			}
		}
	}
	delete(f.records, id)
	delete(f.contents, id)
	delete(f.owners, id)
	return nil
}

// Copy 模拟复用同一份内容：新记录复制源文件的 size，内容按值拷贝一份，
// 这样删除源文件不会影响副本。
func (f *fakeFileService) Copy(ctx context.Context, ownerID int, id, parentID, name string) (file.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	source, ok := f.records[id]
	if !ok || f.owners[id] != ownerID {
		return file.File{}, file.ErrNotFound
	}
	if source.Kind != model.KindFile {
		return file.File{}, file.ErrIsDirectory
	}
	if err := f.checkParent(ownerID, parentID); err != nil {
		return file.File{}, err
	}
	if f.nameTaken(ownerID, parentID, name) {
		return file.File{}, file.ErrAlreadyExists
	}

	f.nextID++
	newID := fmt.Sprintf("fake-%d", f.nextID)
	record := file.File{ID: newID, Name: name, Size: source.Size, Kind: model.KindFile}
	if parentID != "" {
		record.ParentID = &parentID
	}
	f.records[newID] = record
	f.contents[newID] = append([]byte(nil), f.contents[id]...)
	f.owners[newID] = ownerID
	return record, nil
}

// Rename 模拟只改显示名：大小、内容和父目录都不变。
func (f *fakeFileService) Rename(ctx context.Context, ownerID int, id, name string) (file.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if owner, isRoot := f.roots[id]; isRoot && owner == ownerID {
		return file.File{}, file.ErrRootDirectory
	}
	record, ok := f.records[id]
	if !ok || f.owners[id] != ownerID {
		return file.File{}, file.ErrNotFound
	}
	if !validFakeName(name) {
		return file.File{}, file.ErrInvalidName
	}
	if record.Name == name {
		return record, nil
	}

	parentID := ""
	if record.ParentID != nil {
		parentID = *record.ParentID
	}
	// 同名检查要排除自己，否则“改成现在的名字”会被误判为重名。
	for otherID, other := range f.records {
		if otherID == id || f.owners[otherID] != ownerID || other.Name != name {
			continue
		}
		if sameParent(other.ParentID, parentID) {
			return file.File{}, file.ErrAlreadyExists
		}
	}

	record.Name = name
	f.records[id] = record
	return record, nil
}

// Move 模拟只改 parent_id：大小、内容和名字都不变。
func (f *fakeFileService) Move(ctx context.Context, ownerID int, id, parentID string) (file.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if owner, isRoot := f.roots[id]; isRoot && owner == ownerID {
		return file.File{}, file.ErrRootDirectory
	}
	source, ok := f.records[id]
	if !ok || f.owners[id] != ownerID {
		return file.File{}, file.ErrNotFound
	}
	if err := f.checkParent(ownerID, parentID); err != nil {
		return file.File{}, err
	}
	// 目录不能移到自己或自己的后代里。
	if source.Kind == model.KindDirectory {
		for current, depth := parentID, 0; current != "" && depth < 1000; depth++ {
			if current == id {
				return file.File{}, file.ErrInvalidMove
			}
			record, ok := f.records[current]
			if !ok || record.ParentID == nil {
				break
			}
			current = *record.ParentID
		}
	}
	if sameParent(source.ParentID, parentID) {
		return source, nil
	}
	if f.nameTaken(ownerID, parentID, source.Name) {
		return file.File{}, file.ErrAlreadyExists
	}

	if parentID == "" {
		source.ParentID = nil
	} else {
		target := parentID
		source.ParentID = &target
	}
	f.records[id] = source
	return source, nil
}

// validFakeName 复刻业务层最基本的名字校验，
// 让 HTTP 层测试也能覆盖 400 invalid_name。
func validFakeName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	return !strings.ContainsAny(name, `/\`) && !strings.ContainsRune(name, 0)
}

// CreateUploadSession 模拟真实服务：默认创建 full 会话。
func (f *fakeFileService) CreateUploadSession(ctx context.Context, ownerID int, parentID, name string, size int64) (file.UploadSessionInfo, error) {
	return f.CreateUploadSessionWithMode(ctx, ownerID, parentID, name, size, model.UploadModeFull)
}

// CreateUploadSessionWithMode 模拟真实服务：校验参数和目录后只登记会话，
// 不创建 File，也不占用文件内容。分片大小固定由服务端给出。
func (f *fakeFileService) CreateUploadSessionWithMode(ctx context.Context, ownerID int, parentID, name string, size int64, mode string) (file.UploadSessionInfo, error) {
	if mode == "" {
		mode = model.UploadModeFull
	}
	if mode != model.UploadModeFull && mode != model.UploadModeChunked {
		return file.UploadSessionInfo{}, file.ErrInvalidUploadMode
	}
	if !validFakeName(name) {
		return file.UploadSessionInfo{}, file.ErrInvalidName
	}
	if size < 0 {
		return file.UploadSessionInfo{}, file.ErrInvalidSize
	}
	if size > file.MaxUploadBytes {
		return file.UploadSessionInfo{}, file.ErrTooLarge
	}

	var chunkSize int64
	var chunkCount int
	if mode == model.UploadModeChunked {
		if size == 0 {
			return file.UploadSessionInfo{}, file.ErrInvalidSize
		}
		chunkSize = file.UploadChunkSize
		chunkCount = int((size + chunkSize - 1) / chunkSize)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.checkParent(ownerID, parentID); err != nil {
		return file.UploadSessionInfo{}, err
	}

	f.nextID++
	info := file.UploadSessionInfo{
		ID:           fmt.Sprintf("fake-session-%d", f.nextID),
		ParentID:     parentID,
		Name:         name,
		ExpectedSize: size,
		ActualSize:   0,
		Mode:         mode,
		ChunkSize:    chunkSize,
		ChunkCount:   chunkCount,
		Status:       model.UploadStatusPending,
		ExpiresAt:    time.Now().Add(file.UploadSessionTTL),
	}
	f.sessions[info.ID] = fakeUploadSession{info: info, ownerID: ownerID}
	return info, nil
}

func (f *fakeFileService) GetUploadSession(ctx context.Context, ownerID int, sessionID string) (file.UploadSessionInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	session, ok := f.sessions[sessionID]
	if !ok || session.ownerID != ownerID {
		return file.UploadSessionInfo{}, file.ErrUploadSessionNotFound
	}
	return session.info, nil
}

// UploadSessionContent 模拟真实服务：只接收一次完整内容，大小必须与创建时声明的一致。
func (f *fakeFileService) UploadSessionContent(ctx context.Context, ownerID int, sessionID string, r io.Reader) (file.UploadSessionInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	session, ok := f.sessions[sessionID]
	if !ok || session.ownerID != ownerID {
		return file.UploadSessionInfo{}, file.ErrUploadSessionNotFound
	}
	if !session.info.ExpiresAt.After(time.Now()) {
		return file.UploadSessionInfo{}, file.ErrUploadSessionExpired
	}
	if session.info.Mode == model.UploadModeChunked {
		return file.UploadSessionInfo{}, file.ErrUploadModeMismatch
	}
	if session.info.Status != model.UploadStatusPending {
		return file.UploadSessionInfo{}, file.ErrUploadSessionConflict
	}

	data, err := io.ReadAll(io.LimitReader(r, session.info.ExpectedSize+1))
	if err != nil {
		return file.UploadSessionInfo{}, err
	}
	if int64(len(data)) != session.info.ExpectedSize {
		return file.UploadSessionInfo{}, file.ErrUploadSizeMismatch
	}

	session.info.ActualSize = int64(len(data))
	session.info.Status = model.UploadStatusReady
	session.content = data
	f.sessions[sessionID] = session
	return session.info, nil
}

// UploadSessionChunk 模拟真实服务：只接受 chunked 会话，分片必须按顺序、
// 长度必须正好等于服务端算出的这一片大小。
func (f *fakeFileService) UploadSessionChunk(ctx context.Context, ownerID int, sessionID string, index int, r io.Reader) (file.UploadSessionInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	session, ok := f.sessions[sessionID]
	if !ok || session.ownerID != ownerID {
		return file.UploadSessionInfo{}, file.ErrUploadSessionNotFound
	}
	if !session.info.ExpiresAt.After(time.Now()) {
		return file.UploadSessionInfo{}, file.ErrUploadSessionExpired
	}
	if session.info.Mode != model.UploadModeChunked {
		return file.UploadSessionInfo{}, file.ErrUploadModeMismatch
	}
	if session.info.Status != model.UploadStatusPending {
		return file.UploadSessionInfo{}, file.ErrUploadSessionConflict
	}
	if index < 0 || index >= session.info.ChunkCount {
		return file.UploadSessionInfo{}, file.ErrInvalidChunkIndex
	}
	expectedIndex := int(session.info.ActualSize / session.info.ChunkSize)
	if index != expectedIndex {
		return file.UploadSessionInfo{}, file.ErrUploadSessionConflict
	}

	chunkBytes := session.info.ChunkSize
	if remaining := session.info.ExpectedSize - session.info.ActualSize; remaining < chunkBytes {
		chunkBytes = remaining
	}
	data, err := io.ReadAll(io.LimitReader(r, chunkBytes+1))
	if err != nil {
		return file.UploadSessionInfo{}, err
	}
	if int64(len(data)) != chunkBytes {
		return file.UploadSessionInfo{}, file.ErrUploadSizeMismatch
	}

	session.content = append(session.content, data...)
	session.info.ActualSize += int64(len(data))
	if session.info.ActualSize >= session.info.ExpectedSize {
		session.info.Status = model.UploadStatusReady
	}
	f.sessions[sessionID] = session
	return session.info, nil
}

// CompleteUploadSession 模拟真实服务：把 ready 会话的内容发布成一条正式 File。
// 已完成时先于过期检查返回原文件 ID；其他状态拒绝，同名冲突返回 409。
func (f *fakeFileService) CompleteUploadSession(ctx context.Context, ownerID int, sessionID string) (file.UploadSessionInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	session, ok := f.sessions[sessionID]
	if !ok || session.ownerID != ownerID {
		return file.UploadSessionInfo{}, file.ErrUploadSessionNotFound
	}

	if session.info.Status == model.UploadStatusCompleted {
		if session.info.ResultFileID == nil || *session.info.ResultFileID == "" {
			return file.UploadSessionInfo{}, file.ErrUploadSessionInconsistent
		}
		return session.info, nil
	}
	if session.info.Status != model.UploadStatusReady {
		return file.UploadSessionInfo{}, file.ErrUploadSessionNotCompletable
	}
	if !session.info.ExpiresAt.After(time.Now()) {
		return file.UploadSessionInfo{}, file.ErrUploadSessionExpired
	}
	if err := f.checkParent(ownerID, session.info.ParentID); err != nil {
		return file.UploadSessionInfo{}, err
	}
	if f.nameTaken(ownerID, session.info.ParentID, session.info.Name) {
		return file.UploadSessionInfo{}, file.ErrAlreadyExists
	}

	f.nextID++
	id := fmt.Sprintf("fake-%d", f.nextID)
	record := file.File{
		ID:   id,
		Name: session.info.Name,
		Size: session.info.ActualSize,
		Kind: model.KindFile,
	}
	if session.info.ParentID != "" {
		parentID := session.info.ParentID
		record.ParentID = &parentID
	}
	f.records[id] = record
	f.contents[id] = session.content
	f.owners[id] = ownerID

	session.info.Status = model.UploadStatusCompleted
	session.info.ResultFileID = &id
	f.sessions[sessionID] = session
	return session.info, nil
}

// CancelUploadSession 模拟真实服务：completed 返回冲突，其余状态改成 canceled。
// 幂等取消不报错；假实现同时丢掉尚未发布的会话内容。
func (f *fakeFileService) CancelUploadSession(ctx context.Context, ownerID int, sessionID string) (file.UploadSessionInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	session, ok := f.sessions[sessionID]
	if !ok || session.ownerID != ownerID {
		return file.UploadSessionInfo{}, file.ErrUploadSessionNotFound
	}
	if session.info.Status == model.UploadStatusCompleted {
		return file.UploadSessionInfo{}, file.ErrUploadSessionCompleted
	}

	session.info.Status = model.UploadStatusCanceled
	session.content = nil
	f.sessions[sessionID] = session
	return session.info, nil
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

// withParent 给请求加上 parent_id 查询参数。
func withParent(req *http.Request, parentID string) *http.Request {
	query := req.URL.Query()
	query.Set("parent_id", parentID)
	req.URL.RawQuery = query.Encode()
	return req
}

// createDir 通过 HTTP 在 parentID 下创建一个目录，默认以 A 登录。
func createDir(t *testing.T, router http.Handler, parentID, name string) file.File {
	t.Helper()

	payload, err := json.Marshal(createDirectoryRequest{ParentID: parentID, Name: name})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/directories", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	authorize(req, testTokenA)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("create directory: wanted 201, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var record file.File
	if err := json.Unmarshal(recorder.Body.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	return record
}

// uploadInto 把文件上传到指定目录。
func uploadInto(t *testing.T, router http.Handler, parentID, filename string, content []byte) file.File {
	t.Helper()

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, withParent(uploadRequest(t, "file", filename, content), parentID))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("upload into %q: wanted 201, got %d: %s", parentID, recorder.Code, recorder.Body.String())
	}

	var record file.File
	if err := json.Unmarshal(recorder.Body.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	return record
}

// listChildren 调用 GET /api/files 并解析响应。
func listChildren(t *testing.T, router http.Handler, parentID, page, pageSize string) file.ListResult {
	t.Helper()

	target := "/api/files?page=" + page + "&page_size=" + pageSize
	if parentID != "" {
		target += "&parent_id=" + parentID
	}
	req := httptest.NewRequest(http.MethodGet, target, nil)
	authorize(req, testTokenA)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("list files: wanted 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var result file.ListResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

// errorCode 解析错误响应里的 code。
func errorCode(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()
	var response ErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	return response.Code
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
		httptest.NewRequest(http.MethodGet, "/api/files", nil),
		httptest.NewRequest(http.MethodGet, "/api/files/whatever", nil),
		httptest.NewRequest(http.MethodDelete, "/api/files/whatever", nil),
		httptest.NewRequest(http.MethodPost, "/api/directories", strings.NewReader(`{"name":"docs"}`)),
		newCopyRequest(t, testTokenA, "whatever", "", "copy.txt"),
		newRenameRequest(t, testTokenA, "whatever", "renamed.txt"),
		newMoveRequest(t, testTokenA, "whatever", ""),
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

// TestCreateDirectoryAndList 覆盖目录接口的闭环：
// 建目录 → 往里传文件 → 列目录 → 列根目录。
func TestCreateDirectoryAndList(t *testing.T) {
	router, _ := newTestRouter(t)

	dir := createDir(t, router, "", "学习资料")
	if dir.ID == "" || dir.Name != "学习资料" || dir.Kind != model.KindDirectory || dir.Size != 0 {
		t.Fatalf("unexpected directory: %+v", dir)
	}

	uploaded := uploadInto(t, router, dir.ID, "笔记.txt", []byte("hello"))
	if uploaded.ParentID == nil || *uploaded.ParentID != dir.ID {
		t.Fatalf("uploaded parent = %v, want %q", uploaded.ParentID, dir.ID)
	}

	list := listChildren(t, router, dir.ID, "1", "20")
	if list.Total != 1 || len(list.Items) != 1 {
		t.Fatalf("unexpected directory listing: %+v", list)
	}
	if item := list.Items[0]; item.ID != uploaded.ID || item.Name != "笔记.txt" ||
		item.Size != 5 || item.Kind != model.KindFile {
		t.Fatalf("unexpected child: %+v", list.Items[0])
	}

	// 根目录只看得到这个目录，看不到目录里的文件。
	rootList := listChildren(t, router, "", "1", "20")
	if rootList.Total != 1 || len(rootList.Items) != 1 || rootList.Items[0].ID != dir.ID {
		t.Fatalf("unexpected root listing: %+v", rootList)
	}

	// 第二页为空，但 total 仍然是 1。
	emptyPage := listChildren(t, router, dir.ID, "2", "1")
	if emptyPage.Total != 1 || len(emptyPage.Items) != 0 {
		t.Fatalf("unexpected second page: %+v", emptyPage)
	}
}

func TestCreateDirectoryDuplicateName(t *testing.T) {
	router, _ := newTestRouter(t)
	createDir(t, router, "", "docs")

	payload, err := json.Marshal(createDirectoryRequest{Name: "docs"})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/directories", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	authorize(req, testTokenA)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("wanted 409, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if code := errorCode(t, recorder); code != "already_exists" {
		t.Fatalf("code = %q, want already_exists", code)
	}
}

func TestDirectoryDownloadRejected(t *testing.T) {
	router, _ := newTestRouter(t)
	dir := createDir(t, router, "", "docs")

	download := httptest.NewRecorder()
	router.ServeHTTP(download, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+dir.ID, nil), testTokenA))
	if download.Code != http.StatusBadRequest {
		t.Fatalf("download directory: wanted 400, got %d: %s", download.Code, download.Body.String())
	}
	if code := errorCode(t, download); code != "is_directory" {
		t.Fatalf("code = %q, want is_directory", code)
	}
}

func TestDirectoryDelete(t *testing.T) {
	router, fake := newTestRouter(t)
	dir := createDir(t, router, "", "docs")
	uploadInto(t, router, dir.ID, "a.txt", []byte("x"))

	// 非空目录：409 directory_not_empty。
	nonEmpty := httptest.NewRecorder()
	router.ServeHTTP(nonEmpty, authorize(httptest.NewRequest(http.MethodDelete, "/api/files/"+dir.ID, nil), testTokenA))
	if nonEmpty.Code != http.StatusConflict {
		t.Fatalf("delete non-empty directory: wanted 409, got %d: %s", nonEmpty.Code, nonEmpty.Body.String())
	}
	if code := errorCode(t, nonEmpty); code != "directory_not_empty" {
		t.Fatalf("code = %q, want directory_not_empty", code)
	}

	// 清掉子文件后，空目录可以删除。
	list := listChildren(t, router, dir.ID, "1", "20")
	for _, item := range list.Items {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, authorize(httptest.NewRequest(http.MethodDelete, "/api/files/"+item.ID, nil), testTokenA))
		if recorder.Code != http.StatusNoContent {
			t.Fatalf("delete child %s: wanted 204, got %d", item.ID, recorder.Code)
		}
	}
	remove := httptest.NewRecorder()
	router.ServeHTTP(remove, authorize(httptest.NewRequest(http.MethodDelete, "/api/files/"+dir.ID, nil), testTokenA))
	if remove.Code != http.StatusNoContent {
		t.Fatalf("delete empty directory: wanted 204, got %d: %s", remove.Code, remove.Body.String())
	}
	if _, ok := fake.records[dir.ID]; ok {
		t.Fatal("directory is still stored after delete")
	}
}

func TestDeleteRootDirectoryRejected(t *testing.T) {
	router, fake := newTestRouter(t)
	fake.roots["root-a"] = testUserAID

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, authorize(httptest.NewRequest(http.MethodDelete, "/api/files/root-a", nil), testTokenA))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("delete root: wanted 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if code := errorCode(t, recorder); code != "root_directory" {
		t.Fatalf("code = %q, want root_directory", code)
	}
}

func TestParentValidation(t *testing.T) {
	router, _ := newTestRouter(t)
	fileRecord := upload(t, router, []byte("x"))

	// parent_id 指向普通文件：400 not_a_directory。
	intoFile := httptest.NewRecorder()
	router.ServeHTTP(intoFile, withParent(uploadRequest(t, "file", "child.txt", []byte("y")), fileRecord.ID))
	if intoFile.Code != http.StatusBadRequest {
		t.Fatalf("upload into a file: wanted 400, got %d: %s", intoFile.Code, intoFile.Body.String())
	}
	if code := errorCode(t, intoFile); code != "not_a_directory" {
		t.Fatalf("code = %q, want not_a_directory", code)
	}

	// parent_id 根本不存在：404 parent_not_found。
	missing := httptest.NewRecorder()
	router.ServeHTTP(missing, withParent(uploadRequest(t, "file", "a.txt", []byte("x")), "does-not-exist"))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("upload into missing directory: wanted 404, got %d", missing.Code)
	}
	if code := errorCode(t, missing); code != "parent_not_found" {
		t.Fatalf("code = %q, want parent_not_found", code)
	}

	// 列一个不存在的目录同样 404。
	listMissing := httptest.NewRecorder()
	router.ServeHTTP(listMissing, authorize(
		httptest.NewRequest(http.MethodGet, "/api/files?parent_id=does-not-exist", nil), testTokenA))
	if listMissing.Code != http.StatusNotFound {
		t.Fatalf("list missing directory: wanted 404, got %d", listMissing.Code)
	}

	// 在别人的目录下建目录：假实现里 B 不是父目录的 owner，等价于不存在。
	dir := createDir(t, router, "", "mine")
	payload, err := json.Marshal(createDirectoryRequest{ParentID: dir.ID, Name: "hack"})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/directories", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	authorize(req, testTokenB)
	foreign := httptest.NewRecorder()
	router.ServeHTTP(foreign, req)
	if foreign.Code != http.StatusNotFound {
		t.Fatalf("B create in A's directory: wanted 404, got %d: %s", foreign.Code, foreign.Body.String())
	}
}

func TestListPaginationValidation(t *testing.T) {
	router, _ := newTestRouter(t)

	for _, target := range []string{
		"/api/files?page=abc",
		"/api/files?page_size=abc",
	} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, authorize(httptest.NewRequest(http.MethodGet, target, nil), testTokenA))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("%s: wanted 400, got %d", target, recorder.Code)
		}
		if code := errorCode(t, recorder); code != "invalid_pagination" {
			t.Fatalf("%s: code = %q, want invalid_pagination", target, code)
		}
	}
}

func TestUploadDuplicateName(t *testing.T) {
	router, _ := newTestRouter(t)
	upload(t, router, []byte("one"))

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, uploadRequest(t, "file", "test.bin", []byte("two")))

	if recorder.Code != http.StatusConflict {
		t.Fatalf("wanted 409, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if code := errorCode(t, recorder); code != "already_exists" {
		t.Fatalf("code = %q, want already_exists", code)
	}
}

// newCopyRequest 构造 POST /api/files/:id/copy 请求。
func newCopyRequest(t *testing.T, token, id, parentID, name string) *http.Request {
	t.Helper()

	payload, err := json.Marshal(copyFileRequest{ParentID: parentID, Name: name})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/files/"+id+"/copy", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	return authorize(req, token)
}

// TestCopyFile 覆盖复制接口的闭环：
// 上传 → 复制到另一个目录 → 源文件和副本都能下载 → 删掉源文件后副本内容还在。
func TestCopyFile(t *testing.T) {
	router, _ := newTestRouter(t)
	content := []byte("copy me\n")
	source := upload(t, router, content)
	target := createDir(t, router, "", "备份资料")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, newCopyRequest(t, testTokenA, source.ID, target.ID, "笔记-副本.txt"))
	if recorder.Code != http.StatusCreated {
		t.Fatalf("wanted 201, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var copied file.File
	if err := json.Unmarshal(recorder.Body.Bytes(), &copied); err != nil {
		t.Fatal(err)
	}
	if copied.ID == source.ID || copied.ID == "" {
		t.Fatalf("copy must get its own id, got %q (source %q)", copied.ID, source.ID)
	}
	if copied.Name != "笔记-副本.txt" || copied.Size != source.Size || copied.Kind != model.KindFile {
		t.Fatalf("unexpected copy: %+v", copied)
	}
	if copied.ParentID == nil || *copied.ParentID != target.ID {
		t.Fatalf("copy parent = %v, want %q", copied.ParentID, target.ID)
	}

	// 源文件和副本都能读到原内容。
	for _, id := range []string{source.ID, copied.ID} {
		download := httptest.NewRecorder()
		router.ServeHTTP(download, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+id, nil), testTokenA))
		if download.Code != http.StatusOK {
			t.Fatalf("download %s: wanted 200, got %d", id, download.Code)
		}
		if !bytes.Equal(download.Body.Bytes(), content) {
			t.Fatalf("download %s: content = %q, want %q", id, download.Body.Bytes(), content)
		}
	}

	// 删掉源文件后，副本仍然可以下载。
	remove := httptest.NewRecorder()
	router.ServeHTTP(remove, authorize(httptest.NewRequest(http.MethodDelete, "/api/files/"+source.ID, nil), testTokenA))
	if remove.Code != http.StatusNoContent {
		t.Fatalf("delete source: wanted 204, got %d", remove.Code)
	}
	after := httptest.NewRecorder()
	router.ServeHTTP(after, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+copied.ID, nil), testTokenA))
	if after.Code != http.StatusOK || !bytes.Equal(after.Body.Bytes(), content) {
		t.Fatalf("copy was damaged by deleting source: status=%d body=%q", after.Code, after.Body.Bytes())
	}
}

func TestCopyFileErrors(t *testing.T) {
	router, _ := newTestRouter(t)
	source := upload(t, router, []byte("x"))
	target := createDir(t, router, "", "target")

	// 同名副本：409 already_exists。
	first := httptest.NewRecorder()
	router.ServeHTTP(first, newCopyRequest(t, testTokenA, source.ID, target.ID, "same.txt"))
	if first.Code != http.StatusCreated {
		t.Fatalf("first copy: wanted 201, got %d: %s", first.Code, first.Body.String())
	}
	duplicate := httptest.NewRecorder()
	router.ServeHTTP(duplicate, newCopyRequest(t, testTokenA, source.ID, target.ID, "same.txt"))
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate copy: wanted 409, got %d: %s", duplicate.Code, duplicate.Body.String())
	}
	if code := errorCode(t, duplicate); code != "already_exists" {
		t.Fatalf("code = %q, want already_exists", code)
	}

	// 源文件不存在：404。
	missing := httptest.NewRecorder()
	router.ServeHTTP(missing, newCopyRequest(t, testTokenA, "does-not-exist", target.ID, "copy.txt"))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing source: wanted 404, got %d", missing.Code)
	}
	if code := errorCode(t, missing); code != "file_or_parent_not_found" {
		t.Fatalf("code = %q, want file_or_parent_not_found", code)
	}

	// 目标目录不存在：404。
	missingParent := httptest.NewRecorder()
	router.ServeHTTP(missingParent, newCopyRequest(t, testTokenA, source.ID, "does-not-exist", "copy.txt"))
	if missingParent.Code != http.StatusNotFound {
		t.Fatalf("missing target: wanted 404, got %d", missingParent.Code)
	}

	// 目标 parent_id 指向普通文件：400 not_a_directory。
	intoFile := httptest.NewRecorder()
	router.ServeHTTP(intoFile, newCopyRequest(t, testTokenA, source.ID, source.ID, "copy.txt"))
	if intoFile.Code != http.StatusBadRequest {
		t.Fatalf("copy into a file: wanted 400, got %d: %s", intoFile.Code, intoFile.Body.String())
	}
	if code := errorCode(t, intoFile); code != "not_a_directory" {
		t.Fatalf("code = %q, want not_a_directory", code)
	}

	// 复制目录：400 is_directory。
	dirCopy := httptest.NewRecorder()
	router.ServeHTTP(dirCopy, newCopyRequest(t, testTokenA, target.ID, target.ID, "dir-copy"))
	if dirCopy.Code != http.StatusBadRequest {
		t.Fatalf("copy directory: wanted 400, got %d: %s", dirCopy.Code, dirCopy.Body.String())
	}
	if code := errorCode(t, dirCopy); code != "is_directory" {
		t.Fatalf("code = %q, want is_directory", code)
	}
}

func TestCopyFileOwnershipIsolation(t *testing.T) {
	router, _ := newTestRouter(t)
	source := upload(t, router, []byte("secret"))
	target := createDir(t, router, "", "target")

	// B 拿着 A 的文件 ID 复制：表现为源文件不存在。
	byB := httptest.NewRecorder()
	router.ServeHTTP(byB, newCopyRequest(t, testTokenB, source.ID, target.ID, "stolen.txt"))
	if byB.Code != http.StatusNotFound {
		t.Fatalf("B copy A's file: wanted 404, got %d: %s", byB.Code, byB.Body.String())
	}

	// B 复制自己的文件到 A 的目录：表现为目标目录不存在。
	breq := uploadRequest(t, "file", "b.txt", []byte("b"))
	breq.Header.Set("Authorization", "Bearer "+testTokenB)
	bup := httptest.NewRecorder()
	router.ServeHTTP(bup, breq)
	if bup.Code != http.StatusCreated {
		t.Fatalf("B upload: wanted 201, got %d: %s", bup.Code, bup.Body.String())
	}
	var bfile file.File
	if err := json.Unmarshal(bup.Body.Bytes(), &bfile); err != nil {
		t.Fatal(err)
	}
	bToA := httptest.NewRecorder()
	router.ServeHTTP(bToA, newCopyRequest(t, testTokenB, bfile.ID, target.ID, "hack.txt"))
	if bToA.Code != http.StatusNotFound {
		t.Fatalf("B copy into A's directory: wanted 404, got %d: %s", bToA.Code, bToA.Body.String())
	}

	// A 的根目录没有被 B 改动：仍然只有源文件和 target 目录。
	list := listChildren(t, router, "", "1", "20")
	if list.Total != 2 {
		t.Fatalf("A's root was modified by B: %+v", list)
	}
	for _, item := range list.Items {
		if item.Name == "stolen.txt" || item.Name == "hack.txt" {
			t.Fatalf("B created an entry in A's tree: %+v", item)
		}
	}
}

// newRenameRequest 构造 PATCH /api/files/:id 请求。
func newRenameRequest(t *testing.T, token, id, name string) *http.Request {
	t.Helper()

	payload, err := json.Marshal(renameFileRequest{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPatch, "/api/files/"+id, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	return authorize(req, token)
}

// TestRenameFile 覆盖普通文件改名的闭环：
// 上传 → 改名 → 元数据变化但 ID、大小、内容和父目录不变。
func TestRenameFile(t *testing.T) {
	router, _ := newTestRouter(t)
	content := []byte("rename must not touch content\n")
	record := upload(t, router, content)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, newRenameRequest(t, testTokenA, record.ID, "新名字.txt"))
	if recorder.Code != http.StatusOK {
		t.Fatalf("wanted 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var renamed file.File
	if err := json.Unmarshal(recorder.Body.Bytes(), &renamed); err != nil {
		t.Fatal(err)
	}
	if renamed.ID != record.ID || renamed.Name != "新名字.txt" {
		t.Fatalf("unexpected renamed record: %+v", renamed)
	}
	if renamed.Size != record.Size || renamed.Kind != model.KindFile {
		t.Fatalf("rename changed size or kind: %+v", renamed)
	}
	if !sameParent(renamed.ParentID, "") || !sameParent(record.ParentID, "") {
		t.Fatalf("rename changed parent: %v -> %v", record.ParentID, renamed.ParentID)
	}

	// 下载内容不变，Content-Disposition 跟着新名字走。
	download := httptest.NewRecorder()
	router.ServeHTTP(download, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+record.ID, nil), testTokenA))
	if download.Code != http.StatusOK || !bytes.Equal(download.Body.Bytes(), content) {
		t.Fatalf("download after rename: status=%d body=%q", download.Code, download.Body.Bytes())
	}
	_, params, err := mime.ParseMediaType(download.Header().Get("Content-Disposition"))
	if err != nil {
		t.Fatalf("parse Content-Disposition: %v", err)
	}
	if params["filename"] != "新名字.txt" {
		t.Fatalf("Content-Disposition filename = %q, want 新名字.txt", params["filename"])
	}

	// 列表里看到的也是新名字。
	list := listChildren(t, router, "", "1", "20")
	if list.Total != 1 || list.Items[0].Name != "新名字.txt" {
		t.Fatalf("unexpected listing after rename: %+v", list)
	}
}

// TestRenameDirectory 目录也能改名，里面的文件不受影响。
func TestRenameDirectory(t *testing.T) {
	router, _ := newTestRouter(t)
	dir := createDir(t, router, "", "旧目录")
	child := uploadInto(t, router, dir.ID, "笔记.txt", []byte("inside"))

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, newRenameRequest(t, testTokenA, dir.ID, "新目录"))
	if recorder.Code != http.StatusOK {
		t.Fatalf("wanted 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var renamed file.File
	if err := json.Unmarshal(recorder.Body.Bytes(), &renamed); err != nil {
		t.Fatal(err)
	}
	if renamed.ID != dir.ID || renamed.Name != "新目录" || renamed.Kind != model.KindDirectory {
		t.Fatalf("unexpected renamed directory: %+v", renamed)
	}

	// 改名不影响目录里的内容。
	list := listChildren(t, router, dir.ID, "1", "20")
	if list.Total != 1 || list.Items[0].ID != child.ID || list.Items[0].Name != "笔记.txt" {
		t.Fatalf("directory children changed after rename: %+v", list)
	}
}

func TestRenameDuplicateName(t *testing.T) {
	router, _ := newTestRouter(t)
	first := uploadInto(t, router, "", "a.txt", []byte("one"))
	second := uploadInto(t, router, "", "b.txt", []byte("two"))
	dir := createDir(t, router, "", "docs")

	cases := []struct {
		id   string
		name string
	}{
		{second.ID, "a.txt"}, // 文件撞文件
		{second.ID, "docs"},  // 文件撞目录
		{dir.ID, "a.txt"},    // 目录撞文件
	}
	for _, tc := range cases {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, newRenameRequest(t, testTokenA, tc.id, tc.name))
		if recorder.Code != http.StatusConflict {
			t.Fatalf("rename %s to %q: wanted 409, got %d: %s", tc.id, tc.name, recorder.Code, recorder.Body.String())
		}
		if code := errorCode(t, recorder); code != "already_exists" {
			t.Fatalf("code = %q, want already_exists", code)
		}
	}

	// 冲突之后两个文件都还在，名字和内容都没变。
	for id, want := range map[string]string{first.ID: "one", second.ID: "two"} {
		download := httptest.NewRecorder()
		router.ServeHTTP(download, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+id, nil), testTokenA))
		if download.Code != http.StatusOK {
			t.Fatalf("download %s after conflict: wanted 200, got %d", id, download.Code)
		}
		if got := download.Body.String(); got != want {
			t.Fatalf("content of %s changed after failed renames: got %q, want %q", id, got, want)
		}
	}
	list := listChildren(t, router, "", "1", "20")
	names := make(map[string]bool, len(list.Items))
	for _, item := range list.Items {
		names[item.Name] = true
	}
	if !names["a.txt"] || !names["b.txt"] || !names["docs"] {
		t.Fatalf("rename conflict changed the directory: %+v", list)
	}

	// 改成现在的名字是幂等的，不算重名。
	same := httptest.NewRecorder()
	router.ServeHTTP(same, newRenameRequest(t, testTokenA, first.ID, "a.txt"))
	if same.Code != http.StatusOK {
		t.Fatalf("renaming to the current name: wanted 200, got %d: %s", same.Code, same.Body.String())
	}
}

// TestRenameOwnershipIsolation 证明 B 知道 A 的 ID 也改不了 A 的节点。
func TestRenameOwnershipIsolation(t *testing.T) {
	router, _ := newTestRouter(t)
	content := []byte("only A can rename this")
	record := upload(t, router, content)

	byB := httptest.NewRecorder()
	router.ServeHTTP(byB, newRenameRequest(t, testTokenB, record.ID, "stolen.txt"))
	if byB.Code != http.StatusNotFound {
		t.Fatalf("B rename A's file: wanted 404, got %d: %s", byB.Code, byB.Body.String())
	}
	if code := errorCode(t, byB); code != "file_not_found" {
		t.Fatalf("code = %q, want file_not_found", code)
	}

	// A 的文件没被改动，内容也完好。
	download := httptest.NewRecorder()
	router.ServeHTTP(download, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+record.ID, nil), testTokenA))
	if download.Code != http.StatusOK || !bytes.Equal(download.Body.Bytes(), content) {
		t.Fatalf("A's file was damaged by B: status=%d body=%q", download.Code, download.Body.Bytes())
	}
	list := listChildren(t, router, "", "1", "20")
	if list.Total != 1 || list.Items[0].Name != record.Name {
		t.Fatalf("A's file name changed by B: %+v", list)
	}

	// 真正的所有者仍然可以改名。
	byA := httptest.NewRecorder()
	router.ServeHTTP(byA, newRenameRequest(t, testTokenA, record.ID, "mine.txt"))
	if byA.Code != http.StatusOK {
		t.Fatalf("A rename own file: wanted 200, got %d: %s", byA.Code, byA.Body.String())
	}
}

func TestRenameInvalidName(t *testing.T) {
	router, _ := newTestRouter(t)
	record := upload(t, router, []byte("x"))

	for _, name := range []string{"", ".", "..", "a/b", `a\b`, "a\x00b"} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, newRenameRequest(t, testTokenA, record.ID, name))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("rename to %q: wanted 400, got %d: %s", name, recorder.Code, recorder.Body.String())
		}
		if code := errorCode(t, recorder); code != "invalid_name" {
			t.Fatalf("rename to %q: code = %q, want invalid_name", name, code)
		}
	}

	// 名字没有被改掉。
	list := listChildren(t, router, "", "1", "20")
	if list.Total != 1 || list.Items[0].Name != record.Name {
		t.Fatalf("invalid names changed the entry: %+v", list)
	}
}

func TestRenameRootDirectoryRejected(t *testing.T) {
	router, fake := newTestRouter(t)
	fake.roots["root-a"] = testUserAID

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, newRenameRequest(t, testTokenA, "root-a", "hacked"))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("rename root: wanted 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if code := errorCode(t, recorder); code != "root_directory" {
		t.Fatalf("code = %q, want root_directory", code)
	}

	// B 连“这是根目录”都不该知道，统一按不存在处理。
	byB := httptest.NewRecorder()
	router.ServeHTTP(byB, newRenameRequest(t, testTokenB, "root-a", "hacked"))
	if byB.Code != http.StatusNotFound {
		t.Fatalf("B rename A's root: wanted 404, got %d: %s", byB.Code, byB.Body.String())
	}
}

func TestRenameErrors(t *testing.T) {
	router, _ := newTestRouter(t)

	// ID 不存在：404。
	missing := httptest.NewRecorder()
	router.ServeHTTP(missing, newRenameRequest(t, testTokenA, "does-not-exist", "x.txt"))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing id: wanted 404, got %d: %s", missing.Code, missing.Body.String())
	}

	record := upload(t, router, []byte("x"))

	// 请求体不是 JSON：400 invalid_json。
	badJSON := httptest.NewRequest(http.MethodPatch, "/api/files/"+record.ID, strings.NewReader("not json"))
	badJSON.Header.Set("Content-Type", "application/json")
	authorize(badJSON, testTokenA)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, badJSON)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid json: wanted 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if code := errorCode(t, recorder); code != "invalid_json" {
		t.Fatalf("code = %q, want invalid_json", code)
	}
}

// newMoveRequest 构造 POST /api/files/:id/move 请求。
func newMoveRequest(t *testing.T, token, id, parentID string) *http.Request {
	t.Helper()

	payload, err := json.Marshal(moveFileRequest{ParentID: parentID})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/files/"+id+"/move", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	return authorize(req, token)
}

// TestMoveFile 覆盖文件移动的闭环：旧目录不可见、新目录可见、下载内容不变。
func TestMoveFile(t *testing.T) {
	router, _ := newTestRouter(t)
	content := []byte("move must not touch content\n")
	record := upload(t, router, content)
	target := createDir(t, router, "", "目标目录")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, newMoveRequest(t, testTokenA, record.ID, target.ID))
	if recorder.Code != http.StatusOK {
		t.Fatalf("wanted 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var moved file.File
	if err := json.Unmarshal(recorder.Body.Bytes(), &moved); err != nil {
		t.Fatal(err)
	}
	if moved.ID != record.ID || moved.Name != record.Name || moved.Size != record.Size {
		t.Fatalf("unexpected moved record: %+v", moved)
	}
	if moved.ParentID == nil || *moved.ParentID != target.ID {
		t.Fatalf("moved parent = %v, want %q", moved.ParentID, target.ID)
	}

	// 旧目录（根）里只剩目标目录，新目录里是移动后的文件。
	rootList := listChildren(t, router, "", "1", "20")
	if rootList.Total != 1 || rootList.Items[0].ID != target.ID {
		t.Fatalf("old directory still lists the moved file: %+v", rootList)
	}
	targetList := listChildren(t, router, target.ID, "1", "20")
	if targetList.Total != 1 || targetList.Items[0].ID != record.ID {
		t.Fatalf("new directory does not list the moved file: %+v", targetList)
	}

	download := httptest.NewRecorder()
	router.ServeHTTP(download, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+record.ID, nil), testTokenA))
	if download.Code != http.StatusOK || !bytes.Equal(download.Body.Bytes(), content) {
		t.Fatalf("download after move: status=%d body=%q", download.Code, download.Body.Bytes())
	}

	// 移回根目录：parent_id 为空表示当前用户的根目录。
	toRoot := httptest.NewRecorder()
	router.ServeHTTP(toRoot, newMoveRequest(t, testTokenA, record.ID, ""))
	if toRoot.Code != http.StatusOK {
		t.Fatalf("move to root: wanted 200, got %d: %s", toRoot.Code, toRoot.Body.String())
	}
	if got := listChildren(t, router, target.ID, "1", "20"); got.Total != 0 {
		t.Fatalf("target should be empty after moving to root: %+v", got)
	}
}

// TestMoveDirectory 目录移动带着子树走；移到自身或后代被拒绝。
func TestMoveDirectory(t *testing.T) {
	router, _ := newTestRouter(t)
	outer := createDir(t, router, "", "outer")
	inner := createDir(t, router, outer.ID, "inner")
	note := uploadInto(t, router, inner.ID, "a.txt", []byte("hello"))
	target := createDir(t, router, "", "target")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, newMoveRequest(t, testTokenA, outer.ID, target.ID))
	if recorder.Code != http.StatusOK {
		t.Fatalf("move directory: wanted 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var moved file.File
	if err := json.Unmarshal(recorder.Body.Bytes(), &moved); err != nil {
		t.Fatal(err)
	}
	if moved.ID != outer.ID || moved.Kind != model.KindDirectory || moved.ParentID == nil || *moved.ParentID != target.ID {
		t.Fatalf("unexpected moved directory: %+v", moved)
	}

	// 子树仍然可达，内容可下载。
	if got := listChildren(t, router, target.ID, "1", "20"); got.Total != 1 || got.Items[0].ID != outer.ID {
		t.Fatalf("unexpected target listing: %+v", got)
	}
	if got := listChildren(t, router, inner.ID, "1", "20"); got.Total != 1 || got.Items[0].ID != note.ID {
		t.Fatalf("unexpected inner listing: %+v", got)
	}
	download := httptest.NewRecorder()
	router.ServeHTTP(download, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+note.ID, nil), testTokenA))
	if download.Code != http.StatusOK || download.Body.String() != "hello" {
		t.Fatalf("download after directory move: status=%d body=%q", download.Code, download.Body.String())
	}

	// 移到自己或自己的后代：400 invalid_move。
	for _, parentID := range []string{outer.ID, inner.ID} {
		cycle := httptest.NewRecorder()
		router.ServeHTTP(cycle, newMoveRequest(t, testTokenA, outer.ID, parentID))
		if cycle.Code != http.StatusBadRequest {
			t.Fatalf("move %s into %s: wanted 400, got %d: %s", outer.ID, parentID, cycle.Code, cycle.Body.String())
		}
		if code := errorCode(t, cycle); code != "invalid_move" {
			t.Fatalf("code = %q, want invalid_move", code)
		}
	}
}

func TestMoveDuplicateName(t *testing.T) {
	router, _ := newTestRouter(t)
	left := createDir(t, router, "", "left")
	right := createDir(t, router, "", "right")
	uploadInto(t, router, left.ID, "same.txt", []byte("one"))
	existing := uploadInto(t, router, right.ID, "same.txt", []byte("two"))

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, newMoveRequest(t, testTokenA, existing.ID, left.ID))
	if recorder.Code != http.StatusConflict {
		t.Fatalf("wanted 409, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if code := errorCode(t, recorder); code != "already_exists" {
		t.Fatalf("code = %q, want already_exists", code)
	}

	// 冲突后目标目录里仍只有原来的同名文件。
	if got := listChildren(t, router, right.ID, "1", "20"); got.Total != 1 || got.Items[0].ID != existing.ID {
		t.Fatalf("target changed after failed move: %+v", got)
	}
}

func TestMoveOwnershipIsolation(t *testing.T) {
	router, fake := newTestRouter(t)
	record := upload(t, router, []byte("secret"))
	aDir := createDir(t, router, "", "a-dir")

	// B 移动 A 的文件：404。
	byB := httptest.NewRecorder()
	router.ServeHTTP(byB, newMoveRequest(t, testTokenB, record.ID, aDir.ID))
	if byB.Code != http.StatusNotFound {
		t.Fatalf("B move A's file: wanted 404, got %d: %s", byB.Code, byB.Body.String())
	}

	// B 把自己的文件移进 A 的目录：目标表现为不存在。
	bRequest := uploadRequest(t, "file", "b.txt", []byte("b"))
	bRequest.Header.Set("Authorization", "Bearer "+testTokenB)
	bUploaded := httptest.NewRecorder()
	router.ServeHTTP(bUploaded, bRequest)
	if bUploaded.Code != http.StatusCreated {
		t.Fatalf("B upload: wanted 201, got %d", bUploaded.Code)
	}
	var bFile file.File
	if err := json.Unmarshal(bUploaded.Body.Bytes(), &bFile); err != nil {
		t.Fatal(err)
	}
	intoA := httptest.NewRecorder()
	router.ServeHTTP(intoA, newMoveRequest(t, testTokenB, bFile.ID, aDir.ID))
	if intoA.Code != http.StatusNotFound {
		t.Fatalf("B move into A's directory: wanted 404, got %d: %s", intoA.Code, intoA.Body.String())
	}

	// A 的文件没有被移动，内容完好。
	if got := listChildren(t, router, "", "1", "20"); got.Total != 2 {
		t.Fatalf("A's root was modified by B: %+v", got)
	}
	download := httptest.NewRecorder()
	router.ServeHTTP(download, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+record.ID, nil), testTokenA))
	if download.Code != http.StatusOK || download.Body.String() != "secret" {
		t.Fatalf("A's file was damaged by B: status=%d body=%q", download.Code, download.Body.String())
	}

	// 移动根目录：400 root_directory。
	fake.roots["root-a"] = testUserAID
	rootMove := httptest.NewRecorder()
	router.ServeHTTP(rootMove, newMoveRequest(t, testTokenA, "root-a", aDir.ID))
	if rootMove.Code != http.StatusBadRequest {
		t.Fatalf("move root: wanted 400, got %d: %s", rootMove.Code, rootMove.Body.String())
	}
	if code := errorCode(t, rootMove); code != "root_directory" {
		t.Fatalf("code = %q, want root_directory", code)
	}
}

func TestMoveErrors(t *testing.T) {
	router, _ := newTestRouter(t)
	record := upload(t, router, []byte("x"))

	// 源不存在：404。
	missing := httptest.NewRecorder()
	router.ServeHTTP(missing, newMoveRequest(t, testTokenA, "does-not-exist", ""))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing source: wanted 404, got %d: %s", missing.Code, missing.Body.String())
	}
	if code := errorCode(t, missing); code != "file_or_parent_not_found" {
		t.Fatalf("code = %q, want file_or_parent_not_found", code)
	}

	// 目标不存在：404。
	missingTarget := httptest.NewRecorder()
	router.ServeHTTP(missingTarget, newMoveRequest(t, testTokenA, record.ID, "does-not-exist"))
	if missingTarget.Code != http.StatusNotFound {
		t.Fatalf("missing target: wanted 404, got %d", missingTarget.Code)
	}

	// 目标是普通文件：400 not_a_directory。
	intoFile := httptest.NewRecorder()
	router.ServeHTTP(intoFile, newMoveRequest(t, testTokenA, record.ID, record.ID))
	if intoFile.Code != http.StatusBadRequest {
		t.Fatalf("move into a file: wanted 400, got %d: %s", intoFile.Code, intoFile.Body.String())
	}
	if code := errorCode(t, intoFile); code != "not_a_directory" {
		t.Fatalf("code = %q, want not_a_directory", code)
	}

	// 请求体不是 JSON：400 invalid_json。
	badJSON := httptest.NewRequest(http.MethodPost, "/api/files/"+record.ID+"/move", strings.NewReader("not json"))
	badJSON.Header.Set("Content-Type", "application/json")
	authorize(badJSON, testTokenA)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, badJSON)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid json: wanted 400, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if code := errorCode(t, recorder); code != "invalid_json" {
		t.Fatalf("code = %q, want invalid_json", code)
	}
}
