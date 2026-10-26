package file_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"luckyCloudFile/internal/auth"
	"luckyCloudFile/internal/database"
	cloudfile "luckyCloudFile/internal/file"
	"luckyCloudFile/internal/httpapi"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
)

const (
	testUsernameA = "A"
	testUsernameB = "B"
	testPasswordA = "A-test-password"
	testPasswordB = "B-test-password"
)

// 这是本阶段的端到端验收：
//
//	显式创建 A/B → 未登录被拒绝 → A 登录/上传/下载 → B 用同一个文件 ID 失败
//	→ B 的失败不影响 A → A 删除自己的文件 → A 退出后旧 Token 失效
//	→ A 重新登录后让 Token 过期，旧 Token 再次失效。
//
// 它使用真实 MySQL 和临时磁盘目录，只有 HTTP 请求是进程内 httptest。
// 没有设置 CLOUDREVE_TEST_MYSQL_DSN 时会跳过。
func TestAuthenticationAndFileIsolation(t *testing.T) {
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run the auth isolation flow")
	}

	ctx := context.Background()
	client, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close(client) })
	resetAuthTables(t, client)

	// 账号必须显式创建；清空后数据库里应为 0 个用户。
	if count := countRows(t, client, &model.User{}); count != 0 {
		t.Fatalf("clean database should have no users, got %d", count)
	}
	for _, account := range []struct {
		username string
		password string
	}{
		{testUsernameA, testPasswordA},
		{testUsernameB, testPasswordB},
	} {
		if _, err := auth.CreateUser(ctx, client, account.username, account.password); err != nil {
			t.Fatal(err)
		}
	}
	// 同名用户不能静默覆盖密码。
	if _, err := auth.CreateUser(ctx, client, testUsernameA, "another-password"); !errors.Is(err, auth.ErrUserExists) {
		t.Fatalf("duplicate username: wanted ErrUserExists, got %v", err)
	}

	userA := mustUser(t, client, testUsernameA)
	userB := mustUser(t, client, testUsernameB)
	if userA.PasswordHash == testPasswordA || userB.PasswordHash == testPasswordB {
		t.Fatal("database must not contain plaintext passwords")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(userA.PasswordHash), []byte(testPasswordA)); err != nil {
		t.Fatalf("user A's stored password_hash cannot verify the real password: %v", err)
	}
	// 建账号时根目录必须在同一个事务里一起建好。
	for _, user := range []*model.User{userA, userB} {
		if user.RootFileID == nil {
			t.Fatalf("user %q has no root directory", user.Username)
		}
		var root model.File
		if err := client.WithContext(ctx).First(&root, "id = ?", *user.RootFileID).Error; err != nil {
			t.Fatal(err)
		}
		if root.Kind != model.KindDirectory || root.Name != "/" || root.ParentID != nil {
			t.Fatalf("unexpected root directory: %+v", root)
		}
	}

	fileService := cloudfile.NewService(client, storage.NewLocal(t.TempDir()))
	authService := auth.NewService(client)
	router := httpapi.NewRouter(slog.New(slog.NewJSONHandler(io.Discard, nil)), fileService, authService)

	// 1. 没有登录，三个文件接口都必须拒绝。
	notLoggedIn := []*http.Request{
		newUploadRequest(t, "", []byte("anonymous")),
		httptest.NewRequest(http.MethodGet, "/api/files/any-id", nil),
		httptest.NewRequest(http.MethodDelete, "/api/files/any-id", nil),
		httptest.NewRequest(http.MethodPost, "/api/files/any-id/copy", strings.NewReader(`{"name":"copy.txt"}`)),
	}
	for _, req := range notLoggedIn {
		recorder := serve(t, router, req)
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s without login: wanted 401, got %d: %s",
				req.Method, req.URL.Path, recorder.Code, recorder.Body.String())
		}
	}
	if count := countWhere(t, client, &model.File{}, "kind = ?", model.KindFile); count != 0 {
		t.Fatalf("unauthenticated uploads left %d files", count)
	}
	t.Log("PASS: no token -> upload/download/delete all rejected with 401")

	// 2. 密码错误不能登录，也不能创建会话。
	wrongPassword := serve(t, router, newLoginRequest(t, testUsernameA, "wrong-password"))
	if wrongPassword.Code != http.StatusUnauthorized {
		t.Fatalf("wrong password: wanted 401, got %d: %s", wrongPassword.Code, wrongPassword.Body.String())
	}
	if count := countRows(t, client, &model.AuthSession{}); count != 0 {
		t.Fatalf("wrong password created %d sessions", count)
	}
	t.Log("PASS: wrong password rejected (401) and no session created")

	// 3. A 登录，检查数据库只保存 Token 哈希。
	tokenA := login(t, router, testUsernameA, testPasswordA)
	var sessionA model.AuthSession
	if err := client.WithContext(ctx).First(&sessionA).Error; err != nil {
		t.Fatal(err)
	}
	if sessionA.UserID != userA.ID {
		t.Fatalf("session user_id = %d, want A=%d", sessionA.UserID, userA.ID)
	}
	if sessionA.TokenHash == tokenA {
		t.Fatal("database stored the raw token instead of its hash")
	}
	if sessionA.TokenHash != sha256Hex(tokenA) {
		t.Fatal("database token_hash is not SHA-256(token)")
	}
	if !sessionA.ExpiresAt.After(time.Now()) {
		t.Fatal("new session is already expired")
	}
	t.Log("PASS: A logged in; auth_sessions stores user_id + SHA-256 token hash + expiry")

	// 4. A 上传时故意添加伪造的 X-User-ID，所有者仍必须来自登录会话。
	payloadA := []byte("file owned by A only\n")
	uploadA := newUploadRequest(t, tokenA, payloadA)
	uploadA.Header.Set("X-User-ID", "999999")
	uploaded := serve(t, router, uploadA)
	if uploaded.Code != http.StatusCreated {
		t.Fatalf("A upload: wanted 201, got %d: %s", uploaded.Code, uploaded.Body.String())
	}
	var recordA cloudfile.File
	if err := json.Unmarshal(uploaded.Body.Bytes(), &recordA); err != nil {
		t.Fatal(err)
	}
	var storedA model.File
	if err := client.WithContext(ctx).Where("id = ?", recordA.ID).First(&storedA).Error; err != nil {
		t.Fatal(err)
	}
	if storedA.OwnerID != userA.ID {
		t.Fatalf("file owner_id = %d, want A=%d (client header must be ignored)", storedA.OwnerID, userA.ID)
	}
	t.Logf("PASS: A uploaded id=%s; owner_id=%d from token, forged X-User-ID ignored", recordA.ID, storedA.OwnerID)

	// 5. A 能下载自己的文件，字节完全一致。
	downloadA := serve(t, router, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+recordA.ID, nil), tokenA))
	if downloadA.Code != http.StatusOK {
		t.Fatalf("A download: wanted 200, got %d: %s", downloadA.Code, downloadA.Body.String())
	}
	if !bytes.Equal(downloadA.Body.Bytes(), payloadA) {
		t.Fatalf("A downloaded wrong content: %q", downloadA.Body.Bytes())
	}
	t.Log("PASS: A downloaded the file with identical content")

	// 6. B 登录后即使知道文件 ID，也不能下载或删除 A 的文件。
	tokenB := login(t, router, testUsernameB, testPasswordB)
	if tokenB == tokenA {
		t.Fatal("two logins produced the same token")
	}

	spoofedDownload := authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+recordA.ID, nil), tokenB)
	spoofedDownload.Header.Set("X-User-ID", strconv.Itoa(userA.ID))
	byB := serve(t, router, spoofedDownload)
	if byB.Code != http.StatusNotFound {
		t.Fatalf("B download A's file: wanted 404, got %d: %s", byB.Code, byB.Body.String())
	}

	spoofedDelete := authorize(httptest.NewRequest(http.MethodDelete, "/api/files/"+recordA.ID, nil), tokenB)
	spoofedDelete.Header.Set("X-User-ID", strconv.Itoa(userA.ID))
	deleteByB := serve(t, router, spoofedDelete)
	if deleteByB.Code != http.StatusNotFound {
		t.Fatalf("B delete A's file: wanted 404, got %d: %s", deleteByB.Code, deleteByB.Body.String())
	}
	t.Log("PASS: B knows A's file id but gets 404 for both download and delete")

	// 7. B 的两次失败后，A 的文件和内容都毫发无损。
	stillThere := serve(t, router, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+recordA.ID, nil), tokenA))
	if stillThere.Code != http.StatusOK || !bytes.Equal(stillThere.Body.Bytes(), payloadA) {
		t.Fatalf("B's failed operations damaged A's file: status=%d body=%q", stillThere.Code, stillThere.Body.Bytes())
	}
	if count := countWhere(t, client, &model.File{}, "kind = ?", model.KindFile); count != 1 {
		t.Fatalf("B's failed operations changed file count to %d", count)
	}
	t.Log("PASS: B's failures did not affect A's file or content")

	// 8. 所有者 A 可以删除自己的文件。
	deleteByA := serve(t, router, authorize(httptest.NewRequest(http.MethodDelete, "/api/files/"+recordA.ID, nil), tokenA))
	if deleteByA.Code != http.StatusNoContent {
		t.Fatalf("A delete: wanted 204, got %d: %s", deleteByA.Code, deleteByA.Body.String())
	}
	afterDelete := serve(t, router, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+recordA.ID, nil), tokenA))
	if afterDelete.Code != http.StatusNotFound {
		t.Fatalf("download after owner delete: wanted 404, got %d", afterDelete.Code)
	}
	t.Log("PASS: A deleted its own file and it is gone")

	// 9. 退出登录会删掉当前会话，旧 Token 立即不能再操作。
	logout := serve(t, router, authorize(httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil), tokenA))
	if logout.Code != http.StatusNoContent {
		t.Fatalf("logout: wanted 204, got %d: %s", logout.Code, logout.Body.String())
	}
	if count := countWhere(t, client, &model.AuthSession{}, "token_hash = ?", sha256Hex(tokenA)); count != 0 {
		t.Fatalf("logout left A's session in the database: %d", count)
	}
	afterLogout := serve(t, router, authorize(httptest.NewRequest(http.MethodGet, "/api/files/anything", nil), tokenA))
	if afterLogout.Code != http.StatusUnauthorized {
		t.Fatalf("old token after logout: wanted 401, got %d", afterLogout.Code)
	}
	// B 的会话不受 A 退出影响：认证通过后只是因为文件不存在而返回 404。
	bStillWorks := serve(t, router, authorize(httptest.NewRequest(http.MethodGet, "/api/files/anything", nil), tokenB))
	if bStillWorks.Code != http.StatusNotFound {
		t.Fatalf("B session was affected by A logout: wanted 404, got %d", bStillWorks.Code)
	}
	t.Log("PASS: logout deleted A's session; old token is 401 while B stays logged in")

	// 10. Token 过期后同样不能再操作。重新登录 A，再上传一个文件用于验证。
	tokenA2 := login(t, router, testUsernameA, testPasswordA)
	payloadA2 := []byte("expired token must not touch this\n")
	uploadA2 := serve(t, router, newUploadRequest(t, tokenA2, payloadA2))
	if uploadA2.Code != http.StatusCreated {
		t.Fatalf("A second upload: wanted 201, got %d: %s", uploadA2.Code, uploadA2.Body.String())
	}
	var recordA2 cloudfile.File
	if err := json.Unmarshal(uploadA2.Body.Bytes(), &recordA2); err != nil {
		t.Fatal(err)
	}

	var sessionA2 model.AuthSession
	if err := client.WithContext(ctx).
		Where("token_hash = ? AND user_id = ?", sha256Hex(tokenA2), userA.ID).
		First(&sessionA2).Error; err != nil {
		t.Fatal(err)
	}
	if err := client.WithContext(ctx).
		Model(&model.AuthSession{}).
		Where("id = ?", sessionA2.ID).
		Update("expires_at", time.Now().Add(-time.Minute)).Error; err != nil {
		t.Fatal(err)
	}

	expired := serve(t, router, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+recordA2.ID, nil), tokenA2))
	if expired.Code != http.StatusUnauthorized {
		t.Fatalf("expired token download: wanted 401, got %d: %s", expired.Code, expired.Body.String())
	}
	expiredDelete := serve(t, router, authorize(httptest.NewRequest(http.MethodDelete, "/api/files/"+recordA2.ID, nil), tokenA2))
	if expiredDelete.Code != http.StatusUnauthorized {
		t.Fatalf("expired token delete: wanted 401, got %d", expiredDelete.Code)
	}

	// 过期 Token 的失败不能删掉文件；用新会话仍能下载，证明只是 Token 失效。
	tokenA3 := login(t, router, testUsernameA, testPasswordA)
	stillReadable := serve(t, router, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+recordA2.ID, nil), tokenA3))
	if stillReadable.Code != http.StatusOK || !bytes.Equal(stillReadable.Body.Bytes(), payloadA2) {
		t.Fatalf("expired token's failed operations damaged the file: status=%d body=%q",
			stillReadable.Code, stillReadable.Body.Bytes())
	}
	t.Log("PASS: expired token rejected with 401; file remained intact and readable by a fresh login")
}

func mustUser(t *testing.T, db *gorm.DB, username string) *model.User {
	t.Helper()
	var user model.User
	if err := db.WithContext(context.Background()).
		Where("username = ?", username).
		First(&user).Error; err != nil {
		t.Fatal(err)
	}
	return &user
}

func resetAuthTables(t *testing.T, db *gorm.DB) {
	t.Helper()
	// files.parent_id 是自引用外键，逐行按顺序删很容易出现“父节点先被删”。
	// 测试里在同一个连接上临时关掉外键检查，清空后立刻打开。
	err := db.Connection(func(conn *gorm.DB) error {
		if err := conn.Exec("SET FOREIGN_KEY_CHECKS = 0").Error; err != nil {
			return err
		}
		for _, table := range []string{"auth_sessions", "files", "entities", "users"} {
			if err := conn.Exec("DELETE FROM " + table).Error; err != nil {
				_ = conn.Exec("SET FOREIGN_KEY_CHECKS = 1").Error
				return err
			}
		}
		return conn.Exec("SET FOREIGN_KEY_CHECKS = 1").Error
	})
	if err != nil {
		t.Fatalf("clear tables: %v", err)
	}
}

// countRows 数一张表有多少行。传 &model.X{} 给 GORM 决定表名。
func countRows(t *testing.T, db *gorm.DB, value any) int64 {
	t.Helper()
	var count int64
	if err := db.Model(value).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

// countWhere 数满足条件的行数。
func countWhere(t *testing.T, db *gorm.DB, value any, query string, args ...any) int64 {
	t.Helper()
	var count int64
	if err := db.Model(value).Where(query, args...).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

func login(t *testing.T, router http.Handler, username, password string) string {
	t.Helper()
	recorder := serve(t, router, newLoginRequest(t, username, password))
	if recorder.Code != http.StatusOK {
		t.Fatalf("login %s: wanted 200, got %d: %s", username, recorder.Code, recorder.Body.String())
	}
	var result auth.LoginResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Token == "" || !result.ExpiresAt.After(time.Now()) {
		t.Fatalf("login %s returned unusable token: %+v", username, result)
	}
	return result.Token
}

func newLoginRequest(t *testing.T, username, password string) *http.Request {
	t.Helper()
	body, err := json.Marshal(map[string]string{"username": username, "password": password})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func newUploadRequest(t *testing.T, token string, content []byte) *http.Request {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "proof.bin")
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
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

func authorize(req *http.Request, token string) *http.Request {
	req.Header.Set("Authorization", "Bearer "+token)
	return req
}

func serve(t *testing.T, router http.Handler, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	return recorder
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// TestDirectoryFlowOverHTTP 用真实 MySQL + 真实 Service 走一遍新接口：
// 建目录 → 往目录里传文件 → 列目录 → 拒绝下载/删除目录 → 别的用户进不来。
func TestDirectoryFlowOverHTTP(t *testing.T) {
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run the directory flow")
	}

	ctx := context.Background()
	client, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close(client) })
	resetAuthTables(t, client)

	for _, account := range []struct{ username, password string }{
		{testUsernameA, testPasswordA},
		{testUsernameB, testPasswordB},
	} {
		if _, err := auth.CreateUser(ctx, client, account.username, account.password); err != nil {
			t.Fatal(err)
		}
	}

	fileService := cloudfile.NewService(client, storage.NewLocal(t.TempDir()))
	router := httpapi.NewRouter(slog.New(slog.NewJSONHandler(io.Discard, nil)),
		fileService, auth.NewService(client))

	tokenA := login(t, router, testUsernameA, testPasswordA)
	tokenB := login(t, router, testUsernameB, testPasswordB)

	// 1. 建目录。
	body := strings.NewReader(`{"name":"学习资料"}`)
	createReq := httptest.NewRequest(http.MethodPost, "/api/directories", body)
	createReq.Header.Set("Content-Type", "application/json")
	createReq = authorize(createReq, tokenA)
	created := serve(t, router, createReq)
	if created.Code != http.StatusCreated {
		t.Fatalf("create directory: wanted 201, got %d: %s", created.Code, created.Body.String())
	}
	var dir cloudfile.File
	if err := json.Unmarshal(created.Body.Bytes(), &dir); err != nil {
		t.Fatal(err)
	}
	if dir.Kind != "directory" || dir.Name != "学习资料" || dir.ParentID == nil {
		t.Fatalf("unexpected directory response: %+v", dir)
	}

	// 2. 往目录里上传文件。
	upload := newUploadRequest(t, tokenA, []byte("hello"))
	query := upload.URL.Query()
	query.Set("parent_id", dir.ID)
	upload.URL.RawQuery = query.Encode()
	uploaded := serve(t, router, upload)
	if uploaded.Code != http.StatusCreated {
		t.Fatalf("upload into directory: wanted 201, got %d: %s", uploaded.Code, uploaded.Body.String())
	}
	var note cloudfile.File
	if err := json.Unmarshal(uploaded.Body.Bytes(), &note); err != nil {
		t.Fatal(err)
	}
	if note.ParentID == nil || *note.ParentID != dir.ID || note.Kind != "file" || note.Size != 5 {
		t.Fatalf("unexpected upload response: %+v", note)
	}

	// 3. 列目录：只有刚传的那一个文件。
	listReq := httptest.NewRequest(http.MethodGet, "/api/files?parent_id="+dir.ID, nil)
	list := serve(t, router, authorize(listReq, tokenA))
	if list.Code != http.StatusOK {
		t.Fatalf("list directory: wanted 200, got %d: %s", list.Code, list.Body.String())
	}
	var listing cloudfile.ListResult
	if err := json.Unmarshal(list.Body.Bytes(), &listing); err != nil {
		t.Fatal(err)
	}
	if listing.Total != 1 || len(listing.Items) != 1 || listing.Items[0].ID != note.ID {
		t.Fatalf("unexpected listing: %+v", listing)
	}

	// 4. 列根目录：只有“学习资料”这一个目录。
	rootReq := httptest.NewRequest(http.MethodGet, "/api/files", nil)
	rootList := serve(t, router, authorize(rootReq, tokenA))
	if rootList.Code != http.StatusOK {
		t.Fatalf("list root: wanted 200, got %d", rootList.Code)
	}
	var rootListing cloudfile.ListResult
	if err := json.Unmarshal(rootList.Body.Bytes(), &rootListing); err != nil {
		t.Fatal(err)
	}
	if rootListing.Total != 1 || rootListing.Items[0].ID != dir.ID {
		t.Fatalf("unexpected root listing: %+v", rootListing)
	}

	// 5. 目录不能下载、不能删除。
	download := serve(t, router, authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+dir.ID, nil), tokenA))
	if download.Code != http.StatusBadRequest {
		t.Fatalf("download directory: wanted 400, got %d", download.Code)
	}
	remove := serve(t, router, authorize(httptest.NewRequest(http.MethodDelete, "/api/files/"+dir.ID, nil), tokenA))
	if remove.Code != http.StatusBadRequest {
		t.Fatalf("delete directory: wanted 400, got %d", remove.Code)
	}

	// 6. 复制：新 File 复用源内容的 Entity，源文件仍然存在。
	copyPayload, err := json.Marshal(map[string]string{
		"parent_id": dir.ID,
		"name":      "笔记-副本.txt",
	})
	if err != nil {
		t.Fatal(err)
	}
	copyReq := httptest.NewRequest(http.MethodPost, "/api/files/"+note.ID+"/copy", bytes.NewReader(copyPayload))
	copyReq.Header.Set("Content-Type", "application/json")
	copied := serve(t, router, authorize(copyReq, tokenA))
	if copied.Code != http.StatusCreated {
		t.Fatalf("copy file: wanted 201, got %d: %s", copied.Code, copied.Body.String())
	}
	var copyRecord cloudfile.File
	if err := json.Unmarshal(copied.Body.Bytes(), &copyRecord); err != nil {
		t.Fatal(err)
	}
	if copyRecord.ID == note.ID || copyRecord.Name != "笔记-副本.txt" ||
		copyRecord.Size != note.Size || copyRecord.ParentID == nil || *copyRecord.ParentID != dir.ID {
		t.Fatalf("unexpected copy response: %+v", copyRecord)
	}

	// 两条 File 记录共享同一个 Entity：ref_count = 2，磁盘上仍只有一份内容。
	var shared model.Entity
	if err := client.WithContext(ctx).First(&shared).Error; err != nil {
		t.Fatal(err)
	}
	if shared.RefCount != 2 || shared.Status != model.EntityStatusActive {
		t.Fatalf("shared entity = %+v, want ref_count 2 and status active", shared)
	}

	// 副本可以下载，内容和源文件一致。
	copyDownload := serve(t, router,
		authorize(httptest.NewRequest(http.MethodGet, "/api/files/"+copyRecord.ID, nil), tokenA))
	if copyDownload.Code != http.StatusOK || copyDownload.Body.String() != "hello" {
		t.Fatalf("download copy: status=%d body=%q", copyDownload.Code, copyDownload.Body.String())
	}

	// 同一目录下再次复制成同名：409。
	duplicateCopy := serve(t, router, authorize(
		httptest.NewRequest(http.MethodPost, "/api/files/"+note.ID+"/copy", bytes.NewReader(copyPayload)),
		tokenA,
	))
	if duplicateCopy.Code != http.StatusConflict {
		t.Fatalf("duplicate copy: wanted 409, got %d: %s", duplicateCopy.Code, duplicateCopy.Body.String())
	}

	// B 拿着 A 的文件 ID 复制：404，不会产生新 File。
	stealPayload, err := json.Marshal(map[string]string{"parent_id": dir.ID, "name": "stolen.txt"})
	if err != nil {
		t.Fatal(err)
	}
	steal := serve(t, router, authorize(
		httptest.NewRequest(http.MethodPost, "/api/files/"+note.ID+"/copy", bytes.NewReader(stealPayload)),
		tokenB,
	))
	if steal.Code != http.StatusNotFound {
		t.Fatalf("B copy A's file: wanted 404, got %d: %s", steal.Code, steal.Body.String())
	}
	if count := countWhere(t, client, &model.File{}, "kind = ?", model.KindFile); count != 2 {
		t.Fatalf("wanted exactly 2 logical files after copy tests, got %d", count)
	}

	// 7. B 不能往 A 的目录里传文件，也看不到 A 的目录内容。
	intruder := newUploadRequest(t, tokenB, []byte("intrusion"))
	query = intruder.URL.Query()
	query.Set("parent_id", dir.ID)
	intruder.URL.RawQuery = query.Encode()
	intruded := serve(t, router, intruder)
	if intruded.Code != http.StatusNotFound {
		t.Fatalf("B upload into A's directory: wanted 404, got %d: %s", intruded.Code, intruded.Body.String())
	}
	peek := serve(t, router, authorize(httptest.NewRequest(http.MethodGet, "/api/files?parent_id="+dir.ID, nil), tokenB))
	if peek.Code != http.StatusNotFound {
		t.Fatalf("B list A's directory: wanted 404, got %d", peek.Code)
	}
}
