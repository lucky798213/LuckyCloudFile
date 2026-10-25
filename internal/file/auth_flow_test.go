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
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"luckyCloudFile/internal/auth"
	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/ent"
	entsession "luckyCloudFile/internal/ent/authsession"
	entfile "luckyCloudFile/internal/ent/file"
	entuser "luckyCloudFile/internal/ent/user"
	cloudfile "luckyCloudFile/internal/file"
	"luckyCloudFile/internal/httpapi"
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
	t.Cleanup(func() { _ = client.Close() })
	resetAuthTables(t, client)

	// 账号必须显式创建；清空后数据库里应为 0 个用户。
	if count := client.User.Query().CountX(ctx); count != 0 {
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

	fileService := cloudfile.NewService(client, storage.NewLocal(t.TempDir()))
	authService := auth.NewService(client)
	router := httpapi.NewRouter(slog.New(slog.NewJSONHandler(io.Discard, nil)), fileService, authService)

	// 1. 没有登录，三个文件接口都必须拒绝。
	notLoggedIn := []*http.Request{
		newUploadRequest(t, "", []byte("anonymous")),
		httptest.NewRequest(http.MethodGet, "/api/files/any-id", nil),
		httptest.NewRequest(http.MethodDelete, "/api/files/any-id", nil),
	}
	for _, req := range notLoggedIn {
		recorder := serve(t, router, req)
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s without login: wanted 401, got %d: %s",
				req.Method, req.URL.Path, recorder.Code, recorder.Body.String())
		}
	}
	if count := client.File.Query().CountX(ctx); count != 0 {
		t.Fatalf("unauthenticated uploads left %d files", count)
	}
	t.Log("PASS: no token -> upload/download/delete all rejected with 401")

	// 2. 密码错误不能登录，也不能创建会话。
	wrongPassword := serve(t, router, newLoginRequest(t, testUsernameA, "wrong-password"))
	if wrongPassword.Code != http.StatusUnauthorized {
		t.Fatalf("wrong password: wanted 401, got %d: %s", wrongPassword.Code, wrongPassword.Body.String())
	}
	if count := client.AuthSession.Query().CountX(ctx); count != 0 {
		t.Fatalf("wrong password created %d sessions", count)
	}
	t.Log("PASS: wrong password rejected (401) and no session created")

	// 3. A 登录，检查数据库只保存 Token 哈希。
	tokenA := login(t, router, testUsernameA, testPasswordA)
	sessionA := client.AuthSession.Query().OnlyX(ctx)
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
	storedA := client.File.Query().Where(entfile.ID(recordA.ID)).OnlyX(ctx)
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
	if count := client.File.Query().CountX(ctx); count != 1 {
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
	if count := client.AuthSession.Query().
		Where(entsession.TokenHash(sha256Hex(tokenA))).
		CountX(ctx); count != 0 {
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

	sessionA2 := client.AuthSession.Query().
		Where(
			entsession.TokenHash(sha256Hex(tokenA2)),
			entsession.UserID(userA.ID),
		).
		OnlyX(ctx)
	client.AuthSession.UpdateOneID(sessionA2.ID).
		SetExpiresAt(time.Now().Add(-time.Minute)).
		SaveX(ctx)

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

func mustUser(t *testing.T, client *ent.Client, username string) *ent.User {
	t.Helper()
	user, err := client.User.Query().Where(entuser.Username(username)).Only(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return user
}

func resetAuthTables(t *testing.T, client *ent.Client) {
	t.Helper()
	ctx := context.Background()
	for _, step := range []struct {
		name string
		run  func() (int, error)
	}{
		{"auth_sessions", func() (int, error) { return client.AuthSession.Delete().Exec(ctx) }},
		{"files", func() (int, error) { return client.File.Delete().Exec(ctx) }},
		{"entities", func() (int, error) { return client.Entity.Delete().Exec(ctx) }},
		{"users", func() (int, error) { return client.User.Delete().Exec(ctx) }},
	} {
		if _, err := step.run(); err != nil {
			t.Fatalf("clear %s: %v", step.name, err)
		}
	}
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
