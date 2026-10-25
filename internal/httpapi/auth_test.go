package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"luckyCloudFile/internal/auth"
)

const (
	testTokenA = "test-token-A"
	testTokenB = "test-token-B"

	testUserAID = 1
	testUserBID = 2
)

// fakeAuthService 只实现认证接口，不访问数据库，让 HTTP 测试能精确控制
// “登录成功 / 密码错误 / 数据库坏了 / Token 已退出”这些情况。
type fakeAuthService struct {
	mu sync.Mutex

	// credentials 是用户名 → 密码。
	credentials map[string]string
	// tokens 是原始 Token → 用户 ID，模拟 auth_sessions 表。
	tokens map[string]int

	loginErr        error
	authenticateErr error
	logoutErr       error
}

func newFakeAuthService() *fakeAuthService {
	return &fakeAuthService{
		credentials: map[string]string{
			"A": "A-password",
			"B": "B-password",
		},
		tokens: map[string]int{
			testTokenA: testUserAID,
			testTokenB: testUserBID,
		},
	}
}

func (f *fakeAuthService) Login(_ context.Context, username, password string) (auth.LoginResult, error) {
	if f.loginErr != nil {
		return auth.LoginResult{}, f.loginErr
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.credentials[username] != password {
		return auth.LoginResult{}, auth.ErrInvalidCredentials
	}

	token := map[string]string{"A": testTokenA, "B": testTokenB}[username]
	userID := map[string]int{"A": testUserAID, "B": testUserBID}[username]
	f.tokens[token] = userID
	return auth.LoginResult{Token: token, ExpiresAt: time.Now().Add(time.Hour)}, nil
}

func (f *fakeAuthService) Authenticate(_ context.Context, rawToken string) (int, error) {
	if f.authenticateErr != nil {
		return 0, f.authenticateErr
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	userID, ok := f.tokens[rawToken]
	if !ok {
		return 0, auth.ErrUnauthorized
	}
	return userID, nil
}

func (f *fakeAuthService) Logout(_ context.Context, rawToken string) error {
	if f.logoutErr != nil {
		return f.logoutErr
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.tokens[rawToken]; !ok {
		return auth.ErrUnauthorized
	}
	delete(f.tokens, rawToken)
	return nil
}

func testRouter(authService authService) *gin.Engine {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewRouter(logger, newFakeFileService(), authService)
}

func newLoginRequest(body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestLogin(t *testing.T) {
	router := testRouter(newFakeAuthService())

	cases := []struct {
		name   string
		body   string
		status int
		code   string
	}{
		{"correct password", `{"username":"A","password":"A-password"}`, http.StatusOK, ""},
		{"wrong password", `{"username":"A","password":"wrong"}`, http.StatusUnauthorized, "invalid_credentials"},
		{"unknown user", `{"username":"nobody","password":"x"}`, http.StatusUnauthorized, "invalid_credentials"},
		{"missing password", `{"username":"A"}`, http.StatusBadRequest, "invalid_request"},
		{"malformed json", `{`, http.StatusBadRequest, "invalid_request"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, newLoginRequest(tc.body))
			if recorder.Code != tc.status {
				t.Fatalf("wanted %d, got %d: %s", tc.status, recorder.Code, recorder.Body.String())
			}
			if tc.code == "" {
				var result auth.LoginResult
				if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if result.Token != testTokenA || result.ExpiresAt.IsZero() {
					t.Fatalf("unexpected login result: %+v", result)
				}
				return
			}
			var response ErrorResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.Code != tc.code {
				t.Fatalf("code = %q, want %q", response.Code, tc.code)
			}
		})
	}
}

func TestLoginInternalErrorIsNotInvalidCredentials(t *testing.T) {
	fake := newFakeAuthService()
	fake.loginErr = errors.New("database unavailable")
	router := testRouter(fake)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, newLoginRequest(`{"username":"A","password":"A-password"}`))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("wanted 500, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestAuthenticateInternalErrorIsNotUnauthorized(t *testing.T) {
	fake := newFakeAuthService()
	fake.authenticateErr = errors.New("database unavailable")
	router := testRouter(fake)

	req := authorize(httptest.NewRequest(http.MethodGet, "/api/files/anything", nil), testTokenA)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("wanted 500, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestLogoutInvalidatesToken(t *testing.T) {
	router := testRouter(newFakeAuthService())

	logoutRequest := authorize(httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil), testTokenA)
	logout := httptest.NewRecorder()
	router.ServeHTTP(logout, logoutRequest)
	if logout.Code != http.StatusNoContent {
		t.Fatalf("wanted 204, got %d: %s", logout.Code, logout.Body.String())
	}

	// 同一个旧 Token 再访问受保护接口必须变成 401。
	oldToken := authorize(httptest.NewRequest(http.MethodGet, "/api/files/anything", nil), testTokenA)
	blocked := httptest.NewRecorder()
	router.ServeHTTP(blocked, oldToken)
	if blocked.Code != http.StatusUnauthorized {
		t.Fatalf("wanted 401 after logout, got %d", blocked.Code)
	}

	// B 的 Token 不受 A 退出影响。
	other := authorize(httptest.NewRequest(http.MethodGet, "/api/files/anything", nil), testTokenB)
	otherRecorder := httptest.NewRecorder()
	router.ServeHTTP(otherRecorder, other)
	// B 没有这个文件，但请求已经通过认证（不是 401）。
	if otherRecorder.Code != http.StatusNotFound {
		t.Fatalf("B wanted 404, got %d: %s", otherRecorder.Code, otherRecorder.Body.String())
	}
}

func TestLogoutRequiresLogin(t *testing.T) {
	router := testRouter(newFakeAuthService())

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil))
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("wanted 401, got %d", recorder.Code)
	}
}

func TestLogoutDatabaseErrorIs500(t *testing.T) {
	fake := newFakeAuthService()
	fake.logoutErr = errors.New("database unavailable")
	router := testRouter(fake)

	req := authorize(httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil), testTokenA)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("wanted 500, got %d: %s", recorder.Code, recorder.Body.String())
	}
}
