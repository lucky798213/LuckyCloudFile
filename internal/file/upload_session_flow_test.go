package file_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"luckyCloudFile/internal/auth"
	"luckyCloudFile/internal/database"
	cloudfile "luckyCloudFile/internal/file"
	"luckyCloudFile/internal/httpapi"
	"luckyCloudFile/internal/model"
)

func uploadSessionHTTPRequest(t *testing.T, method, path, body, token string) *http.Request {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req = authorize(req, token)
	}
	return req
}

// TestUploadSessionFlowOverHTTP 用真实 MySQL + 真实 Service 走完整流程：
// 创建会话 → 查询自己的会话 → 别人/不存在都是 404 → 无效参数不留记录
// → 根目录没有正式文件 → 数据库里只有一条 pending 会话和 uploading/0 占位实体。
func TestUploadSessionFlowOverHTTP(t *testing.T) {
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run the upload session flow")
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
	userA := mustUser(t, client, testUsernameA)
	userB := mustUser(t, client, testUsernameB)

	router := httpapi.NewRouter(
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
		newTestFileService(t, client),
		auth.NewService(client),
	)
	tokenA := login(t, router, testUsernameA, testPasswordA)
	tokenB := login(t, router, testUsernameB, testPasswordB)

	// 1. 创建空文件会话，不传 parent_id：parent_id 必须落成 A 的真实根目录。
	created := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodPost, "/api/upload-sessions",
		`{"name":"照片.jpg","size":0}`, tokenA,
	))
	if created.Code != http.StatusCreated {
		t.Fatalf("create session: wanted 201, got %d: %s", created.Code, created.Body.String())
	}
	var info cloudfile.UploadSessionInfo
	if err := json.Unmarshal(created.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.ID == "" || info.Name != "照片.jpg" ||
		info.ParentID != *userA.RootFileID ||
		info.ExpectedSize != 0 || info.ActualSize != 0 ||
		info.Status != model.UploadStatusPending ||
		info.ResultFileID != nil {
		t.Fatalf("unexpected session response: %+v", info)
	}
	if !info.ExpiresAt.After(time.Now()) {
		t.Fatalf("session expiry is not in the future: %v", info.ExpiresAt)
	}

	// 2. 查询自己的会话。
	got := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodGet, "/api/upload-sessions/"+info.ID, "", tokenA,
	))
	if got.Code != http.StatusOK {
		t.Fatalf("get own session: wanted 200, got %d: %s", got.Code, got.Body.String())
	}
	var fetched cloudfile.UploadSessionInfo
	if err := json.Unmarshal(got.Body.Bytes(), &fetched); err != nil {
		t.Fatal(err)
	}
	if fetched.ID != info.ID || fetched.ParentID != info.ParentID ||
		fetched.Name != info.Name || fetched.Status != model.UploadStatusPending {
		t.Fatalf("fetched session = %+v, want %+v", fetched, info)
	}

	// 3. 别人的会话和不存在的会话返回同一个 404 + code。
	for _, check := range []struct {
		name  string
		token string
		path  string
	}{
		{"foreign session", tokenB, "/api/upload-sessions/" + info.ID},
		{"missing session", tokenA, "/api/upload-sessions/does-not-exist"},
	} {
		resp := serve(t, router, uploadSessionHTTPRequest(
			t, http.MethodGet, check.path, "", check.token,
		))
		if resp.Code != http.StatusNotFound {
			t.Fatalf("%s: wanted 404, got %d: %s", check.name, resp.Code, resp.Body.String())
		}
		var body httpapi.ErrorResponse
		if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Code != "upload_session_not_found" {
			t.Fatalf("%s: code = %q, want upload_session_not_found", check.name, body.Code)
		}
	}

	// 4. 无效创建请求：全部拒绝，且不能留下新的 Session/Entity。
	invalid := []struct {
		name     string
		body     string
		wantCode int
		wantErr  string
	}{
		{
			name:     "missing size",
			body:     `{"name":"x.bin"}`,
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_size",
		},
		{
			name:     "negative size",
			body:     `{"name":"x.bin","size":-1}`,
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_size",
		},
		{
			name:     "too large",
			body:     fmt.Sprintf(`{"name":"x.bin","size":%d}`, cloudfile.MaxUploadBytes+1),
			wantCode: http.StatusRequestEntityTooLarge,
			wantErr:  "file_too_large",
		},
		{
			name:     "invalid name",
			body:     `{"name":"../escape","size":0}`,
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_name",
		},
		{
			name:     "foreign parent",
			body:     `{"parent_id":"` + *userB.RootFileID + `","name":"x.bin","size":0}`,
			wantCode: http.StatusNotFound,
			wantErr:  "parent_not_found",
		},
	}
	for _, test := range invalid {
		test := test
		t.Run(test.name, func(t *testing.T) {
			resp := serve(t, router, uploadSessionHTTPRequest(
				t, http.MethodPost, "/api/upload-sessions", test.body, tokenA,
			))
			if resp.Code != test.wantCode {
				t.Fatalf("wanted %d, got %d: %s", test.wantCode, resp.Code, resp.Body.String())
			}
			var body httpapi.ErrorResponse
			if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Code != test.wantErr {
				t.Fatalf("code = %q, want %q", body.Code, test.wantErr)
			}
			if got := countRows(t, client, &model.UploadSession{}); got != 1 {
				t.Fatalf("invalid create left %d session(s), want 1", got)
			}
			if got := countRows(t, client, &model.Entity{}); got != 1 {
				t.Fatalf("invalid create left %d entity/entities, want 1", got)
			}
		})
	}

	// 5. 创建会话不产生正式 File；根目录列表仍然是空的。
	if got := countWhere(t, client, &model.File{}, "kind = ?", model.KindFile); got != 0 {
		t.Fatalf("upload session created %d formal file(s)", got)
	}
	rootList := serve(t, router, authorize(
		httptest.NewRequest(http.MethodGet, "/api/files", nil), tokenA,
	))
	if rootList.Code != http.StatusOK {
		t.Fatalf("list root: wanted 200, got %d: %s", rootList.Code, rootList.Body.String())
	}
	var listing cloudfile.ListResult
	if err := json.Unmarshal(rootList.Body.Bytes(), &listing); err != nil {
		t.Fatal(err)
	}
	if listing.Total != 0 || len(listing.Items) != 0 {
		t.Fatalf("root directory should be empty, got %+v", listing)
	}

	// 6. 数据库里只有一条会话和一条上传占位实体。
	if got := countRows(t, client, &model.UploadSession{}); got != 1 {
		t.Fatalf("wanted 1 session, got %d", got)
	}
	var session model.UploadSession
	if err := client.First(&session, "id = ?", info.ID).Error; err != nil {
		t.Fatal(err)
	}
	var entity model.Entity
	if err := client.First(&entity, session.EntityID).Error; err != nil {
		t.Fatal(err)
	}
	if entity.Status != model.EntityStatusUploading || entity.RefCount != 0 || entity.Size != 0 {
		t.Fatalf("unexpected placeholder entity: %+v", entity)
	}
}
