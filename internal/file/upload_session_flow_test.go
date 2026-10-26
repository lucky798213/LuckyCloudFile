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

// TestUploadSessionContentFlowOverHTTP 用真实 MySQL + 真实 Service 走完整内容上传：
// 创建会话 → PUT 原始内容 → 会话 ready → 目录仍为空、普通下载仍 404。
func TestUploadSessionContentFlowOverHTTP(t *testing.T) {
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run the upload content flow")
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

	router := httpapi.NewRouter(
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
		newTestFileService(t, client),
		auth.NewService(client),
	)
	tokenA := login(t, router, testUsernameA, testPasswordA)
	tokenB := login(t, router, testUsernameB, testPasswordB)

	// 1. 创建声明 5 字节的会话。
	created := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodPost, "/api/upload-sessions",
		`{"name":"content.bin","size":5}`, tokenA,
	))
	if created.Code != http.StatusCreated {
		t.Fatalf("create session: wanted 201, got %d: %s", created.Code, created.Body.String())
	}
	var info cloudfile.UploadSessionInfo
	if err := json.Unmarshal(created.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}

	// 2. 别人的会话不能写内容。
	foreign := serve(t, router, uploadSessionContentHTTPRequest(t, info.ID, "hello", tokenB))
	if foreign.Code != http.StatusNotFound {
		t.Fatalf("foreign content upload: wanted 404, got %d: %s", foreign.Code, foreign.Body.String())
	}

	// 3. 正确的 5 字节内容：200 + ready。
	uploaded := serve(t, router, uploadSessionContentHTTPRequest(t, info.ID, "hello", tokenA))
	if uploaded.Code != http.StatusOK {
		t.Fatalf("upload content: wanted 200, got %d: %s", uploaded.Code, uploaded.Body.String())
	}
	var ready cloudfile.UploadSessionInfo
	if err := json.Unmarshal(uploaded.Body.Bytes(), &ready); err != nil {
		t.Fatal(err)
	}
	if ready.ID != info.ID || ready.Status != model.UploadStatusReady ||
		ready.ActualSize != 5 || ready.ExpectedSize != 5 || ready.ResultFileID != nil {
		t.Fatalf("unexpected ready session: %+v", ready)
	}

	// 4. 查询会话是 ready；普通下载接口仍然找不到它。
	got := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodGet, "/api/upload-sessions/"+info.ID, "", tokenA,
	))
	if got.Code != http.StatusOK {
		t.Fatalf("get ready session: wanted 200, got %d: %s", got.Code, got.Body.String())
	}
	var fetched cloudfile.UploadSessionInfo
	if err := json.Unmarshal(got.Body.Bytes(), &fetched); err != nil {
		t.Fatal(err)
	}
	if fetched.Status != model.UploadStatusReady || fetched.ActualSize != 5 {
		t.Fatalf("fetched session = %+v", fetched)
	}
	download := serve(t, router, authorize(
		httptest.NewRequest(http.MethodGet, "/api/files/"+info.ID, nil), tokenA,
	))
	if download.Code != http.StatusNotFound {
		t.Fatalf("content must not be downloadable yet: wanted 404, got %d", download.Code)
	}

	// 5. 目录里没有正式文件；数据库里 Entity 仍是 uploading/0，只更新了 size。
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
		t.Fatalf("upload content created formal files: %+v", listing)
	}

	var session model.UploadSession
	if err := client.First(&session, "id = ?", info.ID).Error; err != nil {
		t.Fatal(err)
	}
	var entity model.Entity
	if err := client.First(&entity, session.EntityID).Error; err != nil {
		t.Fatal(err)
	}
	if session.Status != model.UploadStatusReady || session.ActualSize != 5 {
		t.Fatalf("stored session = %+v", session)
	}
	if entity.Status != model.EntityStatusUploading ||
		entity.RefCount != 0 ||
		entity.Size != 5 {
		t.Fatalf("stored placeholder entity = %+v", entity)
	}

	// 6. ready 之后重复写入被拒绝。
	again := serve(t, router, uploadSessionContentHTTPRequest(t, info.ID, "again", tokenA))
	if again.Code != http.StatusConflict {
		t.Fatalf("second content upload: wanted 409, got %d: %s", again.Code, again.Body.String())
	}
}

func uploadSessionContentHTTPRequest(t *testing.T, sessionID, body, token string) *http.Request {
	t.Helper()

	req := httptest.NewRequest(
		http.MethodPut,
		"/api/upload-sessions/"+sessionID+"/content",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/octet-stream")
	return authorize(req, token)
}

// TestUploadCompleteFlowOverHTTP 用真实 MySQL + 真实 Service 走完整上传闭环：
// 创建会话 → 上传内容 → 确认完成 → 目录可见 → 下载一致 → 重复确认幂等。
func TestUploadCompleteFlowOverHTTP(t *testing.T) {
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run the upload complete flow")
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

	router := httpapi.NewRouter(
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
		newTestFileService(t, client),
		auth.NewService(client),
	)
	tokenA := login(t, router, testUsernameA, testPasswordA)
	tokenB := login(t, router, testUsernameB, testPasswordB)

	const source = "hello 上传完成\n"
	// 1. 创建会话并上传完整内容。
	created := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodPost, "/api/upload-sessions",
		fmt.Sprintf(`{"name":"闭环.txt","size":%d}`, len(source)), tokenA,
	))
	if created.Code != http.StatusCreated {
		t.Fatalf("create session: wanted 201, got %d: %s", created.Code, created.Body.String())
	}
	var sessionInfo cloudfile.UploadSessionInfo
	if err := json.Unmarshal(created.Body.Bytes(), &sessionInfo); err != nil {
		t.Fatal(err)
	}

	uploaded := serve(t, router, uploadSessionContentHTTPRequest(t, sessionInfo.ID, source, tokenA))
	if uploaded.Code != http.StatusOK {
		t.Fatalf("upload content: wanted 200, got %d: %s", uploaded.Code, uploaded.Body.String())
	}

	// 2. 别人不能确认完成。
	foreign := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodPost, "/api/upload-sessions/"+sessionInfo.ID+"/complete", "", tokenB,
	))
	if foreign.Code != http.StatusNotFound {
		t.Fatalf("foreign complete: wanted 404, got %d: %s", foreign.Code, foreign.Body.String())
	}

	// 3. 确认完成：200 + completed + result_file_id。
	completed := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodPost, "/api/upload-sessions/"+sessionInfo.ID+"/complete", "", tokenA,
	))
	if completed.Code != http.StatusOK {
		t.Fatalf("complete: wanted 200, got %d: %s", completed.Code, completed.Body.String())
	}
	var result cloudfile.UploadSessionInfo
	if err := json.Unmarshal(completed.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.ID != sessionInfo.ID || result.Status != model.UploadStatusCompleted {
		t.Fatalf("completed session = %+v", result)
	}
	if result.ResultFileID == nil || *result.ResultFileID == "" {
		t.Fatalf("completed session has no result_file_id: %s", completed.Body.String())
	}
	fileID := *result.ResultFileID

	// 4. 查询会话也能看到 completed 和同一个结果文件。
	got := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodGet, "/api/upload-sessions/"+sessionInfo.ID, "", tokenA,
	))
	if got.Code != http.StatusOK {
		t.Fatalf("get completed session: wanted 200, got %d: %s", got.Code, got.Body.String())
	}
	var fetched cloudfile.UploadSessionInfo
	if err := json.Unmarshal(got.Body.Bytes(), &fetched); err != nil {
		t.Fatal(err)
	}
	if fetched.Status != model.UploadStatusCompleted ||
		fetched.ResultFileID == nil || *fetched.ResultFileID != fileID {
		t.Fatalf("fetched completed session = %+v", fetched)
	}

	// 5. 目录里能看见正式文件，大小和上传时一致。
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
	if listing.Total != 1 || len(listing.Items) != 1 ||
		listing.Items[0].ID != fileID || listing.Items[0].Name != "闭环.txt" ||
		listing.Items[0].Size != int64(len(source)) {
		t.Fatalf("root listing = %+v", listing)
	}

	// 6. 通过现有下载接口取得内容，摘要与源文件一致。
	download := serve(t, router, authorize(
		httptest.NewRequest(http.MethodGet, "/api/files/"+fileID, nil), tokenA,
	))
	if download.Code != http.StatusOK {
		t.Fatalf("download: wanted 200, got %d: %s", download.Code, download.Body.String())
	}
	if download.Body.String() != source {
		t.Fatalf("downloaded content = %q, want %q", download.Body.String(), source)
	}
	if gotDigest, wantDigest := sha256Hex(download.Body.String()), sha256Hex(source); gotDigest != wantDigest {
		t.Fatalf("download digest = %s, want %s", gotDigest, wantDigest)
	}

	// 7. 重复确认返回同一个文件 ID，不新增正式文件。
	repeat := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodPost, "/api/upload-sessions/"+sessionInfo.ID+"/complete", "", tokenA,
	))
	if repeat.Code != http.StatusOK {
		t.Fatalf("repeat complete: wanted 200, got %d: %s", repeat.Code, repeat.Body.String())
	}
	var repeated cloudfile.UploadSessionInfo
	if err := json.Unmarshal(repeat.Body.Bytes(), &repeated); err != nil {
		t.Fatal(err)
	}
	if repeated.ResultFileID == nil || *repeated.ResultFileID != fileID {
		t.Fatalf("repeat complete returned %+v, want %q", repeated, fileID)
	}
	if got := countWhere(t, client, &model.File{}, "kind = ?", model.KindFile); got != 1 {
		t.Fatalf("repeat complete left %d formal file(s), want 1", got)
	}

	// 8. 会话过期后重复确认仍然返回原结果。
	if err := client.Model(&model.UploadSession{}).
		Where("id = ?", sessionInfo.ID).
		UpdateColumn("expires_at", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatal(err)
	}
	expiredReplay := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodPost, "/api/upload-sessions/"+sessionInfo.ID+"/complete", "", tokenA,
	))
	if expiredReplay.Code != http.StatusOK {
		t.Fatalf("expired replay: wanted 200, got %d: %s", expiredReplay.Code, expiredReplay.Body.String())
	}
	var replayed cloudfile.UploadSessionInfo
	if err := json.Unmarshal(expiredReplay.Body.Bytes(), &replayed); err != nil {
		t.Fatal(err)
	}
	if replayed.ResultFileID == nil || *replayed.ResultFileID != fileID {
		t.Fatalf("expired replay returned %+v, want %q", replayed, fileID)
	}

	// 9. 数据库里的三个变化都已提交：File、Entity active/1、Session completed。
	var storedSession model.UploadSession
	if err := client.First(&storedSession, "id = ?", sessionInfo.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedSession.Status != model.UploadStatusCompleted ||
		storedSession.ResultFileID == nil || *storedSession.ResultFileID != fileID {
		t.Fatalf("stored session = %+v", storedSession)
	}
	var entity model.Entity
	if err := client.First(&entity, storedSession.EntityID).Error; err != nil {
		t.Fatal(err)
	}
	if entity.Status != model.EntityStatusActive || entity.RefCount != 1 ||
		entity.Size != int64(len(source)) {
		t.Fatalf("stored entity = %+v", entity)
	}
}

// TestUploadCancelFlowOverHTTP 用真实 MySQL + 真实 Service 覆盖取消上传：
// ready 取消 → 202 canceled、占位内容和实体被清理、不能再完成；
// completed 取消 → 409，正式文件仍可下载。
func TestUploadCancelFlowOverHTTP(t *testing.T) {
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run the upload cancel flow")
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

	router := httpapi.NewRouter(
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
		newTestFileService(t, client),
		auth.NewService(client),
	)
	tokenA := login(t, router, testUsernameA, testPasswordA)
	tokenB := login(t, router, testUsernameB, testPasswordB)

	const source = "cancel me\n"

	// 1. 创建会话并上传完整内容。
	created := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodPost, "/api/upload-sessions",
		fmt.Sprintf(`{"name":"取消.txt","size":%d}`, len(source)), tokenA,
	))
	if created.Code != http.StatusCreated {
		t.Fatalf("create session: wanted 201, got %d: %s", created.Code, created.Body.String())
	}
	var sessionInfo cloudfile.UploadSessionInfo
	if err := json.Unmarshal(created.Body.Bytes(), &sessionInfo); err != nil {
		t.Fatal(err)
	}
	uploaded := serve(t, router, uploadSessionContentHTTPRequest(t, sessionInfo.ID, source, tokenA))
	if uploaded.Code != http.StatusOK {
		t.Fatalf("upload content: wanted 200, got %d: %s", uploaded.Code, uploaded.Body.String())
	}

	var stored model.UploadSession
	if err := client.First(&stored, "id = ?", sessionInfo.ID).Error; err != nil {
		t.Fatal(err)
	}
	entityID := stored.EntityID

	// 2. 越权取消不影响会话。
	foreign := serve(t, router, uploadSessionCancelHTTPRequest(t, sessionInfo.ID, tokenB))
	if foreign.Code != http.StatusNotFound {
		t.Fatalf("foreign cancel: wanted 404, got %d: %s", foreign.Code, foreign.Body.String())
	}

	// 3. 正常取消：202 + canceled，占位实体被清理。
	canceled := serve(t, router, uploadSessionCancelHTTPRequest(t, sessionInfo.ID, tokenA))
	if canceled.Code != http.StatusAccepted {
		t.Fatalf("cancel: wanted 202, got %d: %s", canceled.Code, canceled.Body.String())
	}
	var cancelInfo cloudfile.UploadSessionInfo
	if err := json.Unmarshal(canceled.Body.Bytes(), &cancelInfo); err != nil {
		t.Fatal(err)
	}
	if cancelInfo.Status != model.UploadStatusCanceled {
		t.Fatalf("canceled info = %+v", cancelInfo)
	}
	if got := countWhere(t, client, &model.Entity{}, "id = ?", entityID); got != 0 {
		t.Fatalf("canceled placeholder entity %d still exists", entityID)
	}
	if got := countWhere(t, client, &model.File{}, "kind = ?", model.KindFile); got != 0 {
		t.Fatalf("cancel created %d formal file(s)", got)
	}

	// 4. 重复取消仍然 202；取消后不能再完成。
	repeated := serve(t, router, uploadSessionCancelHTTPRequest(t, sessionInfo.ID, tokenA))
	if repeated.Code != http.StatusAccepted {
		t.Fatalf("repeat cancel: wanted 202, got %d: %s", repeated.Code, repeated.Body.String())
	}
	completeAfterCancel := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodPost, "/api/upload-sessions/"+sessionInfo.ID+"/complete", "", tokenA,
	))
	if completeAfterCancel.Code != http.StatusConflict {
		t.Fatalf("complete after cancel: wanted 409, got %d: %s",
			completeAfterCancel.Code, completeAfterCancel.Body.String())
	}
	var errorBody httpapi.ErrorResponse
	if err := json.Unmarshal(completeAfterCancel.Body.Bytes(), &errorBody); err != nil {
		t.Fatal(err)
	}
	if errorBody.Code != "upload_session_not_completable" {
		t.Fatalf("complete after cancel code = %q", errorBody.Code)
	}

	// 5. 目录和下载接口都看不到被取消的内容。
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
		t.Fatalf("canceled upload is visible in the directory: %+v", listing)
	}

	// 6. completed 取消返回 409，正式文件仍可下载。
	doneCreated := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodPost, "/api/upload-sessions",
		fmt.Sprintf(`{"name":"保留.txt","size":%d}`, len(source)), tokenA,
	))
	if doneCreated.Code != http.StatusCreated {
		t.Fatalf("create completed session: wanted 201, got %d: %s", doneCreated.Code, doneCreated.Body.String())
	}
	var doneSession cloudfile.UploadSessionInfo
	if err := json.Unmarshal(doneCreated.Body.Bytes(), &doneSession); err != nil {
		t.Fatal(err)
	}
	if resp := serve(t, router, uploadSessionContentHTTPRequest(t, doneSession.ID, source, tokenA)); resp.Code != http.StatusOK {
		t.Fatalf("upload completed content: wanted 200, got %d: %s", resp.Code, resp.Body.String())
	}
	doneComplete := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodPost, "/api/upload-sessions/"+doneSession.ID+"/complete", "", tokenA,
	))
	if doneComplete.Code != http.StatusOK {
		t.Fatalf("complete: wanted 200, got %d: %s", doneComplete.Code, doneComplete.Body.String())
	}
	var doneInfo cloudfile.UploadSessionInfo
	if err := json.Unmarshal(doneComplete.Body.Bytes(), &doneInfo); err != nil {
		t.Fatal(err)
	}
	fileID := *doneInfo.ResultFileID

	cancelCompleted := serve(t, router, uploadSessionCancelHTTPRequest(t, doneSession.ID, tokenA))
	if cancelCompleted.Code != http.StatusConflict {
		t.Fatalf("cancel completed: wanted 409, got %d: %s", cancelCompleted.Code, cancelCompleted.Body.String())
	}
	if err := json.Unmarshal(cancelCompleted.Body.Bytes(), &errorBody); err != nil {
		t.Fatal(err)
	}
	if errorBody.Code != "upload_session_completed" {
		t.Fatalf("cancel completed code = %q", errorBody.Code)
	}
	download := serve(t, router, authorize(
		httptest.NewRequest(http.MethodGet, "/api/files/"+fileID, nil), tokenA,
	))
	if download.Code != http.StatusOK || download.Body.String() != source {
		t.Fatalf("completed file must survive cancel: status=%d body=%q",
			download.Code, download.Body.String())
	}
}

func uploadSessionCancelHTTPRequest(t *testing.T, sessionID, token string) *http.Request {
	t.Helper()

	req := httptest.NewRequest(http.MethodDelete, "/api/upload-sessions/"+sessionID, nil)
	return authorize(req, token)
}
