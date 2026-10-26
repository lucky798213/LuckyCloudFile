package file_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"

	"luckyCloudFile/internal/auth"
	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/httpapi"
	"luckyCloudFile/internal/model"
)

// TestUploadSessionQuotaExceededOverHTTP 是容量闭环的 HTTP 验收：
// 创建会话时的预留不足要返回 507 insufficient_storage，并且不能留下会话或占位记录。
func TestUploadSessionQuotaExceededOverHTTP(t *testing.T) {
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run the upload session quota flow")
	}

	ctx := context.Background()
	client, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close(client) })
	resetAuthTables(t, client)

	if _, err := auth.CreateUser(ctx, client, testUsernameA, testPasswordA); err != nil {
		t.Fatal(err)
	}
	userA := mustUser(t, client, testUsernameA)
	const quotaBytes = int64(10)
	if err := client.Model(&model.User{}).
		Where("id = ?", userA.ID).
		UpdateColumn("quota_bytes", quotaBytes).Error; err != nil {
		t.Fatal(err)
	}

	router := httpapi.NewRouter(
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
		newTestFileService(t, client),
		auth.NewService(client),
	)
	tokenA := login(t, router, testUsernameA, testPasswordA)

	// 声明 11 字节，但配额只有 10：直接拒绝。
	over := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodPost, "/api/upload-sessions",
		`{"name":"too-big.bin","size":11}`, tokenA,
	))
	if over.Code != http.StatusInsufficientStorage {
		t.Fatalf("over-quota create: wanted 507, got %d: %s", over.Code, over.Body.String())
	}
	var body httpapi.ErrorResponse
	if err := json.Unmarshal(over.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Code != "insufficient_storage" {
		t.Fatalf("over-quota create code = %q, want insufficient_storage", body.Code)
	}

	// 失败请求不能留下会话、占位 Entity 或预留。
	if got := countRows(t, client, &model.UploadSession{}); got != 0 {
		t.Fatalf("rejected create left %d session(s)", got)
	}
	if got := countRows(t, client, &model.Entity{}); got != 0 {
		t.Fatalf("rejected create left %d entity/entities", got)
	}
	var storedUser model.User
	if err := client.First(&storedUser, userA.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedUser.ReservedBytes != 0 {
		t.Fatalf("rejected create left %d reserved bytes", storedUser.ReservedBytes)
	}

	// 正好用满：允许创建，并在用户行上记录预留。
	exact := serve(t, router, uploadSessionHTTPRequest(
		t, http.MethodPost, "/api/upload-sessions",
		`{"name":"exact.bin","size":10}`, tokenA,
	))
	if exact.Code != http.StatusCreated {
		t.Fatalf("exact-fit create: wanted 201, got %d: %s", exact.Code, exact.Body.String())
	}
	if err := client.First(&storedUser, userA.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedUser.ReservedBytes != quotaBytes || storedUser.UsedBytes != 0 {
		t.Fatalf("accounting after exact-fit create: used=%d reserved=%d",
			storedUser.UsedBytes, storedUser.ReservedBytes)
	}
}

// TestLegacyUploadQuotaExceededOverHTTP 确认旧上传接口也不能绕过配额：
// 发布阶段容量不足返回 507，且不会留下 File 记录。
func TestLegacyUploadQuotaExceededOverHTTP(t *testing.T) {
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run the upload quota flow")
	}

	ctx := context.Background()
	client, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close(client) })
	resetAuthTables(t, client)

	if _, err := auth.CreateUser(ctx, client, testUsernameA, testPasswordA); err != nil {
		t.Fatal(err)
	}
	userA := mustUser(t, client, testUsernameA)
	if err := client.Model(&model.User{}).
		Where("id = ?", userA.ID).
		UpdateColumn("quota_bytes", int64(10)).Error; err != nil {
		t.Fatal(err)
	}

	router := httpapi.NewRouter(
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
		newTestFileService(t, client),
		auth.NewService(client),
	)
	tokenA := login(t, router, testUsernameA, testPasswordA)

	first := serve(t, router, newUploadRequest(t, tokenA, []byte("hello")))
	if first.Code != http.StatusCreated {
		t.Fatalf("first upload: wanted 201, got %d: %s", first.Code, first.Body.String())
	}

	over := serve(t, router, newUploadRequest(t, tokenA, []byte("hello!")))
	if over.Code != http.StatusInsufficientStorage {
		t.Fatalf("over-quota upload: wanted 507, got %d: %s", over.Code, over.Body.String())
	}
	var body httpapi.ErrorResponse
	if err := json.Unmarshal(over.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Code != "insufficient_storage" {
		t.Fatalf("over-quota upload code = %q, want insufficient_storage", body.Code)
	}

	if got := countWhere(t, client, &model.File{}, "kind = ?", model.KindFile); got != 1 {
		t.Fatalf("rejected upload left %d formal file(s), want 1", got)
	}
	var storedUser model.User
	if err := client.First(&storedUser, userA.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedUser.UsedBytes != 5 || storedUser.ReservedBytes != 0 {
		t.Fatalf("accounting after rejected upload: used=%d reserved=%d",
			storedUser.UsedBytes, storedUser.ReservedBytes)
	}
}
