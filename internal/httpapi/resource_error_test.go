package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"luckyCloudFile/internal/file"
)

// TestHandleFileErrorMapsResourceErrorsToDistinctConflicts 验证资源锁相关的
// 两个业务错误都返回 409，但错误码必须不同。
func TestHandleFileErrorMapsResourceErrorsToDistinctConflicts(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code string
	}{
		{
			name: "locked",
			err:  file.ErrResourceLocked,
			code: "resource_locked",
		},
		{
			name: "locked wrapped",
			err:  fmt.Errorf("rename file: %w", file.ErrResourceLocked),
			code: "resource_locked",
		},
		{
			name: "changed",
			err:  file.ErrResourceChanged,
			code: "resource_changed",
		},
		{
			name: "changed wrapped",
			err:  fmt.Errorf("delete file: %w", file.ErrResourceChanged),
			code: "resource_changed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)

			if !handleFileError(ctx, tc.err, "not_found", "not found") {
				t.Fatalf("handleFileError did not handle %v", tc.err)
			}
			if recorder.Code != http.StatusConflict {
				t.Fatalf("status = %d, want %d: %s",
					recorder.Code, http.StatusConflict, recorder.Body.String())
			}
			assertErrorCode(t, recorder, tc.code)
		})
	}
}

// TestHandleFileErrorDoesNotClaimExpiredSession 验证公共映射不会吞掉上传
// 会话过期错误，上传处理器仍然保留 410 upload_session_expired 的语义。
func TestHandleFileErrorDoesNotClaimExpiredSession(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	if handleFileError(ctx, file.ErrUploadSessionExpired, "not_found", "not found") {
		t.Fatal("handleFileError claimed upload session expiry")
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("handleFileError wrote a response for upload session expiry: %s",
			recorder.Body.String())
	}
}

// TestUploadChunkKeepsGoneForExpiredSession 验证分片处理器在 default 分支
// 接入公共映射后，真正过期的会话仍然是 410，而不是被映射成 409。
func TestUploadChunkKeepsGoneForExpiredSession(t *testing.T) {
	router, fake := newTestRouter(t)

	created := httptest.NewRecorder()
	router.ServeHTTP(created, uploadSessionJSONRequest(
		t, http.MethodPost, "/api/upload-sessions",
		`{"name":"expired.bin","size":4,"mode":"chunked"}`,
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

	// 测试替身按 expires_at 判断过期；把它改到过去即可走真实的 410 分支。
	fake.mu.Lock()
	session := fake.sessions[info.ID]
	session.info.ExpiresAt = time.Now().Add(-time.Minute)
	fake.sessions[info.ID] = session
	fake.mu.Unlock()

	expired := httptest.NewRecorder()
	router.ServeHTTP(expired, uploadSessionJSONRequest(
		t, http.MethodPut, "/api/upload-sessions/"+info.ID+"/chunks/0",
		"abcd", testTokenA,
	))
	if expired.Code != http.StatusGone {
		t.Fatalf("expired chunk session: wanted 410, got %d: %s",
			expired.Code, expired.Body.String())
	}
	assertErrorCode(t, expired, "upload_session_expired")
}
