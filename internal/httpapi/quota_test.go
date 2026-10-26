package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"luckyCloudFile/internal/file"
)

// TestHandleFileErrorMapsQuotaExceeded 确认容量不足有稳定的 HTTP 表达：
// 507 Insufficient Storage + code=insufficient_storage，而不是笼统的 500。
func TestHandleFileErrorMapsQuotaExceeded(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)

	// 业务层通常会把哨兵错误再包装一层，映射必须仍然命中。
	err := fmt.Errorf("publish file: %w", file.ErrQuotaExceeded)
	if !handleFileError(c, err, "file_not_found", "file not found") {
		t.Fatal("handleFileError did not recognize ErrQuotaExceeded")
	}

	if recorder.Code != http.StatusInsufficientStorage {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInsufficientStorage)
	}
	var body ErrorResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Code != "insufficient_storage" {
		t.Fatalf("error code = %q, want insufficient_storage", body.Code)
	}

	// 其他未知错误不能被误判成容量不足，否则客户端会以为调大配额就能解决。
	other := httptest.NewRecorder()
	otherContext, _ := gin.CreateTestContext(other)
	if handleFileError(otherContext, errors.New("database is down"), "file_not_found", "file not found") {
		t.Fatal("unknown error must not be mapped to a quota error")
	}
}
