package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRoutes(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	r := NewRouter(logger, newFakeFileService())
	r.GET("/panic", func(c *gin.Context) { panic("SECRET") })
	for _, tc := range []struct {
		method, path string
		status       int
		key, value   string
	}{
		{http.MethodGet, "/healthz", http.StatusOK, "status", "ok"},
		{http.MethodGet, "/missing", http.StatusNotFound, "code", "not_found"},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed, "code", "method_not_allowed"},
		{http.MethodGet, "/panic", http.StatusInternalServerError, "code", "internal_error"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
			if w.Code != tc.status {
				t.Fatalf("wanted %d, got %d", tc.status, w.Code)
			}
			var body map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body[tc.key] != tc.value || strings.Contains(w.Body.String(), "SECRET") {
				t.Fatalf("unexpected response: %s", w.Body.String())
			}
		})
	}
}
