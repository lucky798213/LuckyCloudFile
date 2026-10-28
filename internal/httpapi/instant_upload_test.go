package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"luckyCloudFile/internal/file"
)

func instantRequest(t *testing.T, body, key string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/files/instant-upload",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	return authorize(req, testTokenA)
}

func TestInstantUploadHTTP(t *testing.T) {
	router, fake := newTestRouter(t)
	fake.instantResult = &file.InstantUploadResult{Instant: true, FileID: "file-1"}

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, instantRequest(t,
		`{"parent_id":"","name":"b.txt","size":5,"sha256":"`+digestOf("hello")+`"}`,
		"key-1",
	))
	if recorder.Code != http.StatusOK {
		t.Fatalf("wanted 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var result file.InstantUploadResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.Instant || result.FileID != "file-1" {
		t.Fatalf("result = %+v", result)
	}
	if fake.instantKey != "key-1" {
		t.Fatalf("idempotency key = %q, want key-1", fake.instantKey)
	}
}

func TestInstantUploadHTTPMiss(t *testing.T) {
	router, _ := newTestRouter(t)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, instantRequest(t,
		`{"name":"a.txt","size":5,"sha256":"`+digestOf("hello")+`"}`,
		"key-2",
	))
	if recorder.Code != http.StatusOK {
		t.Fatalf("wanted 200, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["instant"] != false {
		t.Fatalf("body = %s", recorder.Body.String())
	}
	if _, ok := result["file_id"]; ok {
		t.Fatalf("miss response must not include file_id: %s", recorder.Body.String())
	}
}

func TestInstantUploadHTTPErrors(t *testing.T) {
	router, fake := newTestRouter(t)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, instantRequest(t,
		`{"name":"a.txt","size":5,"sha256":"`+digestOf("hello")+`"}`,
		"",
	))
	if recorder.Code != http.StatusBadRequest || errorCode(t, recorder) != "invalid_idempotency_key" {
		t.Fatalf("missing key: %d %s", recorder.Code, recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, instantRequest(t, `{"name":"a.txt","sha256":"00"}`, "key"))
	if recorder.Code != http.StatusBadRequest || errorCode(t, recorder) != "invalid_size" {
		t.Fatalf("missing size: %d %s", recorder.Code, recorder.Body.String())
	}

	fake.instantErr = file.ErrInvalidContentSHA256
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, instantRequest(t,
		`{"name":"a.txt","size":5,"sha256":"`+digestOf("hello")+`"}`,
		"key",
	))
	if recorder.Code != http.StatusBadRequest || errorCode(t, recorder) != "invalid_sha256" {
		t.Fatalf("invalid digest should be rejected by the service, got %d %s",
			recorder.Code, recorder.Body.String())
	}

	fake.instantErr = file.ErrIdempotencyConflict
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, instantRequest(t,
		`{"name":"a.txt","size":5,"sha256":"`+digestOf("hello")+`"}`,
		"key",
	))
	if recorder.Code != http.StatusConflict || errorCode(t, recorder) != "idempotency_conflict" {
		t.Fatalf("conflict: %d %s", recorder.Code, recorder.Body.String())
	}
}

func digestOf(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
