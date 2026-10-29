package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// 这里模拟 S3 HTTP 协议，不 mock Store：请求仍经过真正的 SDK、签名和序列化。
// 它不替代对真实对象存储的兼容性验收（见 TestS3LiveContract）。
type s3TestServer struct {
	mu                            sync.Mutex
	objects                       map[string][]byte
	denyPut, denyDelete, denyHead bool
	putThenFail                   bool
	handler                       http.Handler
}

func newS3TestServer(t *testing.T) *s3TestServer {
	t.Helper()
	s := &s3TestServer{objects: make(map[string][]byte)}
	s.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
			t.Error("S3 request did not use Signature V4")
		}
		if r.URL.Path == "/test-bucket/" || r.URL.Path == "/test-bucket" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/test-bucket/") {
			t.Errorf("unexpected bucket path: %s", r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
		deny := r.Method == http.MethodPut && s.denyPut ||
			r.Method == http.MethodDelete && s.denyDelete || r.Method == http.MethodHead && s.denyHead
		if deny {
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, "<Error><Code>AccessDenied</Code></Error>")
			return
		}
		switch r.Method {
		case http.MethodPut:
			body := r.Body
			if body == nil {
				body = http.NoBody
			}
			data, err := io.ReadAll(body)
			if err != nil {
				t.Error(err)
			}
			digest := md5.Sum(data)
			if got := r.Header.Get("Content-MD5"); got != base64.StdEncoding.EncodeToString(digest[:]) {
				t.Errorf("PUT integrity checksum = %q", got)
			}
			s.objects[key] = data
			if s.putThenFail {
				w.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(w, "<Error><Code>AccessDenied</Code></Error>")
				return
			}
			w.Header().Set("ETag", fmt.Sprintf("\"%x\"", digest))
		case http.MethodGet, http.MethodHead:
			data, ok := s.objects[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, "<Error><Code>NoSuchKey</Code></Error>")
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.Header().Set("ETag", "\"test-etag\"")
			if r.Method == http.MethodGet {
				_, _ = w.Write(data)
			}
		case http.MethodDelete:
			delete(s.objects, key)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected S3 method: %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	return s
}

func (s *s3TestServer) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	w := httptest.NewRecorder()
	s.handler.ServeHTTP(w, r)
	resp := w.Result()
	resp.Request = r
	return resp, nil
}

func (s *s3TestServer) options(t *testing.T) S3Options {
	t.Helper()
	return S3Options{Endpoint: "http://s3.test", Region: "us-east-1", Bucket: "test-bucket",
		Prefix: "tenant/files", SpoolDir: t.TempDir(), ForcePathStyle: true,
		AccessKeyID: "test-access-key", SecretAccessKey: "test-secret-key", Transport: s}
}

func newTestS3(t *testing.T, server *s3TestServer) *S3 {
	t.Helper()
	store, err := NewS3(server.options(t))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestS3StoreContract(t *testing.T) {
	testStoreContract(t, func(t *testing.T) Store {
		return newTestS3(t, newS3TestServer(t))
	})
}

func TestS3SaveBoundariesAndPrefix(t *testing.T) {
	server := newS3TestServer(t)
	store := newTestS3(t, server)
	ctx := context.Background()
	if err := store.Check(ctx); err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{"", "hello"} {
		if n, err := store.Save(ctx, "key", strings.NewReader(data), int64(len(data))); err != nil || n != int64(len(data)) {
			t.Fatalf("Save(%q) = %d, %v", data, n, err)
		}
		server.mu.Lock()
		got := string(server.objects["tenant/files/key"])
		server.mu.Unlock()
		if got != data {
			t.Fatalf("prefixed cloud content = %q, want %q", got, data)
		}
	}
	entries, err := os.ReadDir(store.spool.root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("successful upload left spool data: %v, %v", entries, err)
	}
	if _, err := store.Save(ctx, "bad", strings.NewReader("x"), -1); err == nil {
		t.Fatal("negative limit accepted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Save(canceled, "cancel", strings.NewReader("x"), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled save: %v", err)
	}
	if _, err := store.Open(canceled, "key"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled open: %v", err)
	}
}

func TestS3ChunksFinalizeRetryAndCleanup(t *testing.T) {
	server := newS3TestServer(t)
	store := newTestS3(t, server)
	ctx := context.Background()
	if err := store.PrepareChunks(ctx, "chunked", 5); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunkAt(ctx, "chunked", 2, []byte("llo")); err != nil {
		t.Fatal(err)
	}
	if err := store.PrepareChunks(ctx, "chunked", 5); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunkAt(ctx, "chunked", 0, []byte("he")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Open(ctx, "chunked"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("unfinalized chunks became visible: %v", err)
	}
	server.mu.Lock()
	server.denyPut = true
	server.mu.Unlock()
	if err := store.FinalizeChunks(ctx, "chunked", 5); err == nil {
		t.Fatal("PUT error was ignored")
	}
	server.mu.Lock()
	server.denyPut = false
	server.mu.Unlock()
	// 上传失败不丢分片；重试后又可以从云端大小核对路径幂等返回。
	for i := 0; i < 2; i++ {
		if err := store.FinalizeChunks(ctx, "chunked", 5); err != nil {
			t.Fatalf("finalize attempt %d: %v", i, err)
		}
	}
	reader, err := store.Open(ctx, "chunked")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(data) != "hello" {
		t.Fatalf("chunked result = %q, %v", data, err)
	}
	if err := store.FinalizeChunks(ctx, "chunked", 6); err == nil {
		t.Fatal("cloud size mismatch accepted")
	}
	if err := store.Remove(ctx, "chunked"); err != nil {
		t.Fatal(err)
	}
	if err := store.PrepareChunks(ctx, "canceled", 5); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteChunkAt(ctx, "canceled", 4, []byte("xx")); !errors.Is(err, ErrChunkOutOfRange) {
		t.Fatalf("out of range chunk: %v", err)
	}
	if err := store.Remove(ctx, "canceled"); err != nil {
		t.Fatalf("remove local-only chunks: %v", err)
	}
	entries, _ := os.ReadDir(store.spool.root)
	if len(entries) != 0 {
		t.Fatalf("cleanup left staged chunks: %v", entries)
	}
}

func TestS3PermissionFailuresAreNotMissingObjects(t *testing.T) {
	server := newS3TestServer(t)
	store := newTestS3(t, server)
	ctx := context.Background()
	if _, err := store.Save(ctx, "protected", bytes.NewReader([]byte("hello")), 5); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	server.denyHead = true
	server.mu.Unlock()
	if _, err := store.Open(ctx, "protected"); err == nil || errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("403 was hidden as success or missing: %v", err)
	}
	if err := store.Remove(ctx, "protected"); err == nil || errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("HEAD failure lost cleanup responsibility: %v", err)
	}
	server.mu.Lock()
	server.denyHead, server.denyDelete = false, true
	server.mu.Unlock()
	if err := store.Remove(ctx, "protected"); err == nil || errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("DELETE failure lost cleanup responsibility: %v", err)
	}
	server.mu.Lock()
	server.denyDelete = false
	server.mu.Unlock()
	if err := store.Remove(ctx, "protected"); err != nil {
		t.Fatalf("cleanup retry: %v", err)
	}
}

func TestS3SaveCleansUpFailedUpload(t *testing.T) {
	server := newS3TestServer(t)
	store := newTestS3(t, server)
	ctx := context.Background()
	if _, err := store.Save(ctx, "reader-error", brokenReader{}, 5); err == nil {
		t.Fatal("source reader failure ignored")
	}
	server.mu.Lock()
	server.putThenFail = true
	server.mu.Unlock()
	if _, err := store.Save(ctx, "ambiguous-put", strings.NewReader("hello"), 5); err == nil {
		t.Fatal("PUT error ignored")
	}
	server.mu.Lock()
	remaining := len(server.objects)
	server.mu.Unlock()
	if remaining != 0 {
		t.Fatal("failed PUT compensation left a remote object")
	}
	entries, err := os.ReadDir(store.spool.root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed upload left spool data: %v, %v", entries, err)
	}
}

func TestS3RejectsInvalidConfigurationAndKeys(t *testing.T) {
	server := newS3TestServer(t)
	base := server.options(t)
	for _, change := range []func(*S3Options){
		func(o *S3Options) { o.Endpoint = "ftp://host" },
		func(o *S3Options) { o.Endpoint = "https://user:secret@host" },
		func(o *S3Options) { o.Endpoint = "https://host/path" },
		func(o *S3Options) { o.Region = "" },
		func(o *S3Options) { o.Bucket = "" },
		func(o *S3Options) { o.AccessKeyID = "" },
		func(o *S3Options) { o.SecretAccessKey = "" },
		func(o *S3Options) { o.SpoolDir = "" },
		func(o *S3Options) { o.Prefix = "../escape" },
	} {
		opts := base
		change(&opts)
		if _, err := NewS3(opts); err == nil {
			t.Fatal("invalid S3 configuration accepted")
		}
	}
	store := newTestS3(t, server)
	for _, key := range []string{"", ".", "..", "../escape", "/absolute", "dir/key", "dir\\key"} {
		if _, err := store.Save(context.Background(), key, strings.NewReader("x"), 1); err == nil {
			t.Fatalf("unsafe key %q accepted", key)
		}
	}
}

func TestS3LiveContract(t *testing.T) {
	endpoint := os.Getenv("CLOUDREVE_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set CLOUDREVE_TEST_S3_* to run the real S3 compatibility contract")
	}
	// 每个子测试隔离前缀，不会读取或覆盖已有对象；只使用专用测试 bucket。
	testStoreContract(t, func(t *testing.T) Store {
		opts := S3Options{Endpoint: endpoint, Region: os.Getenv("CLOUDREVE_TEST_S3_REGION"),
			Bucket: os.Getenv("CLOUDREVE_TEST_S3_BUCKET"), ForcePathStyle: true,
			AccessKeyID:     os.Getenv("CLOUDREVE_TEST_S3_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("CLOUDREVE_TEST_S3_SECRET_ACCESS_KEY"),
			SessionToken:    os.Getenv("CLOUDREVE_TEST_S3_SESSION_TOKEN"),
			Prefix:          fmt.Sprintf("contract/%d", time.Now().UnixNano()), SpoolDir: t.TempDir()}
		store, err := NewS3(opts)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := store.Check(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			for _, key := range []string{"contract-key", "too-large"} {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				err := store.Remove(ctx, key)
				cancel()
				if err != nil && !errors.Is(err, fs.ErrNotExist) {
					t.Errorf("cleanup live object: %v", err)
				}
			}
		})
		return store
	})
}
