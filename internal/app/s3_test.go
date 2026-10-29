package app

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"luckyCloudFile/internal/config"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
)

type s3BucketTransport func(*http.Request) (*http.Response, error)

func (f s3BucketTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestBuildStorageRegistryWithLocalAndS3(t *testing.T) {
	cfg := config.Storage{LocalRoot: t.TempDir(), S3: map[string]storage.S3Options{
		"cloud": {Endpoint: "https://s3.test", Region: "us-east-1", Bucket: "test-bucket",
			SpoolDir: t.TempDir(), ForcePathStyle: true, AccessKeyID: "test-key", SecretAccessKey: "test-secret",
			Transport: s3BucketTransport(func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodHead || (r.URL.Path != "/test-bucket/" && r.URL.Path != "/test-bucket") {
					t.Errorf("startup made unexpected request: %s %s", r.Method, r.URL.Path)
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
			}),
		},
	}}
	policies := []model.StoragePolicy{
		{ID: 1, Type: model.StorageTypeLocal, ConfigKey: "local"},
		{ID: 2, Type: model.StorageTypeS3, ConfigKey: "cloud"},
	}
	registry, err := buildStorageRegistry(policies, cfg)
	if err != nil {
		t.Fatal(err)
	}
	local, err := registry.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := local.(*storage.Local); !ok {
		t.Fatal("local policy was not kept local")
	}
	cloud, err := registry.Get(2)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cloud.(*storage.S3); !ok {
		t.Fatal("S3 policy was not routed to S3")
	}
	// 重启重建后 ID 绑定仍相同，不依赖映射遍历顺序。
	if _, err := buildStorageRegistry(policies, cfg); err != nil {
		t.Fatalf("rebuild registry: %v", err)
	}
	delete(cfg.S3, "cloud")
	if _, err := buildStorageRegistry(policies, cfg); err == nil || !strings.Contains(err.Error(), "no S3 config") {
		t.Fatalf("missing S3 config silently fell back to local: %v", err)
	}
	if _, err := registry.Get(3); err == nil {
		t.Fatal("unknown policy silently fell back")
	}
	if _, err := local.Save(context.Background(), "local-still-works", strings.NewReader("ok"), 2); err != nil {
		t.Fatal(err)
	}
}

func TestBuildStorageRegistryRejectsUnavailableS3Bucket(t *testing.T) {
	cfg := config.Storage{S3: map[string]storage.S3Options{"cloud": {
		Endpoint: "https://s3.test", Region: "us-east-1", Bucket: "test-bucket", SpoolDir: t.TempDir(),
		AccessKeyID: "test-key", SecretAccessKey: "test-secret", ForcePathStyle: true,
		Transport: s3BucketTransport(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusForbidden, Header: make(http.Header),
				Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
		}),
	}}}
	_, err := buildStorageRegistry([]model.StoragePolicy{{ID: 2, Type: model.StorageTypeS3, ConfigKey: "cloud"}}, cfg)
	if err == nil {
		t.Fatal("unavailable S3 bucket accepted at startup")
	}
}
