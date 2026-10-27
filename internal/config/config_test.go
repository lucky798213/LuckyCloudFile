package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validINI = `[System]
LogLevel = info
[HTTP]
Listen = 127.0.0.1:0
ShutdownTimeoutSeconds = 5
[Storage]
LocalRoot = data
[Database]
Host = HOST
Port = 3306
User = DB_USER
Name = DB_NAME
Password = file-password
`

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "conf.ini")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad(t *testing.T) {
	t.Setenv("CLOUDREVE_DB_PASSWORD", "env-password")
	path := writeConfig(t, validINI)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Host != "HOST" || cfg.Database.Port != 3306 || cfg.Database.Password != "env-password" {
		t.Fatal("database fields were not mapped or password override was not applied")
	}
	if cfg.Storage.LocalRoot != filepath.Join(filepath.Dir(path), "data") {
		t.Fatalf("relative storage path resolved incorrectly: %s", cfg.Storage.LocalRoot)
	}
	if _, err := os.Stat(cfg.Storage.LocalRoot); !os.IsNotExist(err) {
		t.Fatal("loading configuration must not create storage or connect to services")
	}
	// 旧配置没有 [Upload] 段：必须保留默认值，不能拒绝启动。
	if cfg.Upload.MaxParallelChunks != 2 {
		t.Fatalf("default MaxParallelChunks = %d, want 2", cfg.Upload.MaxParallelChunks)
	}
}

// TestLoadUploadParallel 验证 [Upload] MaxParallelChunks 真正从配置读入，
// 并且允许范围内的 1、2、8 都能启动。
func TestLoadUploadParallel(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int
	}{
		{"1", 1},
		{"2", 2},
		{"8", 8},
	} {
		t.Run(tc.value, func(t *testing.T) {
			cfg, err := Load(writeConfig(
				t, validINI+"\n[Upload]\nMaxParallelChunks = "+tc.value+"\n",
			))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Upload.MaxParallelChunks != tc.want {
				t.Fatalf("MaxParallelChunks = %d, want %d",
					cfg.Upload.MaxParallelChunks, tc.want)
			}
		})
	}
}

// TestLoadRejectsInvalidUploadParallel 验证显式写入 0、负数、超范围和
// 非整数时启动失败，而不是悄悄回退到默认值。
func TestLoadRejectsInvalidUploadParallel(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		want  string
	}{
		{"zero", "0", "Upload.MaxParallelChunks"},
		{"negative", "-1", "Upload.MaxParallelChunks"},
		{"too large", "9", "Upload.MaxParallelChunks"},
		{"not integer", "nope", "parse [Upload]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(
				t, validINI+"\n[Upload]\nMaxParallelChunks = "+tc.value+"\n",
			))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted error containing %q; got %v", tc.want, err)
			}
		})
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, old, replacement, want string
	}{
		{"invalid integer", "Port = 3306", "Port = nope", "parse [Database]"},
		{"missing listen", "Listen = 127.0.0.1:0", "Listen =", "HTTP.Listen"},
		{"public listen", "127.0.0.1:0", "0.0.0.0:0", "loopback"},
		{"invalid port", "127.0.0.1:0", "127.0.0.1:65536", "port"},
		{"invalid timeout", "ShutdownTimeoutSeconds = 5", "ShutdownTimeoutSeconds = 0", "ShutdownTimeoutSeconds"},
		{"missing storage", "LocalRoot = data", "LocalRoot =", "Storage.LocalRoot"},
		{"invalid log level", "LogLevel = info", "LogLevel = invalid", "LogLevel"},
		{"missing database host", "Host = HOST", "Host =", "Database.Host"},
		{"missing database user", "User = DB_USER", "User =", "Database.User"},
		{"missing database name", "Name = DB_NAME", "Name =", "Database.Name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, strings.Replace(validINI, tc.old, tc.replacement, 1)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted error containing %q; got %v", tc.want, err)
			}
		})
	}
}

func TestLoadStorageRootsByConfigKey(t *testing.T) {
	content := validINI + `
[StorageLocal]
archive = data-archive
`
	path := writeConfig(t, content)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	base := filepath.Dir(path)
	local, ok := cfg.Storage.RootFor("local")
	if !ok || local != filepath.Join(base, "data") {
		t.Fatalf("local root = %q, ok=%v", local, ok)
	}
	archive, ok := cfg.Storage.RootFor("archive")
	if !ok || archive != filepath.Join(base, "data-archive") {
		t.Fatalf("archive root = %q, ok=%v", archive, ok)
	}
	if _, ok := cfg.Storage.RootFor("missing"); ok {
		t.Fatal("missing config key must not resolve to another directory")
	}
}

func TestLoadRejectsInvalidStorageLocal(t *testing.T) {
	for _, tc := range []struct {
		name    string
		section string
		want    string
	}{
		{"empty root", "[StorageLocal]\narchive =\n", "must not be empty"},
		{"reserved key", "[StorageLocal]\nlocal = other-data\n", "reserved"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, validINI+tc.section))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted error containing %q; got %v", tc.want, err)
			}
		})
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "missing.ini"))
	if err == nil || !strings.Contains(err.Error(), "read config") {
		t.Fatalf("wanted missing-file error, got %v", err)
	}
}
