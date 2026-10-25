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
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, strings.Replace(validINI, tc.old, tc.replacement, 1)))
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
