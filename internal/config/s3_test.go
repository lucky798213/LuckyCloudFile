package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const s3INI = `
[StorageS3 cloud]
Endpoint = http://127.0.0.1:9000
Region = us-east-1
Bucket = test-bucket
Prefix = files
SpoolDir = ./s3-spool
ForcePathStyle = true
AccessKeyIDEnv = LUCKY_CONFIG_TEST_ACCESS_KEY
SecretAccessKeyEnv = LUCKY_CONFIG_TEST_SECRET_KEY
`

func setS3TestCredentials(t *testing.T) {
	t.Helper()
	t.Setenv("LUCKY_CONFIG_TEST_ACCESS_KEY", "test-access")
	t.Setenv("LUCKY_CONFIG_TEST_SECRET_KEY", "test-secret")
}

func TestLoadS3Storage(t *testing.T) {
	setS3TestCredentials(t)
	path := writeConfig(t, validINI+s3INI)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	opts, ok := cfg.Storage.S3["cloud"]
	if !ok || opts.Endpoint != "http://127.0.0.1:9000" || opts.Bucket != "test-bucket" ||
		opts.AccessKeyID != "test-access" || opts.SecretAccessKey != "test-secret" || !opts.ForcePathStyle {
		t.Fatal("S3 fields or credential environment references were not loaded")
	}
	if opts.SpoolDir != filepath.Join(filepath.Dir(path), "s3-spool") {
		t.Fatalf("spool path = %s", opts.SpoolDir)
	}
	if _, err := os.Stat(opts.SpoolDir); !os.IsNotExist(err) {
		t.Fatal("loading S3 config must not create directories")
	}
}

func TestLoadRejectsInvalidS3Storage(t *testing.T) {
	setS3TestCredentials(t)
	for _, tc := range []struct{ name, section, want string }{
		{"missing credentials", strings.ReplaceAll(s3INI, "LUCKY_CONFIG_TEST_SECRET_KEY", "LUCKY_CONFIG_MISSING_KEY"), "secret access key"},
		{"missing env reference", strings.ReplaceAll(s3INI, "AccessKeyIDEnv = LUCKY_CONFIG_TEST_ACCESS_KEY", ""), "AccessKeyIDEnv"},
		{"endpoint path", strings.ReplaceAll(s3INI, "http://127.0.0.1:9000", "https://host/path"), "endpoint"},
		{"reserved local key", strings.ReplaceAll(s3INI, "StorageS3 cloud", "StorageS3 local"), "reserved"},
		{"conflicting local key", s3INI + "\n[StorageLocal]\ncloud = ./local-cloud\n", "StorageLocal"},
		{"bad bool", strings.ReplaceAll(s3INI, "ForcePathStyle = true", "ForcePathStyle = invalid"), "parse [StorageS3"},
		{"missing spool", strings.ReplaceAll(s3INI, "SpoolDir = ./s3-spool", ""), "spool"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, validINI+tc.section))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted error containing %q, got %v", tc.want, err)
			}
			if strings.Contains(err.Error(), "test-secret") {
				t.Fatal("configuration error leaked credentials")
			}
		})
	}
}

func TestLoadS3SessionToken(t *testing.T) {
	setS3TestCredentials(t)
	t.Setenv("LUCKY_CONFIG_TEST_TOKEN", "test-session-token")
	ini := validINI + s3INI + "SessionTokenEnv = LUCKY_CONFIG_TEST_TOKEN\n"
	cfg, err := Load(writeConfig(t, ini))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.S3["cloud"].SessionToken != "test-session-token" {
		t.Fatal("session token was not loaded")
	}
	t.Setenv("LUCKY_CONFIG_TEST_TOKEN", "")
	if _, err := Load(writeConfig(t, ini)); err == nil {
		t.Fatal("explicit but missing STS token accepted")
	}
}
