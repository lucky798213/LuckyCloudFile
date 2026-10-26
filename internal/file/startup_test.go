package file_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"

	"luckyCloudFile/internal/app"
	"luckyCloudFile/internal/config"
	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/model"
)

// TestNormalStartupDoesNotCreateUsers 验证正常启动只初始化服务，
// 不会偷偷补建任何默认账号。账号必须通过 cmd/createuser 显式创建。
//
// 用一个被占用的端口启动，可以完整经过“读配置 → 连库/迁移 → 建路由 → 监听失败”
// 这条启动路径，然后在端口绑定前停下，方便检查数据库里的用户数量。
func TestNormalStartupDoesNotCreateUsers(t *testing.T) {
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run the startup account check")
	}

	ctx := context.Background()
	client, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close(client) })
	resetAuthTables(t, client)

	if count := countRows(t, client, &model.User{}); count != 0 {
		t.Fatalf("clean database should have no users, got %d", count)
	}

	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = occupied.Close() })

	cfg := config.Config{
		HTTP: config.HTTP{
			Listen:                 occupied.Addr().String(),
			ShutdownTimeoutSeconds: 1,
		},
		Storage:  storageConfigForPolicies(t, client),
		Database: databaseConfig(t, dsn),
	}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	err = app.Run(ctx, cfg, logger)
	if err == nil || !strings.Contains(err.Error(), "listen HTTP") {
		t.Fatalf("wanted occupied-port error, got %v", err)
	}

	if count := countRows(t, client, &model.User{}); count != 0 {
		t.Fatalf("normal startup created %d default user(s), want 0", count)
	}
	t.Log("PASS: normal startup created no users")
}

// storageConfigForPolicies 给测试库里现有的每条策略配一个临时目录，
// 避免和别的测试包共用数据库时，启动因为临时策略缺少配置而失败。
func storageConfigForPolicies(t *testing.T, db *gorm.DB) config.Storage {
	t.Helper()

	storageCfg := config.Storage{
		LocalRoot: t.TempDir(),
		// 同包的存储策略测试会创建这条策略；提前配好目录，避免启动测试
		// 在它清理之前运行时误报“策略缺少配置”。
		LocalRoots: map[string]string{"test-policy-b": t.TempDir()},
	}
	var policies []model.StoragePolicy
	if err := db.Find(&policies).Error; err != nil {
		t.Fatal(err)
	}
	for _, policy := range policies {
		if policy.ConfigKey == model.LocalStorageConfigKey {
			continue
		}
		storageCfg.LocalRoots[policy.ConfigKey] = t.TempDir()
	}
	return storageCfg
}

func databaseConfig(t *testing.T, dsn string) config.Database {
	t.Helper()
	mc, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	host, portText, err := net.SplitHostPort(mc.Addr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	return config.Database{
		Host: host, Port: port, User: mc.User, Password: mc.Passwd, Name: mc.DBName,
	}
}
