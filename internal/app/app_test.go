package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"

	"luckyCloudFile/internal/config"
	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/model"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func testListener(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func waitError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server or request")
		return nil
	}
}

func waitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for synchronization point")
	}
}

type observedListener struct {
	net.Listener
	closed chan struct{}
	once   sync.Once
}

func (l *observedListener) Close() error {
	err := l.Listener.Close()
	l.once.Do(func() { close(l.closed) })
	return err
}

func TestShutdownWaitsForInflightRequestAndReleasesPort(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		_, _ = io.WriteString(w, "finished")
	})}
	t.Cleanup(func() { _ = server.Close() })
	listener := &observedListener{Listener: testListener(t), closed: make(chan struct{})}
	address := listener.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, server, listener, time.Second, testLogger()) }()
	requestDone := make(chan error, 1)
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	go func() {
		response, err := client.Get("http://" + address)
		if err != nil {
			requestDone <- err
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err == nil && string(body) != "finished" {
			err = errors.New("inflight response was interrupted")
		}
		requestDone <- err
	}()
	waitSignal(t, started)
	cancel()
	// 确认服务已开始关闭监听后，才放行在途请求，避免仅靠调度碰巧通过。
	waitSignal(t, listener.closed)
	select {
	case err := <-done:
		t.Fatalf("server exited before inflight request finished: %v", err)
	default:
	}
	unblock()
	if err := waitError(t, requestDone); err != nil {
		t.Fatal(err)
	}
	if err := waitError(t, done); err != nil {
		t.Fatal(err)
	}
	rebound, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("port not released: %v", err)
	}
	_ = rebound.Close()
}

func TestShutdownTimeoutClosesRemainingConnections(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(stopped)
	})}
	t.Cleanup(func() { _ = server.Close() })
	listener := testListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, server, listener, 30*time.Millisecond, testLogger()) }()
	requestDone := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 3 * time.Second}
		defer client.CloseIdleConnections()
		response, err := client.Get("http://" + listener.Addr().String())
		if response != nil {
			_ = response.Body.Close()
		}
		requestDone <- err
	}()
	waitSignal(t, started)
	cancel()
	if err := waitError(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wanted shutdown deadline error, got %v", err)
	}
	waitSignal(t, stopped)
	_ = waitError(t, requestDone)
}

// testDatabaseConfig 从集成测试用的 DSN 里拆出 config.Database。
// 没设置 DSN 时跳过：Run 现在启动时必须连上 MySQL。
func testDatabaseConfig(t *testing.T) config.Database {
	t.Helper()

	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run tests that start the full app")
	}

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

// testStorageConfig 给数据库里现有的每条策略都配一个临时目录。
// 测试库可能和别的测试包共用，这样启动测试不会因为暂时存在的策略失败。
func testStorageConfig(t *testing.T, db *gorm.DB) config.Storage {
	t.Helper()

	storageCfg := config.Storage{
		LocalRoot: t.TempDir(),
		// 文件测试包会创建这条策略；提前配好目录，避免多个测试包共用
		// 数据库时启动测试恰好撞上它。
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

func TestNewStorageRegistryFailsForReferencedMissingConfig(t *testing.T) {
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run the storage registry check")
	}
	db, err := database.OpenDSN(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	// 先注册关闭，后注册删除；t.Cleanup 是后进先出，
	// 所以删除策略时连接还开着。
	t.Cleanup(func() { _ = database.Close(db) })

	policy := model.StoragePolicy{
		Name:      "missing",
		Type:      model.StorageTypeLocal,
		ConfigKey: "missing-" + strconv.FormatInt(time.Now().UnixNano(), 10),
	}
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	user := model.User{
		Username:               "missing-policy-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		PasswordHash:           "x",
		DefaultStoragePolicyID: policy.ID,
	}
	if err := db.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Delete(&user).Error
		_ = db.Delete(&policy).Error
	})

	cfg := testStorageConfig(t, db)
	delete(cfg.LocalRoots, policy.ConfigKey)

	_, err = newStorageRegistry(db, cfg)
	if err == nil || !strings.Contains(err.Error(), policy.ConfigKey) {
		t.Fatalf("wanted a missing-config error for %q, got %v", policy.ConfigKey, err)
	}
}

func TestRunRejectsOccupiedPort(t *testing.T) {
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run tests that start the full app")
	}
	db, err := database.OpenDSN(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(db)

	listener := testListener(t)
	cfg := config.Config{
		HTTP:     config.HTTP{Listen: listener.Addr().String(), ShutdownTimeoutSeconds: 1},
		Storage:  testStorageConfig(t, db),
		Database: testDatabaseConfig(t),
	}
	if err := Run(context.Background(), cfg, testLogger()); err == nil || !strings.Contains(err.Error(), "listen HTTP") {
		t.Fatalf("wanted occupied-port error, got %v", err)
	}
}

// freeAddress 借一个空闲端口再立即释放，让 Run 能真正完成绑定。
//
// 这里只测试“恢复失败就终止启动”，不会真的开始服务，所以不会和
// 其它测试包争用同一个测试库里的上传会话。
func freeAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

// TestRunStopsBeforeServingWhenUploadRecoveryFails 覆盖启动恢复失败：
// Run 必须返回错误并释放端口，不能开始处理 HTTP 请求。
//
// 通过替换恢复函数来制造失败：真实数据库故障很难在启动测试里稳定复现，
// 但“恢复失败必须终止启动”是这个阶段最关键的保证。
func TestRunStopsBeforeServingWhenUploadRecoveryFails(t *testing.T) {
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run tests that start the full app")
	}
	db, err := database.OpenDSN(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(db)

	cfg := config.Config{
		HTTP:     config.HTTP{Listen: freeAddress(t), ShutdownTimeoutSeconds: 1},
		Storage:  testStorageConfig(t, db),
		Database: testDatabaseConfig(t),
	}

	sentinel := errors.New("forced upload recovery failure")
	gotBatchSize := -1
	recovery := func(_ context.Context, svc *file.Service, batchSize int) error {
		if svc == nil {
			t.Error("recovery received a nil file service")
		}
		if batchSize != file.DefaultRecoveryBatchSize {
			t.Errorf("recovery batch size = %d, want %d", batchSize, file.DefaultRecoveryBatchSize)
		}
		gotBatchSize = batchSize
		return sentinel
	}

	err = run(context.Background(), cfg, testLogger(), recovery)
	if !errors.Is(err, sentinel) {
		t.Fatalf("run error = %v, want wrapped recovery failure", err)
	}
	if !strings.Contains(err.Error(), "invalidate unfinished upload sessions") {
		t.Fatalf("run error = %v, want the startup step name", err)
	}
	if gotBatchSize == -1 {
		t.Fatal("recovery was never called")
	}

	// Run 已经返回，说明它没有进入 Serve；绑定过的端口也必须已经释放。
	conn, dialErr := net.DialTimeout("tcp", cfg.HTTP.Listen, 300*time.Millisecond)
	if dialErr == nil {
		_ = conn.Close()
		t.Fatalf("server still accepts connections on %s after recovery failure", cfg.HTTP.Listen)
	}
}

func TestRunRejectsUnavailableDatabase(t *testing.T) {
	cfg := config.Config{
		HTTP:    config.HTTP{Listen: "127.0.0.1:0", ShutdownTimeoutSeconds: 1},
		Storage: config.Storage{LocalRoot: t.TempDir()},
		// 端口 1 基本不可能有 MySQL 在监听。
		Database: config.Database{Host: "127.0.0.1", Port: 1, User: "nobody", Name: "nothing"},
	}
	if err := Run(context.Background(), cfg, testLogger()); err == nil || !strings.Contains(err.Error(), "initialize database") {
		t.Fatalf("wanted database error, got %v", err)
	}
}

func TestCheckStorage(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	if err := checkStorage(root); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("write probe was not removed: %v", err)
	}
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkStorage(file); err == nil {
		t.Fatal("regular file accepted as storage directory")
	}
}
