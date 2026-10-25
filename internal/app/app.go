// Package app 负责“把服务组装起来”和“管理它的生命周期”：
// 启动前检查存储目录、绑定端口、跑 HTTP 服务、收到退出信号后优雅关闭。
//
// 分工很清楚：
//   - 解析配置 → internal/config
//   - 处理请求 → internal/httpapi
//   - 让服务跑起来、停下来 → 就是这个包
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"luckyCloudFile/internal/auth"
	"luckyCloudFile/internal/config"
	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/httpapi"
	"luckyCloudFile/internal/storage"
)

// Run 启动 HTTP 服务，并一直阻塞到 ctx 被取消（例如用户按下 Ctrl+C）才返回。
//
// 整个启动过程是“一步一步来”的，任何一步失败都直接返回错误：
//
//	检查存储目录 → 连接 MySQL → 创建 HTTP Server → 绑定端口
//	→ 启动后台清理 → 开始服务
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	// 第一步：确认存储目录存在且可写。放在最前面，避免服务跑起来后才发现存不了文件。
	if err := checkStorage(cfg.Storage.LocalRoot); err != nil {
		return err
	}

	// 第二步：连接 MySQL。连不上就在这里失败，而不是等第一个请求才报错。
	// database.Open 还会自动建表（P2 先用 AutoMigrate）。
	db, err := database.Open(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("initialize database: %w", err)
	}
	defer database.Close(db)

	// 第三步：创建“存储 → 文件业务 → 认证 → 路由”这条调用链。
	// 这里是整个程序唯一一处把它们组装起来的地方，
	// 以后换成 S3，只要改这一行的存储实现。
	store := storage.NewLocal(cfg.Storage.LocalRoot)
	fileService := file.NewService(db, store).WithLogger(logger)
	authService := auth.NewService(db)

	// 第四步：创建 HTTP Server。它只是一个“配置好的对象”，还没开始监听。
	// Handler 就是路由表，来自 internal/httpapi。
	server := &http.Server{
		Handler: httpapi.NewRouter(logger, fileService, authService),
		// 读请求头的超时，防止有人只连接不发数据把连接一直占着。
		ReadHeaderTimeout: 5 * time.Second,
		// 读整个请求（含上传的文件）最多花 5 分钟。
		// 上传需要时间，但也不能让一个请求无限占着连接。
		ReadTimeout: 5 * time.Minute,
		// 空闲连接最多保留 60 秒。
		IdleTimeout: 60 * time.Second,
	}

	// 第五步：真正占用端口。
	// 这里故意用同步的 net.Listen，而不是在 goroutine 里 ListenAndServe：
	// 端口被占用时能立刻拿到错误，不会先打印“启动成功”再失败。
	listener, err := net.Listen("tcp", cfg.HTTP.Listen)
	if err != nil {
		return fmt.Errorf("listen HTTP: %w", err)
	}
	defer listener.Close()

	// 第六步：开始后台物理清理。
	// 先立即扫描一次（补上上次进程崩溃留下的 pending_delete），之后定时扫描。
	// 它只在监听成功后启动；Run 返回时一定先取消并等它退出，不留下孤儿 goroutine。
	cleanupCtx, stopCleanup := context.WithCancel(ctx)
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		fileService.CleanupLoop(cleanupCtx, file.DefaultCleanupInterval, file.DefaultCleanupBatchSize)
	}()

	// 第七步：开始服务，直到收到退出信号。
	shutdownTimeout := time.Duration(cfg.HTTP.ShutdownTimeoutSeconds) * time.Second
	serveErr := serve(ctx, server, listener, shutdownTimeout, logger)

	// 显式取消并等待后台清理退出，保证 Run 返回前没有孤儿 goroutine。
	// ctx 被取消时清理循环已经开始退出；这里再做一次是为了覆盖 serve 自己出错退出的情况。
	stopCleanup()
	<-cleanupDone
	return serveErr
}

// serve 让服务器一直跑，直到下面两件事之一发生：
//
//	① 服务器自己出错退出        → 返回错误
//	② ctx 被取消（用户按 Ctrl+C）→ 优雅关闭所有请求后返回 nil
//
// 为什么需要 goroutine：server.Serve 是阻塞调用，它不返回，
// 我们就没法同时等待“服务器出错”和“用户按 Ctrl+C”这两个事件。
// 所以把它放进后台 goroutine，让它把结果通过 channel 送回来。
func serve(ctx context.Context, server *http.Server, listener net.Listener, shutdownTimeout time.Duration, logger *slog.Logger) error {
	// 兜底：无论从哪条路径返回，最后都确保服务器被关闭。
	defer server.Close()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	// 这一行只在端口真正绑定成功后才会打印，所以它能当作“启动成功”的信号。
	logger.Info("http listening", "address", listener.Addr().String(), "database_connected", true)

	// 等第一个发生的事件。
	select {
	case err := <-serveErr:
		// 情况①：还没收到退出信号，服务器就自己停了。
		// Shutdown 或 Close 导致的停止属于正常结束，不算错误。
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)

	case <-ctx.Done():
		// 情况②：用户要退出，继续往下走优雅关闭流程。
		logger.Info("shutting down HTTP")
	}

	// 注意这里新建了一个 context，而不是沿用 ctx：
	// 此刻 ctx 已经因为收到信号被取消了，如果拿它去 Shutdown，
	// Shutdown 会以为“时间已经到”，直接放弃等待正在处理的请求。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		// 等到超时还有请求没处理完：强制关掉剩下的连接，避免进程永远卡住。
		_ = server.Close()
		<-serveErr
		return fmt.Errorf("shutdown HTTP: %w", err)
	}

	// Shutdown 返回之后 Serve 才会返回。把它收干净，再退出 main。
	<-serveErr
	logger.Info("http stopped")
	return nil
}

// checkStorage 确认存储目录存在，并且当前用户真的能往里面写文件。
//
// 只检查“目录存在”是不够的：目录可能属于别的用户，或者挂载成了只读，
// 这种情况下 MkdirAll 不报错，但写文件会失败。
// 所以这里实际创建一个临时文件来试，试完立刻删掉。
func checkStorage(root string) error {
	// 1. 目录不存在就创建。0o750 表示：自己可读写执行，同组可读执行，其他人无权限。
	if err := os.MkdirAll(root, 0o750); err != nil {
		return fmt.Errorf("create storage directory: %w", err)
	}

	// 2. 创建一个临时文件，能创建成功就说明目录可写。
	probe, err := os.CreateTemp(root, ".write-check-*")
	if err != nil {
		return fmt.Errorf("storage directory is not writable: %w", err)
	}
	// 3. 无论后面成功还是失败，都把这个探测文件删掉，不留下垃圾。
	defer os.Remove(probe.Name())

	if err := probe.Close(); err != nil {
		return fmt.Errorf("close storage write check: %w", err)
	}
	return nil
}
