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

	"gorm.io/gorm"

	"luckyCloudFile/internal/archive"
	"luckyCloudFile/internal/auth"
	"luckyCloudFile/internal/config"
	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/httpapi"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/pathlock"
	"luckyCloudFile/internal/queue"
	"luckyCloudFile/internal/storage"
	"luckyCloudFile/internal/task"
)

// uploadRecoverer 是启动恢复这一步的调用签名。
//
// 单独抽成参数，而不是在 run 里直接调用具体方法，是为了让测试能稳定地
// 构造“恢复失败”：真实数据库故障很难在启动测试里精确制造，而
// “恢复失败必须终止启动”是这个阶段最关键的保证，不能只靠代码审查。
type uploadRecoverer func(ctx context.Context, svc *file.Service, batchSize int) error

// recoverUnfinishedUploads 是生产实现：交给文件服务作废旧会话。
func recoverUnfinishedUploads(ctx context.Context, svc *file.Service, batchSize int) error {
	return svc.InvalidateUnfinishedUploads(ctx, batchSize)
}

// Run 启动 HTTP 服务，并一直阻塞到 ctx 被取消（例如用户按下 Ctrl+C）才返回。
//
// 整个启动过程是“一步一步来”的，任何一步失败都直接返回错误：
//
//	检查存储目录 → 连接 MySQL → 创建 HTTP Server → 绑定端口
//	→ 作废旧上传会话 → 启动后台清理 → 启动任务调度器 → 开始服务
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	return run(ctx, cfg, logger, recoverUnfinishedUploads)
}

// run 是 Run 的实际启动流程。recoverUploads 单独传入，供测试替换。
func run(
	ctx context.Context,
	cfg config.Config,
	logger *slog.Logger,
	recoverUploads uploadRecoverer,
) error {
	// 第一步：确认存储目录存在且可写。放在最前面，避免服务跑起来后才发现存不了文件。
	if err := checkStorage(cfg.Storage.LocalRoot); err != nil {
		return err
	}
	// 任务工作目录同样必须在开始调度前可写：phase=publish 依赖这里保存的
	// 完整 ZIP，不能等到第一个任务执行到一半才发现目录不可用。
	if cfg.Task.WorkDir != "" {
		if err := checkStorage(cfg.Task.WorkDir); err != nil {
			return fmt.Errorf("check task work directory: %w", err)
		}
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
	//
	// 数据库里的策略必须在配置里找到对应目录，否则启动直接失败：
	// 不能让旧内容落到猜测的位置。
	stores, err := newStorageRegistry(db, cfg.Storage)
	if err != nil {
		return fmt.Errorf("initialize storage: %w", err)
	}
	// 分片并行度是每个会话的上限，来自 [Upload] MaxParallelChunks。
	// 路径锁管理器只在这里创建一次，注入给所有文件写操作共用；
	// 不能每个请求或每个方法各自新建，否则锁之间不可见。
	pathLocks := pathlock.NewManager()
	fileService := file.NewService(db, stores).
		WithLogger(logger).
		WithPathLocks(pathLocks).
		WithChunkParallel(cfg.Upload.MaxParallelChunks)
	authService := auth.NewService(db)

	// 作废上一次进程留下的未完成上传会话。
	//
	// 这一步必须同步执行，而且必须在处理遗留任务、开始调度和接受 HTTP
	// 请求之前完成：旧进程已经退出，旧会话不能再上传或发布。单个会话的
	// expires_at 可能还在未来，所以不能只靠后面的到期扫描。
	//
	// 失败必须终止启动，不能只记日志继续：那样会有一部分旧会话仍然可用。
	// 这里只切断数据库里的资格；磁盘、Entity、Session 都交给清理任务继续
	// 回收，启动不必等物理删除完成。
	if err := recoverUploads(ctx, fileService, file.DefaultRecoveryBatchSize); err != nil {
		return fmt.Errorf("invalidate unfinished upload sessions: %w", err)
	}

	// 任务服务负责持久化事实：提交先做准入并落库，成功后才唤醒调度器。
	// MaxPendingTasks 限制数据库里的未终态积压，不随内存队列上限浮动。
	taskService := task.NewService(db).WithMaxPending(cfg.Task.MaxPendingTasks)

	// 任务类型注册表：type → 重建工厂。工厂依赖在这里注入一次；恢复时
	// 只读取 tasks 记录里的 owner、phase 和 state_json，没有旧 Job 指针。
	taskFactories := task.NewRegistry()
	if err := taskFactories.Register(
		archive.TaskType,
		archive.NewTaskFactory(
			fileService,
			taskService,
			archive.Options{Logger: logger, WorkDir: cfg.Task.WorkDir},
		),
	); err != nil {
		return fmt.Errorf("register archive task factory: %w", err)
	}

	// 处理上一次进程留下的 running 任务。此时还没有启动调度器，也没有
	// 开始接受 HTTP 请求，所以数据库里的 running 记录不可能仍由本进程持有。
	// 恢复先核对 task_results：结果已经发布就补写 completed，绝不重跑发布；
	// 没有结果才保留 phase/state 放回 queued，或者在上限用完后明确失败。
	recovered, err := taskService.RecoverRunning(ctx, time.Now())
	if err != nil {
		return fmt.Errorf("recover interrupted tasks: %w", err)
	}
	if recovered.Completed > 0 || recovered.Requeued > 0 || recovered.Failed > 0 {
		logger.Info("recovered interrupted tasks",
			"completed", recovered.Completed,
			"requeued", recovered.Requeued,
			"failed", recovered.Failed,
		)
	}

	// 任务队列随应用生命周期创建和关闭。关闭顺序是：
	// HTTP 先停止接收（serve 内部完成）→ 调度器停止扫描 → 队列取消执行并
	// 等 worker 退出 → database.Close（更早注册的 defer 最后执行）。
	// 任务可能还在读写数据库，所以队列必须先于数据库关闭。
	//
	// Workers / QueueCapacity 限制同时存在的排队和执行任务；
	// Retention / MaxTerminalRecords 限制完成后的内存快照，两部分都来自 [Task]。
	taskQueue, err := queue.New(queue.Options{
		Workers:            cfg.Task.Workers,
		Capacity:           cfg.Task.QueueCapacity,
		Timeout:            time.Duration(cfg.Task.TimeoutSeconds) * time.Second,
		Retention:          time.Duration(cfg.Task.RetentionSeconds) * time.Second,
		MaxTerminalRecords: cfg.Task.MaxTerminalRecords,
	})
	if err != nil {
		return fmt.Errorf("initialize task queue: %w", err)
	}
	defer taskQueue.Shutdown()

	// 调度器按数据库里的原 taskID 入队，不经过 queue.Submit，因此恢复或
	// 重复扫描不会生成第二个任务 ID。提交通知丢失时，周期扫描会兜底。
	taskDispatcher, err := task.NewDispatcher(task.DispatcherOptions{
		Service:      taskService,
		Queue:        taskQueue,
		Runner:       taskFactories,
		ScanInterval: time.Duration(cfg.Task.ScanIntervalSeconds) * time.Second,
		BatchSize:    cfg.Task.ScanBatchSize,
		Logger:       logger,
	})
	if err != nil {
		return fmt.Errorf("initialize task dispatcher: %w", err)
	}
	// 注册在 taskQueue.Shutdown 之后：defer 后进先出，调度器先停止扫描，
	// 再让队列取消并等待正在执行的任务退出。
	defer taskDispatcher.Shutdown()

	// 第四步：创建 HTTP Server。它只是一个“配置好的对象”，还没开始监听。
	// Handler 就是路由表，来自 internal/httpapi。
	server := &http.Server{
		Handler: httpapi.NewRouterWithTasks(
			logger, fileService, authService, taskService, taskQueue,
		),
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
	// 它只在监听和旧会话作废都成功后才启动；Run 返回时一定先取消并等它退出，
	// 不留下孤儿 goroutine。
	cleanupCtx, stopCleanup := context.WithCancel(ctx)
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		fileService.CleanupLoop(cleanupCtx, file.DefaultCleanupInterval, file.DefaultCleanupBatchSize)
	}()

	// 第七步：旧上传会话已经作废，现在才开始调度数据库里遗留的任务。
	//
	// 顺序很重要：调度器启动时会立即扫描一次，若它更早启动，重启前的
	// queued 任务可能在旧会话还没失效时就开始执行。HTTP 也还没开始接收
	// 新请求，所以此刻被唤醒的任务都来自上次进程。
	taskDispatcher.Start()

	// 第九步：开始服务，直到收到退出信号。
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

// newStorageRegistry 把数据库里的每条存储策略绑定到配置中的本地目录。
//
// 策略只记录 Type 和 ConfigKey；实际目录由配置提供。
// 任何一条策略找不到目录、类型不认识或 ID 重复，都让启动失败，
// 而不是让它悄悄回退到 LocalRoot。
func newStorageRegistry(db *gorm.DB, cfg config.Storage) (*storage.Registry, error) {
	var policies []model.StoragePolicy
	if err := db.Order("id ASC").Find(&policies).Error; err != nil {
		return nil, fmt.Errorf("query storage policies: %w", err)
	}
	if len(policies) == 0 {
		return nil, errors.New("no storage policies configured")
	}

	registry := storage.NewRegistry()
	for _, policy := range policies {
		if policy.Type != model.StorageTypeLocal {
			return nil, fmt.Errorf(
				"storage policy %d (%q) has unsupported type %q",
				policy.ID, policy.Name, policy.Type,
			)
		}

		root, ok := cfg.RootFor(policy.ConfigKey)
		if !ok {
			return nil, fmt.Errorf(
				"storage policy %d (%q) has no local directory for config key %q",
				policy.ID, policy.Name, policy.ConfigKey,
			)
		}
		if err := checkStorage(root); err != nil {
			return nil, fmt.Errorf(
				"storage policy %d (%q): %w",
				policy.ID, policy.Name, err,
			)
		}
		if err := registry.Register(policy.ID, storage.NewLocal(root)); err != nil {
			return nil, err
		}
	}
	return registry, nil
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
