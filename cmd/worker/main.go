// Package main 是独立任务 worker 的入口。
//
// 它刻意不复用 API 的完整启动函数 app.Run：那个函数会在启动时作废所有
// 未完成上传、恢复数据库里的 running 任务，启动第二个实例会干扰第一个。
//
// worker 只装配自己真正需要的东西：
//
//	数据库 → 存储 Registry → 任务类型 Registry → 领取/心跳/执行/收尾循环
//
// 多个 worker 进程共用同一个 MySQL，用租约竞争领取任务；内容从共享
// 对象存储读取（同机演示时也可以是同一个本地目录）。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"luckyCloudFile/internal/app"
	"luckyCloudFile/internal/config"
	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/task"
	"luckyCloudFile/internal/verify"
)

func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("conf", "configs/conf.ini", "配置文件路径（INI 格式）")
	workerID := flag.String("id", "", "worker 标识（默认取 [Worker] ID，再退回 hostname-pid）")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "启动失败:", err)
		return 1
	}

	logger, err := newLogger(cfg.System.LogLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, "启动失败:", err)
		return 1
	}

	id := firstNonEmpty(*workerID, cfg.Worker.ID, defaultWorkerID())
	if strings.TrimSpace(id) == "" {
		fmt.Fprintln(os.Stderr, "启动失败: worker 标识为空")
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := database.Open(ctx, cfg.Database)
	if err != nil {
		fmt.Fprintln(os.Stderr, "启动失败:", err)
		return 1
	}
	defer database.Close(db)

	// 和 API 用同一套策略解析规则：worker 必须从策略 ID 指向的
	// Local/S3 位置读取内容，不能猜目录。
	stores, err := app.NewStorageRegistry(db, cfg.Storage)
	if err != nil {
		logger.Error("初始化存储失败", "error", err)
		return 1
	}

	taskService := task.NewService(db).WithMaxPending(cfg.Task.MaxPendingTasks)

	factories := task.NewRegistry()
	if err := factories.Register(verify.TaskType, verify.NewFactory(verify.Options{
		DB:     db,
		Stores: stores,
		Tasks:  taskService,
	})); err != nil {
		logger.Error("注册内容校验任务失败", "error", err)
		return 1
	}

	if err := factories.Register(file.DirectVerifyTask, file.NewService(db, stores).DirectVerifyFactory(taskService)); err != nil {
		logger.Error("register direct verify", "error", err)
		return 1
	}
	worker, err := task.NewWorker(task.WorkerOptions{
		Service:           taskService,
		Registry:          factories,
		WorkerID:          id,
		Slots:             cfg.Worker.Slots,
		LeaseDuration:     time.Duration(cfg.Worker.LeaseSeconds) * time.Second,
		HeartbeatInterval: time.Duration(cfg.Worker.HeartbeatSeconds) * time.Second,
		PollInterval:      time.Duration(cfg.Worker.PollSeconds) * time.Second,
		Logger:            logger,
	})
	if err != nil {
		logger.Error("创建 worker 失败", "error", err)
		return 1
	}

	if err := worker.Run(ctx); err != nil {
		logger.Error("worker 异常退出", "error", err)
		return 1
	}
	return 0
}

// firstNonEmpty 返回第一个非空字符串。命令行参数优先于配置文件，
// 配置文件优先于自动生成的值。
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// defaultWorkerID 用 hostname-pid 生成默认标识。
//
// 同一个数据库上的两个 worker 必须不同；拿不到 hostname 时至少还有 pid，
// 但这只适合单机演示，多机部署应在配置或启动参数里显式指定。
func defaultWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "worker"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

// newLogger 和 cmd/server 保持同一种 JSON 日志格式，方便一起收集。
func newLogger(levelName string) (*slog.Logger, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(levelName)); err != nil {
		return nil, fmt.Errorf("日志级别 %q 无效（可选：debug / info / warn / error）", levelName)
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(handler), nil
}
