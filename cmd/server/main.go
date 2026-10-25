// Package main 是程序的入口。
//
// 它只做四件事，按顺序：
//  1. 读命令行参数（--conf 指定配置文件）
//  2. 读配置并校验
//  3. 准备日志
//  4. 启动服务，然后一直等到用户按 Ctrl+C
//
// 真正的功能都在 internal/ 下面。这个文件故意写得很短，
// 让你一眼就能看到“程序是怎么开始的、又是怎么结束的”。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"luckyCloudFile/internal/app"
	"luckyCloudFile/internal/config"

	"github.com/gin-gonic/gin"
)

func main() {
	// 真正的工作写在 run() 里，由 main 统一负责退出码。
	//
	// 为什么不直接把代码写在 main 里？因为 os.Exit 会立刻结束进程，
	// 连 defer（比如关闭文件、清理临时资源）都不会执行。
	// 放进 run() 之后，run 里面的 defer 可以正常执行，main 只负责最后那一下退出。
	os.Exit(run())
}

// run 返回进程退出码：0 表示正常，非 0 表示失败。
func run() int {
	// 只有一个启动命令，标准库 flag 就够了，不需要 Cobra 之类的框架。
	configPath := flag.String("conf", "configs/conf.ini", "配置文件路径（INI 格式）")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		// 配置有错时，服务还没启动，日志还没准备好，所以直接写 stderr。
		fmt.Fprintln(os.Stderr, "启动失败:", err)
		return 1
	}

	logger, err := newLogger(cfg.System.LogLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, "启动失败:", err)
		return 1
	}

	// 用 ReleaseMode 关掉 Gin 的调试输出，让日志只有我们自己打的那几行。
	gin.SetMode(gin.ReleaseMode)

	// NotifyContext 把系统信号（Ctrl+C 或 SIGTERM）变成 ctx 的“完成”通知。
	// 于是我们后面只需要一直等 ctx.Done()，不用自己处理信号。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// app.Run 会一直阻塞在这里跑服务，直到收到退出信号才返回。
	if err := app.Run(ctx, cfg, logger); err != nil {
		logger.Error("服务异常退出", "error", err)
		return 1
	}
	return 0
}

// newLogger 根据配置里的日志级别创建日志对象。
//
// 日志用 JSON 格式输出，每行一条，方便以后接入日志收集工具。
func newLogger(levelName string) (*slog.Logger, error) {
	var level slog.Level
	// UnmarshalText 把 "info"、"debug" 这样的字符串转成 slog.Level 常量。
	if err := level.UnmarshalText([]byte(levelName)); err != nil {
		return nil, fmt.Errorf("日志级别 %q 无效（可选：debug / info / warn / error）", levelName)
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(handler), nil
}
