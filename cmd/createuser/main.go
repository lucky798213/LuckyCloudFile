// Package main 提供一个只用于开发/测试的账号初始化命令。
//
// 正常启动服务不会创建任何账号。需要账号时手动执行：
//
//	CLOUDREVE_USER_PASSWORD='...' go run ./cmd/createuser --username A
//
// 不设置环境变量时会在终端里隐藏回显地提示输入密码。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"golang.org/x/term"

	"luckyCloudFile/internal/auth"
	"luckyCloudFile/internal/config"
	"luckyCloudFile/internal/database"
)

const passwordEnv = "CLOUDREVE_USER_PASSWORD"

func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("conf", "configs/conf.ini", "配置文件路径（INI 格式）")
	username := flag.String("username", "", "要创建的用户名（必填）")
	ifNotExists := flag.Bool("if-not-exists", false, "用户已存在时不报错，也不修改密码")
	flag.Parse()

	if strings.TrimSpace(*username) == "" {
		fmt.Fprintln(os.Stderr, "错误: 必须通过 --username 指定用户名")
		flag.Usage()
		return 2
	}

	password, err := readPassword()
	if err != nil {
		fmt.Fprintln(os.Stderr, "错误:", err)
		return 1
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "读取配置失败:", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := database.Open(ctx, cfg.Database)
	if err != nil {
		fmt.Fprintln(os.Stderr, "连接数据库失败:", err)
		return 1
	}
	defer database.Close(client)

	user, err := auth.CreateUser(ctx, client, *username, password)
	if err != nil {
		if errors.Is(err, auth.ErrUserExists) {
			if *ifNotExists {
				fmt.Printf("用户 %q 已存在，密码保持不变\n", strings.TrimSpace(*username))
				return 0
			}
			fmt.Fprintf(os.Stderr, "创建用户失败: 用户 %q 已存在\n", strings.TrimSpace(*username))
			return 1
		}
		fmt.Fprintln(os.Stderr, "创建用户失败:", err)
		return 1
	}

	fmt.Printf("已创建用户 %q（id=%d）\n", user.Username, user.ID)
	return 0
}

// readPassword 优先读取环境变量；没有环境变量时从终端隐藏回显地读取。
func readPassword() (string, error) {
	if password := os.Getenv(passwordEnv); password != "" {
		return password, nil
	}

	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		return "", fmt.Errorf("未设置 %s，且当前输入不是终端；请设置环境变量或从终端运行", passwordEnv)
	}

	fmt.Fprint(os.Stderr, "密码: ")
	password, err := term.ReadPassword(fd)
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", fmt.Errorf("读取密码: %w", err)
	}
	if len(password) == 0 {
		return "", errors.New("密码不能为空")
	}

	fmt.Fprint(os.Stderr, "再次输入密码: ")
	confirmation, err := term.ReadPassword(fd)
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", fmt.Errorf("读取确认密码: %w", err)
	}
	if string(password) != string(confirmation) {
		return "", errors.New("两次输入的密码不一致")
	}

	return string(password), nil
}
