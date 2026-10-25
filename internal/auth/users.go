package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/bcrypt"

	"luckyCloudFile/internal/ent"
)

var (
	// ErrUserExists 表示用户名已经被占用。
	ErrUserExists = errors.New("user already exists")

	// ErrEmptyUsername 表示没有提供用户名。
	ErrEmptyUsername = errors.New("username must not be empty")

	// ErrEmptyPassword 表示没有提供密码。
	ErrEmptyPassword = errors.New("password must not be empty")
)

// CreateUser 创建账号，只保存 bcrypt 哈希。
//
// 它不会更新已有账号：用户名重复时返回 ErrUserExists，密码保持不变。
// 本阶段不开放 HTTP 注册，只有开发初始化命令会调用它。
func CreateUser(ctx context.Context, client *ent.Client, username, password string) (*ent.User, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil, ErrEmptyUsername
	}
	if password == "" {
		return nil, ErrEmptyPassword
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	user, err := client.User.Create().
		SetUsername(username).
		SetPasswordHash(string(hash)).
		Save(ctx)
	if err != nil {
		if ent.IsConstraintError(err) {
			return nil, ErrUserExists
		}
		return nil, fmt.Errorf("create user: %w", err)
	}
	return user, nil
}
