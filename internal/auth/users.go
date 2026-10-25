package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"luckyCloudFile/internal/idgen"
	"luckyCloudFile/internal/model"
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
//
// 账号和根目录必须一起成功：客户端不传 parent_id 时，业务层要靠
// users.root_file_id 找到起点。所以两步放在同一个事务里。
func CreateUser(ctx context.Context, db *gorm.DB, username, password string) (*model.User, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil, ErrEmptyUsername
	}
	if password == "" {
		return nil, ErrEmptyPassword
	}

	// 先查一次：用户名已存在是预期内的结果，不该变成一条数据库错误日志。
	// 真正并发创建时仍由 users.username 的唯一索引兜底（见下面的 ErrDuplicatedKey）。
	var existing int64
	if err := db.WithContext(ctx).
		Model(&model.User{}).
		Where("username = ?", username).
		Count(&existing).Error; err != nil {
		return nil, fmt.Errorf("check existing user: %w", err)
	}
	if existing > 0 {
		return nil, ErrUserExists
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}
	rootID, err := idgen.New()
	if err != nil {
		return nil, err
	}

	user := model.User{
		Username:     username,
		PasswordHash: string(hash),
	}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Create 会把自增 ID 回填到 user.ID。
		if err := tx.Create(&user).Error; err != nil {
			return err
		}

		root := model.File{
			ID:      rootID,
			Name:    "/",
			Kind:    model.KindDirectory,
			OwnerID: user.ID,
		}
		if err := tx.Create(&root).Error; err != nil {
			return fmt.Errorf("create root directory: %w", err)
		}

		if err := tx.Model(&model.User{}).
			Where("id = ?", user.ID).
			Update("root_file_id", rootID).Error; err != nil {
			return fmt.Errorf("set user root_file_id: %w", err)
		}
		user.RootFileID = &rootID
		return nil
	})
	if err != nil {
		// TranslateError 打开后，重复用户名会变成 gorm.ErrDuplicatedKey。
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return nil, ErrUserExists
		}
		return nil, fmt.Errorf("create user: %w", err)
	}
	return &user, nil
}
