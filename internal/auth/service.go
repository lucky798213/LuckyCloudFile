// Package auth 负责登录、密码校验和登录会话。
//
// 安全边界很简单：
//   - 数据库只保存 bcrypt 生成的 password_hash；
//   - 客户端只拿到随机 Token，数据库只保存 Token 的 SHA-256；
//   - 文件接口使用 Token 对应的用户 ID，绝不使用客户端提交的用户 ID。
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
)

const (
	// TokenTTL 是一次登录的有效期。P3 先固定为 24 小时，不开放配置。
	TokenTTL = 24 * time.Hour

	// tokenBytes 是随机 Token 的熵。32 字节足够抵御猜测。
	tokenBytes = 32
)

var (
	// ErrInvalidCredentials 表示用户名不存在或密码不匹配。
	// 两种情况返回同一个错误，避免告诉攻击者“这个用户名存在”。
	ErrInvalidCredentials = errors.New("invalid username or password")

	// ErrUnauthorized 表示 Token 缺失、无效或已过期。
	ErrUnauthorized = errors.New("invalid or expired token")
)

// LoginResult 是登录成功后返回给客户端的数据。
// 原始 Token 只在这里出现一次，数据库里只有它的哈希。
type LoginResult struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Service 负责登录、会话校验和退出。
type Service struct {
	db *gorm.DB
}

// NewService 创建认证服务。
func NewService(db *gorm.DB) *Service {
	return &Service{db: db}
}

// Login 校验用户名和密码，成功时创建会话并返回新的原始 Token。
func (s *Service) Login(ctx context.Context, username, password string) (LoginResult, error) {
	var user model.User
	err := s.db.WithContext(ctx).
		Where("username = ?", username).
		First(&user).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return LoginResult{}, ErrInvalidCredentials
		}
		return LoginResult{}, fmt.Errorf("query user: %w", err)
	}

	// 关键：用已保存的 password_hash 调用校验函数，让 bcrypt 从哈希里
	// 读取原来的盐和代价参数；不是重新加盐后再比较两个哈希字符串。
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
			return LoginResult{}, ErrInvalidCredentials
		}
		// 哈希本身损坏属于服务端数据问题，不能伪装成“密码错误”。
		return LoginResult{}, fmt.Errorf("verify password hash: %w", err)
	}

	rawToken, tokenHash, err := newToken()
	if err != nil {
		return LoginResult{}, err
	}
	expiresAt := time.Now().Add(TokenTTL)

	session := model.AuthSession{
		TokenHash: tokenHash,
		ExpiresAt: expiresAt,
		UserID:    user.ID,
	}
	if err := s.db.WithContext(ctx).Create(&session).Error; err != nil {
		return LoginResult{}, fmt.Errorf("create session: %w", err)
	}

	return LoginResult{Token: rawToken, ExpiresAt: expiresAt}, nil
}

// Authenticate 把请求里的原始 Token 换成可信的当前用户 ID。
//
// 会话不存在、已过期或关联用户不可用时，统一返回 ErrUnauthorized；
// 数据库故障会返回普通 error，让 HTTP 层可以区分 401 和 500。
func (s *Service) Authenticate(ctx context.Context, rawToken string) (int, error) {
	if rawToken == "" {
		return 0, ErrUnauthorized
	}

	var session model.AuthSession
	err := s.db.WithContext(ctx).
		Preload("User").
		Where("token_hash = ? AND expires_at > ?", hashToken(rawToken), time.Now()).
		First(&session).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, ErrUnauthorized
		}
		return 0, fmt.Errorf("query session: %w", err)
	}
	// Preload 没有找到关联用户时，User 会是 nil。
	if session.User == nil {
		return 0, ErrUnauthorized
	}

	return session.User.ID, nil
}

// Logout 删除当前 Token 对应的会话。删除成功后，旧 Token 立即失效。
func (s *Service) Logout(ctx context.Context, rawToken string) error {
	if rawToken == "" {
		return ErrUnauthorized
	}

	result := s.db.WithContext(ctx).
		Where("token_hash = ? AND expires_at > ?", hashToken(rawToken), time.Now()).
		Delete(&model.AuthSession{})
	if result.Error != nil {
		return fmt.Errorf("delete session: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrUnauthorized
	}
	return nil
}

// newToken 生成原始 Token，以及写入数据库的 SHA-256 哈希。
func newToken() (rawToken, tokenHash string, err error) {
	buf := make([]byte, tokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", "", fmt.Errorf("generate token: %w", err)
	}
	rawToken = hex.EncodeToString(buf)
	return rawToken, hashToken(rawToken), nil
}

// hashToken 把 Token 转成定长的 SHA-256 十六进制字符串。
// Token 是高熵随机值，不需要像密码那样使用 bcrypt。
func hashToken(rawToken string) string {
	sum := sha256.Sum256([]byte(rawToken))
	return hex.EncodeToString(sum[:])
}
