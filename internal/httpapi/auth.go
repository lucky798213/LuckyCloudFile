// 这个文件负责 HTTP 层的登录、退出和 Token 校验。
// 密码怎么校验、会话存在哪里，都在 internal/auth 里。
package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"luckyCloudFile/internal/auth"
)

const (
	// userIDKey 是鉴权中间件写入 Gin Context 的可信用户 ID。
	// 文件处理函数只能读它，不能从请求参数里读用户 ID。
	userIDKey = "auth_user_id"

	// tokenKey 保存当前请求的原始 Token，退出登录时用它删除会话。
	tokenKey = "auth_token"
)

// authService 是路由需要的认证能力。测试时可以换成假实现。
type authService interface {
	Login(ctx context.Context, username, password string) (auth.LoginResult, error)
	Authenticate(ctx context.Context, rawToken string) (int, error)
	Logout(ctx context.Context, rawToken string) error
}

// loginRequest 是 POST /api/auth/login 的请求体。
// 用户 ID 不在请求里：登录成功后由服务端从会话中得到。
type loginRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

// login 处理 POST /api/auth/login。
func login(logger *slog.Logger, svc authService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var request loginRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Code:    "invalid_request",
				Message: "username and password are required",
			})
			return
		}

		result, err := svc.Login(c.Request.Context(), request.Username, request.Password)
		switch {
		case errors.Is(err, auth.ErrInvalidCredentials):
			// 不区分“用户不存在”和“密码错误”，避免泄露用户名是否存在。
			c.JSON(http.StatusUnauthorized, ErrorResponse{
				Code:    "invalid_credentials",
				Message: "invalid username or password",
			})
		case err != nil:
			// 数据库或哈希数据损坏是服务端错误，不能伪装成登录失败。
			logger.Error("login", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "internal_error",
				Message: "internal server error",
			})
		default:
			c.JSON(http.StatusOK, result)
		}
	}
}

// requireAuth 保护所有文件接口和退出接口。
//
// 它从 Authorization: Bearer <token> 中取 Token，查询未过期的会话，
// 然后把可信的用户 ID 放进 Gin Context。
func requireAuth(logger *slog.Logger, svc authService) gin.HandlerFunc {
	return func(c *gin.Context) {
		rawToken, ok := bearerToken(c.Request)
		if !ok {
			abortUnauthorized(c)
			return
		}

		userID, err := svc.Authenticate(c.Request.Context(), rawToken)
		switch {
		case errors.Is(err, auth.ErrUnauthorized):
			abortUnauthorized(c)
		case err != nil:
			// 数据库故障必须返回 500，不能被当成未登录。
			logger.Error("authenticate token", "error", err)
			c.AbortWithStatusJSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "internal_error",
				Message: "internal server error",
			})
		default:
			c.Set(userIDKey, userID)
			c.Set(tokenKey, rawToken)
			c.Next()
		}
	}
}

// logout 处理 POST /api/auth/logout。
// 能走到这里说明 Token 刚刚校验通过；删除对应会话后，旧 Token 立即失效。
func logout(logger *slog.Logger, svc authService) gin.HandlerFunc {
	return func(c *gin.Context) {
		value, exists := c.Get(tokenKey)
		rawToken, ok := value.(string)
		if !exists || !ok || rawToken == "" {
			abortUnauthorized(c)
			return
		}

		err := svc.Logout(c.Request.Context(), rawToken)
		switch {
		case errors.Is(err, auth.ErrUnauthorized):
			abortUnauthorized(c)
		case err != nil:
			logger.Error("logout", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "internal_error",
				Message: "internal server error",
			})
		default:
			c.Status(http.StatusNoContent)
		}
	}
}

// bearerToken 只接受标准的 Authorization: Bearer <token> 头。
func bearerToken(r *http.Request) (string, bool) {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", false
	}
	return token, true
}

// abortUnauthorized 返回统一的 401 响应。
func abortUnauthorized(c *gin.Context) {
	c.Header("WWW-Authenticate", "Bearer")
	c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorResponse{
		Code:    "unauthorized",
		Message: "missing, invalid or expired token",
	})
}

// currentUserID 读取鉴权中间件写入的用户 ID。
// 这个值不是客户端提交的，而是从数据库里的登录会话得到的。
func currentUserID(c *gin.Context) (int, bool) {
	value, exists := c.Get(userIDKey)
	if !exists {
		return 0, false
	}
	userID, ok := value.(int)
	return userID, ok && userID > 0
}
