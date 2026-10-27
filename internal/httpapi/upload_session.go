// 上传会话接口：只登记“要传什么、传到哪”，本阶段不接收文件内容。
package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"luckyCloudFile/internal/file"
)

// createUploadSessionRequest 是 POST /api/upload-sessions 的请求体。
//
// size 用指针是为了区分“没传”和“传了 0”：
// 没传是参数错误，传 0 是合法的空文件。
//
// mode 可选，不传就是 full；分片大小由服务端决定，请求里不接受 chunk_size。
type createUploadSessionRequest struct {
	ParentID string `json:"parent_id"`
	Name     string `json:"name"`
	Size     *int64 `json:"size"`
	Mode     string `json:"mode"`
}

// createUploadSession 处理 POST /api/upload-sessions。
//
// 成功返回 201 和 pending 状态的会话；此时数据库里只有占位 Entity，
// 不会创建 File，也不会写磁盘。
func createUploadSession(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "create upload session: missing authenticated user")
			return
		}

		var request createUploadSessionRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Code:    "invalid_json",
				Message: `request body must be JSON like {"name":"a.bin","size":1024}`,
			})
			return
		}
		if request.Size == nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Code:    "invalid_size",
				Message: "size is required; use 0 for an empty file",
			})
			return
		}

		info, err := svc.CreateUploadSessionWithMode(
			c.Request.Context(),
			ownerID,
			request.ParentID,
			request.Name,
			*request.Size,
			request.Mode,
		)
		if err != nil {
			if errors.Is(err, file.ErrInvalidUploadMode) {
				c.JSON(http.StatusBadRequest, ErrorResponse{
					Code:    "invalid_mode",
					Message: `mode must be "full" or "chunked"; omit it for full`,
				})
				return
			}
			// 兜底：上传会话自身的状态冲突。目标路径被占住属于
			// ErrResourceLocked，会走下面的公共映射返回 resource_locked。
			if errors.Is(err, file.ErrUploadSessionConflict) {
				c.JSON(http.StatusConflict, ErrorResponse{
					Code:    "upload_session_conflict",
					Message: "upload session conflicts with the target or its current state",
				})
				return
			}
			if handleFileError(c, err, "parent_not_found", "parent directory not found") {
				return
			}
			logger.Error("create upload session", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "create_upload_session_failed",
				Message: "failed to create upload session",
			})
			return
		}

		c.JSON(http.StatusCreated, info)
	}
}

// getUploadSession 处理 GET /api/upload-sessions/:id。
//
// 只返回当前用户自己的会话；别人的和不存在的都返回同一个 404，
// 不泄露会话是否存在。这里不自动处理过期：客户端从 status 和 expires_at
// 判断会话还能不能继续。注意 expired 不一定代表 expires_at 已过：
// 服务重启也会让重启前的未完成会话立即失效，此时 expires_at 可能还在未来。
func getUploadSession(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "get upload session: missing authenticated user")
			return
		}

		info, err := svc.GetUploadSession(
			c.Request.Context(),
			ownerID,
			c.Param("id"),
		)
		if err != nil {
			if errors.Is(err, file.ErrUploadSessionNotFound) {
				c.JSON(http.StatusNotFound, ErrorResponse{
					Code:    "upload_session_not_found",
					Message: "upload session not found",
				})
				return
			}
			logger.Error("get upload session", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "get_upload_session_failed",
				Message: "failed to get upload session",
			})
			return
		}

		c.JSON(http.StatusOK, info)
	}
}
