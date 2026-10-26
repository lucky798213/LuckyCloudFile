// 上传会话内容接口：把完整文件字节写进会话已经选好的占位位置。
package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"luckyCloudFile/internal/file"
)

// uploadSessionContent 处理 PUT /api/upload-sessions/:id/content。
//
// 请求体就是原始文件内容，不再传文件名、目录或存储策略：
// 这些信息在创建会话时已经确定，并且只属于当前登录用户。
//
// 成功返回 200 和 ready 状态的会话；此时仍然没有正式 File，
// 不能通过普通下载接口访问。
func uploadSessionContent(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "upload session content: missing authenticated user")
			return
		}

		info, err := svc.UploadSessionContent(
			c.Request.Context(),
			ownerID,
			c.Param("id"),
			c.Request.Body,
		)
		if err != nil {
			switch {
			case errors.Is(err, file.ErrUploadSessionNotFound):
				c.JSON(http.StatusNotFound, ErrorResponse{
					Code:    "upload_session_not_found",
					Message: "upload session not found",
				})
			case errors.Is(err, file.ErrUploadSessionExpired):
				// 410 覆盖两种失效：超过 expires_at，以及服务重启后
				// 启动恢复把旧会话作废（此时 expires_at 可能还没到）。
				c.JSON(http.StatusGone, ErrorResponse{
					Code:    "upload_session_expired",
					Message: "upload session has expired",
				})
			case errors.Is(err, file.ErrUploadSessionConflict):
				c.JSON(http.StatusConflict, ErrorResponse{
					Code:    "upload_session_conflict",
					Message: "upload session is not accepting content",
				})
			case errors.Is(err, file.ErrUploadSizeMismatch):
				c.JSON(http.StatusBadRequest, ErrorResponse{
					Code:    "size_mismatch",
					Message: "uploaded content size does not match the declared size",
				})
			default:
				logger.Error("upload session content", "error", err)
				c.JSON(http.StatusInternalServerError, ErrorResponse{
					Code:    "upload_content_failed",
					Message: "failed to store upload content",
				})
			}
			return
		}

		c.JSON(http.StatusOK, info)
	}
}
