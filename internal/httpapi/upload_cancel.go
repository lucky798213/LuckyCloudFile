// 取消上传接口：先让会话不可再发布，再异步/尽量清理占位内容。
package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"luckyCloudFile/internal/file"
)

// cancelUploadSession 处理 DELETE /api/upload-sessions/:id。
//
// 成功统一返回 202 Accepted：取消状态已经落库，但正在传输的请求
// 可能尚未完全退出，物理清理也可能在后台重试。
// 响应体仍是完整的 UploadSessionInfo：普通取消的 status 是 canceled；
// 已经过期的会话保持 expired，取消只继续推进清理，不覆盖结束原因。
func cancelUploadSession(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "cancel upload session: missing authenticated user")
			return
		}

		info, err := svc.CancelUploadSession(
			c.Request.Context(),
			ownerID,
			c.Param("id"),
		)
		if err != nil {
			switch {
			case errors.Is(err, file.ErrUploadSessionNotFound):
				c.JSON(http.StatusNotFound, ErrorResponse{
					Code:    "upload_session_not_found",
					Message: "upload session not found",
				})
			case errors.Is(err, file.ErrUploadSessionCompleted):
				c.JSON(http.StatusConflict, ErrorResponse{
					Code:    "upload_session_completed",
					Message: "a completed upload session cannot be canceled",
				})
			default:
				logger.Error("cancel upload session", "error", err)
				c.JSON(http.StatusInternalServerError, ErrorResponse{
					Code:    "cancel_upload_session_failed",
					Message: "failed to cancel upload session",
				})
			}
			return
		}

		c.JSON(http.StatusAccepted, info)
	}
}
