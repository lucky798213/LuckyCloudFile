// 上传完成接口：把 ready 会话的内容正式发布到用户目录。
package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"luckyCloudFile/internal/file"
)

// completeUploadSession 处理 POST /api/upload-sessions/:id/complete。
//
// 请求不需要文件内容，也不需要重新提交名称或目录：这些信息在创建会话时
// 已经确定。首次完成和重复完成都返回 200，响应是完整的 UploadSessionInfo，
// 其中 status=completed、result_file_id 是正式文件 ID。
func completeUploadSession(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "complete upload session: missing authenticated user")
			return
		}

		info, err := svc.CompleteUploadSession(
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
			case errors.Is(err, file.ErrUploadSessionExpired):
				c.JSON(http.StatusGone, ErrorResponse{
					Code:    "upload_session_expired",
					Message: "upload session has expired",
				})
			case errors.Is(err, file.ErrUploadSessionNotCompletable):
				c.JSON(http.StatusConflict, ErrorResponse{
					Code:    "upload_session_not_completable",
					Message: "upload session is not ready to complete",
				})
			case errors.Is(err, file.ErrUploadSessionConflict):
				c.JSON(http.StatusConflict, ErrorResponse{
					Code:    "upload_session_conflict",
					Message: "upload session content is not available for completion",
				})
			case errors.Is(err, file.ErrUploadSessionInconsistent):
				logger.Error("complete upload session: inconsistent completed session", "error", err)
				c.JSON(http.StatusInternalServerError, ErrorResponse{
					Code:    "upload_session_inconsistent",
					Message: "upload session is in an inconsistent state",
				})
			default:
				// 目录失效、同名冲突等错误复用现有映射：
				// 404 parent_not_found、400 not_a_directory、409 already_exists。
				if handleFileError(c, err, "parent_not_found", "target directory not found") {
					return
				}
				logger.Error("complete upload session", "error", err)
				c.JSON(http.StatusInternalServerError, ErrorResponse{
					Code:    "complete_upload_session_failed",
					Message: "failed to complete upload session",
				})
			}
			return
		}

		c.JSON(http.StatusOK, info)
	}
}
