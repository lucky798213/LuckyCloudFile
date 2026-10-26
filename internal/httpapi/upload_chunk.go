// 分片上传接口：把 chunked 会话的内容一片一片写进同一个占位位置。
package httpapi

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"luckyCloudFile/internal/file"
)

// uploadSessionChunk 处理 PUT /api/upload-sessions/:id/chunks/:index。
//
// :index 从 0 开始，必须严格按顺序上传。请求体就是这一片的原始字节，
// 长度由服务端根据会话大小和固定分片大小算出来，客户端不能自己声明。
//
// 成功返回 200 和会话最新进度；最后一片完成后 status 是 ready，
// 仍然需要调用确认完成接口才会出现在用户目录里。
func uploadSessionChunk(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "upload session chunk: missing authenticated user")
			return
		}

		index, err := strconv.Atoi(c.Param("index"))
		if err != nil || index < 0 {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Code:    "invalid_chunk_index",
				Message: "chunk index must be a non-negative integer",
			})
			return
		}

		info, err := svc.UploadSessionChunk(
			c.Request.Context(),
			ownerID,
			c.Param("id"),
			index,
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
				c.JSON(http.StatusGone, ErrorResponse{
					Code:    "upload_session_expired",
					Message: "upload session has expired",
				})
			case errors.Is(err, file.ErrUploadModeMismatch):
				c.JSON(http.StatusConflict, ErrorResponse{
					Code:    "upload_mode_mismatch",
					Message: "full sessions must upload through the content endpoint",
				})
			case errors.Is(err, file.ErrInvalidChunkIndex):
				c.JSON(http.StatusBadRequest, ErrorResponse{
					Code:    "invalid_chunk_index",
					Message: "chunk index is outside the session's chunk range",
				})
			case errors.Is(err, file.ErrUploadSessionConflict):
				c.JSON(http.StatusConflict, ErrorResponse{
					Code:    "upload_session_conflict",
					Message: "chunk conflicts with uploaded content or the session is not accepting content",
				})
			case errors.Is(err, file.ErrUploadSizeMismatch):
				c.JSON(http.StatusBadRequest, ErrorResponse{
					Code:    "size_mismatch",
					Message: "chunk size does not match the expected size",
				})
			default:
				logger.Error("upload session chunk", "error", err)
				c.JSON(http.StatusInternalServerError, ErrorResponse{
					Code:    "upload_chunk_failed",
					Message: "failed to store upload chunk",
				})
			}
			return
		}

		c.JSON(http.StatusOK, info)
	}
}
