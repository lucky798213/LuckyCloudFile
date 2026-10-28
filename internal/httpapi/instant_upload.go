// 秒传接口：命中已有内容时只创建元数据，不接收文件字节。
package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"luckyCloudFile/internal/file"
)

// instantUploadRequest 是 POST /api/files/instant-upload 的请求体。
//
// size 用指针区分“没传”和“传了 0”；sha256 只能是 64 位十六进制，
// 但服务端仍然会重新校验格式，不会直接把它当成可信摘要写库。
type instantUploadRequest struct {
	ParentID string `json:"parent_id"`
	Name     string `json:"name"`
	Size     *int64 `json:"size"`
	SHA256   string `json:"sha256"`
}

// instantUpload 处理 POST /api/files/instant-upload。
//
// 命中返回 200 {"instant":true,"file_id":"..."}；
// 未命中返回 200 {"instant":false}，客户端继续走普通上传会话。
// 同一个 Idempotency-Key 重试返回原结果；同键不同参数返回 409。
func instantUpload(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "instant upload: missing authenticated user")
			return
		}

		idempotencyKey := c.GetHeader("Idempotency-Key")
		if idempotencyKey == "" {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Code:    "invalid_idempotency_key",
				Message: "Idempotency-Key header is required",
			})
			return
		}

		var request instantUploadRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Code:    "invalid_json",
				Message: `request body must be JSON like {"name":"a.bin","size":1024,"sha256":"..."}`,
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

		result, err := svc.InstantUpload(
			c.Request.Context(),
			ownerID,
			request.ParentID,
			request.Name,
			*request.Size,
			request.SHA256,
			idempotencyKey,
		)
		if err != nil {
			switch {
			case errors.Is(err, file.ErrInvalidIdempotencyKey):
				c.JSON(http.StatusBadRequest, ErrorResponse{
					Code:    "invalid_idempotency_key",
					Message: "Idempotency-Key must be non-empty and at most 255 bytes",
				})
			case errors.Is(err, file.ErrIdempotencyConflict):
				c.JSON(http.StatusConflict, ErrorResponse{
					Code:    "idempotency_conflict",
					Message: "Idempotency-Key was already used with different request parameters",
				})
			case errors.Is(err, file.ErrInvalidContentSHA256):
				c.JSON(http.StatusBadRequest, ErrorResponse{
					Code:    "invalid_sha256",
					Message: "sha256 must be a 64-character hexadecimal string",
				})
			default:
				if handleFileError(c, err, "parent_not_found", "parent directory not found") {
					return
				}
				logger.Error("instant upload", "error", err)
				c.JSON(http.StatusInternalServerError, ErrorResponse{
					Code:    "instant_upload_failed",
					Message: "failed to complete instant upload",
				})
			}
			return
		}

		c.JSON(http.StatusOK, result)
	}
}
