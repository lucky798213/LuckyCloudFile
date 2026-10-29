package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"luckyCloudFile/internal/file"
)

// Register only when the concrete service offers cloud direct upload; existing
// small router test doubles do not need cloud capabilities.
func registerDirectUploads(g *gin.RouterGroup, logger *slog.Logger, svc fileService) {
	s, ok := svc.(*file.Service)
	if !ok {
		return
	}
	run := func(operation string) gin.HandlerFunc {
		return func(c *gin.Context) {
			owner, ok := currentUserID(c)
			if !ok {
				return
			}
			ctx := c.Request.Context()
			id := c.Param("id")
			var result any
			var err error
			status := http.StatusOK
			switch operation {
			case "create":
				var req instantUploadRequest
				if c.ShouldBindJSON(&req) != nil || req.Size == nil {
					c.JSON(400, ErrorResponse{Code: "invalid_request", Message: "name, size and sha256 are required"})
					return
				}
				result, err = s.CreateDirectUpload(ctx, owner, req.ParentID, req.Name, *req.Size, req.SHA256, c.GetHeader("Idempotency-Key"))
				status = 201
			case "get":
				result, err = s.GetDirectUpload(ctx, owner, id)
			case "sign":
				var req struct {
					Numbers []int `json:"part_numbers"`
				}
				if c.ShouldBindJSON(&req) != nil {
					c.JSON(400, ErrorResponse{Code: "invalid_request", Message: "part_numbers required"})
					return
				}
				result, err = s.SignDirectParts(ctx, owner, id, req.Numbers)
			case "complete":
				result, err = s.CompleteDirectUpload(ctx, owner, id)
				status = 202
			}
			if err != nil {
				switch {
				case errors.Is(err, file.ErrDirectUnavailable):
					c.JSON(409, ErrorResponse{Code: "direct_unavailable", Message: err.Error()})
				case errors.Is(err, file.ErrDirectConflict), errors.Is(err, file.ErrIdempotencyConflict):
					c.JSON(409, ErrorResponse{Code: "direct_conflict", Message: err.Error()})
				case errors.Is(err, file.ErrInvalidIdempotencyKey), errors.Is(err, file.ErrInvalidContentSHA256):
					c.JSON(400, ErrorResponse{Code: "invalid_request", Message: err.Error()})
				default:
					if !handleFileError(c, err, "not_found", "upload or directory not found") {
						logger.Error("direct upload", "operation", operation, "error", err)
						c.JSON(503, ErrorResponse{Code: "direct_upload_unavailable", Message: "Operation unconfirmed; query the same session or retry with the same Idempotency-Key"})
					}
				}
				return
			}
			c.JSON(status, result)
		}
	}
	g.POST("/direct-upload-sessions", run("create"))
	g.GET("/direct-upload-sessions/:id", run("get"))
	g.POST("/direct-upload-sessions/:id/part-urls", run("sign"))
	g.POST("/direct-upload-sessions/:id/complete", run("complete"))
}
