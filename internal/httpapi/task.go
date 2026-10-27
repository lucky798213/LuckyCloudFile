// 这个文件负责 ZIP 异步任务的 HTTP 入口：提交任务和查询任务。
//
// 真正的压缩、发布、进度更新都在 internal/archive 和 internal/queue；
// 这里只做认证、参数校验、错误码映射和 JSON 格式。
package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"luckyCloudFile/internal/archive"
	"luckyCloudFile/internal/queue"
)

// taskQueue 是任务接口需要的最小队列能力。
// 生产实现是 *queue.Queue，测试可以注入假实现。
type taskQueue interface {
	Submit(ownerID int, job queue.Job) (string, error)
	Get(ownerID int, id string) (queue.Snapshot, error)
}

// maxArchiveTaskBodyBytes 限制提交任务的 JSON 大小。
// 正常请求最多 100 个文件 ID，1 MiB 足够宽裕。
const maxArchiveTaskBodyBytes = 1 << 20

// archiveTaskRequest 是 POST /api/archive-tasks 的请求体。
// ownerID 不在请求里，只来自登录态。
type archiveTaskRequest struct {
	FileIDs  []string `json:"file_ids"`
	ParentID string   `json:"parent_id"`
	Name     string   `json:"name"`
}

// archiveTaskResponse 是成功入队后的响应。
type archiveTaskResponse struct {
	TaskID string `json:"task_id"`
}

// taskStatusResponse 是 GET /api/tasks/:id 的响应。
type taskStatusResponse struct {
	TaskID         string `json:"task_id"`
	Status         string `json:"status"`
	Stage          string `json:"stage"`
	ProcessedFiles int    `json:"processed_files"`
	TotalFiles     int    `json:"total_files"`
	Error          string `json:"error,omitempty"`
	ResultFileID   string `json:"result_file_id"`
}

// createArchiveTask 处理 POST /api/archive-tasks。
func createArchiveTask(logger *slog.Logger, files fileService, tasks taskQueue) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "create archive task: missing authenticated user")
			return
		}

		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxArchiveTaskBodyBytes)

		var request archiveTaskRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Code:    "invalid_json",
				Message: `request body must be JSON like {"file_ids":["..."],"parent_id":"...","name":"bundle.zip"}`,
			})
			return
		}

		job, err := archive.New(
			files,
			ownerID,
			request.FileIDs,
			request.ParentID,
			request.Name,
			archive.Options{},
		)
		if err != nil {
			if isArchiveInputError(err) {
				c.JSON(http.StatusBadRequest, ErrorResponse{
					Code:    "invalid_request",
					Message: err.Error(),
				})
				return
			}
			logger.Error("create archive task", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "internal_error",
				Message: "internal server error",
			})
			return
		}

		taskID, err := tasks.Submit(ownerID, job)
		switch {
		case errors.Is(err, queue.ErrQueueFull):
			c.JSON(http.StatusServiceUnavailable, ErrorResponse{
				Code:    "queue_full",
				Message: "task queue is full, try again later",
			})
		case errors.Is(err, queue.ErrQueueClosed):
			c.JSON(http.StatusServiceUnavailable, ErrorResponse{
				Code:    "queue_shutdown",
				Message: "server is shutting down",
			})
		case err != nil:
			logger.Error("submit archive task", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "internal_error",
				Message: "internal server error",
			})
		default:
			c.JSON(http.StatusAccepted, archiveTaskResponse{TaskID: taskID})
		}
	}
}

// isArchiveInputError 只把用户输入问题映射成 400；
// 其它错误（例如内部状态异常）继续按 500 处理。
func isArchiveInputError(err error) bool {
	return errors.Is(err, archive.ErrNoSources) ||
		errors.Is(err, archive.ErrTooManyFiles) ||
		errors.Is(err, archive.ErrDuplicateFile) ||
		errors.Is(err, archive.ErrInvalidFileID) ||
		errors.Is(err, archive.ErrInvalidName)
}

// getTask 处理 GET /api/tasks/:id。
// 非本人任务和不存在任务统一 404，避免泄露任务是否存在。
func getTask(logger *slog.Logger, tasks taskQueue) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "get task: missing authenticated user")
			return
		}

		task, err := tasks.Get(ownerID, c.Param("id"))
		switch {
		case errors.Is(err, queue.ErrNotFound):
			c.JSON(http.StatusNotFound, ErrorResponse{
				Code:    "task_not_found",
				Message: "task not found",
			})
		case err != nil:
			logger.Error("get task", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "internal_error",
				Message: "internal server error",
			})
		default:
			c.JSON(http.StatusOK, taskStatusResponse{
				TaskID:         task.ID,
				Status:         string(task.Status),
				Stage:          task.Stage,
				ProcessedFiles: task.Processed,
				TotalFiles:     task.Total,
				Error:          task.Error,
				ResultFileID:   task.Result,
			})
		}
	}
}
