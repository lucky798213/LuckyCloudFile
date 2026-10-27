// 这个文件负责 ZIP 异步任务的 HTTP 入口：提交任务和查询任务。
//
// 提交顺序是“先落库、再通知”：task.Service 生成稳定 ID 并把任务写进
// tasks 表，提交事务成功后才唤醒调度器；内存队列满或通知丢失都不会让
// 已经接收的任务失败，周期扫描会兜底。查询以数据库为准，内存快照只
// 提供展示用进度。
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"luckyCloudFile/internal/archive"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/queue"
	"luckyCloudFile/internal/task"
)

// taskService 是任务接口需要的最小持久化能力。
// 生产实现是 *task.Service，测试可以注入假实现。
type taskService interface {
	Submit(ctx context.Context, params task.CreateParams) (*model.Task, error)
	GetOwned(ctx context.Context, ownerID int, id string) (*model.Task, error)
}

// taskSnapshots 是运行期进度来源，只用于展示。
// 它不是事实来源：任务状态、错误和结果都来自数据库。
type taskSnapshots interface {
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
//
// 只有“准入拒绝”会在落库前返回错误；一旦任务已经写入数据库，
// 即使接下来内存队列满、唤醒通知丢失，也返回 202 和原 taskID。
func createArchiveTask(logger *slog.Logger, tasks taskService) gin.HandlerFunc {
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

		// 静态校验在落库之前完成：数量和名字不合法时不应该产生一条
		// 永远跑不起来的任务记录。
		state, err := archive.NewTaskState(
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

		stateJSON, err := json.Marshal(state)
		if err != nil {
			logger.Error("encode archive task state", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "internal_error",
				Message: "internal server error",
			})
			return
		}

		submitted, err := tasks.Submit(c.Request.Context(), task.CreateParams{
			Type:         archive.TaskType,
			OwnerID:      ownerID,
			Phase:        archive.PhasePrepare,
			StateVersion: archive.TaskStateVersion,
			StateJSON:    string(stateJSON),
		})
		switch {
		case errors.Is(err, task.ErrTooManyPending):
			// 这是落库前的准入拒绝，不是“已经接收但队列满”。
			c.JSON(http.StatusServiceUnavailable, ErrorResponse{
				Code:    "task_limit",
				Message: "too many unfinished tasks, try again later",
			})
		case err != nil:
			logger.Error("submit archive task", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "internal_error",
				Message: "internal server error",
			})
		default:
			c.JSON(http.StatusAccepted, archiveTaskResponse{TaskID: submitted.ID})
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
func getTask(
	logger *slog.Logger,
	tasks taskService,
	snapshots taskSnapshots,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "get task: missing authenticated user")
			return
		}

		record, err := tasks.GetOwned(c.Request.Context(), ownerID, c.Param("id"))
		switch {
		case errors.Is(err, task.ErrNotFound):
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
			c.JSON(http.StatusOK, taskStatusFromRecord(record, progressOf(snapshots, ownerID, record)))
		}
	}
}

// progressOf 读取运行期快照。快照不存在（例如进程刚重启）不是错误：
// 状态和结果仍然能由数据库完整回答。
func progressOf(
	snapshots taskSnapshots,
	ownerID int,
	record *model.Task,
) queue.Snapshot {
	if snapshots == nil {
		return queue.Snapshot{}
	}
	snapshot, err := snapshots.Get(ownerID, record.ID)
	if err != nil {
		return queue.Snapshot{}
	}
	return snapshot
}

// taskStatusFromRecord 以数据库记录为准拼响应，只从内存快照补充进度。
//
// 状态、错误和结果不能取自快照：等待重试时快照可能已经显示 failed，
// 但数据库才是“任务接下来还会不会执行”的唯一答案。
func taskStatusFromRecord(record *model.Task, snapshot queue.Snapshot) taskStatusResponse {
	stage := record.Phase
	switch record.Status {
	case model.TaskStatusCompleted, model.TaskStatusFailed:
		stage = record.Status
	case model.TaskStatusQueued, model.TaskStatusRunning:
		// 只有内存快照仍处于排队/执行时，它的 stage 才代表当前这一轮。
		// 快照可能是上一轮留下的 failed，而数据库已经重新排队。
		if snapshot.Stage != "" &&
			(snapshot.Status == queue.Queued || snapshot.Status == queue.Running) {
			stage = snapshot.Stage
		}
	}

	resultFileID := ""
	if record.ResultFileID != nil {
		resultFileID = *record.ResultFileID
	}

	return taskStatusResponse{
		TaskID:         record.ID,
		Status:         record.Status,
		Stage:          stage,
		ProcessedFiles: snapshot.Processed,
		TotalFiles:     snapshot.Total,
		Error:          record.LastError,
		ResultFileID:   resultFileID,
	}
}
