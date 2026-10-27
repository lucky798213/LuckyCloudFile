// Package httpapi 只负责 HTTP 这一层：路由、中间件、请求和响应的格式。
//
// 它不负责监听端口（那是 internal/app 的事），也不包含文件业务逻辑。
// 这样以后加“上传/下载”接口时，只需要在这个包里加路由和处理函数。
package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"luckyCloudFile/internal/file"
)

// ErrorResponse 是所有错误响应的统一格式。
// 前端只要认 code（程序用来判断）和 message（给人看的说明）两个字段就够了。
type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// fileService 是路由需要文件服务提供的能力。
//
// 这里用接口而不是直接写 *file.Service，是为了让 HTTP 层的测试
// 可以用一个不连数据库的假实现；生产代码传进去的仍然是 *file.Service。
type fileService interface {
	Upload(ctx context.Context, ownerID int, parentID, name string, r io.Reader) (file.File, error)
	Open(ctx context.Context, ownerID int, id string) (file.File, io.ReadCloser, error)
	Delete(ctx context.Context, ownerID int, id string) error
	CreateDirectory(ctx context.Context, ownerID int, parentID, name string) (file.File, error)
	ListChildren(ctx context.Context, ownerID int, parentID string, page, pageSize int) (file.ListResult, error)
	Copy(ctx context.Context, ownerID int, id, parentID, name string) (file.File, error)
	Rename(ctx context.Context, ownerID int, id, name string) (file.File, error)
	Move(ctx context.Context, ownerID int, id, parentID string) (file.File, error)
	CreateUploadSession(ctx context.Context, ownerID int, parentID, name string, size int64) (file.UploadSessionInfo, error)
	CreateUploadSessionWithMode(ctx context.Context, ownerID int, parentID, name string, size int64, mode string) (file.UploadSessionInfo, error)
	GetUploadSession(ctx context.Context, ownerID int, sessionID string) (file.UploadSessionInfo, error)
	UploadSessionContent(ctx context.Context, ownerID int, sessionID string, r io.Reader) (file.UploadSessionInfo, error)
	UploadSessionChunk(ctx context.Context, ownerID int, sessionID string, index int, r io.Reader) (file.UploadSessionInfo, error)
	CompleteUploadSession(ctx context.Context, ownerID int, sessionID string) (file.UploadSessionInfo, error)
	CancelUploadSession(ctx context.Context, ownerID int, sessionID string) (file.UploadSessionInfo, error)
}

// NewRouter 创建不带 ZIP 任务接口的路由。
//
// 任务接口需要任务队列，所以由 NewRouterWithTasks 注册。
// 保留这个构造函数是为了让只关心文件接口的测试和调用方不用准备队列。
func NewRouter(logger *slog.Logger, fileService fileService, authService authService) *gin.Engine {
	return newRouter(logger, fileService, authService, nil)
}

// NewRouterWithTasks 在文件接口之外注册 ZIP 异步任务接口。
func NewRouterWithTasks(
	logger *slog.Logger,
	fileService fileService,
	authService authService,
	tasks taskQueue,
) *gin.Engine {
	return newRouter(logger, fileService, authService, tasks)
}

// newRouter 是实际组装路由的地方。
//
// logger、fileService、authService 和 tasks 都从参数传进来，而不是在包里自己创建：
// 这样整个程序共用同一套对象，测试时也能换成假的实现。
func newRouter(
	logger *slog.Logger,
	fileService fileService,
	authService authService,
	tasks taskQueue,
) *gin.Engine {
	// gin.New() 创建一个空引擎。
	// 不用 gin.Default()，是因为它自带日志和 panic 恢复中间件，
	// 而我们的日志格式要自己控制（JSON、带耗时），所以中间件自己加。
	r := gin.New()

	// 打开这个开关后，“路径存在但请求方法不对”（比如 POST /healthz）
	// 会走下面的 NoMethod，返回 405，而不是笼统地返回 404。
	r.HandleMethodNotAllowed = true

	// 中间件按注册顺序执行，就像洋葱：先注册的包在最外层。
	r.Use(requestLogger(logger))
	r.Use(recoverPanic(logger))

	// 健康检查：证明 HTTP 进程还活着，不需要登录。
	r.GET("/healthz", health)

	// 登录是唯一的公开业务接口：没有 Token 的人才需要它。
	r.POST("/api/auth/login", login(logger, authService))

	// 下面这些接口都先经过 Token 校验，再进入各自的处理函数。
	protected := r.Group("/api")
	protected.Use(requireAuth(logger, authService))
	protected.POST("/auth/logout", logout(logger, authService))
	// 目录：只插入一行 File，不写磁盘。
	protected.POST("/directories", createDirectory(logger, fileService))
	// 文件上传：请求体是 multipart/form-data，文件放在 file 字段里；
	// parent_id 是查询参数，不传表示当前用户的根目录。
	protected.POST("/files", uploadFile(logger, fileService))
	// 目录列表：GET /api/files?parent_id=...&page=1&page_size=20
	protected.GET("/files", listFiles(logger, fileService))
	// 文件下载和删除：:id 是上传时返回的服务端 ID，且必须属于当前用户。
	protected.GET("/files/:id", downloadFile(logger, fileService))
	protected.DELETE("/files/:id", deleteFile(logger, fileService))
	// 复制：只创建新的 File 并复用源 Entity，不搬运文件内容。
	protected.POST("/files/:id/copy", copyFile(logger, fileService))
	// 重命名：只改 files.name，文件和目录都支持，不改内容。
	protected.PATCH("/files/:id", renameFile(logger, fileService))
	// 移动：只改 files.parent_id，文件和目录都支持，不搬磁盘内容。
	protected.POST("/files/:id/move", moveFile(logger, fileService))
	// 上传会话：只登记要传的文件和目标目录，本阶段不接收内容。
	protected.POST("/upload-sessions", createUploadSession(logger, fileService))
	protected.GET("/upload-sessions/:id", getUploadSession(logger, fileService))
	// 内容上传：请求体是原始字节，写入会话创建时选定的占位位置。
	// 只用于 full 模式；chunked 会话必须用下面的分片接口。
	protected.PUT("/upload-sessions/:id/content", uploadSessionContent(logger, fileService))
	// 分片上传：:index 从 0 开始，必须按顺序逐片上传。
	protected.PUT("/upload-sessions/:id/chunks/:index", uploadSessionChunk(logger, fileService))
	// 确认完成：把已经 ready 的内容正式发布成用户目录里的文件。
	protected.POST("/upload-sessions/:id/complete", completeUploadSession(logger, fileService))
	// 取消上传：未完成会话标记 canceled；已过期会话保持 expired，
	// 两种情况都会在写入停止后清理占位内容。
	protected.DELETE("/upload-sessions/:id", cancelUploadSession(logger, fileService))

	// ZIP 异步任务：提交只入队并立即返回 task_id；查询读取内存快照。
	// 没有注入队列时不注册这两条路由，避免 nil 处理函数。
	if tasks != nil {
		protected.POST("/archive-tasks", createArchiveTask(logger, fileService, tasks))
		protected.GET("/tasks/:id", getTask(logger, tasks))
	}

	// 兜底：没匹配到任何路由时，返回统一的 JSON 错误，而不是 Gin 默认的纯文本 404。
	r.NoRoute(notFound)
	r.NoMethod(methodNotAllowed)

	return r
}

// requestLogger 在每次请求处理完之后打一行日志：方法、路径、状态码、耗时。
func requestLogger(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		// c.Next() 表示“先把后面的处理函数跑完”，跑完之后才轮到下面这几行。
		// 所以这里能拿到最终的响应状态码和总耗时。
		c.Next()

		logger.Info("http request",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"duration", time.Since(start),
		)
	}
}

// recoverPanic 兜住处理函数里的 panic，把它变成 500 的 JSON 响应。
//
// 两点要留意：
//   - panic 的详细内容只写进日志，不返回给客户端，避免泄露内部信息（堆栈、SQL 等）
//   - 这里用 io.Discard，是为了不让 Gin 再往终端打一份重复的堆栈
func recoverPanic(logger *slog.Logger) gin.HandlerFunc {
	return gin.CustomRecoveryWithWriter(io.Discard, func(c *gin.Context, _ any) {
		logger.Error("http handler panic", "path", c.Request.URL.Path)
		c.AbortWithStatusJSON(http.StatusInternalServerError, ErrorResponse{
			Code:    "internal_error",
			Message: "internal server error",
		})
	})
}

// health 是健康检查接口。
//
// 它只回答“HTTP 进程还活着”，不代表数据库可用。
// 以后接上数据库，可以再单独加一个 /readyz 来判断依赖是否就绪。
func health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// notFound 处理“路径不存在”。
func notFound(c *gin.Context) {
	c.JSON(http.StatusNotFound, ErrorResponse{Code: "not_found", Message: "route not found"})
}

// methodNotAllowed 处理“路径存在，但请求方法不对”。
func methodNotAllowed(c *gin.Context) {
	c.JSON(http.StatusMethodNotAllowed, ErrorResponse{Code: "method_not_allowed", Message: "method not allowed"})
}
