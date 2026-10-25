// Package httpapi 只负责 HTTP 这一层：路由、中间件、请求和响应的格式。
//
// 它不负责监听端口（那是 internal/app 的事），也不包含文件业务逻辑。
// 这样以后加“上传/下载”接口时，只需要在这个包里加路由和处理函数。
package httpapi

import (
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

// NewRouter 创建并配置好路由。
//
// logger 和 fileService 都从参数传进来，而不是在包里自己创建：
// 这样整个程序共用同一套对象，测试时也能换成临时目录和“不输出”的日志。
func NewRouter(logger *slog.Logger, fileService *file.Service) *gin.Engine {
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

	// 健康检查：证明 HTTP 进程还活着。
	r.GET("/healthz", health)

	// 文件上传：请求体是 multipart/form-data，文件放在 file 字段里。
	r.POST("/api/files", uploadFile(logger, fileService))
	// 文件下载和删除：:id 是上传时返回的服务端 ID。
	r.GET("/api/files/:id", downloadFile(logger, fileService))
	r.DELETE("/api/files/:id", deleteFile(logger, fileService))

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
