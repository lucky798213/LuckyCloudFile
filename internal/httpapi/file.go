// 这个文件只做 HTTP 层的事：读请求、调用 Service、写响应。
// 真正的“文件怎么存”在 internal/file 和 internal/storage 里。
package httpapi

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"

	"github.com/gin-gonic/gin"

	"luckyCloudFile/internal/file"
)

// uploadFieldName 是上传接口约定的表单字段名。
// 客户端必须用这个字段名，接口行为才可预测。
const uploadFieldName = "file"

// multipartOverheadBytes 是留给 multipart 表单头、边界、结尾的余量。
//
// 为什么要单独留余量？因为 http.MaxBytesReader 限制的是“整个请求体”，
// 而我们要限制的是“文件内容”。如果两者用同一个数字，
// 一个正好 32 MiB 的文件会因为多出几百字节的表单头而被误判为超限。
const multipartOverheadBytes int64 = 1 << 20 // 1 MiB，足够放下表单头

var (
	// errMissingFile 表示请求里根本没有文件字段。
	errMissingFile = errors.New("missing file part")
	// errWrongField 表示有文件字段，但字段名不是 file。
	errWrongField = errors.New("file part must be named file")
)

// createDirectoryRequest 是 POST /api/directories 的请求体。
// parent_id 为空表示建在当前用户的根目录下。
type createDirectoryRequest struct {
	ParentID string `json:"parent_id"`
	Name     string `json:"name"`
}

// createDirectory 处理 POST /api/directories。
//
//	curl -X POST "$BASE/api/directories" \
//	  -H "Authorization: Bearer $TOKEN" \
//	  -H 'Content-Type: application/json' \
//	  -d '{"name":"学习资料"}'
//
// 成功返回 201 和这个目录的 {id,name,kind,parent_id}。
func createDirectory(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "create directory: missing authenticated user")
			return
		}

		var request createDirectoryRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Code:    "invalid_json",
				Message: `request body must be JSON like {"name":"docs","parent_id":"..."}`,
			})
			return
		}

		record, err := svc.CreateDirectory(
			c.Request.Context(),
			ownerID,
			request.ParentID,
			request.Name,
		)
		if err != nil {
			if handleFileError(c, err, "parent_not_found", "parent directory not found") {
				return
			}
			logger.Error("create directory", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "create_failed",
				Message: "failed to create directory",
			})
			return
		}

		c.JSON(http.StatusCreated, record)
	}
}

// uploadFile 处理 POST /api/files。
//
// 请求必须是 multipart/form-data，文件放在名为 file 的字段里；
// 目标目录用查询参数 parent_id 指定，不传就是当前用户的根目录：
//
//	curl -F "file=@/path/to/a.txt" "$BASE/api/files?parent_id=<目录ID>"
//
// 成功返回 201：
//
//	{"id":"3f2a...","name":"a.txt","size":1234,"kind":"file","parent_id":"..."}
func uploadFile(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 用户 ID 只能来自鉴权中间件写入的登录会话。
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "upload file: missing authenticated user")
			return
		}

		parentID := c.Query("parent_id")

		// 第一道限制：整个请求体不能无限大，防止有人往死里发。
		// 注意这里加上了 multipart 的余量，真正的“32 MiB”由下面的 Service 精确检查。
		c.Request.Body = http.MaxBytesReader(
			c.Writer,
			c.Request.Body,
			file.MaxUploadBytes+multipartOverheadBytes,
		)

		// 找到文件字段。注意这里没有用 c.FormFile：
		// c.FormFile 会先把整个文件读进内存或临时目录，再让我们复制第二遍；
		// 用 MultipartReader 才能让下面的 io.Copy 直接从网络读到磁盘。
		part, err := firstFilePart(c.Request)
		if err != nil {
			switch {
			case isTooLarge(err):
				respondTooLarge(c)
			case errors.Is(err, errMissingFile):
				c.JSON(http.StatusBadRequest, ErrorResponse{
					Code:    "missing_file",
					Message: "multipart field 'file' is required",
				})
			case errors.Is(err, errWrongField):
				c.JSON(http.StatusBadRequest, ErrorResponse{
					Code:    "invalid_field",
					Message: "file must be uploaded in the 'file' field",
				})
			default:
				c.JSON(http.StatusBadRequest, ErrorResponse{
					Code:    "invalid_multipart",
					Message: "request must be multipart/form-data with a file part",
				})
			}
			return
		}
		defer part.Close()

		// part 是一个“还没读完的数据流”，直接交给 Service。
		// 文件名只作为元数据保存，磁盘上的文件名用服务端生成的 ID。
		record, err := svc.Upload(
			c.Request.Context(),
			ownerID,
			parentID,
			filepath.Base(part.FileName()),
			part,
		)
		if err != nil {
			// 父目录不存在/不是目录、名字非法、同名冲突都在这里统一映射。
			if handleFileError(c, err, "parent_not_found", "parent directory not found") {
				return
			}
			// 服务端自己的问题（磁盘满了、没权限等）：细节只写日志，不返回给客户端。
			logger.Error("upload file", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "upload_failed",
				Message: "failed to save file",
			})
			return
		}

		c.JSON(http.StatusCreated, record)
	}
}

// copyFileRequest 是 POST /api/files/:id/copy 的请求体。
// parent_id 是目标目录，不传表示当前用户的根目录；name 是副本的新名字。
type copyFileRequest struct {
	ParentID string `json:"parent_id"`
	Name     string `json:"name"`
}

// copyFile 处理 POST /api/files/:id/copy。
//
//	curl -X POST "$BASE/api/files/$FILE_ID/copy" \
//	  -H "Authorization: Bearer $TOKEN" \
//	  -H 'Content-Type: application/json' \
//	  -d '{"parent_id":"<目标目录ID>","name":"笔记-副本.txt"}'
//
// 成功返回 201 和新 File。请求不携带文件内容：新 File 复用源文件的 Entity。
// 源文件或目标目录不存在、不属于当前用户，统一返回 404；
// 目标目录已有同名节点返回 409。
func copyFile(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "copy file: missing authenticated user")
			return
		}

		var request copyFileRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Code:    "invalid_json",
				Message: `request body must be JSON like {"parent_id":"...","name":"copy.txt"}`,
			})
			return
		}

		record, err := svc.Copy(
			c.Request.Context(),
			ownerID,
			c.Param("id"),
			request.ParentID,
			request.Name,
		)
		if err != nil {
			if handleFileError(c, err, "file_or_parent_not_found", "source file or target directory not found") {
				return
			}
			logger.Error("copy file", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "copy_failed",
				Message: "failed to copy file",
			})
			return
		}

		c.JSON(http.StatusCreated, record)
	}
}

// moveFileRequest 是 POST /api/files/:id/move 的请求体。
// parent_id 是目标目录，不传表示当前用户的根目录。
type moveFileRequest struct {
	ParentID string `json:"parent_id"`
}

// moveFile 处理 POST /api/files/:id/move。
//
//	curl -X POST "$BASE/api/files/$ID/move" \
//	  -H "Authorization: Bearer $TOKEN" \
//	  -H 'Content-Type: application/json' \
//	  -d '{"parent_id":"<目标目录ID>"}'
//
// 文件和目录都能移动，只改 parent_id，不搬磁盘内容、不改 Entity。
// 成功返回 200 和移动后的节点；源节点或目标目录不存在、不属于当前用户返回 404；
// 目标目录已有同名节点返回 409；目录移到自己或后代返回 400 invalid_move；
// 移动用户根目录返回 400 root_directory。
func moveFile(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "move file: missing authenticated user")
			return
		}

		var request moveFileRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Code:    "invalid_json",
				Message: `request body must be JSON like {"parent_id":"..."}`,
			})
			return
		}

		record, err := svc.Move(c.Request.Context(), ownerID, c.Param("id"), request.ParentID)
		if err != nil {
			if handleFileError(c, err, "file_or_parent_not_found", "source file or target directory not found") {
				return
			}
			logger.Error("move file", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "move_failed",
				Message: "failed to move file",
			})
			return
		}

		c.JSON(http.StatusOK, record)
	}
}

// renameFileRequest 是 PATCH /api/files/:id 的请求体。
type renameFileRequest struct {
	Name string `json:"name"`
}

// renameFile 处理 PATCH /api/files/:id。
//
//	curl -X PATCH "$BASE/api/files/$ID" \
//	  -H "Authorization: Bearer $TOKEN" \
//	  -H 'Content-Type: application/json' \
//	  -d '{"name":"新名字.txt"}'
//
// 文件和目录都能改名，只改显示名，不移动位置、不改变内容。
// 成功返回 200 和改名后的节点；不存在或不属于当前用户返回 404；
// 同一目录已有同名节点返回 409；名字非法返回 400。
func renameFile(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "rename file: missing authenticated user")
			return
		}

		var request renameFileRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Code:    "invalid_json",
				Message: `request body must be JSON like {"name":"new-name.txt"}`,
			})
			return
		}

		record, err := svc.Rename(c.Request.Context(), ownerID, c.Param("id"), request.Name)
		if err != nil {
			if handleFileError(c, err, "file_not_found", "file not found") {
				return
			}
			logger.Error("rename file", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "rename_failed",
				Message: "failed to rename file",
			})
			return
		}

		c.JSON(http.StatusOK, record)
	}
}

// listFiles 处理 GET /api/files。
//
// 只返回 parentID 这一层的直接子节点，不递归整棵树。
// parent_id 为空表示当前用户的根目录；page 从 1 开始。
func listFiles(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "list files: missing authenticated user")
			return
		}

		page, err := queryInt(c, "page", 1)
		if err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Code:    "invalid_pagination",
				Message: "page must be a positive integer",
			})
			return
		}
		pageSize, err := queryInt(c, "page_size", 20)
		if err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Code:    "invalid_pagination",
				Message: "page_size must be a positive integer",
			})
			return
		}

		result, err := svc.ListChildren(
			c.Request.Context(),
			ownerID,
			c.Query("parent_id"),
			page,
			pageSize,
		)
		if err != nil {
			if handleFileError(c, err, "parent_not_found", "parent directory not found") {
				return
			}
			logger.Error("list files", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "list_failed",
				Message: "failed to list directory",
			})
			return
		}

		c.JSON(http.StatusOK, result)
	}
}

// downloadFile 处理 GET /api/files/:id。
//
// Service 会同时按文件 ID 和当前用户 ID 查询：查不到直接 404，
// 既不会拿任意字符串去拼磁盘路径，也不会把别人的文件暴露给当前用户。
// 查到了就打开文件，用 io.Copy 流式写回响应，不把整个文件读进内存。
// 目录没有内容，返回 400 is_directory。
func downloadFile(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "download file: missing authenticated user")
			return
		}

		record, f, err := svc.Open(c.Request.Context(), ownerID, c.Param("id"))
		if err != nil {
			if handleFileError(c, err, "file_not_found", "file not found") {
				return
			}
			logger.Error("open file", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "download_failed",
				Message: "failed to open file",
			})
			return
		}
		defer f.Close()

		// 响应头必须在写正文之前设置，一旦开始写就不能再改状态码了。
		// Content-Disposition 让浏览器用原始文件名保存，而不是用 ID。
		disposition := mime.FormatMediaType("attachment", map[string]string{"filename": record.Name})
		c.Header("Content-Disposition", disposition)
		c.Header("Content-Type", "application/octet-stream")
		c.Header("Content-Length", strconv.FormatInt(record.Size, 10))
		c.Status(http.StatusOK)

		// 流式返回：读一块、写一块，内存占用和文件大小无关。
		if _, err := io.Copy(c.Writer, f); err != nil {
			// 这时响应头已经发出去了，改不了状态码，只能记录日志。
			logger.Error("send file", "error", err)
		}
	}
}

// deleteFile 处理 DELETE /api/files/:id。
//
// 成功返回 204（No Content，没有响应正文）。
// 普通文件：204 只表示逻辑文件已经删除，不承诺磁盘空间已经立即释放：
// Service 在事务提交后尝试物理清理，失败会保留 pending_delete 标记，
// 由启动扫描和后台定时扫描继续重试。
// 目录：只能删除空目录（返回 204）；非空目录返回 409 directory_not_empty，
// 用户根目录返回 400 root_directory。
// 文件不存在或不属于当前用户，统一返回 404。
func deleteFile(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		ownerID, ok := currentUserID(c)
		if !ok {
			respondInternalError(c, logger, "delete file: missing authenticated user")
			return
		}

		err := svc.Delete(c.Request.Context(), ownerID, c.Param("id"))
		if err != nil {
			if handleFileError(c, err, "file_not_found", "file not found") {
				return
			}
			logger.Error("delete file", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "delete_failed",
				Message: "failed to delete file",
			})
			return
		}
		c.Status(http.StatusNoContent)
	}
}

// handleFileError 把业务层的哨兵错误映射成 HTTP 状态码。
//
// 返回 true 表示已经写过响应；false 表示这是未知错误，交给调用方记日志并返回 500。
// notFoundCode/notFoundMessage 让“文件不存在”和“父目录不存在”可以用不同的提示。
func handleFileError(c *gin.Context, err error, notFoundCode, notFoundMessage string) bool {
	switch {
	case errors.Is(err, file.ErrNotFound):
		c.JSON(http.StatusNotFound, ErrorResponse{Code: notFoundCode, Message: notFoundMessage})
	case errors.Is(err, file.ErrTooLarge):
		respondTooLarge(c)
	case errors.Is(err, file.ErrInvalidName):
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Code:    "invalid_name",
			Message: "name must not be empty, '.', '..', or contain path separators",
		})
	case errors.Is(err, file.ErrNotDirectory):
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Code:    "not_a_directory",
			Message: "parent_id must refer to a directory",
		})
	case errors.Is(err, file.ErrIsDirectory):
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Code:    "is_directory",
			Message: "this endpoint only operates on files, not directories",
		})
	case errors.Is(err, file.ErrAlreadyExists):
		c.JSON(http.StatusConflict, ErrorResponse{
			Code:    "already_exists",
			Message: "an entry with the same name already exists in this directory",
		})
	case errors.Is(err, file.ErrRootDirectory):
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Code:    "root_directory",
			Message: "the user root directory cannot be modified",
		})
	case errors.Is(err, file.ErrDirectoryNotEmpty):
		c.JSON(http.StatusConflict, ErrorResponse{
			Code:    "directory_not_empty",
			Message: "directory must be empty before it can be deleted",
		})
	case errors.Is(err, file.ErrInvalidMove):
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Code:    "invalid_move",
			Message: "a directory cannot be moved into itself or its descendants",
		})
	default:
		return false
	}
	return true
}

// queryInt 读取整数查询参数，缺省时返回 defaultValue。
func queryInt(c *gin.Context, name string, defaultValue int) (int, error) {
	raw := c.Query(name)
	if raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	return value, nil
}

// firstFilePart 从 multipart 请求里取出“字段名是 file、并且带文件名”的那个 part。
//
// 普通表单字段（比如 name=abc）没有文件名，跳过它们；就是循环读取字段，直到找到 file
// 有文件名但字段名不是 file 的，按接口约定直接拒绝，而不是悄悄收下。
func firstFilePart(r *http.Request) (*multipart.Part, error) {
	reader, err := r.MultipartReader()
	if err != nil {
		return nil, err
	}

	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			// 读完了也没遇到文件字段。
			return nil, errMissingFile
		}
		if err != nil {
			return nil, err
		}

		if part.FileName() != "" {
			if part.FormName() != uploadFieldName {
				_ = part.Close()
				return nil, errWrongField
			}
			return part, nil
		}
		_ = part.Close()
	}
}

// isTooLarge 判断错误是不是“超过大小上限”。
//
// 有两种可能：
//   - 整个请求体超过 MaxBytesReader 的上限 → *http.MaxBytesError
//   - 文件内容超过 32 MiB → file.ErrTooLarge
func isTooLarge(err error) bool {
	var maxErr *http.MaxBytesError
	return errors.As(err, &maxErr) || errors.Is(err, file.ErrTooLarge)
}

// respondTooLarge 返回 413，并在提示里写明上限。
func respondTooLarge(c *gin.Context) {
	c.JSON(http.StatusRequestEntityTooLarge, ErrorResponse{
		Code:    "file_too_large",
		Message: fmt.Sprintf("file must be at most %d MiB", file.MaxUploadBytes/(1<<20)),
	})
}

// respondInternalError 记录只给服务端看的细节，对客户端返回统一的 500。
func respondInternalError(c *gin.Context, logger *slog.Logger, message string) {
	logger.Error(message, "path", c.Request.URL.Path)
	c.JSON(http.StatusInternalServerError, ErrorResponse{
		Code:    "internal_error",
		Message: "internal server error",
	})
}
