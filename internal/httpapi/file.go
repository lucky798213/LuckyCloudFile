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

// uploadFile 处理 POST /api/files。
//
// 请求必须是 multipart/form-data，文件放在名为 file 的字段里：
//
//	curl -F "file=@/path/to/a.txt" http://127.0.0.1:8080/api/files
//
// 成功返回 201：
//
//	{"id":"3f2a...","name":"a.txt","size":1234}
func uploadFile(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
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
		record, err := svc.Upload(c.Request.Context(), filepath.Base(part.FileName()), part)
		if err != nil {
			if isTooLarge(err) {
				respondTooLarge(c)
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

// downloadFile 处理 GET /api/files/:id。
//
// 它先把 ID 拿到内存索引里查：查不到直接 404，不会拿任意字符串去拼磁盘路径。
// 查到了就打开文件，用 io.Copy 流式写回响应，不把整个文件读进内存。
func downloadFile(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		record, f, err := svc.Open(c.Request.Context(), c.Param("id"))
		if errors.Is(err, file.ErrNotFound) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Code:    "file_not_found",
				Message: "file not found",
			})
			return
		}
		if err != nil {
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
// 成功返回 204（No Content，没有响应正文）；ID 不存在返回 404。
func deleteFile(logger *slog.Logger, svc fileService) gin.HandlerFunc {
	return func(c *gin.Context) {
		err := svc.Delete(c.Request.Context(), c.Param("id"))
		switch {
		case errors.Is(err, file.ErrNotFound):
			c.JSON(http.StatusNotFound, ErrorResponse{
				Code:    "file_not_found",
				Message: "file not found",
			})
		case err != nil:
			logger.Error("delete file", "error", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Code:    "delete_failed",
				Message: "failed to delete file",
			})
		default:
			c.Status(http.StatusNoContent)
		}
	}
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
