// Package archive 把用户已有的一组文件打包成一个 ZIP，并发布成普通文件。
//
// 它只负责压缩业务：按 ownerID + fileID 通过 file.Service.Open 读取源文件，
// 写服务器自己的临时 ZIP，完成后通过 file.Service.Upload 发布。
// 它不知道 HTTP、队列调度和数据库表结构。
//
// 失败语义：任一源文件打不开或读不完，整个任务失败，不发布半成品 ZIP；
// 临时文件无论成功失败都会删除。
package archive

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"strings"
	"unicode/utf8"

	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/queue"
)

// 默认限制。输出上限与普通上传一致：结果 ZIP 之后还要走 Upload。
// ZIP 有额外开销，所以源文件总大小和输出大小分开检查，不能只靠前者。
const (
	DefaultMaxFiles        = 100
	DefaultMaxSourceBytes  = file.MaxUploadBytes
	DefaultMaxArchiveBytes = file.MaxUploadBytes
)

var (
	// ErrNoSources 表示没有选择任何源文件。
	ErrNoSources = errors.New("archive: no source files")

	// ErrTooManyFiles 表示源文件数量超过上限。
	ErrTooManyFiles = errors.New("archive: too many source files")

	// ErrDuplicateFile 表示同一个 fileID 被选择了多次。
	ErrDuplicateFile = errors.New("archive: duplicate source file")

	// ErrUnsafeName 表示来源文件名不能安全地作为 ZIP 条目名。
	ErrUnsafeName = errors.New("archive: unsafe entry name")

	// ErrDuplicateName 表示两个来源会生成同名 ZIP 条目。
	ErrDuplicateName = errors.New("archive: duplicate entry name")

	// ErrSourceTooLarge 表示源文件累计字节超过输入上限。
	ErrSourceTooLarge = errors.New("archive: source files exceed input limit")

	// ErrArchiveTooLarge 表示写出的 ZIP 超过输出上限。
	ErrArchiveTooLarge = errors.New("archive: archive exceeds output limit")
)

// FileService 是压缩任务需要的最小文件业务接口。
// 生产实现是 *file.Service，测试可以注入假实现。
type FileService interface {
	Open(ctx context.Context, ownerID int, id string) (file.File, io.ReadCloser, error)
	Upload(ctx context.Context, ownerID int, parentID, name string, r io.Reader) (file.File, error)
}

var _ FileService = (*file.Service)(nil)

// Options 是压缩任务的限制。零值使用对应默认值。
type Options struct {
	// TempDir 是临时 ZIP 所在目录；为空时使用 os.TempDir()。
	// 文件名由 os.CreateTemp 生成，不使用客户端的任何名字。
	TempDir string

	MaxFiles        int
	MaxSourceBytes  int64
	MaxArchiveBytes int64

	// Logger 只用于记录临时文件清理失败；为空时使用 slog.Default()。
	Logger *slog.Logger
}

// Job 是一个 ZIP 打包任务，实现 queue.Job。
type Job struct {
	files     FileService
	ownerID   int
	sourceIDs []string
	parentID  string
	name      string

	tempDir         string
	maxSourceBytes  int64
	maxArchiveBytes int64
	logger          *slog.Logger
}

var _ queue.Job = (*Job)(nil)

// New 构造一个 ZIP 任务。sourceIDs 必须是同一用户的普通文件 ID；
// 发布目标是 parentID 目录下的普通文件名 name。
//
// 数量和重复 ID 在提交时检查；文件名是否安全、是否重名、是否可读
// 只能在执行时检查，因为排队期间来源文件可能被改名或删除。
func New(
	files FileService,
	ownerID int,
	sourceIDs []string,
	parentID, name string,
	opts Options,
) (*Job, error) {
	if files == nil {
		return nil, errors.New("archive: file service is required")
	}
	if ownerID <= 0 {
		return nil, errors.New("archive: owner ID must be positive")
	}
	if len(sourceIDs) == 0 {
		return nil, ErrNoSources
	}

	maxFiles := opts.MaxFiles
	if maxFiles <= 0 {
		maxFiles = DefaultMaxFiles
	}
	if len(sourceIDs) > maxFiles {
		return nil, fmt.Errorf("%w: limit is %d", ErrTooManyFiles, maxFiles)
	}

	seen := make(map[string]struct{}, len(sourceIDs))
	for _, id := range sourceIDs {
		if id == "" {
			return nil, errors.New("archive: source file ID is required")
		}
		if _, ok := seen[id]; ok {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateFile, id)
		}
		seen[id] = struct{}{}
	}

	maxSourceBytes := opts.MaxSourceBytes
	if maxSourceBytes <= 0 {
		maxSourceBytes = DefaultMaxSourceBytes
	}
	maxArchiveBytes := opts.MaxArchiveBytes
	if maxArchiveBytes <= 0 {
		maxArchiveBytes = DefaultMaxArchiveBytes
	}
	tempDir := opts.TempDir
	if tempDir == "" {
		tempDir = os.TempDir()
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Job{
		files:           files,
		ownerID:         ownerID,
		sourceIDs:       append([]string(nil), sourceIDs...),
		parentID:        parentID,
		name:            name,
		tempDir:         tempDir,
		maxSourceBytes:  maxSourceBytes,
		maxArchiveBytes: maxArchiveBytes,
		logger:          logger,
	}, nil
}

// Run 生成 ZIP 并发布，返回发布后的结果文件 ID。
//
// 只有 ZIP writer、文件 Close 和发布都成功才返回 nil；
// 临时 ZIP 在 Run 返回前删除。
func (j *Job) Run(ctx context.Context) (string, error) {
	out, err := os.CreateTemp(j.tempDir, "archive-*.zip")
	if err != nil {
		return "", fmt.Errorf("archive: create temporary file: %w", err)
	}
	path := out.Name()
	// 先注册删除、再注册关闭：返回时先关闭句柄再删文件（Windows 上也能删）。
	// 正常路径仍显式 Close 并检查错误，这里只兜 panic 跳过的关闭。
	defer j.removeTemp(path)
	defer out.Close()

	// 输出上限包在 zip.Writer 外层：ZIP 的条目头和中央目录也会被计入。
	zw := zip.NewWriter(&limitWriter{w: out, remaining: j.maxArchiveBytes})

	buildErr := j.writeEntries(ctx, zw)
	// 无论成功失败都关闭 writer 和文件，避免句柄泄漏；
	// 但失败路径只报告 buildErr，避免用半成品的关闭错误掩盖真实原因。
	zipErr := zw.Close()
	fileErr := out.Close()

	if buildErr != nil {
		return "", buildErr
	}
	if zipErr != nil {
		return "", fmt.Errorf("archive: finalize zip: %w", zipErr)
	}
	if fileErr != nil {
		return "", fmt.Errorf("archive: close temporary file: %w", fileErr)
	}

	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("archive: stat temporary file: %w", err)
	}
	// limitWriter 已经拦截超限；这里再按落盘大小核对一次。
	if info.Size() > j.maxArchiveBytes {
		return "", fmt.Errorf("%w: limit is %d bytes", ErrArchiveTooLarge, j.maxArchiveBytes)
	}

	// 压缩期间被取消：不要在这个边界之后发布结果。
	if err := ctx.Err(); err != nil {
		return "", err
	}

	src, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("archive: reopen temporary file: %w", err)
	}
	// 正常路径在发布后显式 Close 并记录错误；panic 时由这里兜底。
	defer src.Close()

	published, uploadErr := j.files.Upload(ctx, j.ownerID, j.parentID, j.name, src)
	closeErr := src.Close()
	if uploadErr != nil {
		return "", fmt.Errorf("archive: publish result: %w", uploadErr)
	}
	// 结果已经发布，读取端 Close 失败不能把成功改写成失败。
	if closeErr != nil {
		j.logger.Warn("archive: close temporary reader", "error", closeErr)
	}

	return published.ID, nil
}

// writeEntries 逐文件写入 ZIP。单个来源交给 writeEntry，
// 每个来源处理完（包括出错或 panic）都会立即关闭，不在循环里累积句柄。
func (j *Job) writeEntries(ctx context.Context, zw *zip.Writer) error {
	seen := make(map[string]struct{}, len(j.sourceIDs))
	remaining := j.maxSourceBytes

	for _, id := range j.sourceIDs {
		if err := ctx.Err(); err != nil {
			return err
		}

		n, err := j.writeEntry(ctx, zw, id, remaining, seen)
		if err != nil {
			return err
		}
		remaining -= n
	}

	return nil
}

// writeEntry 打开并写入一个源文件。
//
// rc.Close 用 defer 统一收口：正常返回时 Close 错误会变成任务错误，
// panic 时也一定会关闭句柄。
func (j *Job) writeEntry(
	ctx context.Context,
	zw *zip.Writer,
	id string,
	remaining int64,
	seen map[string]struct{},
) (n int64, err error) {
	source, rc, err := j.files.Open(ctx, j.ownerID, id)
	if err != nil {
		return 0, fmt.Errorf("archive: open source %s: %w", id, err)
	}
	defer func() {
		if closeErr := rc.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("archive: close source %q: %w", source.Name, closeErr)
		}
	}()

	if !safeEntryName(source.Name) {
		return 0, fmt.Errorf("%w: %q", ErrUnsafeName, source.Name)
	}
	if _, ok := seen[source.Name]; ok {
		return 0, fmt.Errorf("%w: %q", ErrDuplicateName, source.Name)
	}
	seen[source.Name] = struct{}{}

	entry, err := zw.Create(source.Name)
	if err != nil {
		return 0, fmt.Errorf("archive: create entry %q: %w", source.Name, err)
	}

	// 多读 1 字节用于区分“刚好到上限”和“超过上限”。
	n, err = io.Copy(entry, io.LimitReader(
		&contextReader{ctx: ctx, r: rc},
		remaining+1,
	))
	if err != nil {
		return 0, fmt.Errorf("archive: copy %q: %w", source.Name, err)
	}
	if n > remaining {
		return 0, fmt.Errorf("%w: limit is %d bytes", ErrSourceTooLarge, j.maxSourceBytes)
	}
	return n, nil
}

func (j *Job) removeTemp(path string) {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		j.logger.Warn("archive: remove temporary file", "path", path, "error", err)
	}
}

// safeEntryName 只允许单层的普通文件名：
// 拒绝空名、"."、".."、绝对路径、路径分隔符、反斜杠、NUL 和非法 UTF-8。
func safeEntryName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if !utf8.ValidString(name) || strings.ContainsRune(name, 0) {
		return false
	}
	return !strings.ContainsAny(name, `/\`)
}

// contextReader 让流复制响应任务取消；底层 Reader 卡在系统调用时仍需它自己返回。
type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// limitWriter 把 ZIP 输出限制在固定字节数内，超出时返回 ErrArchiveTooLarge。
type limitWriter struct {
	w         io.Writer
	remaining int64
}

func (w *limitWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, ErrArchiveTooLarge
	}
	n, err := w.w.Write(p)
	w.remaining -= int64(n)
	return n, err
}
