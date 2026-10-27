// 这个文件给本地存储增加“预分配 + 随机偏移写入 + 最后发布”的分片能力。
//
// 它仍然是 storage 包的一部分：业务层只拿到存储接口，不会看到临时文件路径，
// 也不会直接操作 os.File。
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
)

// ChunkStore 是支持分片写入的可选存储能力。
//
// 使用顺序固定为：PrepareChunks → 多次 WriteChunkAt → FinalizeChunks。
// 它不负责记录“哪些分片已经写过”，那是业务层的进度；存储层只保证
// 临时对象按指定长度预分配、按偏移写入，并在最后原子地发布成正式对象。
type ChunkStore interface {
	Store

	// PrepareChunks 创建或复用 .upload-<key>，首次初始化时把长度设置为 totalSize。
	// 重复调用不会清空已经写入的分片。
	PrepareChunks(
		ctx context.Context,
		key string,
		totalSize int64,
	) error

	// WriteChunkAt 把 data 写到临时文件的 [offset, offset+len(data)) 区间。
	// 必须先用 PrepareChunks 初始化；越界写入返回 ErrChunkOutOfRange。
	WriteChunkAt(
		ctx context.Context,
		key string,
		offset int64,
		data []byte,
	) error

	// FinalizeChunks 确认没有并发写入后，把临时文件重命名为正式对象。
	// 它可以安全重试：Rename 已经成功、但调用方后续步骤失败时，
	// 再次调用会直接返回成功。
	FinalizeChunks(
		ctx context.Context,
		key string,
		totalSize int64,
	) error
}

var (
	// Local 必须同时满足 Store 和 ChunkStore。
	_ ChunkStore = (*Local)(nil)

	// ErrChunkOutOfRange 表示分片写入超出了预分配的文件范围。
	ErrChunkOutOfRange = errors.New("chunk write out of range")
)

// PrepareChunks 创建或复用 .upload-<key>，首次初始化时设置文件长度。
//
// 文件长度被预先设置成 totalSize，不代表内容已经完整：中间可以全是
// 还没写过的空洞，完整性由业务层根据分片进度判断。
//
// 关键点：
//   - 只在文件大小还是 0（首次创建，或上次没来得及设置长度）时 Truncate；
//   - 已经初始化过的文件直接复用，不会把已有分片清空；
//   - 文件大小与 totalSize 不一致时拒绝继续，避免误以为是同一次上传。
func (l *Local) PrepareChunks(
	ctx context.Context,
	key string,
	totalSize int64,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if totalSize < 0 {
		return fmt.Errorf("prepare chunks: negative total size %d", totalSize)
	}

	l.chunkMu.Lock()
	defer l.chunkMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	tempPath := l.tempPath(key)
	file, err := os.OpenFile(tempPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("open chunk temp file: %w", err)
	}

	// 旧版本或外部创建的文件可能权限更宽，这里统一收紧。
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return fmt.Errorf("set chunk temp file permissions: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("stat chunk temp file: %w", err)
	}

	switch info.Size() {
	case totalSize:
		// 已经初始化过（或空文件）：保留现有内容，不再 Truncate。
	case 0:
		// 首次创建，或上次只创建了空文件就退出：现在设置长度。
		// Truncate 只补长度，不会清空已经存在的字节。
		if err := file.Truncate(totalSize); err != nil {
			_ = file.Close()
			return fmt.Errorf("preallocate chunk temp file: %w", err)
		}
	default:
		_ = file.Close()
		return fmt.Errorf(
			"chunk temp file %q has size %d, want %d; refusing to truncate existing chunks",
			tempPath, info.Size(), totalSize,
		)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("close chunk temp file: %w", err)
	}
	return nil
}

// WriteChunkAt 把 data 写到临时文件的指定偏移。
//
// 每次都重新打开、写入并关闭句柄，不在请求之间持有文件句柄：
// 这样进程重启后仍可继续写入，也不会因为客户端放弃上传而泄漏描述符。
// 写入范围必须在 PrepareChunks 预分配的长度内。
func (l *Local) WriteChunkAt(
	ctx context.Context,
	key string,
	offset int64,
	data []byte,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if offset < 0 {
		return fmt.Errorf("write chunk at %q: negative offset %d", key, offset)
	}

	// 读锁表示“共享进入”，不是说文件操作本身是只读：
	// 不同请求可以同时 WriteAt 不同偏移，Prepare/Finalize/Remove 仍然独占。
	l.chunkMu.RLock()
	defer l.chunkMu.RUnlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	file, err := os.OpenFile(l.tempPath(key), os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open chunk temp file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("stat chunk temp file: %w", err)
	}
	if offset > info.Size() || int64(len(data)) > info.Size()-offset {
		_ = file.Close()
		return fmt.Errorf(
			"%w: key=%q offset=%d length=%d file_size=%d",
			ErrChunkOutOfRange, key, offset, len(data), info.Size(),
		)
	}

	written, err := file.WriteAt(data, offset)
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("write chunk: %w", err)
	}
	if written != len(data) {
		_ = file.Close()
		return fmt.Errorf(
			"%w: wrote %d of %d bytes at offset %d",
			io.ErrShortWrite, written, len(data), offset,
		)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close chunk temp file: %w", err)
	}

	// 数据可能已经落到文件里，但请求已经取消：按失败返回，
	// 调用方会用同一偏移重试，覆盖同一段区间。
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// FinalizeChunks 把已经预分配好的临时文件发布成正式对象。
//
// 本实现不在两次调用之间保留文件句柄：WriteChunkAt 每次写完后都会关闭，
// chunkMu 又保证这里没有正在进行的写入，所以此时重命名是安全的。
//
// 重试规则：
//   - 正式对象已经存在且长度正确：说明上一次 Rename 已经成功，直接返回成功；
//     这覆盖“Rename 成功、但后续数据库更新失败”的重试；
//   - 临时对象存在且长度正确：执行 Rename；
//   - 两者都不存在：返回 fs.ErrNotExist。
//
// 注意：文件长度等于 totalSize 只说明预分配和写入范围合法，不代表每个
// 分片都真正写过；空洞不会被这里检查，完整性由业务层的分片进度保证。
func (l *Local) FinalizeChunks(
	ctx context.Context,
	key string,
	totalSize int64,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if totalSize < 0 {
		return fmt.Errorf("finalize chunks: negative total size %d", totalSize)
	}

	l.chunkMu.Lock()
	defer l.chunkMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	finalPath := l.Path(key)
	if info, err := os.Stat(finalPath); err == nil {
		if info.IsDir() || info.Size() != totalSize {
			return fmt.Errorf(
				"finalized chunk content %q has size %d, want %d",
				key, info.Size(), totalSize,
			)
		}
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat finalized chunk content: %w", err)
	}

	tempPath := l.tempPath(key)
	info, err := os.Stat(tempPath)
	if err != nil {
		return fmt.Errorf("stat chunk temp file: %w", err)
	}
	if info.Size() != totalSize {
		return fmt.Errorf(
			"chunk temp file %q has size %d, want %d",
			tempPath, info.Size(), totalSize,
		)
	}

	// 同一个目录内的 Rename 是原子的：要么还没有正式对象，要么已经是完整长度。
	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("publish chunk content: %w", err)
	}
	return nil
}
