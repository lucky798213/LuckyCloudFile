// Package storage 负责真正和磁盘打交道：把文件写进去、打开、删掉。
//
// 它不知道 HTTP，也不知道文件 ID 有什么含义，
// 只认一句话：“给我一个名字，我把它存到存储目录里。”
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// ErrTooLarge 表示写进来的内容超过了调用方允许的最大字节数。
// 上层可以用 errors.Is 判断它，然后决定返回 413 还是别的状态码。
var ErrTooLarge = errors.New("content too large")

// Local 表示“文件存在本机某个目录”，是 Store 的本地实现。
//
// 业务层只依赖 Store 接口；以后接 S3 时新增一个实现就行，不用改 internal/file。
type Local struct {
	root string
}

// Local 必须满足 Store 接口。接口签名一旦对不上，编译时就会失败，
// 而不是等到运行到某个上传请求才发现。
var _ Store = (*Local)(nil)

// NewLocal 创建一个本地存储，所有文件都放在 root 目录下。
func NewLocal(root string) *Local {
	return &Local{root: root}
}

// Save 把 r 里的内容写成 root/key 这个文件，最多接受 maxBytes 字节。
//
// 步骤是固定的三步：
//
//  1. 在同一个目录里创建临时文件
//  2. 把内容流式复制进去，然后关闭
//  3. 重命名为正式文件名
//
// 为什么要先写临时文件？因为如果直接写正式文件，别人下载时可能读到“写了一半”的内容。
// 重命名在同一个文件系统里是原子的，要么没有这个文件，要么内容是完整的。
//
// 复制过程中会按块检查 ctx，取消后不会继续发布文件。但底层 Reader 的一次 Read
// 仍可能阻塞在系统调用里：这里不能用 cancel 强行打断它，HTTP 层的读超时和
// 客户端断开仍然负责让阻塞的 Read 返回。
func (l *Local) Save(ctx context.Context, key string, r io.Reader, maxBytes int64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	// 临时文件必须和最终文件在同一个目录，这样 Rename 才是原子操作。
	// 名字带上 StorageKey，清理时即使进程被强杀，也能通过 Remove(key)
	// 找到这份“还没发布”的临时内容。
	//
	// 显式用 0600 创建：内容在重命名后沿用临时文件的权限，
	// 不能依赖进程 umask（常见的 022 会让 os.Create 得到 0644）。
	tmp, err := os.OpenFile(
		l.tempPath(key),
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0o600,
	)
	if err != nil {
		return 0, fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()

	// 正常退出时再兜底删一次；进程被强杀时不会执行，
	// 所以清理路径还必须自己处理临时文件（见 Remove）。
	defer os.Remove(tmpPath)

	// O_CREATE 不会修改已存在文件的权限：如果目录里残留了旧版本
	// 创建的 0644 临时文件，这里再收紧一次，避免发布后变成可被同组读取。
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return 0, fmt.Errorf("set temp file permissions: %w", err)
	}

	// 多读 1 个字节，是为了区分“刚好等于上限”和“超过上限”：
	//   - 读到 maxBytes 就结束 → 正好等于上限，放行
	//   - 还能读到第 maxBytes+1 个字节 → 超了，拒绝
	// io.Copy 会边读边写，不会把整个文件装进内存。
	src := io.LimitReader(&contextReader{ctx: ctx, r: r}, maxBytes+1)
	size, err := io.Copy(tmp, src)
	if err != nil {
		_ = tmp.Close()
		return 0, fmt.Errorf("write file: %w", err)
	}
	if size > maxBytes {
		_ = tmp.Close()
		return 0, fmt.Errorf("%w: limit is %d bytes", ErrTooLarge, maxBytes)
	}

	// 内容刚好读完后请求被取消：不能把“已经取消的上传”发布成正式文件。
	if err := ctx.Err(); err != nil {
		_ = tmp.Close()
		return 0, err
	}

	// Close 的错误必须检查：数据可能还在缓冲区，关闭时才真正刷到磁盘。
	if err := tmp.Close(); err != nil {
		return 0, fmt.Errorf("close file: %w", err)
	}

	// 发布前再看一次 ctx，避免关闭文件后才收到取消。
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	// 写完了才“发布”成正式文件。走到这里内容已经是完整的。
	if err := os.Rename(tmpPath, l.Path(key)); err != nil {
		return 0, fmt.Errorf("publish file: %w", err)
	}
	return size, nil
}

// contextReader 在每次 Read 前检查 ctx。
//
// 它只能保证“两次 Read 之间”能及时停止；一次已经进入内核的阻塞读
// 仍然要靠 HTTP 读超时或连接关闭来唤醒。
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

// Open 打开一个已经存好的文件，用来读取（下载会用到）。
// 用完必须由调用方 Close。
func (l *Local) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return os.Open(l.Path(key))
}

// Remove 删除一个 key 对应的正式内容和可能的临时内容。
//
// 只删正式对象不够：进程在 Save 中途被强杀时，会留下 .upload-<key>。
// 这里同时删两者，任何一个删除失败都会返回错误，让清理逻辑保留记录并重试。
// 两个都不存在时返回 fs.ErrNotExist，和原来“删除不存在的对象”的约定一致。
func (l *Local) Remove(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	finalErr := os.Remove(l.Path(key))
	if err := ctx.Err(); err != nil {
		return err
	}
	tempErr := os.Remove(l.tempPath(key))

	finalMissing := errors.Is(finalErr, fs.ErrNotExist)
	tempMissing := errors.Is(tempErr, fs.ErrNotExist)

	// 真实删除错误优先返回：此时不能假装清理已经完成。
	if finalErr != nil && !finalMissing {
		return fmt.Errorf("remove content %q: %w", key, finalErr)
	}
	if tempErr != nil && !tempMissing {
		return fmt.Errorf("remove temp content %q: %w", key, tempErr)
	}
	if finalMissing && tempMissing {
		return fs.ErrNotExist
	}
	return nil
}

// Path 返回某个 key 对应的磁盘路径。
//
// 它只属于本地实现，刻意不放进 Store 接口：S3 没有可供 os.Remove 使用的本地路径，
// 业务层也不应该知道内容具体放在哪块磁盘上。
//
// 只有数据库里存在的 key 才会走到这里，所以外部传进来的随机字符串
// 不可能用来拼出 ../ 之类的越界路径。
func (l *Local) Path(key string) string {
	return filepath.Join(l.root, key)
}

// tempPath 是 Save 过程中使用的临时文件名。
// 它必须和 Path(key) 在同一个目录，Rename 才是原子的。
func (l *Local) tempPath(key string) string {
	return l.Path(".upload-" + key)
}
