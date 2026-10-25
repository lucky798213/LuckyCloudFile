// Package storage 负责真正和磁盘打交道：把文件写进去、打开、删掉。
//
// 它不知道 HTTP，也不知道文件 ID 有什么含义，
// 只认一句话：“给我一个名字，我把它存到存储目录里。”
package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ErrTooLarge 表示写进来的内容超过了调用方允许的最大字节数。
// 上层可以用 errors.Is 判断它，然后决定返回 413 还是别的状态码。
var ErrTooLarge = errors.New("content too large")

// Local 表示“文件存在本机某个目录”。
//
// 现在就这一种存储方式，所以直接写具体类型，不抽接口。
// 等以后要接 S3 之类的对象存储时，再抽接口也不迟。
type Local struct {
	root string
}

// NewLocal 创建一个本地存储，所有文件都放在 root 目录下。
func NewLocal(root string) *Local {
	return &Local{root: root}
}

// Save 把 r 里的内容写成 root/id 这个文件，最多接受 maxBytes 字节。
//
// 步骤是固定的三步：
//
//  1. 在同一个目录里创建临时文件
//  2. 把内容流式复制进去，然后关闭
//  3. 重命名为正式文件名
//
// 为什么要先写临时文件？因为如果直接写正式文件，别人下载时可能读到“写了一半”的内容。
// 重命名在同一个文件系统里是原子的，要么没有这个文件，要么内容是完整的。
func (l *Local) Save(id string, r io.Reader, maxBytes int64) (int64, error) {
	// 临时文件必须和最终文件在同一个目录，这样 Rename 才是原子操作。
	tmp, err := os.CreateTemp(l.root, ".upload-*")
	if err != nil {
		return 0, fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()

	// 不管成功还是失败，最后都尝试删除临时文件。
	// 成功重命名后它已经不存在了，删除会失败，但无所谓，所以忽略错误。
	defer os.Remove(tmpPath)

	// 多读 1 个字节，是为了区分“刚好等于上限”和“超过上限”：
	//   - 读到 maxBytes 就结束 → 正好等于上限，放行
	//   - 还能读到第 maxBytes+1 个字节 → 超了，拒绝
	// io.Copy 会边读边写，不会把整个文件装进内存。
	size, err := io.Copy(tmp, io.LimitReader(r, maxBytes+1))
	if err != nil {
		_ = tmp.Close()
		return 0, fmt.Errorf("write file: %w", err)
	}
	if size > maxBytes {
		_ = tmp.Close()
		return 0, fmt.Errorf("%w: limit is %d bytes", ErrTooLarge, maxBytes)
	}

	// Close 的错误必须检查：数据可能还在缓冲区，关闭时才真正刷到磁盘。
	if err := tmp.Close(); err != nil {
		return 0, fmt.Errorf("close file: %w", err)
	}

	// 写完了才“发布”成正式文件。走到这里内容已经是完整的。
	if err := os.Rename(tmpPath, l.Path(id)); err != nil {
		return 0, fmt.Errorf("publish file: %w", err)
	}
	return size, nil
}

// Open 打开一个已经存好的文件，用来读取（下载会用到）。
// 用完必须由调用方 Close。
func (l *Local) Open(id string) (*os.File, error) {
	return os.Open(l.Path(id))
}

// Remove 删除一个文件。文件不存在时返回的错误可以用 os.IsNotExist 判断。
func (l *Local) Remove(id string) error {
	return os.Remove(l.Path(id))
}

// Path 返回某个 ID 对应的磁盘路径。
//
// 只有内存索引里存在的 ID 才会走到这里，所以外部传进来的随机字符串
// 不可能用来拼出 ../ 之类的越界路径。
func (l *Local) Path(id string) string {
	return filepath.Join(l.root, id)
}
