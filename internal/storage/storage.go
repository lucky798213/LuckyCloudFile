package storage

import (
	"context"
	"io"
)

// Store 操作物理内容：按 key 写入、读取、删除字节。
//
// 它不知道 HTTP，也不知道用户、目录和数据库记录：
// 权限检查、事务顺序、引用计数都在 internal/file，
// 业务层只依赖这个接口，不关心内容最终放在磁盘还是对象存储。
type Store interface {
	// Save 成功表示完整内容已写入，并返回实际字节数。
	// 内容超过 maxBytes 时返回 ErrTooLarge。
	Save(
		ctx context.Context,
		key string,
		r io.Reader,
		maxBytes int64,
	) (int64, error)

	// Open 返回可读取的内容，调用方负责 Close。
	Open(ctx context.Context, key string) (io.ReadCloser, error)

	// Remove 删除物理内容。
	Remove(ctx context.Context, key string) error
}
