// 这个文件只做分片几何计算：给定文件大小、分片大小和分片编号，
// 算出总片数、这一片的偏移和必须收到的长度。
//
// 它不查数据库、不碰磁盘、不依赖上传会话状态，方便单独测试。
package upload

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidGeometry 表示文件大小或分片大小不合法：
	// size < 0 或 chunkSize <= 0。
	ErrInvalidGeometry = errors.New("invalid upload geometry")

	// ErrChunkIndexOutOfRange 表示分片编号不在 [0, chunkCount) 内。
	// 空文件没有分片，任何编号（包括 0）都会返回这个错误。
	ErrChunkIndexOutOfRange = errors.New("upload chunk index out of range")
)

// ChunkCount 返回从 0 开始编号时的总片数。
//
//	chunkCount = size / chunkSize
//	如果 size % chunkSize != 0，再加 1
//
// 空文件（size=0）返回 0：没有分片需要上传，可以直接走完成接口。
// size < 0 或 chunkSize <= 0 返回 ErrInvalidGeometry。
//
// 文件大小上限（当前 32 MiB）仍由业务层在创建会话时检查；这里只保证分片
// 公式本身正确，不把存储策略写进纯计算函数。
func ChunkCount(size, chunkSize int64) (int, error) {
	if size < 0 || chunkSize <= 0 {
		return 0, fmt.Errorf(
			"%w: size=%d chunk_size=%d", ErrInvalidGeometry, size, chunkSize,
		)
	}
	if size == 0 {
		return 0, nil
	}
	// 和 (size+chunkSize-1)/chunkSize 等价，但不会在 size 接近上限时溢出。
	return int((size-1)/chunkSize + 1), nil
}

// ChunkAt 返回第 index 片在文件里的起始偏移和必须收到的长度。
//
//	offset     = index * chunkSize
//	partLength = min(chunkSize, size-offset)
//
// 编号从 0 开始。size/chunkSize 不合法时返回 ErrInvalidGeometry；
// index < 0 或 index >= chunkCount 时返回 ErrChunkIndexOutOfRange。
func ChunkAt(size, chunkSize int64, index int) (offset, partLength int64, err error) {
	count, err := ChunkCount(size, chunkSize)
	if err != nil {
		return 0, 0, err
	}
	if index < 0 || index >= count {
		return 0, 0, fmt.Errorf(
			"%w: index=%d chunk_count=%d", ErrChunkIndexOutOfRange, index, count,
		)
	}

	offset = int64(index) * chunkSize
	partLength = chunkSize
	if remaining := size - offset; remaining < partLength {
		partLength = remaining
	}
	return offset, partLength, nil
}
