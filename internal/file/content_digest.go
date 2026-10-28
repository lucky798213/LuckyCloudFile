package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"

	"luckyCloudFile/internal/storage"
)

// ErrInvalidContentSHA256 表示客户端提交的内容摘要不是 64 位十六进制。
var ErrInvalidContentSHA256 = errors.New("invalid content sha256")

// emptyContentSHA256 是空内容的 SHA-256。
// 空文件不产生读取成本，但同样要有一条可信摘要参与去重。
const emptyContentSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// sumSHA256 把 hasher 的当前状态编码成小写十六进制摘要。
func sumSHA256(hasher hash.Hash) string {
	return hex.EncodeToString(hasher.Sum(nil))
}

// normalizeContentSHA256 校验并规范化客户端提交的摘要。
//
// 只接受 64 个十六进制字符；大写会转成小写。这个值只用于查找内容映射，
// 不能直接当成 Entity 的可信摘要写入数据库。
func normalizeContentSHA256(value string) (string, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return "", ErrInvalidContentSHA256
	}
	return hex.EncodeToString(decoded), nil
}

// hashStoredContent 在内容已经完整写入存储后，按字节重新计算摘要。
//
// 分片上传不能把各分片摘要拼起来当整文件摘要，所以首版在合并完成后
// 用 Store.Open 多读一遍对象。读取长度必须正好等于声明大小，否则不能
// 把这份内容登记成可复用的映射。
func hashStoredContent(
	ctx context.Context,
	store storage.Store,
	storageKey string,
	expectedSize int64,
) (string, error) {
	if expectedSize == 0 {
		return emptyContentSHA256, nil
	}

	reader, err := store.Open(ctx, storageKey)
	if err != nil {
		return "", fmt.Errorf("open content for digest: %w", err)
	}
	defer reader.Close()

	hasher := sha256.New()
	read, err := io.Copy(hasher, io.LimitReader(reader, expectedSize+1))
	if err != nil {
		return "", fmt.Errorf("read content for digest: %w", err)
	}
	if read != expectedSize {
		return "", fmt.Errorf(
			"content size for digest is %d, want %d", read, expectedSize,
		)
	}
	return sumSHA256(hasher), nil
}
