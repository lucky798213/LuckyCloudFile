// Package idgen 生成对外可见的随机 ID。
//
// File 的公开 ID 和磁盘上的 storage key 都用它：32 个十六进制字符（16 字节随机数）。
// 用随机数而不是自增数字，别人就没法靠猜 ID 访问到不属于他的文件。
package idgen

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// New 返回一个新的随机 ID。
func New() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
