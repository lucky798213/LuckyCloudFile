package upload

import (
	"errors"
	"strconv"
	"testing"
)

func TestChunkCount(t *testing.T) {
	const chunkSize = int64(1 << 20) // 1 MiB

	tests := []struct {
		name      string
		size      int64
		chunkSize int64
		want      int
		wantErr   bool
	}{
		{name: "empty file", size: 0, chunkSize: chunkSize, want: 0},
		{name: "one byte", size: 1, chunkSize: chunkSize, want: 1},
		{name: "less than one chunk", size: chunkSize - 1, chunkSize: chunkSize, want: 1},
		{name: "exact one chunk", size: chunkSize, chunkSize: chunkSize, want: 1},
		{name: "exact two chunks", size: 2 * chunkSize, chunkSize: chunkSize, want: 2},
		{name: "last chunk partial", size: 2*chunkSize + chunkSize/2, chunkSize: chunkSize, want: 3},
		{name: "negative size", size: -1, chunkSize: chunkSize, wantErr: true},
		{name: "zero chunk size", size: 10, chunkSize: 0, wantErr: true},
		{name: "negative chunk size", size: 10, chunkSize: -1, wantErr: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := ChunkCount(test.size, test.chunkSize)
			if test.wantErr {
				if !errors.Is(err, ErrInvalidGeometry) {
					t.Fatalf("ChunkCount(%d, %d) error = %v, want ErrInvalidGeometry",
						test.size, test.chunkSize, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ChunkCount(%d, %d): %v", test.size, test.chunkSize, err)
			}
			if got != test.want {
				t.Fatalf("ChunkCount(%d, %d) = %d, want %d",
					test.size, test.chunkSize, got, test.want)
			}
		})
	}
}

// TestChunkAtExamples 覆盖需求里的 2.5 MiB 例子：
//
//	编号 0 → 偏移 0      长度 1 MiB
//	编号 1 → 偏移 1 MiB  长度 1 MiB
//	编号 2 → 偏移 2 MiB  长度 0.5 MiB
func TestChunkAtExamples(t *testing.T) {
	const (
		chunkSize = int64(1 << 20)
		size      = 2*chunkSize + chunkSize/2
	)

	tests := []struct {
		index  int
		offset int64
		length int64
	}{
		{index: 0, offset: 0, length: chunkSize},
		{index: 1, offset: chunkSize, length: chunkSize},
		{index: 2, offset: 2 * chunkSize, length: chunkSize / 2},
	}

	for _, test := range tests {
		test := test
		t.Run(strconv.Itoa(test.index), func(t *testing.T) {
			offset, length, err := ChunkAt(size, chunkSize, test.index)
			if err != nil {
				t.Fatalf("ChunkAt(index=%d): %v", test.index, err)
			}
			if offset != test.offset || length != test.length {
				t.Fatalf("ChunkAt(index=%d) = offset %d length %d, want %d/%d",
					test.index, offset, length, test.offset, test.length)
			}
		})
	}
}

// TestChunkAtExactDivision 验证整除时最后一片仍然是完整分片。
func TestChunkAtExactDivision(t *testing.T) {
	const (
		chunkSize = int64(4)
		size      = 8
	)

	offset, length, err := ChunkAt(size, chunkSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 4 || length != 4 {
		t.Fatalf("last exact chunk = offset %d length %d, want 4/4", offset, length)
	}
}

// TestChunkAtRejectsBadIndex 覆盖负编号、越界编号和空文件的任何编号。
func TestChunkAtRejectsBadIndex(t *testing.T) {
	const (
		chunkSize = int64(1 << 20)
		size      = 2*chunkSize + 1
	)

	for _, index := range []int{-1, 3, 100} {
		index := index
		t.Run(strconv.Itoa(index), func(t *testing.T) {
			if _, _, err := ChunkAt(size, chunkSize, index); !errors.Is(err, ErrChunkIndexOutOfRange) {
				t.Fatalf("ChunkAt(index=%d) error = %v, want ErrChunkIndexOutOfRange",
					index, err)
			}
		})
	}

	// 空文件没有分片：编号 0 也必须拒绝。
	if _, _, err := ChunkAt(0, chunkSize, 0); !errors.Is(err, ErrChunkIndexOutOfRange) {
		t.Fatalf("empty file ChunkAt(index=0) error = %v, want ErrChunkIndexOutOfRange", err)
	}
}

// TestChunkAtRejectsInvalidGeometry 验证非法大小在 ChunkAt 里也会被拒绝。
func TestChunkAtRejectsInvalidGeometry(t *testing.T) {
	for _, test := range []struct {
		size      int64
		chunkSize int64
	}{
		{size: -1, chunkSize: 1 << 20},
		{size: 1, chunkSize: 0},
		{size: 1, chunkSize: -1},
	} {
		if _, _, err := ChunkAt(test.size, test.chunkSize, 0); !errors.Is(err, ErrInvalidGeometry) {
			t.Fatalf("ChunkAt(%d, %d, 0) error = %v, want ErrInvalidGeometry",
				test.size, test.chunkSize, err)
		}
	}
}
