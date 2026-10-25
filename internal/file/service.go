// Package file 是文件的业务层：生成 ID、记录元数据、协调存储。
//
// 它不碰 HTTP（那是 internal/httpapi），也不碰 os.Open/os.Rename（那是 internal/storage）。
// 它只决定“这件事该按什么顺序做”。
package file

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"

	"luckyCloudFile/internal/storage"
)

// MaxUploadBytes 是单个文件内容的大小上限：32 MiB。
// 注意它限制的是“文件内容”，不包含 multipart 的表单头和边界。
const MaxUploadBytes int64 = 32 << 20

var (
	// ErrNotFound 表示这个 ID 没有对应的文件。
	ErrNotFound = errors.New("file not found")
	// ErrTooLarge 表示上传的文件内容超过了 MaxUploadBytes。
	ErrTooLarge = errors.New("file too large")
)

// File 是一条文件记录。
//
// ID 由服务端生成，用来命名磁盘文件和之后查询；
// Name 只是原始文件名，用来给用户看，绝不参与拼接磁盘路径。
// 这样同名文件不会互相覆盖，也不用担心文件名里带 ../ 之类的路径穿越。
type File struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// Service 管理所有文件记录，并把文件内容交给 storage 落盘。
type Service struct {
	store *storage.Local

	// HTTP 请求是并发进来的，map 本身不是并发安全的，所以用互斥锁保护。
	mu    sync.Mutex
	files map[string]File
}

// NewService 创建文件服务。
func NewService(store *storage.Local) *Service {
	return &Service{
		store: store,
		files: make(map[string]File),
	}
}

// Upload 把 r 的内容存成一个文件，登记元数据，然后返回这条记录。
//
// 顺序很重要：先真正写成功，再登记。
// 如果写磁盘失败就直接返回错误，不留下“索引里有、磁盘上却没有”的坏记录。
func (s *Service) Upload(name string, r io.Reader) (File, error) {
	// 1. 生成服务端 ID
	id, err := newID()
	if err != nil {
		return File{}, err
	}

	// 2. 落盘（storage 内部会先写临时文件，写完再重命名）
	size, err := s.store.Save(id, r, MaxUploadBytes)
	if err != nil {
		// 把存储层的“太大了”翻译成业务层的错误，HTTP 层只需要认识 file.ErrTooLarge。
		if errors.Is(err, storage.ErrTooLarge) {
			return File{}, ErrTooLarge
		}
		return File{}, err
	}

	// 3. 写成功了，才登记到内存里
	record := File{ID: id, Name: name, Size: size}
	s.mu.Lock()
	s.files[id] = record
	s.mu.Unlock()

	return record, nil
}

// Get 按 ID 查一条记录。
func (s *Service) Get(id string) (File, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.files[id]
	return record, ok
}

// Open 按 ID 打开文件，同时返回元数据。
// 下载接口用它：先确认 ID 存在，再去磁盘打开，避免拿任意字符串去拼路径。
func (s *Service) Open(id string) (File, *os.File, error) {
	s.mu.Lock()
	record, ok := s.files[id]
	s.mu.Unlock()
	if !ok {
		return File{}, nil, ErrNotFound
	}

	f, err := s.store.Open(id)
	if err != nil {
		// 索引里有、磁盘上却没有：对外都当成“文件不存在”。
		if errors.Is(err, fs.ErrNotExist) {
			return File{}, nil, ErrNotFound
		}
		return File{}, nil, err
	}
	return record, f, nil
}

// Delete 删除文件：先删磁盘文件，再从内存索引里移除。
//
// 磁盘删除失败时保留索引并返回错误，这样调用方知道“没删干净”；
// 如果磁盘文件本来就不在了，就当成已删除，继续清索引。
func (s *Service) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.files[id]; !ok {
		return ErrNotFound
	}

	if err := s.store.Remove(id); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	delete(s.files, id)
	return nil
}

// newID 生成 32 个字符的随机十六进制 ID（16 字节随机数）。
//
// 用随机数而不是 1、2、3 这样的自增数字，
// 别人就没法靠猜 ID 访问到不属于他的文件。
func newID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate file id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
