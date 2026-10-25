// Package file 是文件的业务层：生成 ID、操作数据库记录、协调磁盘存储。
//
// 它不碰 HTTP（那是 internal/httpapi），也不碰 os.Open/os.Rename（那是 internal/storage）。
// 它只决定“这件事该按什么顺序做、失败了怎么收拾”。
package file

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"

	"luckyCloudFile/internal/ent"
	entfile "luckyCloudFile/internal/ent/file"
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

// File 是返回给 HTTP 层的文件信息（不包含数据库内部字段）。
//
// 它和 ent 生成的 ent.File 不是同一个类型：
// ent.File 是数据库记录，下面这个只是“客户端需要看到的三个字段”。
type File struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// Service 负责文件业务：落盘 + 写数据库。
type Service struct {
	client *ent.Client
	store  *storage.Local
}

// NewService 创建文件服务。
func NewService(client *ent.Client, store *storage.Local) *Service {
	return &Service{client: client, store: store}
}

// Upload 把 r 的内容存成文件，并在数据库里登记 File 和 Entity。
//
// 顺序是：
//  1. 生成两个随机 ID：File 的公开 ID、Entity 的磁盘 key
//  2. 先把内容写进磁盘（临时文件 → Rename）
//  3. 再在一个事务里写 Entity 和 File 两行
//
// 如果第 3 步失败，会把第 2 步写好的磁盘文件删掉，避免“磁盘有、库里没有”的孤儿文件。
func (s *Service) Upload(ctx context.Context, name string, r io.Reader) (File, error) {
	fileID, err := newID()
	if err != nil {
		return File{}, err
	}
	storageKey, err := newID()
	if err != nil {
		return File{}, err
	}

	// 先落盘。磁盘写失败时数据库还没动过，直接返回即可。
	size, err := s.store.Save(storageKey, r, MaxUploadBytes)
	if err != nil {
		if errors.Is(err, storage.ErrTooLarge) {
			return File{}, ErrTooLarge
		}
		return File{}, err
	}

	// 再写数据库。失败就回滚磁盘，保证两边一致。
	if err := s.saveRecords(ctx, fileID, storageKey, name, size); err != nil {
		_ = s.store.Remove(storageKey)
		return File{}, err
	}

	return File{ID: fileID, Name: name, Size: size}, nil
}

// saveRecords 在一个事务里写入 Entity 和 File。
//
// 两个表必须一起成功：只有 File 没有 Entity，就不知道内容在哪；
// 只有 Entity 没有 File，就是一份没人引用的孤儿内容。
func (s *Service) saveRecords(ctx context.Context, fileID, storageKey, name string, size int64) error {
	tx, err := s.client.Tx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	entity, err := tx.Entity.Create().
		SetStorageKey(storageKey).
		SetSize(size).
		SetPolicy("local").
		SetRefCount(1).
		Save(ctx)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("create entity: %w", err)
	}

	if _, err := tx.File.Create().
		SetID(fileID).
		SetName(name).
		SetEntity(entity).
		Save(ctx); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("create file: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// Open 按 ID 查数据库，然后打开对应的磁盘文件。
//
// 这里不会拿客户端传来的字符串去拼路径：
// 只有数据库里真实存在的 ID 才会走到 Entity.StorageKey。
func (s *Service) Open(ctx context.Context, id string) (File, io.ReadCloser, error) {
	record, err := s.client.File.Query().
		Where(entfile.ID(id)).
		WithEntity().
		Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return File{}, nil, ErrNotFound
		}
		return File{}, nil, fmt.Errorf("query file: %w", err)
	}

	entity := record.Edges.Entity
	if entity == nil {
		return File{}, nil, fmt.Errorf("file %s has no entity", id)
	}

	f, err := s.store.Open(entity.StorageKey)
	if err != nil {
		// 数据库有记录但磁盘文件没了：对外也当作“不存在”。
		if errors.Is(err, fs.ErrNotExist) {
			return File{}, nil, ErrNotFound
		}
		return File{}, nil, fmt.Errorf("open content: %w", err)
	}

	return File{ID: record.ID, Name: record.Name, Size: entity.Size}, f, nil
}

// Delete 删除文件：先删磁盘内容，再在事务里删数据库记录。
//
// 为什么先删磁盘？如果磁盘删除失败，数据库不动，文件还能下载、也能重试删除。
// 反过来先删数据库，磁盘失败就会留下永远没人认识的孤儿文件。
//
// 如果磁盘删完、事务却失败了：数据库里还有记录，但磁盘文件已经没了。
// 再调一次 Delete 时，磁盘不存在会被当成“已经删了”，然后继续把数据库记录删掉，
// 所以重试是安全的。
func (s *Service) Delete(ctx context.Context, id string) error {
	record, err := s.client.File.Query().
		Where(entfile.ID(id)).
		WithEntity().
		Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return ErrNotFound
		}
		return fmt.Errorf("query file: %w", err)
	}

	entity := record.Edges.Entity
	if entity == nil {
		return fmt.Errorf("file %s has no entity", id)
	}

	if err := s.store.Remove(entity.StorageKey); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove content: %w", err)
	}

	tx, err := s.client.Tx(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	if err := tx.File.DeleteOneID(record.ID).Exec(ctx); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("delete file: %w", err)
	}

	// 引用数减到 0 才删 Entity，这样以后多个 File 共享一份内容时不会被误删。
	if entity.RefCount <= 1 {
		if err := tx.Entity.DeleteOneID(entity.ID).Exec(ctx); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("delete entity: %w", err)
		}
	} else {
		if _, err := tx.Entity.UpdateOneID(entity.ID).
			SetRefCount(entity.RefCount - 1).
			Save(ctx); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("update ref count: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
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
