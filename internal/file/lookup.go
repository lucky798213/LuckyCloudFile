package file

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
)

// OwnedFile 按 ID 和所有者读取一个节点的基本信息，不打开内容。
//
// 它只用于只依赖元数据的操作（例如提交内容校验任务）；下载、复制和
// 发布仍然走各自的加锁流程。目录也会返回，调用方需要自己按 Kind 拒绝。
func (s *Service) OwnedFile(ctx context.Context, ownerID int, id string) (File, error) {
	var record model.File
	err := s.db.WithContext(ctx).
		Where("id = ? AND owner_id = ?", id, ownerID).
		First(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return File{}, ErrNotFound
	}
	if err != nil {
		return File{}, fmt.Errorf("query file %q: %w", id, err)
	}

	return File{
		ID:       record.ID,
		Name:     record.Name,
		Kind:     record.Kind,
		ParentID: record.ParentID,
	}, nil
}
