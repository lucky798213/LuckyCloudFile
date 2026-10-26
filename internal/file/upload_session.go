package file

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/idgen"
	"luckyCloudFile/internal/model"
)

// UploadSessionTTL 是上传会话的固定有效期。
//
// 这里先统一写死，后续需要配置化时再从 config 读取。
const UploadSessionTTL = time.Hour

var (
	// ErrInvalidSize 表示客户端声明的大小不合法（当前只允许 >= 0）。
	ErrInvalidSize = errors.New("invalid upload size")

	// ErrUploadSessionNotFound 表示会话不存在，或者不属于当前用户。
	// 两种情况返回同一个错误，避免客户端探测别人的会话 ID。
	ErrUploadSessionNotFound = errors.New("upload session not found")
)

// UploadSessionInfo 是返回给客户端的上传会话信息。
//
// 它不包含 EntityID、StorageKey、StoragePolicyID 等内部存储字段：
// 客户端只需要知道“传什么、传到哪、已经收了多少、现在是什么状态”。
type UploadSessionInfo struct {
	ID           string    `json:"id"`
	ParentID     string    `json:"parent_id"`
	Name         string    `json:"name"`
	ExpectedSize int64     `json:"expected_size"`
	ActualSize   int64     `json:"actual_size"`
	Status       string    `json:"status"`
	ExpiresAt    time.Time `json:"expires_at"`
	ResultFileID *string   `json:"result_file_id"`
}

// CreateUploadSession 创建一个只有占位实体的上传会话。
//
// 本阶段不接收文件内容，也不创建 File 或磁盘文件：
// 会话和占位 Entity 只是为后续传输建立“上传单”。
//
// 顺序：
//  1. 校验名字和大小；
//  2. 校验目标目录（为空时解析成当前用户根目录）；
//  3. 读取一次用户默认存储策略，确认驱动已注册；
//  4. 生成会话 ID 和占位内容的 storage key；
//  5. 在一个事务里重新检查目录，写入占位 Entity 和 UploadSession。
//
// 两条记录必须一起成功：会话插入失败时，占位 Entity 会随事务回滚。
func (s *Service) CreateUploadSession(
	ctx context.Context,
	ownerID int,
	parentID, name string,
	size int64,
) (UploadSessionInfo, error) {
	if err := validateName(name); err != nil {
		return UploadSessionInfo{}, err
	}
	if size < 0 {
		return UploadSessionInfo{}, ErrInvalidSize
	}
	if size > MaxUploadBytes {
		return UploadSessionInfo{}, ErrTooLarge
	}

	// 先做一次目录校验，快速拒绝明显不合法的请求；事务里还会再查一次。
	parent, err := s.requireDirectory(ctx, ownerID, parentID)
	if err != nil {
		return UploadSessionInfo{}, err
	}

	// 本次上传只选一次策略。占位实体记住这个策略 ID，
	// 后续传输内容时直接使用它，不再跟随用户默认策略变化。
	policyID, _, err := s.uploadStore(ctx, ownerID)
	if err != nil {
		return UploadSessionInfo{}, err
	}

	sessionID, err := idgen.New()
	if err != nil {
		return UploadSessionInfo{}, err
	}
	storageKey, err := idgen.New()
	if err != nil {
		return UploadSessionInfo{}, err
	}

	session := model.UploadSession{
		ID:           sessionID,
		OwnerID:      ownerID,
		ParentID:     parent.ID,
		Name:         name,
		ExpectedSize: size,
		ActualSize:   0,
		Status:       model.UploadStatusPending,
		ExpiresAt:    time.Now().Add(UploadSessionTTL),
	}

	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 接收期间目录可能被删除或换到别人名下，事务内必须重新核对。
		if _, err := queryDirectory(ctx, tx, ownerID, parent.ID); err != nil {
			return err
		}

		entity := model.Entity{
			StorageKey:      storageKey,
			Size:            0,
			StoragePolicyID: policyID,
			RefCount:        0,
			Status:          model.EntityStatusUploading,
		}
		if err := tx.Create(&entity).Error; err != nil {
			return fmt.Errorf("create placeholder entity: %w", err)
		}

		session.EntityID = entity.ID
		if err := tx.Create(&session).Error; err != nil {
			return fmt.Errorf("create upload session: %w", err)
		}
		return nil
	})
	if err != nil {
		return UploadSessionInfo{}, err
	}

	return toUploadSessionInfo(session), nil
}

// GetUploadSession 返回 ownerID 用户自己的上传会话。
//
// 查询条件必须同时带 owner_id：只按 ID 查会把别人的会话信息返回给前端。
// 不存在和属于别人都返回 ErrUploadSessionNotFound，HTTP 层统一映射成 404。
//
// 本方法只读：不自动修改过期状态，也不清理占位实体。
func (s *Service) GetUploadSession(
	ctx context.Context,
	ownerID int,
	sessionID string,
) (UploadSessionInfo, error) {
	var session model.UploadSession
	err := s.db.WithContext(ctx).
		Where("id = ? AND owner_id = ?", sessionID, ownerID).
		First(&session).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return UploadSessionInfo{}, ErrUploadSessionNotFound
		}
		return UploadSessionInfo{}, fmt.Errorf("query upload session: %w", err)
	}
	return toUploadSessionInfo(session), nil
}

// toUploadSessionInfo 只挑客户端需要的字段，内部存储信息不对外返回。
func toUploadSessionInfo(session model.UploadSession) UploadSessionInfo {
	return UploadSessionInfo{
		ID:           session.ID,
		ParentID:     session.ParentID,
		Name:         session.Name,
		ExpectedSize: session.ExpectedSize,
		ActualSize:   session.ActualSize,
		Status:       session.Status,
		ExpiresAt:    session.ExpiresAt,
		ResultFileID: session.ResultFileID,
	}
}
