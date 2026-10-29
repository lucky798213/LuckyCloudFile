package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"luckyCloudFile/internal/idgen"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/pathlock"
	"luckyCloudFile/internal/quota"
	"luckyCloudFile/internal/storage"
)

var (
	// ErrInvalidIdempotencyKey 表示秒传请求缺少幂等键，或者键太长。
	ErrInvalidIdempotencyKey = errors.New("idempotency key is required")

	// ErrIdempotencyConflict 表示同一个幂等键被用于不同参数的请求。
	// 这是客户端错误，不能覆盖上一次的结果。
	ErrIdempotencyConflict = errors.New("idempotency key was used with different parameters")
)

// InstantUploadResult 是秒传接口的响应。
//
// 未命中时只返回 {"instant":false}，客户端继续走原来的上传会话；
// 命中时返回新 File 的 ID，不搬运任何文件内容。
type InstantUploadResult struct {
	Instant bool   `json:"instant"`
	FileID  string `json:"file_id,omitempty"`
}

// InstantUpload 在已有内容映射时直接创建一个新的逻辑文件。
//
// 它不创建上传会话，也不接收文件内容。命中条件严格限制在
// “当前用户 + 当前存储策略 + 摘要 + 大小”，第一版不跨用户或跨策略合并。
//
// 幂等键在事务内、用户行锁之后检查：同一键的并发请求会串行化，
// 后到的请求看到已经提交的记录并返回原结果，不会重复创建文件或重复计费。
// 同键不同参数返回 ErrIdempotencyConflict。
//
// 秒传没有提前预留容量，所以使用和复制、旧上传一致的 quota.Consume：
// 只增加 Entity 引用而不记逻辑容量会绕过配额。
func (s *Service) InstantUpload(
	ctx context.Context,
	ownerID int,
	parentID, name string,
	size int64,
	clientSHA256, idempotencyKey string,
) (InstantUploadResult, error) {
	if err := validateName(name); err != nil {
		return InstantUploadResult{}, err
	}
	if size < 0 {
		return InstantUploadResult{}, ErrInvalidSize
	}
	limit := MaxUploadBytes
	if _, st, err := s.uploadStore(ctx, ownerID); err == nil {
		if _, ok := st.(storage.MultipartStore); ok {
			limit = s.directMax
		}
	}
	if size > limit {
		return InstantUploadResult{}, ErrTooLarge
	}
	digest, err := normalizeContentSHA256(clientSHA256)
	if err != nil {
		return InstantUploadResult{}, err
	}
	if idempotencyKey == "" || len(idempotencyKey) > 255 {
		return InstantUploadResult{}, ErrInvalidIdempotencyKey
	}

	requestDigest := instantRequestDigest(ownerID, parentID, name, size, digest)
	var result InstantUploadResult

	err = s.runLockedOperation(ctx,
		func() ([]pathLockRequest, error) {
			target, err := s.resolveTargetPath(ctx, ownerID, parentID, name)
			if err != nil {
				return nil, err
			}
			return []pathLockRequest{{Snapshot: target, Scope: pathlock.Node}}, nil
		},
		func(tx *gorm.DB) error {
			// 同一用户的发布、删除和幂等登记都先锁 users 行，
			// 这样同键并发请求不会同时插入幂等记录。
			if _, err := quota.LockUser(tx, ownerID); err != nil {
				return err
			}

			// 幂等命中要优先返回，即使目录里的其他状态已经变化，
			// 也不能让同一个逻辑操作变成第二次创建。
			var request model.InstantUploadRequest
			err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("owner_id = ? AND idempotency_key = ?", ownerID, idempotencyKey).
				First(&request).Error
			if err == nil {
				if request.RequestDigest != requestDigest {
					return ErrIdempotencyConflict
				}
				if request.ResultFileID != nil && *request.ResultFileID != "" {
					result = InstantUploadResult{Instant: true, FileID: *request.ResultFileID}
					return nil
				}
				result = InstantUploadResult{}
				return nil
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("query instant upload request: %w", err)
			}

			var user model.User
			if err := tx.Select("id", "default_storage_policy_id").
				First(&user, ownerID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return fmt.Errorf("user %d not found", ownerID)
				}
				return fmt.Errorf("query user storage policy: %w", err)
			}
			if user.DefaultStoragePolicyID <= 0 {
				return fmt.Errorf("user %d has no default storage policy", ownerID)
			}
			// 映射命中后客户端会直接下载，策略必须仍然可解析。
			if _, err := s.stores.Get(user.DefaultStoragePolicyID); err != nil {
				return fmt.Errorf(
					"resolve storage policy %d: %w", user.DefaultStoragePolicyID, err,
				)
			}

			// 目录和名字只在“第一次执行这个幂等键”时校验。
			// parent_id 为空时这里会解析成当前用户的根目录，写入 File 的
			// 必须是真实目录 ID，不能保留空字符串。
			parent, err := queryDirectory(ctx, tx, ownerID, parentID)
			if err != nil {
				return err
			}

			entity, err := lockReusableContent(
				tx, ownerID, user.DefaultStoragePolicyID, digest, size,
			)
			if err != nil {
				return err
			}
			if entity == nil {
				return s.recordInstantUploadMiss(tx, ownerID, idempotencyKey, requestDigest, &result)
			}

			// 命中：逻辑文件仍然要占一份容量。
			if err := quota.Consume(tx, ownerID, size); err != nil {
				return err
			}

			fileID, err := idgen.New()
			if err != nil {
				return err
			}
			record := model.File{
				ID:       fileID,
				Name:     name,
				Kind:     model.KindFile,
				ParentID: &parent.ID,
				EntityID: &entity.ID,
				OwnerID:  ownerID,
			}
			if err := tx.Create(&record).Error; err != nil {
				if errors.Is(err, gorm.ErrDuplicatedKey) {
					return ErrAlreadyExists
				}
				return fmt.Errorf("create instant file: %w", err)
			}

			incremented := tx.Model(&model.Entity{}).
				Where("id = ? AND status = ? AND ref_count > 0",
					entity.ID, model.EntityStatusActive).
				UpdateColumn("ref_count", gorm.Expr("ref_count + 1"))
			if incremented.Error != nil {
				return fmt.Errorf("increment instant ref count: %w", incremented.Error)
			}
			if incremented.RowsAffected != 1 {
				return fmt.Errorf(
					"%w: entity %d can no longer be referenced",
					ErrUploadSessionConflict, entity.ID,
				)
			}

			created := model.InstantUploadRequest{
				OwnerID:        ownerID,
				IdempotencyKey: idempotencyKey,
				RequestDigest:  requestDigest,
				ResultFileID:   &fileID,
			}
			if err := tx.Create(&created).Error; err != nil {
				return fmt.Errorf("record instant upload result: %w", err)
			}
			result = InstantUploadResult{Instant: true, FileID: fileID}
			return nil
		})
	if err != nil {
		return InstantUploadResult{}, err
	}
	return result, nil
}

// recordInstantUploadMiss 记录“这次请求没有命中”，让同键重试得到相同结果。
func (s *Service) recordInstantUploadMiss(
	tx *gorm.DB,
	ownerID int,
	idempotencyKey, requestDigest string,
	result *InstantUploadResult,
) error {
	request := model.InstantUploadRequest{
		OwnerID:        ownerID,
		IdempotencyKey: idempotencyKey,
		RequestDigest:  requestDigest,
	}
	if err := tx.Create(&request).Error; err != nil {
		return fmt.Errorf("record instant upload miss: %w", err)
	}
	*result = InstantUploadResult{}
	return nil
}

// instantRequestDigest 把幂等键对应的请求参数规范化成一个摘要。
// 同键不同参数必须能区分开，不能只比较其中一部分字段。
func instantRequestDigest(ownerID int, parentID, name string, size int64, digest string) string {
	hasher := sha256.New()
	fmt.Fprintf(hasher, "%d\x00%s\x00%s\x00%d\x00%s", ownerID, parentID, name, size, digest)
	return hex.EncodeToString(hasher.Sum(nil))
}
