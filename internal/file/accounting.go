package file

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/quota"
)

// 这个文件放容量记账和上传会话的共用事务片段。
//
// 所有会改变用户容量的上传路径都遵守同一个加锁顺序：
//
//	User → Session 或 File → Entity
//
// 先锁用户行，再锁会话/文件，最后锁实体。任何一条路径都不能反过来，
// 否则两个事务可能互相等待成环。也不要在持有这些锁的时候做上传或物理删除。

// lockUserAndUploadSession 在事务里先锁用户、再锁会话。
//
// 返回 ErrUploadSessionNotFound 表示会话不存在或不属于这个用户。
// 调用方必须先拿到用户锁，因此在拿到会话锁之前，用户的容量不会被别人改动。
func lockUserAndUploadSession(
	ctx context.Context,
	tx *gorm.DB,
	ownerID int,
	sessionID string,
) (*model.UploadSession, error) {
	if _, err := quota.LockUser(tx, ownerID); err != nil {
		return nil, err
	}

	var session model.UploadSession
	err := tx.WithContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ? AND owner_id = ?", sessionID, ownerID).
		First(&session).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrUploadSessionNotFound
		}
		return nil, fmt.Errorf("lock upload session: %w", err)
	}
	return &session, nil
}

// releaseUploadReservation 按会话上持久化的 ReservedBytes 释放预留容量。
//
// 返回实际释放的字节数：会话预留已经是 0 时返回 0，表示本次没有任何改动。
// 这正是“重复取消、重复扫描不重复释放”的依据——不能按 ExpectedSize 重算。
//
// 必须在持有用户行锁和会话行锁的事务里调用。调用方还必须在这同一个事务里
// 把会话改成终态并把 ReservedBytes 清零，不能先提交终态再单独释放容量。
func releaseUploadReservation(tx *gorm.DB, session *model.UploadSession) (int64, error) {
	reserved := session.ReservedBytes
	if reserved == 0 {
		return 0, nil
	}
	if reserved < 0 {
		return 0, fmt.Errorf(
			"%w: upload session %s has negative reserved_bytes %d",
			quota.ErrInconsistent, session.ID, reserved,
		)
	}
	if err := quota.ReleaseReserved(tx, session.OwnerID, reserved); err != nil {
		return 0, err
	}
	// 内存里的会话也同步清零，调用方写状态时可以直接使用。
	session.ReservedBytes = 0
	return reserved, nil
}
