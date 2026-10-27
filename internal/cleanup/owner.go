package cleanup

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
)

// OwnerID 返回内部清理任务使用的所有者。
//
// tasks.owner_id 有指向 users 的外键，但清理是全局维护工作，没有真正的
// 业务所有者。这里复用数据库里第一个真实账号作为归属，而不是在启动时
// 偷偷创建一个系统账号：正常启动仍然不创建任何用户。
//
// 数据库里还没有用户时返回 ok=false。此时也没有任何用户数据需要清理，
// 跳过调度即可；账号创建后下一次启动会重新开始。
func OwnerID(ctx context.Context, db *gorm.DB) (ownerID int, ok bool, err error) {
	var user model.User
	err = db.WithContext(ctx).
		Select("id").
		Order("id ASC").
		First(&user).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("query cleanup task owner: %w", err)
	}
	return user.ID, true, nil
}
