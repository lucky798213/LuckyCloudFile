package cleanup

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"luckyCloudFile/internal/auth"
	"luckyCloudFile/internal/idgen"
	"luckyCloudFile/internal/model"
)

// SystemOwnerUsername 是内部清理任务的所有者。
//
// tasks.owner_id 有指向 users 的外键，所以内部任务也需要一个真实行。
// 这个账号没有可用配额，密码是启动时生成的随机值且从不输出，不能用来
// 登录或上传；它只作为清理任务的归属，不会出现在任何用户接口里。
const SystemOwnerUsername = "__system_cleanup__"

// EnsureOwner 返回内部清理任务所有者，不存在时创建一次。
//
// 重复启动是幂等的；并发创建由 users.username 唯一索引兜底。创建失败
// 会让启动失败，而不是在没有清理任务的情况下继续运行。
func EnsureOwner(ctx context.Context, db *gorm.DB) (*model.User, error) {
	var user model.User
	err := db.WithContext(ctx).
		Where("username = ?", SystemOwnerUsername).
		First(&user).Error
	if err == nil {
		return &user, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("query cleanup owner: %w", err)
	}

	// 密码只用于让账号有一个合法哈希，随后立即丢弃；任何登录都不知道它。
	password, err := idgen.New()
	if err != nil {
		return nil, err
	}

	created, err := auth.CreateUserWithQuota(ctx, db, SystemOwnerUsername, password, 0)
	if err == nil {
		return created, nil
	}
	if !errors.Is(err, auth.ErrUserExists) {
		return nil, fmt.Errorf("create cleanup owner: %w", err)
	}

	// 另一个实例刚创建成功，重新读取即可。
	if err := db.WithContext(ctx).
		Where("username = ?", SystemOwnerUsername).
		First(&user).Error; err != nil {
		return nil, fmt.Errorf("reload cleanup owner: %w", err)
	}
	return &user, nil
}
