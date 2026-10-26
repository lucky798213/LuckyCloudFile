// Package quota 维护用户的逻辑容量账目。
//
// 计费口径固定为“逻辑文件用量”，与磁盘上实际占用的字节无关：
//
//	上传一个文件：占用文件大小
//	复制一个文件：再占一份大小，即使底层共用同一个 Entity
//	删除一个文件：释放一份大小，不必等物理内容被删除
//	目录、改名、移动：不改变用量
//
// users 表上三个字段共同描述这份账目：
//
//	quota_bytes    总配额
//	used_bytes     已经发布、仍然存在的逻辑文件大小之和
//	reserved_bytes 尚未完成的上传会话预留的大小之和
//
// 任何时刻都必须满足：
//
//	used_bytes >= 0
//	reserved_bytes >= 0
//	used_bytes + reserved_bytes <= quota_bytes
//
// 本包只提供“受条件约束的原子更新”，不判断业务状态：
// 调用方必须先按全局加锁顺序取得 User 行锁，再锁 Session 或 File，最后锁 Entity。
// 只先查询剩余容量、再无条件增加用量的写法是禁止的：两个并发请求可能同时通过检查。
package quota

import (
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"luckyCloudFile/internal/model"
)

// DefaultBytes 是服务端给新账号分配的初始配额：10 GiB。
//
// 约定 0 表示“没有可用容量”，不是“无限”。需要无限容量的部署必须自己
// 调大 quota_bytes，不能指望把 0 当开关：那样会让超额检查失效。
const DefaultBytes int64 = 10 << 30

var (
	// ErrExceeded 表示剩余容量不足，操作必须整体回滚。
	ErrExceeded = errors.New("insufficient storage quota")

	// ErrInconsistent 表示容量账目本身已经不一致，例如：
	// 释放的预留量超过了用户当前预留量，或释放的已用量超过了用户已用量。
	// 这不是“容量不足”，不能当作用户可重试的错误，必须回滚并暴露出来。
	ErrInconsistent = errors.New("user quota accounting is inconsistent")
)

// LockUser 在事务里锁住 users 行，并读出最新的容量字段。
//
// 它是容量事务的第一步。所有会改变 used_bytes / reserved_bytes 的事务
// 都必须先取得这个行锁，再锁 Session / File / Entity，保证全局加锁顺序是：
//
//	User → Session 或 File → Entity
//
// 反过来（先锁 Session/File，再等 User）会和别的路径互相等待成环。
func LockUser(tx *gorm.DB, userID int) (*model.User, error) {
	var user model.User
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "quota_bytes", "used_bytes", "reserved_bytes").
		First(&user, userID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("user %d not found while locking quota", userID)
		}
		return nil, fmt.Errorf("lock user %d quota: %w", userID, err)
	}
	return &user, nil
}

// Reserve 原子地预留 size 字节容量。
//
// 只更新一行，而且把“剩余容量是否足够”写进 WHERE 条件：
//
//	UPDATE users SET reserved_bytes = reserved_bytes + size
//	WHERE id = ? AND quota_bytes - used_bytes - reserved_bytes >= size
//
// 返回 ErrExceeded 表示用户存在但剩余容量不足。调用者必须让整个事务回滚，
// 占位 Entity 和 UploadSession 会一起撤销。
//
// size == 0 是合法的空文件：不增加预留，也不能把“MySQL 更新零行”
// 误判成容量不足，所以直接返回成功。
//
// 调用前必须已经用 LockUser 锁住这一行。
func Reserve(tx *gorm.DB, userID int, size int64) error {
	if size == 0 {
		return nil
	}
	if size < 0 {
		return fmt.Errorf("%w: reserve %d bytes for user %d", ErrInconsistent, size, userID)
	}

	result := tx.Model(&model.User{}).
		Where(
			"id = ? AND quota_bytes - used_bytes - reserved_bytes >= ?",
			userID, size,
		).
		UpdateColumn("reserved_bytes", gorm.Expr("reserved_bytes + ?", size))
	if result.Error != nil {
		return fmt.Errorf("reserve %d bytes for user %d: %w", size, userID, result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: user %d cannot reserve %d bytes", ErrExceeded, userID, size)
	}
	return nil
}

// Consume 原子地把 size 字节从“未占用”直接变成“已用”。
//
// 用于没有提前预留的发布路径：旧版完整上传、复制文件。
// 条件和 Reserve 一样，只是加到 used_bytes 上。
//
// 发布事务必须调用它，否则旧上传接口和复制接口就会绕过配额。
// 调用前必须已经用 LockUser 锁住这一行。
func Consume(tx *gorm.DB, userID int, size int64) error {
	if size == 0 {
		return nil
	}
	if size < 0 {
		return fmt.Errorf("%w: consume %d bytes for user %d", ErrInconsistent, size, userID)
	}

	result := tx.Model(&model.User{}).
		Where(
			"id = ? AND quota_bytes - used_bytes - reserved_bytes >= ?",
			userID, size,
		).
		UpdateColumn("used_bytes", gorm.Expr("used_bytes + ?", size))
	if result.Error != nil {
		return fmt.Errorf("consume %d bytes for user %d: %w", size, userID, result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: user %d cannot consume %d bytes", ErrExceeded, userID, size)
	}
	return nil
}

// Settle 把会话预留的 size 字节转成已用容量：
//
//	used_bytes     += size
//	reserved_bytes -= size
//
// 两者在同一个 UPDATE 里完成，所以“预留已经消失、已用还没记上”的
// 中间状态不会提交。完成后总量 used+reserved 不变，配额上界自然保持。
//
// 条件 reserved_bytes >= size 是最后一道防线：预留量不足时整体回滚，
// 绝不会把 used_bytes 加到一个没有预留支撑的文件上。
//
// 调用前必须已经用 LockUser 锁住这一行。
func Settle(tx *gorm.DB, userID int, size int64) error {
	if size == 0 {
		return nil
	}
	if size < 0 {
		return fmt.Errorf("%w: settle %d bytes for user %d", ErrInconsistent, size, userID)
	}

	result := tx.Model(&model.User{}).
		Where("id = ? AND reserved_bytes >= ?", userID, size).
		UpdateColumns(map[string]any{
			"used_bytes":     gorm.Expr("used_bytes + ?", size),
			"reserved_bytes": gorm.Expr("reserved_bytes - ?", size),
		})
	if result.Error != nil {
		return fmt.Errorf("settle %d bytes for user %d: %w", size, userID, result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf(
			"%w: user %d has less than %d reserved bytes",
			ErrInconsistent, userID, size,
		)
	}
	return nil
}

// ReleaseReserved 释放会话预留的 size 字节。
//
// 取消、过期、重启作废都必须在“改成终态”的同一个事务里调用它。
// 条件 reserved_bytes >= size 保证预留量不会减成负数；重复释放
// （会话预留已经清零）不应该再调用这里，调用方要先看 Session.ReservedBytes。
func ReleaseReserved(tx *gorm.DB, userID int, size int64) error {
	if size == 0 {
		return nil
	}
	if size < 0 {
		return fmt.Errorf("%w: release reserved %d bytes for user %d", ErrInconsistent, size, userID)
	}

	result := tx.Model(&model.User{}).
		Where("id = ? AND reserved_bytes >= ?", userID, size).
		UpdateColumn("reserved_bytes", gorm.Expr("reserved_bytes - ?", size))
	if result.Error != nil {
		return fmt.Errorf("release %d reserved bytes for user %d: %w", size, userID, result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf(
			"%w: user %d has less than %d reserved bytes",
			ErrInconsistent, userID, size,
		)
	}
	return nil
}

// ReleaseUsed 释放一个被删除逻辑文件占用的 size 字节。
//
// 删除 File 时调用，和删除记录在同一个事务里。是否还有别的 File 引用
// 同一个 Entity 与这里无关：逻辑用量按 File 计费，删除一份就释放一份。
//
// 条件 used_bytes >= size 防止把已用量减成负数；账目不一致时整个删除回滚，
// 而不是提交一个“文件没了、用量还是对的”的假象。
func ReleaseUsed(tx *gorm.DB, userID int, size int64) error {
	if size == 0 {
		return nil
	}
	if size < 0 {
		return fmt.Errorf("%w: release used %d bytes for user %d", ErrInconsistent, size, userID)
	}

	result := tx.Model(&model.User{}).
		Where("id = ? AND used_bytes >= ?", userID, size).
		UpdateColumn("used_bytes", gorm.Expr("used_bytes - ?", size))
	if result.Error != nil {
		return fmt.Errorf("release %d used bytes for user %d: %w", size, userID, result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf(
			"%w: user %d has less than %d used bytes",
			ErrInconsistent, userID, size,
		)
	}
	return nil
}
