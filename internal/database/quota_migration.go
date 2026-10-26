// 这个文件负责容量账目的一次性迁移。
//
// 新增 users.quota_bytes / used_bytes / reserved_bytes 和
// upload_sessions.reserved_bytes 之后，旧数据不能直接全部填 0：
//
//   - 0 配额表示“没有可用容量”，老账号会在升级后突然无法上传；
//   - 已用容量必须按历史逻辑文件回填，否则删除文件时释放的用量对不上账；
//   - 旧上传会话原本没有预留记录，不能把 ExpectedSize 当成预留量补上，
//     否则会凭空多占一份容量。它们由启动恢复统一作废即可（预留本来就是 0）。
//
// 回填只能执行一次。这里用 schema_migrations 里的标记记录“已经完成”，
// 而不是每次启动都按当前文件重算 used_bytes：账目在运行期会因为上传、
// 复制、删除而合法地偏离“当前文件之和”，重算会把真实账目抹掉。
package database

import (
	"fmt"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/quota"
)

// quotaAccountingMigration 是容量回填在 schema_migrations 里的名字。
// 改回填规则时要换一个新名字，不能复用：旧标记会让新逻辑永远不执行。
const quotaAccountingMigration = "quota_accounting_v1"

// migrateQuotaAccounting 在需要时回填历史容量，并在成功后写一次性标记。
//
// 顺序很重要：AutoMigrate 已经先把新列加出来了，这里只负责数据。
// 回填或校验失败时不会写标记，启动会失败；运维调整配额后重新启动会再次
// 执行同一条回填（幂等），直到账目满足约束。
func migrateQuotaAccounting(db *gorm.DB) error {
	if err := ensureSchemaMigrations(db); err != nil {
		return err
	}

	var applied int64
	if err := db.Table("schema_migrations").
		Where("name = ?", quotaAccountingMigration).
		Count(&applied).Error; err != nil {
		return fmt.Errorf("check quota migration marker: %w", err)
	}
	if applied > 0 {
		// 已经完成过：绝不重算 used_bytes/reserved_bytes。
		return nil
	}

	if err := backfillQuotaAccounting(db); err != nil {
		return err
	}
	if err := validateQuotaAccounting(db); err != nil {
		return err
	}

	if err := db.Exec(
		"INSERT INTO schema_migrations (name, applied_at) VALUES (?, ?)",
		quotaAccountingMigration, time.Now(),
	).Error; err != nil {
		return fmt.Errorf("record quota migration: %w", err)
	}
	return nil
}

// ensureSchemaMigrations 建一张只记录“哪些一次性迁移已经完成”的小表。
//
// 没有引入完整的迁移框架：当前项目用 AutoMigrate 管结构，这张表只补上
// “数据回填只能跑一次”这个需求。表名不用 GORM 模型，避免它被误当成业务表。
func ensureSchemaMigrations(db *gorm.DB) error {
	err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			name VARCHAR(128) NOT NULL PRIMARY KEY,
			applied_at DATETIME NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`).Error
	if err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	return nil
}

// backfillQuotaAccounting 按历史逻辑文件计算每个用户的已用容量。
//
// 计费口径和运行期一致：每个 kind=file 的 File 各算一份 Entity.Size；
// 同一个 Entity 被两个 File 引用时计算两份，不能按 Entity 去重。
// 目录没有 Entity，不参与统计。
func backfillQuotaAccounting(db *gorm.DB) error {
	// 旧库没有配额概念，新列刚加出来全是 0。给还没设置过配额的账号
	// 补服务端默认值；运维已经显式调大过的（>0）保持原样。
	if err := db.Exec(
		"UPDATE users SET quota_bytes = ? WHERE quota_bytes = 0",
		quota.DefaultBytes,
	).Error; err != nil {
		return fmt.Errorf("backfill users.quota_bytes: %w", err)
	}

	if !db.Migrator().HasTable("files") || !db.Migrator().HasTable("entities") {
		// 没有文件表就没有历史文件可统计；只把容量字段初始化成 0。
		if err := db.Exec(
			"UPDATE users SET used_bytes = 0, reserved_bytes = 0",
		).Error; err != nil {
			return fmt.Errorf("initialize users.used_bytes: %w", err)
		}
		return nil
	}

	// LEFT JOIN 保证没有文件的用户也会被清零。
	// 子查询由 files 驱动：共享 Entity 的每个 File 都会加一次 SUM。
	err := db.Exec(`
		UPDATE users u
		LEFT JOIN (
			SELECT f.owner_id AS owner_id, SUM(e.size) AS used
			FROM files f
			JOIN entities e ON e.id = f.entity_id
			WHERE f.kind = ? AND f.entity_id IS NOT NULL
			GROUP BY f.owner_id
		) usage_by_user ON usage_by_user.owner_id = u.id
		SET u.used_bytes = COALESCE(usage_by_user.used, 0),
		    u.reserved_bytes = 0`,
		model.KindFile,
	).Error
	if err != nil {
		return fmt.Errorf("backfill users.used_bytes: %w", err)
	}
	return nil
}

// validateQuotaAccounting 确认回填后每个用户都满足容量约束。
//
// 历史用量超过初始配额时明确报错，并给出第一个超限账号的具体数字，
// 绝不截断用量：截断会让账目看起来合法，但下一次删除就会释放不存在的容量。
// 运维可以把该账号的 quota_bytes 调大后重启；标记还没写入，回填会重跑。
func validateQuotaAccounting(db *gorm.DB) error {
	type violation struct {
		ID            int
		Username      string
		QuotaBytes    int64
		UsedBytes     int64
		ReservedBytes int64
	}

	var violations []violation
	err := db.Raw(`
		SELECT id, username, quota_bytes, used_bytes, reserved_bytes
		FROM users
		WHERE quota_bytes < 0
		   OR used_bytes < 0
		   OR reserved_bytes < 0
		   OR used_bytes + reserved_bytes > quota_bytes
		ORDER BY id ASC
		LIMIT 5`).Scan(&violations).Error
	if err != nil {
		return fmt.Errorf("validate quota accounting: %w", err)
	}
	if len(violations) == 0 {
		return nil
	}

	var total int64
	if err := db.Raw(`
		SELECT COUNT(*) FROM users
		WHERE quota_bytes < 0
		   OR used_bytes < 0
		   OR reserved_bytes < 0
		   OR used_bytes + reserved_bytes > quota_bytes`).
		Row().Scan(&total); err != nil {
		return fmt.Errorf("count quota violations: %w", err)
	}

	first := violations[0]
	return fmt.Errorf(
		"%w: %d user(s) do not satisfy 0 <= used_bytes+reserved_bytes <= quota_bytes; "+
			"first is user %d (%q) used=%d reserved=%d quota=%d; "+
			"raise quota_bytes for the affected accounts and restart",
		quota.ErrInconsistent, total, first.ID, first.Username,
		first.UsedBytes, first.ReservedBytes, first.QuotaBytes,
	)
}
