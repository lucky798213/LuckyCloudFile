package database_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/quota"
)

// prepareQuotaLegacySchema 构造“上一个版本”的库：
// users/upload_sessions 还没有容量字段，但已经存在正式文件和未完成会话。
//
// 数据和结构都刻意保持最简：不需要真实外键，只要能验证回填的 SQL 口径。
func prepareQuotaLegacySchema(t *testing.T, dsn string, extra ...string) {
	t.Helper()

	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Ping(); err != nil {
		t.Fatalf("connect migration test database: %v", err)
	}

	statements := []string{
		"SET FOREIGN_KEY_CHECKS = 0",
		"DROP TABLE IF EXISTS upload_sessions, auth_sessions, files, entities, users, storage_policies, schema_migrations",
		"SET FOREIGN_KEY_CHECKS = 1",
		`CREATE TABLE users (
			id BIGINT NOT NULL AUTO_INCREMENT,
			username VARCHAR(255) NOT NULL,
			password_hash VARCHAR(255) NOT NULL,
			root_file_id VARCHAR(255) NULL,
			PRIMARY KEY (id),
			UNIQUE KEY username (username)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
		`CREATE TABLE entities (
			id BIGINT NOT NULL AUTO_INCREMENT,
			storage_key VARCHAR(255) NOT NULL,
			size BIGINT NOT NULL,
			policy VARCHAR(255) NOT NULL DEFAULT 'local',
			ref_count BIGINT NOT NULL DEFAULT 1,
			status VARCHAR(16) NOT NULL DEFAULT 'active',
			PRIMARY KEY (id),
			UNIQUE KEY idx_entities_storage_key (storage_key)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
		`CREATE TABLE files (
			id VARCHAR(255) NOT NULL,
			name VARCHAR(255) NOT NULL,
			kind VARCHAR(16) NOT NULL DEFAULT 'file',
			parent_id VARCHAR(255) NULL,
			entity_id BIGINT NULL,
			owner_id BIGINT NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
		// 上一版已经有上传会话，但还没有 reserved_bytes。
		`CREATE TABLE upload_sessions (
			id VARCHAR(255) NOT NULL,
			owner_id BIGINT NOT NULL,
			parent_id VARCHAR(255) NOT NULL,
			name VARCHAR(255) NOT NULL,
			entity_id BIGINT NOT NULL,
			expected_size BIGINT NOT NULL,
			actual_size BIGINT NOT NULL,
			status VARCHAR(16) NOT NULL DEFAULT 'pending',
			expires_at DATETIME NOT NULL,
			result_file_id VARCHAR(255) NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
		`INSERT INTO users (id, username, password_hash) VALUES
			(1, 'quota-a', 'not-a-real-hash'),
			(2, 'quota-b', 'not-a-real-hash')`,
		// 同一份 Entity 会被两个 File 引用，用来验证“不能按 Entity 去重”。
		`INSERT INTO entities (id, storage_key, size, policy, ref_count, status) VALUES
			(1, 'shared-content', 5, 'local', 2, 'active'),
			(2, 'other-content', 3, 'local', 1, 'active'),
			(3, 'legacy-placeholder', 0, 'local', 0, 'uploading')`,
		`INSERT INTO files (id, name, kind, parent_id, entity_id, owner_id) VALUES
			('q-file-1', 'a1.txt', 'file', NULL, 1, 1),
			('q-file-2', 'a2.txt', 'file', NULL, 1, 1),
			('q-file-3', 'a3.txt', 'file', NULL, 2, 1),
			('q-dir-1', 'docs', 'directory', NULL, NULL, 1),
			('q-file-4', 'b1.txt', 'file', NULL, 2, 2)`,
		`INSERT INTO upload_sessions
			(id, owner_id, parent_id, name, entity_id, expected_size, actual_size, status, expires_at, result_file_id)
			VALUES ('legacy-session', 1, 'q-dir-1', 'pending.bin', 3, 7, 0, 'pending', NOW() + INTERVAL 1 HOUR, NULL)`,
	}
	statements = append(statements, extra...)

	for _, statement := range statements {
		if _, err := conn.Exec(statement); err != nil {
			t.Fatalf("prepare quota legacy schema: %v\n%s", err, statement)
		}
	}
}

// TestQuotaAccountingMigrationBackfillsLogicalUsage 验证一次性回填：
//   - 已用容量 = 每个逻辑 File 的 Entity.Size 之和，共享 Entity 算两份；
//   - 老账号补上服务端默认配额，旧会话的预留量是 0（不能按 ExpectedSize 补）；
//   - 回填只执行一次，重启不会把运行期账目重算回“当前文件之和”。
func TestQuotaAccountingMigrationBackfillsLogicalUsage(t *testing.T) {
	dsn := migrationDSN(t)
	ctx := context.Background()
	prepareQuotaLegacySchema(t, dsn)

	db, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}

	var userA, userB model.User
	if err := db.First(&userA, 1).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.First(&userB, 2).Error; err != nil {
		t.Fatal(err)
	}

	// A: 5 + 5 + 3 = 13（共享实体计算两份），目录不占容量。
	if userA.UsedBytes != 13 || userA.ReservedBytes != 0 {
		t.Fatalf("user A accounting = used %d reserved %d, want 13/0",
			userA.UsedBytes, userA.ReservedBytes)
	}
	if userA.QuotaBytes != quota.DefaultBytes {
		t.Fatalf("user A quota = %d, want default %d", userA.QuotaBytes, quota.DefaultBytes)
	}
	// B: 只引用 size=3 的内容。
	if userB.UsedBytes != 3 || userB.ReservedBytes != 0 {
		t.Fatalf("user B accounting = used %d reserved %d, want 3/0",
			userB.UsedBytes, userB.ReservedBytes)
	}
	if userB.QuotaBytes != quota.DefaultBytes {
		t.Fatalf("user B quota = %d, want default %d", userB.QuotaBytes, quota.DefaultBytes)
	}

	// 旧会话原本没有预留记录：补出来的值必须是 0，不能把 ExpectedSize 当成占用。
	var legacySession model.UploadSession
	if err := db.First(&legacySession, "id = ?", "legacy-session").Error; err != nil {
		t.Fatal(err)
	}
	if legacySession.ReservedBytes != 0 {
		t.Fatalf("legacy session reserved_bytes = %d, want 0", legacySession.ReservedBytes)
	}
	if legacySession.ExpectedSize != 7 || legacySession.Status != model.UploadStatusPending {
		t.Fatalf("legacy session changed during migration: %+v", legacySession)
	}

	// 回填完成后写了一次性标记。
	var markers int64
	if err := db.Table("schema_migrations").
		Where("name = ?", "quota_accounting_v1").
		Count(&markers).Error; err != nil {
		t.Fatal(err)
	}
	if markers != 1 {
		t.Fatalf("quota migration markers = %d, want 1", markers)
	}

	// 模拟运行期账目：把 used 改成 42，重启后不能被“当前文件之和”覆盖。
	if err := db.Model(&model.User{}).
		Where("id = ?", userA.ID).
		UpdateColumn("used_bytes", int64(42)).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Close(db); err != nil {
		t.Fatal(err)
	}

	reopened, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatalf("reopen after quota migration: %v", err)
	}
	defer database.Close(reopened)

	var storedA model.User
	if err := reopened.First(&storedA, userA.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedA.UsedBytes != 42 {
		t.Fatalf("restart reset used_bytes to %d, want 42", storedA.UsedBytes)
	}
	if storedA.ReservedBytes != 0 || storedA.QuotaBytes != quota.DefaultBytes {
		t.Fatalf("restart changed quota accounting: %+v", storedA)
	}
}

// TestQuotaAccountingMigrationRejectsHistoricalOverage 验证历史用量超过初始配额时
// 不回填成合法值、也不截断用量：迁移直接失败；调大配额后重启可以正常完成。
func TestQuotaAccountingMigrationRejectsHistoricalOverage(t *testing.T) {
	dsn := migrationDSN(t)
	ctx := context.Background()

	overSized := quota.DefaultBytes + 1
	prepareQuotaLegacySchema(t, dsn,
		`INSERT INTO users (id, username, password_hash) VALUES
			(3, 'quota-over', 'not-a-real-hash')`,
		fmt.Sprintf(`INSERT INTO entities (id, storage_key, size, policy, ref_count, status) VALUES
			(10, 'oversized-content', %d, 'local', 1, 'active')`, overSized),
		`INSERT INTO files (id, name, kind, parent_id, entity_id, owner_id) VALUES
			('q-file-over', 'huge.bin', 'file', NULL, 10, 3)`,
	)

	if _, err := database.OpenDSN(ctx, dsn); err == nil {
		t.Fatal("migration must fail when historical usage exceeds the initial quota")
	} else if !errors.Is(err, quota.ErrInconsistent) {
		t.Fatalf("over-quota migration error = %v, want quota.ErrInconsistent", err)
	}

	// 失败时不能写标记；运维调大配额后重启，回填和校验会重新执行。
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Exec(
		"UPDATE users SET quota_bytes = ? WHERE id = 3",
		quota.DefaultBytes*2,
	); err != nil {
		t.Fatalf("raise quota for over-quota user: %v", err)
	}

	reopened, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatalf("reopen after raising quota: %v", err)
	}
	defer database.Close(reopened)

	var over model.User
	if err := reopened.First(&over, 3).Error; err != nil {
		t.Fatal(err)
	}
	if over.UsedBytes != overSized {
		t.Fatalf("over-quota user used_bytes = %d, want %d (must not be truncated)",
			over.UsedBytes, overSized)
	}
	if over.QuotaBytes != quota.DefaultBytes*2 {
		t.Fatalf("over-quota user quota = %d, want the operator-set %d",
			over.QuotaBytes, quota.DefaultBytes*2)
	}
	var markers int64
	if err := reopened.Table("schema_migrations").
		Where("name = ?", "quota_accounting_v1").
		Count(&markers).Error; err != nil {
		t.Fatal(err)
	}
	if markers != 1 {
		t.Fatalf("quota migration markers after retry = %d, want 1", markers)
	}
}
