package database_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/model"
)

// migrationTestDatabase 是专门给旧结构迁移测试用的库。
// scripts/mysql-init.sql 会创建它；测试每次先清空里面的表。
const migrationTestDatabase = "luckycloudfile_test_migration"

func migrationDSN(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run MySQL migration tests")
	}
	mc, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	mc.DBName = migrationTestDatabase
	mc.ParseTime = true
	return mc.FormatDSN()
}

// prepareLegacySchema 把迁移测试库恢复成“本步之前”的结构：
// users 没有 default_storage_policy_id，entities 只有旧的 policy 字符串。
func prepareLegacySchema(t *testing.T, dsn string) {
	t.Helper()

	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Ping(); err != nil {
		t.Fatalf("connect %s (create it with scripts/mysql-init.sql): %v", migrationTestDatabase, err)
	}

	statements := []string{
		"SET FOREIGN_KEY_CHECKS = 0",
		"DROP TABLE IF EXISTS auth_sessions, files, entities, users, storage_policies",
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
		`INSERT INTO users (username, password_hash)
			VALUES ('legacy-user', 'not-a-real-hash')`,
		`INSERT INTO entities (storage_key, size, policy, ref_count, status)
			VALUES ('legacy-storage-key', 3, 'local', 1, 'active')`,
	}
	for _, statement := range statements {
		if _, err := conn.Exec(statement); err != nil {
			t.Fatalf("prepare legacy schema: %v\n%s", err, statement)
		}
	}
}

// TestStoragePolicyMigrationFromLegacySchema 是本步的迁移验收：
//   - 旧 users/entities 补上默认本地策略，不移动已有内容；
//   - 重复启动不重复建策略，也不覆盖已经调整过的默认策略；
//   - 新列最终是 NOT NULL，并带 RESTRICT 外键。
func TestStoragePolicyMigrationFromLegacySchema(t *testing.T) {
	dsn := migrationDSN(t)
	ctx := context.Background()
	prepareLegacySchema(t, dsn)

	db, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}

	var policies []model.StoragePolicy
	if err := db.Find(&policies).Error; err != nil {
		t.Fatal(err)
	}
	if len(policies) != 1 || policies[0].ConfigKey != model.LocalStorageConfigKey {
		t.Fatalf("wanted exactly one local policy, got %+v", policies)
	}
	localID := policies[0].ID

	var user model.User
	if err := db.Where("username = ?", "legacy-user").First(&user).Error; err != nil {
		t.Fatal(err)
	}
	if user.DefaultStoragePolicyID != localID {
		t.Fatalf("legacy user default policy = %d, want %d", user.DefaultStoragePolicyID, localID)
	}

	var entity model.Entity
	if err := db.Where("storage_key = ?", "legacy-storage-key").First(&entity).Error; err != nil {
		t.Fatal(err)
	}
	if entity.StoragePolicyID != localID {
		t.Fatalf("legacy entity policy = %d, want %d", entity.StoragePolicyID, localID)
	}
	if entity.Size != 3 {
		t.Fatalf("legacy entity size changed to %d", entity.Size)
	}

	for _, check := range []struct {
		table  string
		column string
	}{
		{"users", "default_storage_policy_id"},
		{"entities", "storage_policy_id"},
	} {
		var nullable string
		if err := db.Raw(`
			SELECT IS_NULLABLE FROM information_schema.columns
			WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
			check.table, check.column).Row().Scan(&nullable); err != nil {
			t.Fatal(err)
		}
		if nullable != "NO" {
			t.Fatalf("%s.%s is still nullable", check.table, check.column)
		}
	}

	var deleteRules []struct {
		DeleteRule string `gorm:"column:DELETE_RULE"`
	}
	if err := db.Raw(`
		SELECT DELETE_RULE FROM information_schema.referential_constraints
		WHERE constraint_schema = DATABASE()
		  AND referenced_table_name = 'storage_policies'
		ORDER BY constraint_name`).Scan(&deleteRules).Error; err != nil {
		t.Fatal(err)
	}
	if len(deleteRules) != 2 {
		t.Fatalf("wanted 2 foreign keys to storage_policies, got %v", deleteRules)
	}
	for _, rule := range deleteRules {
		if rule.DeleteRule != "RESTRICT" {
			t.Fatalf("foreign key delete rule = %q, want RESTRICT", rule.DeleteRule)
		}
	}

	// 还有引用时不能删除策略。预期会失败，不要把它打成 SQL 错误日志。
	silent := db.Session(&gorm.Session{Logger: logger.Discard})
	if err := silent.Delete(&model.StoragePolicy{}, localID).Error; err == nil {
		t.Fatal("deleting a referenced storage policy must fail")
	}

	// 模拟服务端调整默认策略，然后重启（重新打开连接并重跑迁移）。
	policyB := model.StoragePolicy{
		Name:      "B",
		Type:      model.StorageTypeLocal,
		ConfigKey: "migration-b",
	}
	if err := db.Create(&policyB).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.User{}).
		Where("id = ?", user.ID).
		UpdateColumn("default_storage_policy_id", policyB.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Close(db); err != nil {
		t.Fatal(err)
	}

	reopened, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatalf("reopen after migration: %v", err)
	}
	defer database.Close(reopened)

	var localCount int64
	if err := reopened.Model(&model.StoragePolicy{}).
		Where("config_key = ?", model.LocalStorageConfigKey).
		Count(&localCount).Error; err != nil {
		t.Fatal(err)
	}
	if localCount != 1 {
		t.Fatalf("restart created %d local policies, want 1", localCount)
	}
	var storedUser model.User
	if err := reopened.First(&storedUser, user.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedUser.DefaultStoragePolicyID != policyB.ID {
		t.Fatalf("restart overwrote default policy: %d, want %d", storedUser.DefaultStoragePolicyID, policyB.ID)
	}
	var storedEntity model.Entity
	if err := reopened.First(&storedEntity, entity.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedEntity.StoragePolicyID != localID {
		t.Fatalf("restart changed legacy entity policy: %d, want %d", storedEntity.StoragePolicyID, localID)
	}
}

// TestStoragePolicyMigrationOnFreshDatabase 验证新库路径：
// 没有旧表时 CreateTable 直接按最终结构建表，策略也会初始化。
func TestStoragePolicyMigrationOnFreshDatabase(t *testing.T) {
	dsn := migrationDSN(t)
	ctx := context.Background()
	resetMigrationSchema(t, dsn)

	db, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(db)

	var policies []model.StoragePolicy
	if err := db.Find(&policies).Error; err != nil {
		t.Fatal(err)
	}
	if len(policies) != 1 || policies[0].ConfigKey != model.LocalStorageConfigKey {
		t.Fatalf("fresh database policies = %+v", policies)
	}

	for _, table := range []string{"users", "entities", "files", "auth_sessions", "storage_policies"} {
		if !db.Migrator().HasTable(table) {
			t.Fatalf("fresh migration did not create %s", table)
		}
	}

	var foreignKeys int64
	if err := db.Raw(`
		SELECT COUNT(*) FROM information_schema.referential_constraints
		WHERE constraint_schema = DATABASE()
		  AND referenced_table_name = 'storage_policies'`).Scan(&foreignKeys).Error; err != nil {
		t.Fatal(err)
	}
	if foreignKeys != 2 {
		t.Fatalf("fresh database has %d foreign keys to storage_policies, want 2", foreignKeys)
	}
}

func resetMigrationSchema(t *testing.T, dsn string) {
	t.Helper()

	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Ping(); err != nil {
		t.Fatalf("connect %s (create it with scripts/mysql-init.sql): %v", migrationTestDatabase, err)
	}
	for _, statement := range []string{
		"SET FOREIGN_KEY_CHECKS = 0",
		"DROP TABLE IF EXISTS auth_sessions, files, entities, users, storage_policies",
		"SET FOREIGN_KEY_CHECKS = 1",
	} {
		if _, err := conn.Exec(statement); err != nil {
			t.Fatalf("reset migration schema: %v", err)
		}
	}
}
