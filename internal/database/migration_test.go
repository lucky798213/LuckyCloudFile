package database_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"luckyCloudFile/internal/auth"
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
		"DROP TABLE IF EXISTS tasks, upload_sessions, auth_sessions, files, entities, users, storage_policies, schema_migrations",
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
	if !db.Migrator().HasTable("upload_sessions") {
		t.Fatal("legacy upgrade did not create upload_sessions")
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
	// 去掉 RefCount 的 GORM default 只影响新插入的零值，
	// 旧库里已有的 active / ref_count=1 必须原样保留。
	if entity.RefCount != 1 || entity.Status != model.EntityStatusActive {
		t.Fatalf("legacy entity lifecycle changed: ref_count=%d status=%q",
			entity.RefCount, entity.Status)
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

	for _, table := range []string{"users", "entities", "files", "auth_sessions", "storage_policies", "upload_sessions", "tasks"} {
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
		"DROP TABLE IF EXISTS tasks, upload_sessions, auth_sessions, files, entities, users, storage_policies, schema_migrations",
		"SET FOREIGN_KEY_CHECKS = 1",
	} {
		if _, err := conn.Exec(statement); err != nil {
			t.Fatalf("reset migration schema: %v", err)
		}
	}
}

// createMigrationUser 创建一个带根目录和默认存储策略的账号。
// 上传会话需要真实的 owner_id 和 parent_id，所以不手写自增 ID。
func createMigrationUser(t *testing.T, db *gorm.DB) (*model.User, *model.StoragePolicy) {
	t.Helper()

	user, err := auth.CreateUser(
		context.Background(), db, "upload-session-owner", "test-password",
	)
	if err != nil {
		t.Fatal(err)
	}

	var policy model.StoragePolicy
	if err := db.Where("config_key = ?", model.LocalStorageConfigKey).
		First(&policy).Error; err != nil {
		t.Fatal(err)
	}
	return user, &policy
}

// TestUploadSessionMigrationAndPlaceholderEntity 验证本步的两个关键点：
//  1. 新库会建出 upload_sessions 表，字段能完整保存；
//  2. 占位实体以 uploading / ref_count=0 落库，不会被 GORM 的默认值改成 1。
func TestUploadSessionMigrationAndPlaceholderEntity(t *testing.T) {
	dsn := migrationDSN(t)
	ctx := context.Background()
	resetMigrationSchema(t, dsn)

	db, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(db)

	if !db.Migrator().HasTable("upload_sessions") {
		t.Fatal("fresh migration did not create upload_sessions")
	}

	user, policy := createMigrationUser(t, db)
	entity := model.Entity{
		StorageKey:      "upload-placeholder-key",
		Size:            0,
		StoragePolicyID: policy.ID,
		RefCount:        0,
		Status:          model.EntityStatusUploading,
	}
	if err := db.WithContext(ctx).Create(&entity).Error; err != nil {
		t.Fatal(err)
	}

	// 必须从数据库重新读，才能确认 Create 没有偷偷把 0 换成默认值 1。
	var storedEntity model.Entity
	if err := db.WithContext(ctx).First(&storedEntity, entity.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedEntity.RefCount != 0 {
		t.Fatalf("placeholder ref_count = %d, want 0", storedEntity.RefCount)
	}
	if storedEntity.Status != model.EntityStatusUploading {
		t.Fatalf("placeholder status = %q, want %q",
			storedEntity.Status, model.EntityStatusUploading)
	}

	session := model.UploadSession{
		ID:           "upload-session-1",
		OwnerID:      user.ID,
		ParentID:     *user.RootFileID,
		Name:         "big.bin",
		EntityID:     storedEntity.ID,
		ExpectedSize: 8 << 20,
		ActualSize:   0,
		Status:       model.UploadStatusPending,
		ExpiresAt:    time.Now().Add(time.Hour),
	}
	if err := db.WithContext(ctx).Create(&session).Error; err != nil {
		t.Fatal(err)
	}

	var storedSession model.UploadSession
	if err := db.WithContext(ctx).First(&storedSession, "id = ?", session.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedSession.OwnerID != user.ID ||
		storedSession.ParentID != *user.RootFileID ||
		storedSession.Name != session.Name ||
		storedSession.EntityID != storedEntity.ID ||
		storedSession.ExpectedSize != session.ExpectedSize ||
		storedSession.ActualSize != 0 ||
		storedSession.Status != model.UploadStatusPending {
		t.Fatalf("stored upload session = %+v", storedSession)
	}
	if storedSession.ResultFileID != nil {
		t.Fatalf("new upload session already has a result file: %q", *storedSession.ResultFileID)
	}
	if storedSession.ExpiresAt.IsZero() || !storedSession.ExpiresAt.After(time.Now()) {
		t.Fatalf("upload session expiry was not stored: %v", storedSession.ExpiresAt)
	}
}

// TestUploadSessionMigrationPreservesExistingFiles 模拟“旧库还没有 upload_sessions”：
// 先写入一个正式文件，再删掉会话表并重启迁移。已有 File/Entity 必须保持原样。
func TestUploadSessionMigrationPreservesExistingFiles(t *testing.T) {
	dsn := migrationDSN(t)
	ctx := context.Background()
	resetMigrationSchema(t, dsn)

	db, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}

	user, policy := createMigrationUser(t, db)
	entity := model.Entity{
		StorageKey:      "existing-content-key",
		Size:            5,
		StoragePolicyID: policy.ID,
		RefCount:        1,
		Status:          model.EntityStatusActive,
	}
	if err := db.WithContext(ctx).Create(&entity).Error; err != nil {
		t.Fatal(err)
	}
	record := model.File{
		ID:       "existing-file-id",
		Name:     "keep.txt",
		Kind:     model.KindFile,
		ParentID: user.RootFileID,
		EntityID: &entity.ID,
		OwnerID:  user.ID,
	}
	if err := db.WithContext(ctx).Create(&record).Error; err != nil {
		t.Fatal(err)
	}

	// 旧版本数据库没有这张表。删掉它再启动，等价于一次真实升级。
	if err := db.Exec("DROP TABLE upload_sessions").Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Close(db); err != nil {
		t.Fatal(err)
	}

	reopened, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatalf("reopen after adding upload_sessions: %v", err)
	}
	defer database.Close(reopened)

	if !reopened.Migrator().HasTable("upload_sessions") {
		t.Fatal("restart did not recreate upload_sessions")
	}

	var stored model.File
	if err := reopened.WithContext(ctx).
		Preload("Entity").
		First(&stored, "id = ?", record.ID).Error; err != nil {
		t.Fatalf("existing file disappeared after migration: %v", err)
	}
	if stored.Name != record.Name ||
		stored.Kind != model.KindFile ||
		stored.OwnerID != user.ID ||
		stored.ParentID == nil ||
		*stored.ParentID != *user.RootFileID {
		t.Fatalf("existing file changed after migration: %+v", stored)
	}
	if stored.Entity == nil {
		t.Fatal("existing file lost its entity after migration")
	}
	if stored.Entity.StorageKey != entity.StorageKey ||
		stored.Entity.Size != entity.Size ||
		stored.Entity.StoragePolicyID != entity.StoragePolicyID ||
		stored.Entity.RefCount != 1 ||
		stored.Entity.Status != model.EntityStatusActive {
		t.Fatalf("existing entity changed after migration: %+v", stored.Entity)
	}
}

// TestTaskTableMigration 验证 P8.1 的任务表：
//   - 新库会建出 tasks，带到期扫描和所有者查询要用的复合索引；
//   - 所有字段都能落库并按原样读回，包括检查点 JSON 和清理标记；
//   - owner 外键指向 users，删除账号时任务一起删除。
func TestTaskTableMigration(t *testing.T) {
	dsn := migrationDSN(t)
	ctx := context.Background()
	resetMigrationSchema(t, dsn)

	db, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close(db)

	if !db.Migrator().HasTable("tasks") {
		t.Fatal("fresh migration did not create tasks")
	}
	for _, index := range []string{"idx_tasks_due", "idx_tasks_owner_created"} {
		if !db.Migrator().HasIndex(&model.Task{}, index) {
			t.Fatalf("tasks is missing index %s", index)
		}
	}

	user, _ := createMigrationUser(t, db)
	nextRunAt := time.Now().Add(time.Minute).Truncate(time.Second)
	task := model.Task{
		ID:             "migration-task-1",
		Type:           "fake_echo",
		OwnerID:        user.ID,
		Status:         model.TaskStatusRetryWait,
		Phase:          "compress",
		StateVersion:   2,
		StateJSON:      `{"input":"hello"}`,
		Attempts:       1,
		MaxAttempts:    3,
		NextRunAt:      nextRunAt,
		LastError:      "temporary",
		CleanupPending: true,
	}
	if err := db.WithContext(ctx).Create(&task).Error; err != nil {
		t.Fatal(err)
	}

	var stored model.Task
	if err := db.WithContext(ctx).First(&stored, "id = ?", task.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Type != task.Type ||
		stored.OwnerID != user.ID ||
		stored.Status != model.TaskStatusRetryWait ||
		stored.Phase != "compress" ||
		stored.StateVersion != 2 ||
		stored.StateJSON != task.StateJSON ||
		stored.Attempts != 1 ||
		stored.MaxAttempts != 3 ||
		stored.LastError != "temporary" ||
		!stored.CleanupPending {
		t.Fatalf("task fields changed after migration: %+v", stored)
	}
	if !stored.NextRunAt.Equal(nextRunAt) {
		t.Fatalf("next_run_at = %s, want %s", stored.NextRunAt, nextRunAt)
	}
	if stored.FinishedAt != nil || stored.ResultFileID != nil {
		t.Fatalf("non-terminal task has result/finish time: %+v", stored)
	}

	// 所有者外键是 CASCADE：账号删除后任务不能留下悬空记录。
	if err := db.Where("owner_id = ?", user.ID).Delete(&model.File{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(&model.User{}, user.ID).Error; err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&model.Task{}).Where("id = ?", task.ID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("deleting the owner left %d task(s) behind", count)
	}
}
