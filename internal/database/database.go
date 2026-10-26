// Package database 负责打开 MySQL 连接、检查是否连得上，并创建表结构。
//
// 它把“怎么连数据库”的细节集中在一个地方：
// 上层只拿到一个 *gorm.DB，不需要知道连接串、驱动、迁移这些事。
package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"luckyCloudFile/internal/config"
	"luckyCloudFile/internal/idgen"
	"luckyCloudFile/internal/model"
)

// Open 根据配置里的 [Database] 打开 MySQL。
//
// 它会做四件事：
//  1. 用 mysql.Config 建立连接串（密码里有特殊字符也不会拼错）
//  2. 用 GORM 打开连接池
//  3. Ping 一次，确认数据库真的连得上（而不是等到第一个请求才报错）
//  4. 自动建表 / 改表（P2 用 AutoMigrate，P3 会换成迁移文件）
func Open(ctx context.Context, cfg config.Database) (*gorm.DB, error) {
	return OpenDSN(ctx, mysqlConfig(cfg).FormatDSN())
}

// OpenDSN 用现成的 DSN 打开数据库。集成测试直接用它。
func OpenDSN(ctx context.Context, dsn string) (*gorm.DB, error) {
	// 先把 DSN 解析成结构体再格式化，顺便强制打开 parseTime：
	// 这样 MySQL 的 DATETIME 才会扫描成 time.Time，而不是 []byte。
	mc, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse mysql dsn: %w", err)
	}
	mc.ParseTime = true
	dsn = mc.FormatDSN()

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		// 把 MySQL 的重复键错误翻译成 gorm.ErrDuplicatedKey，
		// 业务层就不用写 1062 这种驱动相关的魔法数字。
		TranslateError: true,
		// 默认只打印慢查询和错误，避免每个请求都刷屏。
		Logger: newLogger(),
	})
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}

	// 连接是懒加载的，Ping 才会真的去连。给 5 秒，避免启动卡死。
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get sql connection pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(pingCtx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("connect mysql: %w", err)
	}

	// 所有结构变更和数据迁移都串行执行：两个进程同时 AutoMigrate 会互相踩，
	// 典型症状是 “Duplicate column name”。MySQL 的 GET_LOCK 是跨连接的全局锁，
	// 正好可以在多个实例同时启动时把迁移排队。
	if err := runMigrations(ctx, db); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	return db, nil
}

// migrationLockName 是迁移期间持有的 MySQL 命名锁。
const migrationLockName = "luckyCloudFile_schema_migration"

// runMigrations 先占一个连接持锁，再完成所有迁移。
// MySQL 的 DDL 会隐式提交，事务包不住 AutoMigrate；
// 但 GET_LOCK 是连接级的，所以单独占住一个 *sql.Conn 来持锁，
// 迁移本身仍然走 GORM 的连接池。
func runMigrations(ctx context.Context, db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("get sql connection pool: %w", err)
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("reserve migration connection: %w", err)
	}
	defer conn.Close()

	var acquired int
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 60)", migrationLockName).Scan(&acquired); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if acquired != 1 {
		return fmt.Errorf("could not acquire migration lock %q within 60s", migrationLockName)
	}
	defer func() {
		// 释放锁用独立 context：即使调用方的 ctx 已经取消，也要把锁还回去。
		_, _ = conn.ExecContext(context.Background(), "DO RELEASE_LOCK(?)", migrationLockName)
	}()

	// 从 ent 换到 GORM 时，旧表结构和新表结构对不上。先做一次性清理。
	if err := dropLegacyEntSchema(db); err != nil {
		return fmt.Errorf("prepare legacy schema: %w", err)
	}

	// 存储策略必须最先迁移：users/entities 的新列要在回填之后才能收紧约束。
	if err := migrateStoragePolicies(db); err != nil {
		return fmt.Errorf("migrate storage policies: %w", err)
	}

	if err := autoMigrate(db); err != nil {
		return fmt.Errorf("migrate schema: %w", err)
	}

	// 把旧的“扁平文件列表”升级成目录树：每个用户一个根目录，旧文件挂到根目录下。
	if err := migrateDirectoryTree(db); err != nil {
		return fmt.Errorf("migrate directory tree: %w", err)
	}

	// 一次性回填容量账目。必须放在目录树迁移之后：回填按逻辑 File 统计，
	// 需要 files.kind 和 files.entity_id 已经是最终结构。
	if err := migrateQuotaAccounting(db); err != nil {
		return fmt.Errorf("migrate quota accounting: %w", err)
	}
	return nil
}

// Close 关闭底层连接池。
//
// *gorm.DB 自己不是 Closer，要先用 DB() 取出 database/sql 的连接池再关。
func Close(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// newLogger 配置 GORM 自己的 SQL 日志。
//
// IgnoreRecordNotFoundError 很重要：查不到记录在业务里是正常的 404，
// 不是故障。不关掉的话，每次用户访问不存在的文件都会打一条红色 ERROR。
func newLogger() logger.Interface {
	return logger.New(
		log.New(os.Stdout, "", log.LstdFlags),
		logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true,
			Colorful:                  false,
		},
	)
}

// autoMigrate 按外键依赖顺序建表：先建被引用的表，再建引用它们的表。
//
// AutoMigrate 只会新增列、索引和表，不会删列改类型，
// 所以它适合当前阶段快速迭代；真正上线前应换成分版本的 migration。
func autoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&model.StoragePolicy{},
		&model.User{},
		&model.Entity{},
		&model.File{},
		&model.AuthSession{},
		&model.UploadSession{},
	)
}

// localPolicyName 是初始化出来的本地策略的展示名。
// 已经存在同名配置键时不会覆盖它，所以运维后来改过名字也不会被启动覆盖。
const localPolicyName = "本地存储"

// migrateStoragePolicies 完成存储策略的分步迁移：
//
//  1. 建 storage_policies 表；
//  2. 保证 users/entities 表存在，但只给旧表补“可空”的新列；
//  3. 初始化本地策略；
//  4. 给已有 users/entities 回填这个策略；
//  5. 校验所有引用都有效；
//  6. 把新列收紧成 NOT NULL；
//  7. 交给 AutoMigrate 补索引和外键约束。
//
// 为什么不能只改 struct 直接 AutoMigrate：旧库里已经有 users/entities 行，
// 直接加 NOT NULL 列要么失败，要么把 0 写进去当默认值。先可空、回填、
// 再收紧，才能保证旧数据明确落到当前使用的本地目录。
func migrateStoragePolicies(db *gorm.DB) error {
	// 被 users/entities 引用的表必须先存在。
	if err := db.AutoMigrate(&model.StoragePolicy{}); err != nil {
		return fmt.Errorf("create storage_policies: %w", err)
	}

	// CreateTable 不会自己跳过已存在的表，所以先判断。
	// 新库按最终结构一次建好；旧库保持原样，后面只补可空列。
	// 顺序满足外键依赖，避免建表时引用还不存在的表。
	for _, value := range []any{
		&model.User{},
		&model.Entity{},
		&model.File{},
		&model.AuthSession{},
	} {
		if db.Migrator().HasTable(value) {
			continue
		}
		if err := db.Migrator().CreateTable(value); err != nil {
			return fmt.Errorf("create table for %T: %w", value, err)
		}
	}

	if err := addNullableStorageColumns(db); err != nil {
		return err
	}

	policyID, err := ensureStoragePolicy(
		db,
		model.LocalStorageConfigKey,
		localPolicyName,
		model.StorageTypeLocal,
	)
	if err != nil {
		return err
	}

	if err := backfillStoragePolicies(db, policyID); err != nil {
		return err
	}

	if err := validateStoragePolicyReferences(db); err != nil {
		return err
	}

	return tightenStorageColumnTypes(db)
}

// addNullableStorageColumns 只给“已经存在但还没有新列”的表补可空列。
//
// 新库由 CreateTable 直接按最终结构建表，这里的 hasColumn 会跳过。
func addNullableStorageColumns(db *gorm.DB) error {
	for _, column := range []struct {
		table  string
		name   string
		column string
	}{
		{"users", "default_storage_policy_id", "BIGINT NULL"},
		{"entities", "storage_policy_id", "BIGINT NULL"},
	} {
		exists, err := hasColumn(db, column.table, column.name)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		sql := fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN %s %s",
			column.table, column.name, column.column,
		)
		if err := db.Exec(sql).Error; err != nil {
			return fmt.Errorf("add %s.%s: %w", column.table, column.name, err)
		}
	}
	return nil
}

// ensureStoragePolicy 按 ConfigKey 找策略，没有才创建。
//
// 已经存在的策略一个字都不改：重复启动不会多建一行，也不会把
// 运维调整过的 Name/Type 覆盖回默认值。
func ensureStoragePolicy(db *gorm.DB, configKey, name, policyType string) (int, error) {
	var policy model.StoragePolicy
	err := db.Where("config_key = ?", configKey).First(&policy).Error
	if err == nil {
		return policy.ID, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, fmt.Errorf("query storage policy %q: %w", configKey, err)
	}

	policy = model.StoragePolicy{
		Name:      name,
		Type:      policyType,
		ConfigKey: configKey,
	}
	if err := db.Create(&policy).Error; err != nil {
		// 多个实例同时启动时，另一个实例可能刚插入成功。
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			if err := db.Where("config_key = ?", configKey).First(&policy).Error; err != nil {
				return 0, fmt.Errorf("reload storage policy %q: %w", configKey, err)
			}
			return policy.ID, nil
		}
		return 0, fmt.Errorf("create storage policy %q: %w", configKey, err)
	}
	return policy.ID, nil
}

// backfillStoragePolicies 把旧数据的空值指向初始本地策略。
//
// 只改 IS NULL 的行：已经显式设置过的默认策略和已经绑定位置的内容
// 不会被重复启动改回去。
func backfillStoragePolicies(db *gorm.DB, policyID int) error {
	if err := db.Exec(
		"UPDATE users SET default_storage_policy_id = ? WHERE default_storage_policy_id IS NULL",
		policyID,
	).Error; err != nil {
		return fmt.Errorf("backfill users.default_storage_policy_id: %w", err)
	}
	if err := db.Exec(
		"UPDATE entities SET storage_policy_id = ? WHERE storage_policy_id IS NULL",
		policyID,
	).Error; err != nil {
		return fmt.Errorf("backfill entities.storage_policy_id: %w", err)
	}
	return nil
}

// validateStoragePolicyReferences 确认回填后没有空值，也没有指向不存在策略的引用。
//
// 这一步必须在收紧 NOT NULL 和外键之前完成，这样数据有问题时能给出明确错误，
// 而不是让 ALTER TABLE 抛一句看不懂的数据库异常。
func validateStoragePolicyReferences(db *gorm.DB) error {
	checks := []struct {
		table string
		query string
	}{
		{
			table: "users",
			query: `SELECT COUNT(*) FROM users u
				LEFT JOIN storage_policies p ON p.id = u.default_storage_policy_id
				WHERE u.default_storage_policy_id IS NULL OR p.id IS NULL`,
		},
		{
			table: "entities",
			query: `SELECT COUNT(*) FROM entities e
				LEFT JOIN storage_policies p ON p.id = e.storage_policy_id
				WHERE e.storage_policy_id IS NULL OR p.id IS NULL`,
		},
	}
	for _, check := range checks {
		var invalid int64
		if err := db.Raw(check.query).Scan(&invalid).Error; err != nil {
			return fmt.Errorf("validate %s storage policy references: %w", check.table, err)
		}
		if invalid > 0 {
			return fmt.Errorf(
				"%d row(s) in %s do not reference an existing storage policy",
				invalid, check.table,
			)
		}
	}
	return nil
}

// tightenStorageColumnTypes 把已经回填好的可空列收紧成 NOT NULL。
//
// 已经是 NOT NULL 时跳过，所以重复启动不会做无意义的 ALTER TABLE。
func tightenStorageColumnTypes(db *gorm.DB) error {
	for _, column := range []struct {
		table string
		name  string
		typ   string
	}{
		{"users", "default_storage_policy_id", "BIGINT NOT NULL"},
		{"entities", "storage_policy_id", "BIGINT NOT NULL"},
	} {
		nullable, err := isColumnNullable(db, column.table, column.name)
		if err != nil {
			return err
		}
		if !nullable {
			continue
		}
		sql := fmt.Sprintf(
			"ALTER TABLE %s MODIFY COLUMN %s %s",
			column.table, column.name, column.typ,
		)
		if err := db.Exec(sql).Error; err != nil {
			return fmt.Errorf("tighten %s.%s: %w", column.table, column.name, err)
		}
	}
	return nil
}

// isColumnNullable 返回 information_schema 里的 IS_NULLABLE。
// 列不存在时返回 false，让调用方把“已经不存在”当成不需要收紧。
func isColumnNullable(db *gorm.DB, table, column string) (bool, error) {
	var nullable string
	err := db.Raw(`
		SELECT IS_NULLABLE FROM information_schema.columns
		WHERE table_schema = DATABASE()
		  AND table_name = ?
		  AND column_name = ?`, table, column).
		Row().Scan(&nullable)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("check nullability of %s.%s: %w", table, column, err)
	}
	return nullable == "YES", nil
}

// errRootAlreadySet 表示另一个进程已经给这个用户补过根目录。
// 它只用于回滚自己刚创建的那条多余记录，不是真正的错误。
var errRootAlreadySet = errors.New("user already has a root directory")

// migrateDirectoryTree 把上一版的“文件平铺在 parent='/' 下”升级成目录树。
//
// 上一版每条 files 记录都只有 parent 字符串（固定 "/"），没有 kind 和 parent_id。
// 升级分四步：
//  1. 旧数据里同一用户有重名文件就先报错；然后删掉 parent 列
//     （它是 NOT NULL 且没有默认值，不删掉会挡住新插入）
//  2. 给还没有根目录的用户补建一个 name="/"、kind=directory 的 File
//  3. 把旧文件挂到这个用户的根目录下
//  4. 确认没有普通文件还留在 parent_id=NULL
//
// 整个函数是幂等的：parent 列不存在时跳过第 1 步，第 2～4 步每次启动都可以安全重跑。
func migrateDirectoryTree(db *gorm.DB) error {
	legacy, err := hasColumn(db, "files", "parent")
	if err != nil {
		return err
	}
	if legacy {
		// 旧 files 表里同一用户可能有两个同名文件；挂到同一个根目录后
		// 会撞唯一索引。先查出来，给一个能看懂的错，而不是一句 MySQL 1062。
		if err := ensureNoDuplicateRootNames(db); err != nil {
			return err
		}
		// 旧 entity_id 是 NOT NULL；目录没有 Entity，必须允许 NULL。
		if err := db.Exec("ALTER TABLE files MODIFY COLUMN entity_id BIGINT NULL").Error; err != nil {
			return fmt.Errorf("make files.entity_id nullable: %w", err)
		}
		if err := db.Exec("ALTER TABLE files DROP COLUMN parent").Error; err != nil {
			return fmt.Errorf("drop legacy parent column: %w", err)
		}
	}

	if err := ensureUserRoots(db); err != nil {
		return err
	}

	// 无条件跑一次：正常情况下没有需要挂载的文件，这是空操作；
	// 如果上次迁移在“删掉 parent 列”之后、挂载之前崩了，这次可以接着修好。
	if err := attachLegacyFilesToRoot(db); err != nil {
		return err
	}

	// 兜底：所有普通文件都必须有父目录，只有根目录可以为 NULL。
	var orphans int64
	if err := db.Model(&model.File{}).
		Where("kind = ? AND parent_id IS NULL", model.KindFile).
		Count(&orphans).Error; err != nil {
		return fmt.Errorf("count files without parent: %w", err)
	}
	if orphans > 0 {
		return fmt.Errorf("%d files still have no parent directory after migration", orphans)
	}
	return nil
}

// ensureUserRoots 给每个还没有根目录的用户补建一个。
//
// 只在启动时跑一次（幂等），不在每次请求里创建。
func ensureUserRoots(db *gorm.DB) error {
	var users []model.User
	if err := db.Select("id").Where("root_file_id IS NULL").Find(&users).Error; err != nil {
		return fmt.Errorf("query users without root directory: %w", err)
	}

	for _, user := range users {
		if err := createUserRoot(db, user.ID); err != nil && !errors.Is(err, errRootAlreadySet) {
			return fmt.Errorf("create root directory for user %d: %w", user.ID, err)
		}
	}
	return nil
}

// createUserRoot 在一个事务里插入根目录，并把 ID 写回 users.root_file_id。
func createUserRoot(db *gorm.DB, ownerID int) error {
	rootID, err := idgen.New()
	if err != nil {
		return err
	}

	return db.Transaction(func(tx *gorm.DB) error {
		root := model.File{
			ID:      rootID,
			Name:    "/",
			Kind:    model.KindDirectory,
			OwnerID: ownerID,
		}
		if err := tx.Create(&root).Error; err != nil {
			return fmt.Errorf("insert root directory: %w", err)
		}

		// 带 root_file_id IS NULL 条件，多个进程同时启动时只有一个能成功。
		result := tx.Model(&model.User{}).
			Where("id = ? AND root_file_id IS NULL", ownerID).
			Update("root_file_id", rootID)
		if result.Error != nil {
			return fmt.Errorf("set user root_file_id: %w", result.Error)
		}
		if result.RowsAffected == 0 {
			// 回滚事务，把自己多建的那条根目录一起撤销。
			return errRootAlreadySet
		}
		return nil
	})
}

// attachLegacyFilesToRoot 把所有还没有 parent_id 的旧文件挂到各自用户的根目录。
func attachLegacyFilesToRoot(db *gorm.DB) error {
	result := db.Exec(`
		UPDATE files f
		JOIN users u ON u.id = f.owner_id
		SET f.parent_id = u.root_file_id
		WHERE f.parent_id IS NULL AND f.kind = ?`, model.KindFile)
	if result.Error != nil {
		return fmt.Errorf("attach legacy files to their root directory: %w", result.Error)
	}
	return nil
}

// ensureNoDuplicateRootNames 检查旧数据里同一用户是否已有重名文件。
//
// 旧结构没有唯一约束，重名是允许的；新结构要求同一目录内不能重名，
// 所以迁移前必须先暴露出来，让人决定保留哪一个。
func ensureNoDuplicateRootNames(db *gorm.DB) error {
	type duplicate struct {
		OwnerID int
		Name    string
		Total   int64
	}
	var duplicates []duplicate
	if err := db.Raw(`
		SELECT owner_id, name, COUNT(*) AS total
		FROM files
		GROUP BY owner_id, name
		HAVING COUNT(*) > 1
		LIMIT 1`).Scan(&duplicates).Error; err != nil {
		return fmt.Errorf("check duplicate file names: %w", err)
	}
	if len(duplicates) > 0 {
		d := duplicates[0]
		return fmt.Errorf(
			"user %d has %d files named %q; rename or remove duplicates before upgrading",
			d.OwnerID, d.Total, d.Name,
		)
	}
	return nil
}

// hasColumn 判断表里有没有某一列。information_schema 对所有 MySQL 版本都适用。
func hasColumn(db *gorm.DB, table, column string) (bool, error) {
	var count int
	err := db.Raw(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE()
		  AND table_name = ?
		  AND column_name = ?`, table, column).
		Row().Scan(&count)
	if err != nil {
		return false, fmt.Errorf("check column %s.%s: %w", table, column, err)
	}
	return count > 0, nil
}

// dropLegacyEntSchema 清理 ent 时代留下的旧表。
//
// 为什么需要它：ent 生成的 files 表用的是 file_entity 列，GORM 用的是 entity_id。
// 如果直接对旧表 AutoMigrate，旧列 file_entity 是 NOT NULL 且没有默认值，
// 新代码插入时又不会填它，结果就是每条上传都失败。
// 同时还会留下两套外键和索引。所以这里做一次性切换：
//
//   - 只有确认 files 还是旧结构、而且里面没有数据时才动手；
//   - 有数据就报错退出，交给人手工迁移，绝不静默丢文件记录；
//   - 只删 files / entities / auth_sessions，users 表原样保留；
//   - auth_sessions 只是登录态，删掉后用户重新登录一次即可。
func dropLegacyEntSchema(db *gorm.DB) error {
	var legacy int
	err := db.Raw(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE()
		  AND table_name = 'files'
		  AND column_name = 'file_entity'`).
		Row().Scan(&legacy)
	if err != nil {
		return fmt.Errorf("detect legacy ent schema: %w", err)
	}
	if legacy == 0 {
		// 新库，或者已经迁移过。
		return nil
	}

	var rows int64
	if err := db.Table("files").Count(&rows).Error; err != nil {
		return fmt.Errorf("count legacy files: %w", err)
	}
	if rows > 0 {
		return fmt.Errorf(
			"legacy ent files table still has %d rows; migrate them before switching to GORM",
			rows,
		)
	}

	// 删除顺序满足外键：files 引用 entities，先删 files。
	// entities 为空、auth_sessions 只是登录态，删除不会丢用户的账号。
	for _, table := range []string{"files", "entities", "auth_sessions"} {
		if err := db.Exec("DROP TABLE IF EXISTS " + table).Error; err != nil {
			return fmt.Errorf("drop legacy table %s: %w", table, err)
		}
	}
	return nil
}

// mysqlConfig 把配置里的数据库字段转成 go-sql-driver 的 Config。
//
// 用结构体而不是手拼 DSN，密码里有 @、/ 之类的字符也不会解析错。
func mysqlConfig(cfg config.Database) *mysqlDriver.Config {
	mc := mysqlDriver.NewConfig()
	mc.User = cfg.User
	mc.Passwd = cfg.Password
	mc.Net = "tcp"
	mc.Addr = net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	mc.DBName = cfg.Name
	// 让 MySQL 的 DATETIME 直接扫成 time.Time。
	mc.ParseTime = true
	mc.Loc = time.Local
	// 连接建立时会执行 SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci，
	// 保证中文文件名不会变成问号。
	mc.Collation = "utf8mb4_unicode_ci"
	return mc
}
