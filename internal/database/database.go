// Package database 负责打开 MySQL 连接、检查是否连得上，并创建表结构。
//
// 它把“怎么连数据库”的细节集中在一个地方：
// 上层只拿到一个 *ent.Client，不需要知道连接串、驱动、迁移这些事。
package database

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	mysqlDriver "github.com/go-sql-driver/mysql"

	"luckyCloudFile/internal/config"
	"luckyCloudFile/internal/ent"
)

// Open 根据配置里的 [Database] 打开 MySQL。
//
// 它会做三件事：
//  1. 用 mysql.Config 建立连接池（密码里有特殊字符也不会拼错）
//  2. Ping 一次，确认数据库真的连得上（而不是等到第一个请求才报错）
//  3. 自动建表 / 改表（P2 用 AutoMigrate，P3 会换成迁移文件）
func Open(ctx context.Context, cfg config.Database) (*ent.Client, error) {
	connector, err := mysqlDriver.NewConnector(mysqlConfig(cfg))
	if err != nil {
		return nil, fmt.Errorf("build mysql connector: %w", err)
	}

	// OpenDB 拿到的是 database/sql 连接池；entsql.OpenDB 把包装成 ent 的驱动。
	drv := entsql.OpenDB(dialect.MySQL, sql.OpenDB(connector))
	return finishOpen(ctx, drv)
}

// OpenDSN 用现成的 DSN 打开数据库。集成测试直接用它。
func OpenDSN(ctx context.Context, dsn string) (*ent.Client, error) {
	drv, err := entsql.Open(dialect.MySQL, dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	return finishOpen(ctx, drv)
}

// finishOpen 负责“连接检查 + 建表”，Open 和 OpenDSN 共用。
func finishOpen(ctx context.Context, drv *entsql.Driver) (*ent.Client, error) {
	// 连接是懒加载的，Ping 才会真的去连。给 5 秒，避免启动卡死。
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := drv.DB().PingContext(pingCtx); err != nil {
		_ = drv.Close()
		return nil, fmt.Errorf("connect mysql: %w", err)
	}

	client := ent.NewClient(ent.Driver(drv))
	// 自动建表。P2 为了少写代码先这样做，数据模型稳定后再换成 migration。
	if err := client.Schema.Create(ctx); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("migrate schema: %w", err)
	}
	return client, nil
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
	// 让 MySQL 的 DATETIME 直接扫成 time.Time（ent 需要）。
	mc.ParseTime = true
	mc.Loc = time.Local
	// 连接建立时会执行 SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci，
	// 保证中文文件名不会变成问号。
	mc.Collation = "utf8mb4_unicode_ci"
	return mc
}
