// Package model 定义数据库表结构（GORM 模型）。
//
// 这里只放“表长什么样”，不放业务规则：
// 查询条件、事务顺序、所有权校验都在 internal/auth 和 internal/file。
package model

import "time"

// File 的两种节点类型。
//
// 目录也是 files 表里的一行：它有名字、有所有者、有父目录，但没有 Entity。
// 这样文件和目录共用同一套树结构，不用再建一张目录表。
const (
	KindFile      = "file"
	KindDirectory = "directory"
)

// 存储策略的驱动类型。本阶段只实现本地目录。
const (
	StorageTypeLocal = "local"

	// LocalStorageConfigKey 是初始本地策略对应的配置键：
	// 它映射到 [Storage] LocalRoot，也是新账号的默认策略。
	LocalStorageConfigKey = "local"
)

// StoragePolicy 是一个具体的存储位置。
//
// 它只描述“用哪种驱动、去哪一组配置里找目录”，不保存目录本身；
// 目录由配置按 ConfigKey 提供。这样数据库里的策略 ID 在重启后仍然稳定。
type StoragePolicy struct {
	ID int `gorm:"primaryKey;autoIncrement"`

	// 展示名称，便于运维识别，不参与定位。
	Name string `gorm:"type:varchar(255);not null"`

	// 驱动类型：本阶段只有 local，以后接 S3 时再扩展。
	Type string `gorm:"type:varchar(32);not null"`

	// 对应哪一组存储配置。配置读取层用它在本地目录表里查找实际路径。
	ConfigKey string `gorm:"type:varchar(64);not null;uniqueIndex:storage_policies_config_key"`
}

func (StoragePolicy) TableName() string { return "storage_policies" }

// User 是登录用户。本阶段不做注册，账号由开发初始化命令显式创建。
type User struct {
	ID int `gorm:"primaryKey;autoIncrement"`

	// 用户名全局唯一。登录时按它查找用户。
	//
	// 索引名固定写成 username，是为了兼容从 ent 迁移过来的旧表：
	// 旧表已经有这个唯一索引，GORM 看到同名索引就不会再建一个。
	Username string `gorm:"type:varchar(255);not null;uniqueIndex:username"`

	// bcrypt 生成的密码哈希。字符串里包含盐和代价参数，
	// 数据库里绝不保存用户输入的明文密码。
	PasswordHash string `gorm:"type:varchar(255);not null"`

	// 用户的根目录 File ID。客户端不传 parent_id 时，业务层用它作为默认父目录。
	// 根目录的 parent_id 是 NULL，它是整棵树的起点。
	RootFileID *string `gorm:"type:varchar(255);uniqueIndex"`

	// 服务器给这个账号分配的新上传位置。它不是用户选择，只在服务端调整。
	// 上传时读取一次，写入新 Entity，之后不再跟随用户默认值变化。
	DefaultStoragePolicyID int `gorm:"not null;index"`

	// 只读关联。删除仍被账号引用的策略会被数据库拒绝（RESTRICT）。
	DefaultStoragePolicy *StoragePolicy `gorm:"foreignKey:DefaultStoragePolicyID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:RESTRICT"`
}

// TableName 显式指定表名，避免依赖 GORM 的复数化规则。
func (User) TableName() string { return "users" }

// Entity 的清理状态。
//
//	active         允许被 File 引用，内容正常可达。
//	pending_delete 引用数已经归零，等待物理清理；这个状态不能再被引用。
//
// 本阶段只有这两个状态：删除事务提交后，磁盘对象可能还没来得及删。
// 更复杂的重试任务队列等真正需要时再加。
const (
	EntityStatusActive        = "active"
	EntityStatusPendingDelete = "pending_delete"
)

// Entity 是“实际存储的那一份内容”。
//
// File 和 Entity 分开，是为了以后能做内容去重：
// 同一个 Entity 可以被多个 File 引用，磁盘上只存一份。
type Entity struct {
	ID int64 `gorm:"primaryKey;autoIncrement"`

	// 磁盘上的文件名（当前是本机存储的 key）。
	// Unique 保证同一份内容不会被登记成两条 Entity。
	StorageKey string `gorm:"type:varchar(255);not null;uniqueIndex"`

	// 内容大小，单位字节。下载时用来设置 Content-Length。
	Size int64 `gorm:"not null"`

	// 这份内容实际存放在哪个策略对应的位置。
	// 下载和清理都以它为准，不看用户当前的默认策略。
	StoragePolicyID int `gorm:"not null;index"`

	// 只读关联。删除仍被内容引用的策略会被数据库拒绝（RESTRICT）。
	StoragePolicy *StoragePolicy `gorm:"foreignKey:StoragePolicyID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:RESTRICT"`

	// 有多少个 File 引用这份内容。删除 File 时减一，减到 0 才进入
	// pending_delete，等待物理清理。
	RefCount int `gorm:"not null;default:1"`

	// 清理状态：active 可以被引用；pending_delete 表示 ref_count 已归零、
	// 正在等待物理清理，禁止再被引用。
	Status string `gorm:"type:varchar(16);not null;default:active;index"`
}

func (Entity) TableName() string { return "entities" }

// File 是用户看到的逻辑节点：普通文件或目录。
//
// 它只记录“叫什么、放在哪、属于谁、内容是哪一份”，
// 真正的字节内容交给 Entity；目录没有 Entity。
type File struct {
	// 服务端生成的公开 ID。用字符串主键，上传时返回给客户端的就是它。
	ID string `gorm:"type:varchar(255);primaryKey"`

	// 用户看到的原始名字，只用于展示和下载时的 Content-Disposition。
	Name string `gorm:"type:varchar(255);not null;uniqueIndex:idx_files_owner_parent_name,priority:3"`

	// file 或 directory。老数据迁移时默认按 file 处理。
	Kind string `gorm:"type:varchar(16);not null;default:file"`

	// 父目录的 File ID。只有用户的根目录是 NULL。
	ParentID *string `gorm:"type:varchar(255);uniqueIndex:idx_files_owner_parent_name,priority:2"`

	// 内容外键。目录没有内容，所以这里是 NULL。
	EntityID *int64

	// 文件所有者的用户 ID。上传后不允许修改，防止文件被转到别的用户名下。
	OwnerID int `gorm:"not null;uniqueIndex:idx_files_owner_parent_name,priority:1"`

	// 只读关联：Preload("Entity") 时一次查出内容信息。
	// 写记录时只填 EntityID，不通过它做级联保存。
	Entity *Entity `gorm:"foreignKey:EntityID;references:ID"`

	// 父目录。自关联，形成一棵树。
	Parent *File `gorm:"foreignKey:ParentID;references:ID"`

	// 所有者。删除用户前必须先删掉他的文件（测试里也是这个顺序）。
	Owner *User `gorm:"foreignKey:OwnerID;references:ID"`
}

func (File) TableName() string { return "files" }

// AuthSession 是一次登录。数据库只保存 Token 的哈希，不保存原始 Token。
type AuthSession struct {
	ID int `gorm:"primaryKey;autoIncrement"`

	// 原始 Token 的 SHA-256 十六进制，唯一。
	TokenHash string `gorm:"type:varchar(255);not null;uniqueIndex"`

	ExpiresAt time.Time `gorm:"not null"`

	// 这次登录属于哪个用户。
	UserID int `gorm:"not null;index"`

	// 只读关联：鉴权时 Preload("User") 拿到用户 ID。
	User *User `gorm:"foreignKey:UserID;references:ID"`
}

func (AuthSession) TableName() string { return "auth_sessions" }
