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

// 存储策略的驱动类型：本地目录或 S3 兼容对象存储。
const (
	StorageTypeLocal = "local"
	StorageTypeS3    = "s3"

	// LocalStorageConfigKey 是初始本地策略对应的配置键：
	// 它映射到 [Storage] LocalRoot，也是新账号的默认策略。
	LocalStorageConfigKey = "local"
)

// StoragePolicy 是一个具体的存储位置。
//
// 它只描述驱动和配置键，不保存目录或云端密钥；实际连接信息由配置提供。
// 这样数据库里的策略 ID 在重启后仍然稳定。
type StoragePolicy struct {
	ID int `gorm:"primaryKey;autoIncrement"`

	// 展示名称，便于运维识别，不参与定位。
	Name string `gorm:"type:varchar(255);not null"`

	// 驱动类型：local 或 s3。
	Type string `gorm:"type:varchar(32);not null"`

	// 对应哪一组存储配置。本地目录或 S3 连接信息均由配置提供。
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

	// 逻辑容量账目，规则见 internal/quota。
	//
	//	QuotaBytes    总配额。0 表示没有可用容量，不表示无限。
	//	UsedBytes     已经发布、仍然存在的逻辑文件大小之和。
	//	ReservedBytes 未完成上传会话预留的大小之和。
	//
	// 任何时刻都必须满足 UsedBytes >= 0、ReservedBytes >= 0、
	// UsedBytes+ReservedBytes <= QuotaBytes。三个字段只能由
	// internal/quota 在持有用户行锁的事务里修改。
	QuotaBytes    int64 `gorm:"not null;default:0"`
	UsedBytes     int64 `gorm:"not null;default:0"`
	ReservedBytes int64 `gorm:"not null;default:0"`

	// 只读关联。删除仍被账号引用的策略会被数据库拒绝（RESTRICT）。
	DefaultStoragePolicy *StoragePolicy `gorm:"foreignKey:DefaultStoragePolicyID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:RESTRICT"`
}

// TableName 显式指定表名，避免依赖 GORM 的复数化规则。
func (User) TableName() string { return "users" }

// Entity 的生命周期状态。
//
//	uploading      上传会话的占位内容：字节还没接收完，还没有 File 引用。
//	active         正式内容：允许被 File 引用，内容正常可达。
//	pending_delete 引用数已经归零，等待物理清理；这个状态不能再被引用。
//
// 占位实体和待清理实体的 ref_count 都是 0，区别只在状态：
// uploading 之后会变成 active（上传完成），pending_delete 之后会被删除。
const (
	EntityStatusUploading     = "uploading"
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

	// 可信内容摘要：SHA-256 十六进制（64 个小写字符）。
	// NULL 表示历史内容还没有计算完整摘要；它只能由服务端实际读取内容后填写，
	// 不能把客户端提交的 hash 直接写进来。
	ContentSHA256 *string `gorm:"column:content_sha256;type:char(64)"`

	// 这份内容实际存放在哪个策略对应的位置。
	// 下载和清理都以它为准，不看用户当前的默认策略。
	StoragePolicyID int `gorm:"not null;index"`

	// 只读关联。删除仍被内容引用的策略会被数据库拒绝（RESTRICT）。
	StoragePolicy *StoragePolicy `gorm:"foreignKey:StoragePolicyID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:RESTRICT"`

	// 有多少个 File 引用这份内容。删除 File 时减一，减到 0 才进入
	// pending_delete，等待物理清理。
	//
	// 这里刻意不写 default:1：GORM 会把“带 default 的零值”当成没有设置，
	// 于是 Create 时 ref_count=0 会被数据库默认值改成 1。上传占位实体
	// 必须真正保存 0，所以所有创建路径都显式写这个字段：
	// 正式实体写 1，占位实体写 0。
	RefCount int `gorm:"not null"`

	// 生命周期状态：uploading 是上传占位；active 可以被引用；
	// pending_delete 表示 ref_count 已归零、正在等待物理清理，禁止再被引用。
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

// UploadSession 的状态。转换规则不在模型里，见 internal/upload。
//
//	pending → receiving → ready → completed
//	             ↓
//	          pending（写入失败且已停止，可以重新上传）
//
// pending/receiving/ready 都可以变成 canceled 或 expired；
// 一旦 completed 就不能再取消或过期。canceled / expired 都是终态，
// 保留各自的原因，只继续推进残留内容的物理清理。
//
// expired 表示“这次上传已经失效”，原因有两种：超过 expires_at，
// 或者服务重启后启动恢复把重启前的未完成会话统一作废。
// 所以 expired 会话的 expires_at 完全可能还在未来，判断是否还能继续
// 上传不能只看时间，必须先看状态。
const (
	UploadStatusPending   = "pending"
	UploadStatusReceiving = "receiving"
	UploadStatusReady     = "ready"
	UploadStatusCompleted = "completed"
	UploadStatusCanceled  = "canceled"
	UploadStatusExpired   = "expired"
)

// UploadSession 的两种上传模式。
//
//	full     整文件上传：请求一次 /content 写完整个文件，保持原有行为。
//	chunked  分片上传：按固定大小逐片写入，最后一片完成后会话变成 ready。
//
// Mode 为空表示旧数据（迁移前的会话），业务层按 full 处理。
const (
	UploadModeFull    = "full"
	UploadModeChunked = "chunked"
)

// UploadSession 是一次上传的持久化状态。
//
// 它把“客户端说要传什么”和“服务端已经收到多少”分开记录：
// 字节本身放在 Entity 占位内容里，会话记录只保存进度和最终结果。
// 会话 ID 用随机字符串，和 File 一样对外可见。
type UploadSession struct {
	ID string `gorm:"type:varchar(255);primaryKey"`

	// 上传用户。查询会话时必须同时匹配它，不能只凭 ID 取别人的会话。
	OwnerID int `gorm:"not null;index:idx_upload_sessions_owner_status,priority:1"`

	// 目标目录和最终文件名。ParentID 指向 files 表里的目录。
	ParentID string `gorm:"type:varchar(255);not null;index"`
	Name     string `gorm:"type:varchar(255);not null"`

	// 对应的占位实体：状态 uploading、ref_count=0。
	// 一个会话只对应一个占位实体，所以这里加唯一索引。
	EntityID int64 `gorm:"not null;uniqueIndex"`

	// 客户端声明的总大小和实际已经收到的大小，单位字节。
	// chunked 模式下 ActualSize 是账本里不重复分片的字节数之和，
	// 分片可以乱序到达；状态变成 ready 时，两者必须相等（由业务层检查）。
	ExpectedSize int64 `gorm:"not null"`
	ActualSize   int64 `gorm:"not null"`

	// 上传模式：full（整文件）或 chunked（分片）。
	// 旧会话和旧请求没有这个字段，数据库默认成 full，行为保持不变。
	Mode string `gorm:"type:varchar(16);not null;default:full"`

	// 分片大小，单位字节。full 模式固定为 0；chunked 模式由服务端决定，
	// 客户端不能指定。分片数由 ExpectedSize 和它计算，不单独落库。
	ChunkSize int64 `gorm:"not null;default:0"`

	// 本会话还没有结算或释放的预留量。
	//
	// 它是释放容量的唯一依据：取消、过期、重启失效时按这个字段的值
	// 从 users.reserved_bytes 里扣除，扣完清零。绝不能按 ExpectedSize
	// 重新扣一遍——会话可能已经被结算或释放过，那样会重复扣减。
	// 空文件会话的预留量是 0。
	ReservedBytes int64 `gorm:"not null;default:0"`

	// 会话状态，取值见上面的 UploadStatus* 常量。
	Status string `gorm:"type:varchar(16);not null;default:pending;index:idx_upload_sessions_owner_status,priority:2"`

	// 过期时间。正常情况下，未完成的会话超过这个时间后由到期扫描变成 expired。
	// 服务重启时，启动恢复会把重启前的未完成会话提前置为 expired，
	// 此时 expires_at 可能还没到，客户端不能只凭这个字段判断能否继续上传。
	ExpiresAt time.Time `gorm:"not null;index"`

	// 完成后生成的文件 ID。只有 completed 状态才有值；
	// 重复确认完成时直接返回它，不再创建新文件。
	ResultFileID *string `gorm:"type:varchar(255);uniqueIndex"`

	// TaskID 非空表示这个会话是某个可恢复任务创建的发布会话。
	// 它用于识别“会话创建成功但检查点还没提交”的占位和预留，
	// 不是“一个任务只能有一个会话”的唯一键：失败的会话可能已经取消。
	TaskID *string `gorm:"type:varchar(255);index"`
}

func (UploadSession) TableName() string { return "upload_sessions" }

// Task 的生命周期状态。转换规则不在模型里，见 internal/task：
//
//	queued → running → completed
//	              ├→ retry_wait → running
//	              └→ failed
//
// queued 还没有取得执行资格；retry_wait 是等退避到期，同样不占 worker；
// completed / failed 是终态，不再被调度。
const (
	TaskStatusQueued    = "queued"
	TaskStatusRunning   = "running"
	TaskStatusRetryWait = "retry_wait"
	TaskStatusCompleted = "completed"
	TaskStatusFailed    = "failed"
)

// Task 是一条可以跨重启恢复的后台任务。
//
// 它只保存“重建执行需要的数据”，不保存 Job 对象、函数、Token、
// context、io.Reader 或锁令牌：这些东西重启后无法还原，也不该落库。
// 真正的执行器从 tasks 行重建任务，内存里的队列快照只是运行期缓存。
//
// state_json 的内容由 Type 对应的业务解释，仓储层只保证原样保存和读回；
// state_version 用来拒绝当前代码不认识的旧格式，避免按零值误恢复。
type Task struct {
	// 服务端生成的公开 ID。提交响应和后续查询都用它。
	// 恢复任务时必须复用原 ID，不能重新生成。
	ID string `gorm:"type:varchar(255);primaryKey"`

	// 任务类型，决定用哪个重建工厂解释 StateJSON。未知类型不执行。
	Type string `gorm:"type:varchar(64);not null;index"`

	// 任务所有者（users.id）。面向用户的查询必须同时匹配它。
	OwnerID int `gorm:"not null;index:idx_tasks_owner_created,priority:1"`

	// 所有者。删除用户时一并删除他的任务；任务本身不应该比账号活得更久。
	Owner *User `gorm:"foreignKey:OwnerID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE"`

	// 通用执行状态，取值见 TaskStatus*。
	Status string `gorm:"type:varchar(16);not null;default:queued;index:idx_tasks_due,priority:1;index:idx_tasks_lease,priority:1"`

	// 下一步业务阶段，例如 prepare / compress / publish / finalize。
	// 它是“接下来做什么”，不是进度百分比：phase=publish 表示完整的
	// 中间产物已经提交，不是“压缩完成了百分之多少”。
	Phase string `gorm:"type:varchar(32);not null;default:''"`

	// 检查点数据的格式版本和内容。仓储层不解释内容，只按原样存取。
	StateVersion int    `gorm:"not null;default:1"`
	StateJSON    string `gorm:"type:longtext;not null"`

	// 已经取得执行资格的次数，以及总次数上限。
	// 每次 TryStart 成功都会加一，崩溃恢复也受这个上限约束。
	Attempts    int `gorm:"not null;default:0"`
	MaxAttempts int `gorm:"not null;default:3"`

	// 最早可执行时间。retry_wait 用它保存退避到期时间，
	// 到期扫描和 TryStart 都按它过滤。
	NextRunAt time.Time `gorm:"not null;index:idx_tasks_due,priority:2"`

	// WorkerID 是最近一次领取这条任务的 worker 实例标识。
	// 空表示还没有被任何 worker 领取：本地调度器执行的任务、旧数据
	// 或者已经释放执行资格、等待重试的任务。
	WorkerID string `gorm:"type:varchar(128);not null;default:''"`

	// LeaseUntil 是本次执行的租约到期时间，按数据库时钟计算。
	//
	// 非空且已经过期，表示领取它的 worker 失联，其他实例可以接管
	// （attempts 加一，worker_id 换成新实例）。完成、失败或进入重试等待
	// 时清空，避免把“已释放的资格”误判成“还在执行”。
	LeaseUntil *time.Time `gorm:"index:idx_tasks_lease,priority:2"`

	// 成功发布后的结果文件 ID。失败或尚未发布时为 NULL。
	// 它是查询用的投影；发布幂等的真正凭据是 task_results 行。
	ResultFileID *string `gorm:"type:varchar(255)"`

	// ResultJSON 是任务成功后的业务结果投影（例如内容校验得到的摘要）。
	//
	// 它只用于查询和演示，不是幂等凭据：真正的业务事实写在自己的表里
	// （例如 entities.content_sha256 和 content_keys）。失败或未完成时为空。
	ResultJSON string `gorm:"type:text;not null"`

	// 最后一次失败或等待重试的原因，只用于查询和排查，不保存堆栈。
	LastError string `gorm:"type:text;not null"`

	// 是否还有临时资源（任务工作目录、部分产物等）需要清理。
	// 它和 Status 分开：任务可以已经 completed，但清理还没做完。
	CleanupPending bool `gorm:"not null;default:false"`

	// 创建、最后更新、进入终态的时间。完成时间在非终态为 NULL。
	CreatedAt  time.Time  `gorm:"not null;autoCreateTime;index:idx_tasks_owner_created,priority:2"`
	UpdatedAt  time.Time  `gorm:"not null;autoUpdateTime"`
	FinishedAt *time.Time `gorm:"index"`
}

func (Task) TableName() string { return "tasks" }

// TaskResult 是任务发布结果的幂等凭据。
//
// 它在 CompleteUploadSession 的同一个事务里和 File、Entity、容量结算一起
// 提交：要么“文件已经发布且结果关联存在”，要么两者都不存在。
//
// 它刻意不对 files 建外键：用户后来删掉结果文件时，这条凭据仍然保留，
// 旧任务恢复后只能复用或收敛这个结果，不能重新发布一份新文件。
type TaskResult struct {
	// TaskID 是主键：一个任务最多对应一份逻辑结果。
	TaskID string `gorm:"type:varchar(255);primaryKey"`

	// 结果属于哪个用户，查询时必须同时匹配，避免用别人的任务 ID 探测结果。
	OwnerID int `gorm:"not null;index"`

	// 发布后的 File ID。它可能已经被用户删除，不能据此重新创建文件。
	FileID string `gorm:"type:varchar(255);not null;index"`

	CreatedAt time.Time `gorm:"not null;autoCreateTime"`
}

func (TaskResult) TableName() string { return "task_results" }

// ContentKey 把“当前用户 + 存储策略 + 内容摘要 + 大小”映射到一份可复用内容。
//
// 第一版只做同一用户、同一存储策略内的去重，不跨用户或跨策略合并：
// 这样不需要全局文件存在性探测，也不会把别人的内容授权给当前用户。
//
// 唯一键 (owner_id, storage_policy_id, sha256, size) 是并发发布的最终兜底。
// EntityID 指向真正可复用的 Entity；物理 StorageKey 仍然是独立随机键，
// 不能直接用摘要当文件名，否则旧 GC 可能删到新一代对象。
type ContentKey struct {
	ID int64 `gorm:"primaryKey;autoIncrement"`

	OwnerID int `gorm:"not null;uniqueIndex:idx_content_keys_scope,priority:1"`

	StoragePolicyID int `gorm:"not null;uniqueIndex:idx_content_keys_scope,priority:2"`

	// 服务端计算出的完整内容摘要，小写十六进制。
	SHA256 string `gorm:"column:sha256;type:char(64);not null;uniqueIndex:idx_content_keys_scope,priority:3"`

	Size int64 `gorm:"not null;uniqueIndex:idx_content_keys_scope,priority:4"`

	// 当前作用域下应该复用的 Entity。
	EntityID int64 `gorm:"not null;index"`
}

func (ContentKey) TableName() string { return "content_keys" }

// InstantUploadRequest 是秒传请求的幂等凭据。
//
// 同一用户、同一 Idempotency-Key 只对应一次逻辑操作：
//   - ResultFileID 非空：重试返回同一个文件，不重复创建、不重复计费；
//   - ResultFileID 为空：第一次请求未命中，重试仍然返回未命中。
//
// RequestDigest 是请求参数（目录、名字、大小、摘要）的规范化摘要；
// 同键不同参数必须拒绝，不能悄悄返回上一次的结果。
type InstantUploadRequest struct {
	ID int64 `gorm:"primaryKey;autoIncrement"`

	OwnerID int `gorm:"not null;uniqueIndex:idx_instant_upload_requests_owner_key,priority:1"`

	IdempotencyKey string `gorm:"type:varchar(255);not null;uniqueIndex:idx_instant_upload_requests_owner_key,priority:2"`

	RequestDigest string `gorm:"column:request_digest;type:char(64);not null"`

	// 命中时创建的文件 ID；未命中时为 NULL。
	ResultFileID *string `gorm:"type:varchar(255)"`

	CreatedAt time.Time `gorm:"not null;autoCreateTime"`
}

func (InstantUploadRequest) TableName() string { return "instant_upload_requests" }
