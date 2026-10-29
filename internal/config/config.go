// Package config 只负责一件事：把 INI 配置文件读成程序可以直接使用的 Config 结构体。
//
// 它不启动 HTTP 服务、不连接数据库、也不创建目录。
// “读配置”这一步只做翻译和检查，不做任何实际动作，
// 这样配置写错时，程序会在启动前就明确报错，而不是跑到一半才出问题。
package config

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-ini/ini"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/storage"
)

// Config 是整份配置。
//
// 它的字段和 INI 文件里的 [Section] 一一对应：
//
//	[System]   → Config.System
//	[HTTP]     → Config.HTTP
//	[Upload]   → Config.Upload
//	[Storage]  → Config.Storage
//	[Database] → Config.Database
//
// 而每个结构体里的字段名，又和 Section 里的 key 一一对应（大小写不敏感）。
// 例如 HTTP.Listen 对应配置文件里的 [HTTP] Listen。
type Config struct {
	System   System
	HTTP     HTTP
	Upload   Upload
	Task     Task
	Worker   Worker
	Storage  Storage
	Database Database
}

// System 对应 [System]。
type System struct {
	LogLevel string // 日志级别：debug / info / warn / error
}

// Upload 对应 [Upload]。
type Upload struct {
	DirectMaxBytes int64
	// MaxParallelChunks 是单个分片会话同时处理多少个分片请求。
	// 它限制的是“每个会话”，不是整个进程：多个会话各自都可以达到这个上限。
	MaxParallelChunks int
}

// Task 对应 [Task]，是 ZIP 异步任务队列和持久化调度的参数。
//
// 内存和数据库各有自己的边界，两层都必须有界：
//   - Workers + QueueCapacity 限制同时存在的排队/执行任务；
//   - RetentionSeconds + MaxTerminalRecords 限制完成后的内存快照；
//   - MaxPendingTasks 限制数据库里未终态任务的积压；
//   - ScanIntervalSeconds + ScanBatchSize 决定通知丢失后多久能找回任务。
type Task struct {
	// Workers 是同时执行 ZIP 任务的 worker 数。
	Workers int
	// QueueCapacity 是等待队列长度，不包含正在执行的任务。
	QueueCapacity int
	// TimeoutSeconds 是单个任务的最长执行时间。
	TimeoutSeconds int
	// RetentionSeconds 是 completed / failed 记录在内存中保留的秒数。
	RetentionSeconds int
	// MaxTerminalRecords 是 completed + failed 记录的数量上限。
	// 超过时优先回收最早结束的记录。
	MaxTerminalRecords int

	// MaxPendingTasks 是数据库中未终态任务（queued / running / retry_wait）
	// 的总数上限。它限制的是持久化积压，不是内存队列容量：达到上限后
	// 新提交在落库之前就被拒绝。
	MaxPendingTasks int

	// ScanIntervalSeconds 是调度器周期扫描数据库中到期任务的间隔（秒）。
	// 即使提交后的唤醒通知丢失，任务也会在这个间隔内被重新发现。
	ScanIntervalSeconds int

	// ScanBatchSize 是单次扫描最多取回多少条到期任务。
	ScanBatchSize int

	// WorkDir 是任务工作目录：可恢复的 ZIP 产物和 .partial 都放在这里。
	// 相对路径以配置文件所在目录为基准。它不能依赖系统临时目录，
	// 否则重启或临时目录清理会让 phase=publish 指向的完整产物丢失。
	WorkDir string
}

// Worker 对应 [Worker]，是独立 worker 进程的参数。
//
// 这些值只影响领取循环，不影响 API 进程：worker 和 API 共用同一份配置
// 文件，但只有 cmd/worker 会读取这一段。
type Worker struct {
	// ID 是本实例写进 tasks.worker_id 的标识。为空时由启动参数或
	// “hostname-pid”生成；同一个数据库上的多个实例必须互不相同。
	ID string

	// Slots 是同时执行的任务数。worker 每次只领取空闲槽位数量，
	// 不会把任务领到本地排队等租约过期。
	Slots int

	// LeaseSeconds 是一次领取的租约时长。进程停止心跳后，任务要等
	// 这么久才会被其他实例接管。
	LeaseSeconds int

	// HeartbeatSeconds 是续租间隔，必须小于 LeaseSeconds。
	HeartbeatSeconds int

	// PollSeconds 是空闲时重新查询可领取任务的间隔。
	PollSeconds int
}

// HTTP 对应 [HTTP]。
type HTTP struct {
	Listen                 string // 监听地址，格式 host:port；端口写 0 表示让操作系统分配
	ShutdownTimeoutSeconds int    // 优雅关闭最多等多少秒
}

// Storage 对应 [Storage]。
type Storage struct {
	COS       map[string]storage.COSOptions `ini:"-"`
	LocalRoot string                        // 文件保存目录；相对路径以配置文件所在目录为基准

	// LocalRoots 是额外的本地目录，键对应 StoragePolicy.ConfigKey。
	// 它来自可选的 [StorageLocal] 段：
	//
	//	[StorageLocal]
	//	archive = ../data-archive
	//
	// LocalRoot 继续表示配置键 "local"，所以旧配置不需要改。
	LocalRoots map[string]string `ini:"-"`

	// S3 的键同样对应 StoragePolicy.ConfigKey；凭据仅从指定环境变量加载。
	S3 map[string]storage.S3Options `ini:"-"`
}

// RootFor 按 StoragePolicy.ConfigKey 找到实际的本地目录。
//
// "local" 始终优先使用 LocalRoot，保持对旧配置的兼容；
// 其它键只在 [StorageLocal] 里查。找不到时返回 false，
// 调用方必须明确报错，不能拿别的目录顶上。
func (s Storage) RootFor(configKey string) (string, bool) {
	if configKey == model.LocalStorageConfigKey && s.LocalRoot != "" {
		return s.LocalRoot, true
	}
	root, ok := s.LocalRoots[configKey]
	if !ok || root == "" {
		return "", false
	}
	return root, true
}

// Database 对应 [Database]。
//
// P2 开始，程序启动时会真的连接 MySQL（见 internal/database）。
// 这四个字段加上密码，会组成 go-sql-driver 的 DSN。
type Database struct {
	Host     string
	Port     int
	User     string
	Password string // 建议用环境变量 CLOUDREVE_DB_PASSWORD 覆盖，不要写进文件
	Name     string
}

// Load 读取配置文件的完整流程，按顺序是：
//
//  1. 先给所有字段填默认值
//  2. 打开配置文件
//  3. 把每个 [Section] 映射到对应的结构体
//  4. 用环境变量覆盖数据库密码
//  5. 校验各项取值是否合法
//  6. 把 Storage 的相对路径转成绝对路径
//
// 任何一步失败都会立刻返回错误，后面的步骤不再执行。
func Load(path string) (Config, error) {
	cfg := defaultConfig()

	file, err := ini.Load(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	if err := mapSections(file, &cfg); err != nil {
		return Config{}, err
	}

	applyEnvOverrides(&cfg)

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}

	if err := cfg.resolveStoragePath(path); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// defaultConfig 是所有字段的默认值。
// 配置文件里没写的项就保持这里的值，所以配置文件可以只写想改的那几项。
func defaultConfig() Config {
	return Config{
		System: System{LogLevel: "info"},
		HTTP:   HTTP{ShutdownTimeoutSeconds: 5},
		// 没有 [Upload] 段的旧配置也使用这个默认值。
		Upload: Upload{MaxParallelChunks: 2, DirectMaxBytes: 128 << 20},
		// 没有 [Task] 段的旧配置也使用这些默认值，和 queue 包的默认值一致。
		Task: Task{
			Workers:             2,
			QueueCapacity:       64,
			TimeoutSeconds:      600,
			RetentionSeconds:    3600,
			MaxTerminalRecords:  10000,
			MaxPendingTasks:     1000,
			ScanIntervalSeconds: 5,
			ScanBatchSize:       50,
			WorkDir:             "../data-tasks",
		},
		Worker: Worker{
			Slots:            2,
			LeaseSeconds:     30,
			HeartbeatSeconds: 10,
			PollSeconds:      2,
		},
		Storage:  Storage{LocalRoots: make(map[string]string), S3: make(map[string]storage.S3Options)},
		Database: Database{Port: 3306},
	}
}

// mapSections 把 INI 的每个 [Section] 复制进结构体。
//
// StrictMapTo 除了按字段名赋值，还会顺手做类型转换：
// 配置文件里的值都是字符串，而 Port 是 int，它负责把 "3306" 转成 3306，
// 转不了（比如写成 "nope"）就返回错误。
//
// 这里每个 Section 分开写，而不是用一个循环加反射：
// 段数很少，分开写时“配置文件的哪一段 → 哪个结构体”一眼就能对上。
func mapSections(file *ini.File, cfg *Config) error {
	if err := file.Section("System").StrictMapTo(&cfg.System); err != nil {
		return fmt.Errorf("parse [System]: %w", err)
	}
	if err := file.Section("HTTP").StrictMapTo(&cfg.HTTP); err != nil {
		return fmt.Errorf("parse [HTTP]: %w", err)
	}
	if err := file.Section("Upload").StrictMapTo(&cfg.Upload); err != nil {
		return fmt.Errorf("parse [Upload]: %w", err)
	}
	if err := file.Section("Task").StrictMapTo(&cfg.Task); err != nil {
		return fmt.Errorf("parse [Task]: %w", err)
	}
	if err := file.Section("Worker").StrictMapTo(&cfg.Worker); err != nil {
		return fmt.Errorf("parse [Worker]: %w", err)
	}
	if err := file.Section("Storage").StrictMapTo(&cfg.Storage); err != nil {
		return fmt.Errorf("parse [Storage]: %w", err)
	}
	if err := file.Section("Database").StrictMapTo(&cfg.Database); err != nil {
		return fmt.Errorf("parse [Database]: %w", err)
	}
	if err := mapStorageRoots(file, cfg); err != nil {
		return err
	}
	if value := os.Getenv("LCF_DIRECT_UPLOAD_MAX_BYTES"); value != "" {
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n <= 0 || n > 10000*(8<<20) {
			return fmt.Errorf("invalid LCF_DIRECT_UPLOAD_MAX_BYTES")
		}
		cfg.Upload.DirectMaxBytes = n
	}
	if os.Getenv("LCF_COS_ENABLED") == "true" {
		cfg.Storage.COS = map[string]storage.COSOptions{"cos": {BucketURL: os.Getenv("LCF_COS_BUCKET_URL"), Prefix: os.Getenv("LCF_COS_PREFIX"), SecretID: os.Getenv("LCF_COS_SECRET_ID"), SecretKey: os.Getenv("LCF_COS_SECRET_KEY"), SessionToken: os.Getenv("LCF_COS_SESSION_TOKEN")}}
		if _, err := storage.NewCOS(cfg.Storage.COS["cos"]); err != nil {
			return err
		}
	}
	return mapS3Storage(file, cfg)
}

// 每个 [StorageS3 CONFIG_KEY] 定义一个后端。INI 只保存凭据的环境变量名，
// 不支持把密钥直接写入配置；Load 仍然没有网络/建目录副作用。
func mapS3Storage(file *ini.File, cfg *Config) error {
	for _, section := range file.Sections() {
		if !strings.HasPrefix(section.Name(), "StorageS3 ") {
			continue
		}
		key := strings.TrimSpace(strings.TrimPrefix(section.Name(), "StorageS3 "))
		if key == "" || key == model.LocalStorageConfigKey {
			return fmt.Errorf("[%s]: invalid or reserved config key", section.Name())
		}
		if _, exists := cfg.Storage.LocalRoots[key]; exists {
			return fmt.Errorf("[%s]: config key also belongs to StorageLocal", section.Name())
		}
		if _, exists := cfg.Storage.S3[key]; exists {
			return fmt.Errorf("[%s]: duplicate S3 config key", section.Name())
		}
		var raw struct {
			Endpoint, Region, Bucket, Prefix, SpoolDir string
			ForcePathStyle                             bool
			AccessKeyIDEnv, SecretAccessKeyEnv         string
			SessionTokenEnv                            string
		}
		if err := section.StrictMapTo(&raw); err != nil {
			return fmt.Errorf("parse [%s]: %w", section.Name(), err)
		}
		if raw.AccessKeyIDEnv == "" || raw.SecretAccessKeyEnv == "" {
			return fmt.Errorf("[%s]: AccessKeyIDEnv and SecretAccessKeyEnv are required", section.Name())
		}
		opts := storage.S3Options{
			Endpoint: raw.Endpoint, Region: raw.Region, Bucket: raw.Bucket,
			Prefix: raw.Prefix, SpoolDir: raw.SpoolDir, ForcePathStyle: raw.ForcePathStyle,
			AccessKeyID: os.Getenv(raw.AccessKeyIDEnv), SecretAccessKey: os.Getenv(raw.SecretAccessKeyEnv),
		}
		if raw.SessionTokenEnv != "" {
			opts.SessionToken = os.Getenv(raw.SessionTokenEnv)
			if opts.SessionToken == "" {
				return fmt.Errorf("[%s]: session token environment variable is empty", section.Name())
			}
		}
		if err := opts.Validate(); err != nil {
			return fmt.Errorf("[%s]: %w", section.Name(), err)
		}
		cfg.Storage.S3[key] = opts
	}
	return nil
}

// mapStorageRoots 读取可选的 [StorageLocal] 段。
//
// 这一段的每个键都是一个 ConfigKey，值是对应目录：
//
//	[StorageLocal]
//	archive = ../data-archive
//
// 该段不存在时是正常的（当前只用一个目录），不会报错。
func mapStorageRoots(file *ini.File, cfg *Config) error {
	section, err := file.GetSection("StorageLocal")
	if err != nil {
		// 可选段：没有就说明只用 LocalRoot。
		return nil
	}
	for _, key := range section.Keys() {
		configKey := strings.TrimSpace(key.Name())
		root := strings.TrimSpace(key.String())
		if configKey == "" || root == "" {
			return fmt.Errorf("parse [StorageLocal]: %s must not be empty", key.Name())
		}
		if configKey == model.LocalStorageConfigKey {
			return fmt.Errorf("parse [StorageLocal]: %q is reserved for Storage.LocalRoot", configKey)
		}
		cfg.Storage.LocalRoots[configKey] = root
	}
	return nil
}

// applyEnvOverrides 用环境变量覆盖配置里的数据库密码。
//
// 密码属于敏感信息，不应该写进会被提交到 Git 的配置文件，
// 所以允许用 CLOUDREVE_DB_PASSWORD 在运行时传入。
// 其它字段目前不支持用环境变量覆盖。
func applyEnvOverrides(cfg *Config) {
	if password, ok := os.LookupEnv("CLOUDREVE_DB_PASSWORD"); ok {
		cfg.Database.Password = password
	}
}

// validate 检查各项取值是否合理。
//
// 这里检查的是“业务规则”，比如端口范围、只允许监听本机；
// 而“能不能转成整数”这种类型问题，前面的 StrictMapTo 已经处理过了。
func (c Config) validate() error {
	// 日志级别必须是 slog 认识的四个之一，提前检查，免得启动后才发现写错。
	var level slog.Level
	if err := level.UnmarshalText([]byte(c.System.LogLevel)); err != nil {
		return fmt.Errorf("System.LogLevel must be debug, info, warn or error")
	}

	// 监听地址必须写成 host:port 的形式，比如 127.0.0.1:8080。
	host, port, err := net.SplitHostPort(c.HTTP.Listen)
	if err != nil {
		return fmt.Errorf("HTTP.Listen must use host:port format: %w", err)
	}

	// 端口必须是 0~65535 的整数；0 是特殊值，表示让操作系统挑一个空闲端口。
	n, err := strconv.Atoi(port)
	if err != nil || n < 0 || n > 65535 {
		return fmt.Errorf("HTTP.Listen port must be an integer between 0 and 65535")
	}

	// 当前还没有登录认证，所以只允许监听本机（127.0.0.1、::1、localhost），
	// 防止有人把开发中的服务不小心暴露到公网。
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return fmt.Errorf("HTTP.Listen must use a loopback address during P0")
	}

	// 关闭超时太短会来不及处理完正在进行的请求，太长又会让退出很慢。
	if c.HTTP.ShutdownTimeoutSeconds < 1 || c.HTTP.ShutdownTimeoutSeconds > 300 {
		return fmt.Errorf("HTTP.ShutdownTimeoutSeconds must be between 1 and 300")
	}

	// 每个分片会话的并行度只允许 1～8。上限既有内存考虑，也避免单个
	// 会话把进程的连接和锁争用占满。旧配置没有这一段时使用默认值 2。
	if c.Upload.MaxParallelChunks < 1 || c.Upload.MaxParallelChunks > 8 {
		return fmt.Errorf("Upload.MaxParallelChunks must be between 1 and 8")
	}

	// 任务队列的每个参数都必须显式落在合理范围内。这里不接受 0：
	// 0 不是“使用默认值”，而是配置写错；默认值只在整段缺失时生效。
	if c.Task.Workers < 1 || c.Task.Workers > 64 {
		return fmt.Errorf("Task.Workers must be between 1 and 64")
	}
	if c.Task.QueueCapacity < 1 || c.Task.QueueCapacity > 100000 {
		return fmt.Errorf("Task.QueueCapacity must be between 1 and 100000")
	}
	if c.Task.TimeoutSeconds < 1 || c.Task.TimeoutSeconds > 86400 {
		return fmt.Errorf("Task.TimeoutSeconds must be between 1 and 86400")
	}
	if c.Task.RetentionSeconds < 1 || c.Task.RetentionSeconds > 604800 {
		return fmt.Errorf("Task.RetentionSeconds must be between 1 and 604800")
	}
	if c.Task.MaxTerminalRecords < 1 || c.Task.MaxTerminalRecords > 1000000 {
		return fmt.Errorf("Task.MaxTerminalRecords must be between 1 and 1000000")
	}
	// 数据库积压和扫描参数同样必须有界：上限太大等于没有上限，
	// 扫描间隔太小会让空闲进程反复查询数据库。
	if c.Task.MaxPendingTasks < 1 || c.Task.MaxPendingTasks > 1000000 {
		return fmt.Errorf("Task.MaxPendingTasks must be between 1 and 1000000")
	}
	if c.Task.ScanIntervalSeconds < 1 || c.Task.ScanIntervalSeconds > 3600 {
		return fmt.Errorf("Task.ScanIntervalSeconds must be between 1 and 3600")
	}
	if c.Task.ScanBatchSize < 1 || c.Task.ScanBatchSize > 1000 {
		return fmt.Errorf("Task.ScanBatchSize must be between 1 and 1000")
	}
	if strings.TrimSpace(c.Task.WorkDir) == "" {
		return fmt.Errorf("Task.WorkDir is required")
	}

	// worker 的租约参数决定故障接管快慢：心跳必须严格短于租约，
	// 否则还没等到下一次续租，任务就已经可以被别的实例接管。
	if c.Worker.Slots < 1 || c.Worker.Slots > 64 {
		return fmt.Errorf("Worker.Slots must be between 1 and 64")
	}
	if c.Worker.LeaseSeconds < 5 || c.Worker.LeaseSeconds > 3600 {
		return fmt.Errorf("Worker.LeaseSeconds must be between 5 and 3600")
	}
	if c.Worker.HeartbeatSeconds < 1 ||
		c.Worker.HeartbeatSeconds >= c.Worker.LeaseSeconds {
		return fmt.Errorf("Worker.HeartbeatSeconds must be between 1 and LeaseSeconds-1")
	}
	if c.Worker.PollSeconds < 1 || c.Worker.PollSeconds > 3600 {
		return fmt.Errorf("Worker.PollSeconds must be between 1 and 3600")
	}

	// 存储目录必须写。至于它到底能不能写，要等启动时实际试一下才知道（见 internal/app）。
	if strings.TrimSpace(c.Storage.LocalRoot) == "" {
		return fmt.Errorf("Storage.LocalRoot is required")
	}
	for key, root := range c.Storage.LocalRoots {
		if strings.TrimSpace(root) == "" {
			return fmt.Errorf("StorageLocal.%s is required", key)
		}
	}

	// 数据库：P2 开始会真的连接 MySQL，所以这几项必须有值。
	if strings.TrimSpace(c.Database.Host) == "" {
		return fmt.Errorf("Database.Host is required")
	}
	if strings.TrimSpace(c.Database.User) == "" {
		return fmt.Errorf("Database.User is required")
	}
	if strings.TrimSpace(c.Database.Name) == "" {
		return fmt.Errorf("Database.Name is required")
	}

	// 数据库端口只检查范围；能不能连上要等启动时 Ping（见 internal/database）。
	if c.Database.Port < 1 || c.Database.Port > 65535 {
		return fmt.Errorf("Database.Port must be between 1 and 65535")
	}

	return nil
}

// resolveStoragePath 把存储目录和任务工作目录统一成绝对路径。
//
// 如果配置文件里写的是相对路径（比如 ../data），就以“配置文件所在的目录”为基准，
// 而不是以你执行 go run 时所在的目录为基准。
// 否则从不同目录启动程序，文件会被写到不同位置，很容易搞混。
func (c *Config) resolveStoragePath(configPath string) error {
	root, err := resolvePath(configPath, c.Storage.LocalRoot)
	if err != nil {
		return fmt.Errorf("resolve Storage.LocalRoot: %w", err)
	}
	c.Storage.LocalRoot = root

	for key, value := range c.Storage.LocalRoots {
		resolved, err := resolvePath(configPath, value)
		if err != nil {
			return fmt.Errorf("resolve StorageLocal.%s: %w", key, err)
		}
		c.Storage.LocalRoots[key] = resolved
	}
	for key, opts := range c.Storage.S3 {
		resolved, err := resolvePath(configPath, opts.SpoolDir)
		if err != nil {
			return fmt.Errorf("resolve StorageS3 %s.SpoolDir: %w", key, err)
		}
		opts.SpoolDir = resolved
		c.Storage.S3[key] = opts
	}

	workDir, err := resolvePath(configPath, c.Task.WorkDir)
	if err != nil {
		return fmt.Errorf("resolve Task.WorkDir: %w", err)
	}
	c.Task.WorkDir = workDir
	return nil
}

// resolvePath 把相对路径按“配置文件所在目录”转成绝对路径。
func resolvePath(configPath, path string) (string, error) {
	if !filepath.IsAbs(path) {
		path = filepath.Join(filepath.Dir(configPath), path)
	}
	return filepath.Abs(path)
}
