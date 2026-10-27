// Package task 负责任务的持久化、接收与调度。
//
// 事实来源永远是 MySQL 里的 tasks 行，内存里的队列快照只是运行期缓存：
//
//   - Submit 先做准入检查、把任务落库，提交成功后才尝试唤醒调度器；
//   - Dispatcher 周期扫描（过期通知也能被找回），按数据库里的原 ID
//     把任务放入有界执行队列；
//   - worker 出队后先 TryStart 做条件更新，只有取得执行资格的那一次
//     才会真正运行业务。
//
// 业务执行通过 Runner 注入（例如 ZIP 打包），本包不直接依赖
// internal/archive。恢复工厂、阶段检查点和结果幂等留给后续步骤。
package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/idgen"
	"luckyCloudFile/internal/model"
)

const (
	// DefaultMaxAttempts 是调用方没有指定次数上限时使用的值。
	DefaultMaxAttempts = 3

	// DefaultStateVersion 是调用方没有指定检查点格式版本时使用的值。
	DefaultStateVersion = 1

	// DefaultListLimit 是 ListDue 的默认批量大小。
	// 扫描器每次只取有限一批，避免一次把积压任务全部读进内存。
	DefaultListLimit = 100

	// DefaultMaxPending 是数据库中未终态任务总数的默认上限。
	// 它限制 queued + running + retry_wait，不是内存队列长度。
	DefaultMaxPending = 1000
)

var (
	// ErrNotFound 表示任务不存在，或者不属于这个用户。
	// 两种情况的对外表现必须一致，避免靠 ID 探测别人的任务。
	ErrNotFound = errors.New("task not found")

	// ErrConflict 表示条件更新没有命中：任务已经进入终态、被另一次执行
	// 接管、还没到执行时间，或者次数已经用完。调用方不应继续本次执行。
	ErrConflict = errors.New("task state changed")

	// ErrTooManyPending 表示数据库里已经有太多未终态任务。
	// 它必须在插入之前返回，不能先落库再用失败响应“补丁式”拒绝。
	ErrTooManyPending = errors.New("too many pending tasks")
)

// CreateParams 是创建一条排队任务的输入。
//
// 状态不需要调用方指定：新任务永远是 queued；进入 running 必须经过
// TryStart，否则 attempts 和状态的先后顺序就失去了唯一负责人。
type CreateParams struct {
	// ID 为空时由服务端生成。恢复、重试必须复用数据库里的原 ID。
	ID string

	// Type 决定以后用哪个重建工厂解释 StateJSON。
	Type string

	// OwnerID 是任务所有者；必须来自登录态，不能由客户端请求体指定。
	OwnerID int

	// Phase 是下一步业务阶段。创建时可以为空，等第一次检查点再写。
	Phase string

	// StateVersion / StateJSON 是继续执行所需的检查点。
	// StateVersion 为 0 时使用 DefaultStateVersion。
	StateVersion int
	StateJSON    string

	// MaxAttempts 为 0 时使用 DefaultMaxAttempts；负数非法。
	MaxAttempts int

	// NextRunAt 是最早可执行时间。为零时表示立即可执行。
	NextRunAt time.Time
}

// Checkpoint 是一次阶段推进。
//
// Phase 表示“下一步做什么”；StateVersion / StateJSON 是这一步之后
// 继续执行需要的全部数据。仓储层不解释内容，也不做部分字段合并：
// 每次保存的都是一个完整的新检查点。
type Checkpoint struct {
	Phase        string
	StateVersion int
	StateJSON    string
}

// Service 是任务表的仓储和接收入口。
//
// 它只持有数据库连接和唤醒通道，不持有任务对象、队列或锁；所以
// “销毁并重建 Service”不会丢失任何已经提交的任务，新对象仍能按原 ID 查询。
type Service struct {
	db *gorm.DB

	// wake 是提交成功后唤醒调度器的信号。容量 1 且只做非阻塞发送：
	// 重复提交合并成一次通知，通知丢失也不影响正确性（周期扫描兜底）。
	wake chan struct{}

	// admitMu 串行化“统计未终态任务 + 插入”这一段。单实例部署下用它
	// 保证准入判断和插入之间不会插进另一个提交；多实例部署仍需要数据库
	// 级的调度槽才能得到同样的上限，属于后续步骤。
	admitMu    sync.Mutex
	maxPending int
}

// NewService 创建任务仓储。db 必须是已经完成迁移的连接。
func NewService(db *gorm.DB) *Service {
	return &Service{
		db:   db,
		wake: make(chan struct{}, 1),
	}
}

// WithMaxPending 设置数据库中未终态任务的总数上限。
// 非正数表示使用 DefaultMaxPending；应在开始接受请求前调用。
func (s *Service) WithMaxPending(limit int) *Service {
	s.maxPending = limit
	return s
}

// Submit 是 HTTP 等外部入口接收任务的唯一方式。
//
// 顺序不能反：
//
//	准入检查 → 插入 queued 行并提交 → 非阻塞唤醒调度器 → 返回原 taskID
//
// 落库之后就不再因为内存队列满而回头报错：任务已经算接收成功，扫描器
// 迟早会把它放进队列。只有插入之前的准入限制（ErrTooManyPending）才是
// 真正的拒绝。
func (s *Service) Submit(ctx context.Context, params CreateParams) (*model.Task, error) {
	s.admitMu.Lock()
	defer s.admitMu.Unlock()

	limit := s.maxPending
	if limit <= 0 {
		limit = DefaultMaxPending
	}

	var pending int64
	err := s.db.WithContext(ctx).
		Model(&model.Task{}).
		Where("status IN ?", []string{
			model.TaskStatusQueued,
			model.TaskStatusRunning,
			model.TaskStatusRetryWait,
		}).
		Count(&pending).Error
	if err != nil {
		return nil, fmt.Errorf("count pending tasks: %w", err)
	}
	if pending >= int64(limit) {
		return nil, ErrTooManyPending
	}

	task, err := s.Create(ctx, params)
	if err != nil {
		return nil, err
	}
	s.wakeDispatcher()
	return task, nil
}

// wakeDispatcher 发出一次非阻塞唤醒。通知只是优化：
// 丢掉了也有周期扫描，不能因此把已落库的任务当成失败。
func (s *Service) wakeDispatcher() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Create 插入一条 queued 任务，但不做准入检查，也不唤醒调度器。
//
// 外部入口（HTTP）必须用 Submit：它会在插入前检查数据库积压上限，并在
// 提交成功后通知调度器。Create 只供恢复、测试和内部步骤使用；直接调用它
// 的任务仍会被周期扫描发现，但绕过了 MaxPending 限制。
func (s *Service) Create(ctx context.Context, params CreateParams) (*model.Task, error) {
	if params.Type == "" {
		return nil, errors.New("task type is required")
	}
	if params.OwnerID <= 0 {
		return nil, errors.New("task owner id must be positive")
	}
	if params.MaxAttempts < 0 {
		return nil, errors.New("task max attempts must not be negative")
	}
	if params.StateVersion < 0 {
		return nil, errors.New("task state version must not be negative")
	}

	id := params.ID
	if id == "" {
		generated, err := idgen.New()
		if err != nil {
			return nil, err
		}
		id = generated
	}

	maxAttempts := params.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultMaxAttempts
	}
	stateVersion := params.StateVersion
	if stateVersion == 0 {
		stateVersion = DefaultStateVersion
	}
	nextRunAt := params.NextRunAt
	if nextRunAt.IsZero() {
		nextRunAt = time.Now()
	}

	task := &model.Task{
		ID:             id,
		Type:           params.Type,
		OwnerID:        params.OwnerID,
		Status:         model.TaskStatusQueued,
		Phase:          params.Phase,
		StateVersion:   stateVersion,
		StateJSON:      params.StateJSON,
		Attempts:       0,
		MaxAttempts:    maxAttempts,
		NextRunAt:      nextRunAt,
		CleanupPending: false,
	}
	if err := s.db.WithContext(ctx).Create(task).Error; err != nil {
		return nil, fmt.Errorf("create task %q: %w", id, err)
	}
	return task, nil
}

// GetOwned 按 ID 和所有者取任务。不存在和越权都返回 ErrNotFound。
func (s *Service) GetOwned(ctx context.Context, ownerID int, id string) (*model.Task, error) {
	var task model.Task
	err := s.db.WithContext(ctx).
		Where("id = ? AND owner_id = ?", id, ownerID).
		First(&task).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get task %q: %w", id, err)
	}
	return &task, nil
}

// ListDue 返回现在可以尝试执行的任务，按最早到期时间排序。
//
// 它只做候选筛选，不保证一定能执行：真正取得资格要以 TryStart 的
// 条件更新结果为准。这里排除次数已经用完的记录，避免扫描器反复取到
// 永远起不来的任务。
func (s *Service) ListDue(ctx context.Context, now time.Time, limit int) ([]model.Task, error) {
	if now.IsZero() {
		now = time.Now()
	}
	if limit <= 0 {
		limit = DefaultListLimit
	}

	var tasks []model.Task
	err := s.db.WithContext(ctx).
		Where("status IN ?", []string{model.TaskStatusQueued, model.TaskStatusRetryWait}).
		Where("next_run_at <= ?", now).
		Where("attempts < max_attempts").
		Order("next_run_at ASC, id ASC").
		Limit(limit).
		Find(&tasks).Error
	if err != nil {
		return nil, fmt.Errorf("list due tasks: %w", err)
	}
	return tasks, nil
}

// TryStart 尝试把任务从 queued / retry_wait 推进到 running。
//
// attempts + 1 和状态切换在同一个条件 UPDATE 里完成，只有 RowsAffected=1
// 的调用者能进入业务。这样同一 ID 被多个 worker 或多次扫描取到，也只有
// 一次执行。返回的任务里 Attempts 已经是本次执行的编号，后续检查点和
// 终态更新都要带上它做围栏。
func (s *Service) TryStart(ctx context.Context, id string, now time.Time) (*model.Task, error) {
	if now.IsZero() {
		now = time.Now()
	}

	result := s.db.WithContext(ctx).
		Model(&model.Task{}).
		Where("id = ?", id).
		Where("status IN ?", []string{model.TaskStatusQueued, model.TaskStatusRetryWait}).
		Where("next_run_at <= ?", now).
		Where("attempts < max_attempts").
		Updates(map[string]any{
			"status":     model.TaskStatusRunning,
			"attempts":   gorm.Expr("attempts + 1"),
			"updated_at": now,
		})
	if result.Error != nil {
		return nil, fmt.Errorf("start task %q: %w", id, result.Error)
	}
	if result.RowsAffected == 0 {
		return nil, s.notStarted(ctx, id)
	}
	return s.getByID(ctx, id)
}

// SaveCheckpoint 保存一次阶段推进。
//
// 更新条件同时核对 status=running 和本次 attempts：任务已经结束、被
// 新一轮执行接管，或尝试次数不一致时返回 ErrConflict，迟到的回调不会
// 覆盖当前状态。
func (s *Service) SaveCheckpoint(ctx context.Context, id string, attempt int, checkpoint Checkpoint) error {
	if checkpoint.StateVersion < 0 {
		return errors.New("task state version must not be negative")
	}
	return s.updateRunning(ctx, id, attempt, map[string]any{
		"phase":         checkpoint.Phase,
		"state_version": checkpoint.StateVersion,
		"state_json":    checkpoint.StateJSON,
	})
}

// MarkRetry 把任务放回等待重试，并把执行资格让出来。
//
// 等待期间任务不占 worker：调度器只按 next_run_at 再次扫描。attempts
// 不变，上限由 TryStart 判断。
func (s *Service) MarkRetry(
	ctx context.Context,
	id string,
	attempt int,
	nextRunAt time.Time,
	cause error,
) error {
	if nextRunAt.IsZero() {
		return errors.New("task next run time is required")
	}
	return s.updateRunning(ctx, id, attempt, map[string]any{
		"status":      model.TaskStatusRetryWait,
		"next_run_at": nextRunAt,
		"last_error":  errorText(cause),
	})
}

// MarkCompleted 把任务标记为成功，并记录结果文件 ID。
//
// resultFileID 为空时保留原值：清理类任务本来就没有结果文件，不能在
// 收尾时把已经发布的关联清掉。
func (s *Service) MarkCompleted(ctx context.Context, id string, attempt int, resultFileID string) error {
	values := map[string]any{
		"status":      model.TaskStatusCompleted,
		"finished_at": time.Now(),
		"last_error":  "",
	}
	if resultFileID != "" {
		values["result_file_id"] = resultFileID
	}
	return s.updateRunning(ctx, id, attempt, values)
}

// MarkFailed 把任务标记为终态失败。
//
// 它只改状态和错误信息，不删除临时资源；需要保留路径时，调用方应在
// 同一轮里把 cleanup_pending 留到后续清理任务处理。
func (s *Service) MarkFailed(ctx context.Context, id string, attempt int, cause error) error {
	return s.updateRunning(ctx, id, attempt, map[string]any{
		"status":      model.TaskStatusFailed,
		"finished_at": time.Now(),
		"last_error":  errorText(cause),
	})
}

// SetCleanupPending 更新“还有临时资源需要清理”的责任标记。
//
// 它和 Status 分开：任务可以已经完成任务发布，但工作目录没删掉。
// 成功清理后也会用 false 把旧标记收回，避免永久留下清理责任。
func (s *Service) SetCleanupPending(
	ctx context.Context,
	id string,
	attempt int,
	pending bool,
) error {
	return s.updateRunning(ctx, id, attempt, map[string]any{
		"cleanup_pending": pending,
	})
}

// updateRunning 是所有执行期更新的唯一出口。
//
// 条件里的 attempts 是本次执行的编号：只有仍然由这一轮执行持有的任务
// 才能被它更新。RowsAffected=0 时区分“根本不存在”和“状态已经变化”，
// 方便调用方决定是忽略还是记录异常。
func (s *Service) updateRunning(
	ctx context.Context,
	id string,
	attempt int,
	values map[string]any,
) error {
	result := s.db.WithContext(ctx).
		Model(&model.Task{}).
		Where("id = ? AND status = ? AND attempts = ?", id, model.TaskStatusRunning, attempt).
		Updates(values)
	if result.Error != nil {
		return fmt.Errorf("update task %q: %w", id, result.Error)
	}
	if result.RowsAffected == 0 {
		return s.notStarted(ctx, id)
	}
	return nil
}

// notStarted 在条件更新没有命中时判断任务是否真实存在。
//
// 任务不存在返回 ErrNotFound；存在但条件不满足返回 ErrConflict。
func (s *Service) notStarted(ctx context.Context, id string) error {
	var count int64
	err := s.db.WithContext(ctx).
		Model(&model.Task{}).
		Where("id = ?", id).
		Count(&count).Error
	if err != nil {
		return fmt.Errorf("check task %q: %w", id, err)
	}
	if count == 0 {
		return ErrNotFound
	}
	return ErrConflict
}

// getByID 读取刚更新过的任务，返回本次执行看到的状态。
func (s *Service) getByID(ctx context.Context, id string) (*model.Task, error) {
	var task model.Task
	err := s.db.WithContext(ctx).Where("id = ?", id).First(&task).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("reload task %q: %w", id, err)
	}
	return &task, nil
}

// errorText 把错误转成可落库的说明。nil 表示没有错误，存空串。
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
