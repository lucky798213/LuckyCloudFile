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
// 独立 worker 走另一条路径：Claim 用 SELECT ... FOR UPDATE SKIP LOCKED
// 直接领取任务并写入 worker_id / attempts / lease_until，执行期间用
// Heartbeat 续租；Complete / Retry / Fail 在同一个事务里先核对领取资格
// 再写结果，被接管的旧执行者会被拒绝。
//
// 业务执行通过 Runner 注入（例如 ZIP 打包和内容校验），本包不直接依赖
// internal/archive 或 internal/verify。
package task

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

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

// unfinishedTaskStatuses 是仍需调度或等待收尾的任务状态。
// completed / failed 是终态，不再进入扫描和周期去重判断。
var unfinishedTaskStatuses = []string{
	model.TaskStatusQueued,
	model.TaskStatusRunning,
	model.TaskStatusRetryWait,
}

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

	// ErrClaimLost 表示这次领取已经不再有效：任务被别的 worker 接管，
	// 或者租约已经过期。迟到的执行者不能再用它写任何业务结果。
	ErrClaimLost = errors.New("task claim is no longer valid")
)

// Claim 是一次领取的执行凭据。
//
// 它记录“谁、第几次执行、租约到什么时候”。写业务结果时必须把整份凭据
// 交给 Complete / Retry / Fail：只带 task ID 无法拒绝上一轮的旧执行者。
type Claim struct {
	TaskID     string
	WorkerID   string
	Attempt    int
	LeaseUntil time.Time

	// LeaseDuration 是领取时申请的租约时长，Heartbeat 用它续租。
	LeaseDuration time.Duration

	// Record 是领取之后的完整任务快照，用来重建对应的执行器。
	// 执行器不能拿它覆盖数据库里的最新状态。
	Record model.Task
}

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
		Where("status IN ?", unfinishedTaskStatuses).
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

// EnsurePeriodic 保证同一类型最多只有一条未终态任务，用于周期任务去重。
//
// 定时触发器每个 tick 都会调用它：已有 queued / running / retry_wait
// 记录时直接复用，不插入新行；只有上一条已经 completed / failed 才创建
// 下一条。这样 tick 频率再高也不会无限堆积同类清理任务。
//
// 它和 Submit 的准入不同：周期清理属于内部维护工作，不能被用户任务
// 积压上限完全饿死。单实例下由 admitMu 串行化“查询 + 插入”；多实例
// 仍需要数据库唯一调度槽，本阶段不做。创建后会唤醒调度器。
func (s *Service) EnsurePeriodic(
	ctx context.Context,
	params CreateParams,
) (*model.Task, error) {
	if params.Type == "" {
		return nil, errors.New("task type is required")
	}

	s.admitMu.Lock()
	defer s.admitMu.Unlock()

	var existing model.Task
	err := s.db.WithContext(ctx).
		Where("type = ? AND status IN ?", params.Type, unfinishedTaskStatuses).
		Order("created_at ASC, id ASC").
		First(&existing).Error
	if err == nil {
		return &existing, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("find pending %q task: %w", params.Type, err)
	}

	created, err := s.Create(ctx, params)
	if err != nil {
		return nil, err
	}
	s.wakeDispatcher()
	return created, nil
}

// ListCleanupPending 返回已经进入终态、但还保留清理责任的记录。
//
// 只选择 completed / failed：running 任务可能正持有工作目录，不能由
// 另一个清理任务并发回收。任务失败也不会删除记录，责任仍在数据库里，
// 后续清理任务可以继续重试。
func (s *Service) ListCleanupPending(
	ctx context.Context,
	limit int,
) ([]model.Task, error) {
	if limit <= 0 {
		limit = DefaultListLimit
	}

	var tasks []model.Task
	err := s.db.WithContext(ctx).
		Where("cleanup_pending = ? AND status IN ?", true, []string{
			model.TaskStatusCompleted,
			model.TaskStatusFailed,
		}).
		Order("updated_at ASC, id ASC").
		Limit(limit).
		Find(&tasks).Error
	if err != nil {
		return nil, fmt.Errorf("list cleanup pending tasks: %w", err)
	}
	return tasks, nil
}

// ClearCleanupPending 收回一条已经清理完成的责任标记。
//
// 它不要求任务仍在 running：执行清理的是另一个清理任务，不是原任务。
// 用 cleanup_pending=true 做条件，重复清理或并发清理都是幂等的。
func (s *Service) ClearCleanupPending(ctx context.Context, id string) error {
	result := s.db.WithContext(ctx).
		Model(&model.Task{}).
		Where("id = ? AND cleanup_pending = ?", id, true).
		UpdateColumn("cleanup_pending", false)
	if result.Error != nil {
		return fmt.Errorf("clear cleanup pending of task %q: %w", id, result.Error)
	}
	return nil
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
//
// taskTypes 为空表示不按类型过滤；本地调度器应传入自己注册的类型，
// 否则会把只应由独立 worker 执行的任务也扫进本地队列。
func (s *Service) ListDue(
	ctx context.Context,
	now time.Time,
	limit int,
	taskTypes ...string,
) ([]model.Task, error) {
	if now.IsZero() {
		now = time.Now()
	}
	if limit <= 0 {
		limit = DefaultListLimit
	}

	query := s.db.WithContext(ctx).
		Where("status IN ?", []string{model.TaskStatusQueued, model.TaskStatusRetryWait}).
		Where("next_run_at <= ?", now).
		Where("attempts < max_attempts")
	if len(taskTypes) > 0 {
		query = query.Where("type IN ?", taskTypes)
	}

	var tasks []model.Task
	if err := query.
		Order("next_run_at ASC, id ASC").
		Limit(limit).
		Find(&tasks).Error; err != nil {
		return nil, fmt.Errorf("list due tasks: %w", err)
	}
	return tasks, nil
}

// Claim 在事务里领取最多 availableSlots 条马上可以执行的任务。
//
// 领取用 SELECT ... FOR UPDATE SKIP LOCKED 只锁住本次选中的行：多个实例
// 同时领取时，一个事务不会等待另一个事务手里的任务，也不会领到同一行。
// UPDATE 同时写入 status=running、worker_id、attempts+1 和租约到期时间。
// 业务执行在事务之外进行。
//
// 可以领取的记录有两类：
//   - queued / retry_wait 且 next_run_at 已到；
//   - running 但 lease_until 已过期（上一个执行者失联，本次接管）。
//
// 每个实例只领取自己马上能执行的数量。先领一大批堆在本地会让租约在
// 排队期间白白过期，别的实例也无法分担。
//
// taskTypes 为空表示不按类型过滤；独立 worker 应只传自己注册的类型。
func (s *Service) Claim(
	ctx context.Context,
	workerID string,
	availableSlots int,
	leaseDuration time.Duration,
	taskTypes []string,
) ([]Claim, error) {
	if strings.TrimSpace(workerID) == "" {
		return nil, errors.New("task worker id is required")
	}
	if leaseDuration <= 0 {
		return nil, errors.New("task lease duration must be positive")
	}
	if availableSlots <= 0 {
		return nil, nil
	}

	var claims []Claim
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 租约过期且次数已经用完的任务不会再被领取。这里明确收敛成
		// failed，避免数据库里永久留下一条“看起来还在跑”的记录。
		if err := tx.Exec(`
			UPDATE tasks
			SET status = ?, finished_at = NOW(6), lease_until = NULL,
			    last_error = ?, updated_at = NOW(6)
			WHERE status = ? AND lease_until <= NOW(6) AND attempts >= max_attempts`,
			model.TaskStatusFailed,
			"task lease expired after reaching the attempt limit",
			model.TaskStatusRunning,
		).Error; err != nil {
			return fmt.Errorf("fail exhausted expired tasks: %w", err)
		}

		query := tx.Model(&model.Task{}).
			Where("attempts < max_attempts").
			Where(
				"((status IN ? AND next_run_at <= ?) OR (status = ? AND lease_until <= NOW(6)))",
				[]string{model.TaskStatusQueued, model.TaskStatusRetryWait},
				time.Now(),
				model.TaskStatusRunning,
			)
		if len(taskTypes) > 0 {
			query = query.Where("type IN ?", taskTypes)
		}

		var due []model.Task
		if err := query.
			Clauses(clause.Locking{
				Strength: clause.LockingStrengthUpdate,
				Options:  clause.LockingOptionsSkipLocked,
			}).
			Order("next_run_at ASC, id ASC").
			Limit(availableSlots).
			Find(&due).Error; err != nil {
			return fmt.Errorf("select claimable tasks: %w", err)
		}
		if len(due) == 0 {
			return nil
		}

		ids := make([]string, 0, len(due))
		for _, record := range due {
			ids = append(ids, record.ID)
		}

		// lease_until 必须用数据库时钟：不同机器上的进程时钟可能不一致，
		// 用应用时间会在接管判断上产生额外误差。
		updated := tx.Model(&model.Task{}).
			Where("id IN ?", ids).
			Updates(map[string]any{
				"status":    model.TaskStatusRunning,
				"worker_id": workerID,
				"attempts":  gorm.Expr("attempts + 1"),
				"lease_until": gorm.Expr(
					"DATE_ADD(NOW(6), INTERVAL ? MICROSECOND)",
					leaseDuration.Microseconds(),
				),
				"updated_at": gorm.Expr("NOW(6)"),
			})
		if updated.Error != nil {
			return fmt.Errorf("claim tasks: %w", updated.Error)
		}
		if updated.RowsAffected != int64(len(ids)) {
			return fmt.Errorf(
				"claimed %d of %d selected tasks", updated.RowsAffected, len(ids),
			)
		}

		// 回读领取之后的行：attempts 已经是本次执行的编号。调用方必须
		// 保留这份快照，不能稍后重新读取一个新 attempt 当作自己的资格。
		var claimed []model.Task
		if err := tx.Where("id IN ?", ids).Find(&claimed).Error; err != nil {
			return fmt.Errorf("reload claimed tasks: %w", err)
		}
		claims = make([]Claim, 0, len(claimed))
		for _, record := range claimed {
			if record.LeaseUntil == nil || record.WorkerID != workerID {
				return fmt.Errorf("task %q was not claimed by this worker", record.ID)
			}
			claims = append(claims, Claim{
				TaskID:        record.ID,
				WorkerID:      record.WorkerID,
				Attempt:       record.Attempts,
				LeaseUntil:    *record.LeaseUntil,
				LeaseDuration: leaseDuration,
				Record:        record,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claims, nil
}

// Heartbeat 延长一条领取的租约。
//
// 条件里同时核对 worker、attempt 和租约尚未过期：任务一旦被接管或租约
// 已经过期，续租返回 ErrClaimLost，执行方应立即停止并丢弃结果。
func (s *Service) Heartbeat(ctx context.Context, claim Claim) error {
	if claim.LeaseDuration <= 0 {
		return errors.New("task claim has no lease duration")
	}

	result := s.db.WithContext(ctx).
		Model(&model.Task{}).
		Where(
			"id = ? AND status = ? AND worker_id = ? AND attempts = ? AND lease_until > NOW(6)",
			claim.TaskID, model.TaskStatusRunning, claim.WorkerID, claim.Attempt,
		).
		Updates(map[string]any{
			"lease_until": gorm.Expr(
				"DATE_ADD(NOW(6), INTERVAL ? MICROSECOND)",
				claim.LeaseDuration.Microseconds(),
			),
			"updated_at": gorm.Expr("NOW(6)"),
		})
	if result.Error != nil {
		return fmt.Errorf("heartbeat task %q: %w", claim.TaskID, result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrClaimLost
	}
	return nil
}

// Complete 在同一个事务里完成“复核领取资格 + 写业务结果 + 标记完成”。
//
// apply 可以为 nil（没有额外数据库副作用的任务）。内容校验用它把
// “确认 Entity 仍有效、回填可信摘要、登记内容映射”和任务完成绑在一起：
// 如果任务已经被接管，整个事务回滚，旧 worker 不会写入任何结果。
//
// 不能拆成“先检查 claim，再另开事务写摘要”：检查之后可能正好发生
// 接管，旧 worker 仍然会把结果写进去。
func (s *Service) Complete(
	ctx context.Context,
	claim Claim,
	resultJSON string,
	apply func(tx *gorm.DB) error,
) error {
	return s.finishClaim(ctx, claim, func(tx *gorm.DB) error {
		if apply != nil {
			if err := apply(tx); err != nil {
				return err
			}
		}

		values := map[string]any{
			"status":      model.TaskStatusCompleted,
			"finished_at": gorm.Expr("NOW(6)"),
			"last_error":  "",
			"lease_until": nil,
			"updated_at":  gorm.Expr("NOW(6)"),
		}
		if resultJSON != "" {
			values["result_json"] = resultJSON
		}
		return tx.Model(&model.Task{}).
			Where("id = ?", claim.TaskID).
			Updates(values).Error
	})
}

// Retry 释放本次执行资格，把任务放回等待重试。
//
// 它和 Complete 用同一套租约围栏：被接管的旧执行者不能把新执行者
// 正在运行的任务改成 retry_wait。
func (s *Service) Retry(
	ctx context.Context,
	claim Claim,
	nextRunAt time.Time,
	cause error,
) error {
	if nextRunAt.IsZero() {
		return errors.New("task next run time is required")
	}
	return s.finishClaim(ctx, claim, func(tx *gorm.DB) error {
		return tx.Model(&model.Task{}).
			Where("id = ?", claim.TaskID).
			Updates(map[string]any{
				"status":      model.TaskStatusRetryWait,
				"next_run_at": nextRunAt,
				"last_error":  errorText(cause),
				"lease_until": nil,
				"updated_at":  gorm.Expr("NOW(6)"),
			}).Error
	})
}

// Fail 在租约围栏内把任务标记为终态失败。
func (s *Service) Fail(ctx context.Context, claim Claim, cause error) error {
	return s.finishClaim(ctx, claim, func(tx *gorm.DB) error {
		return tx.Model(&model.Task{}).
			Where("id = ?", claim.TaskID).
			Updates(map[string]any{
				"status":      model.TaskStatusFailed,
				"finished_at": gorm.Expr("NOW(6)"),
				"last_error":  errorText(cause),
				"lease_until": nil,
				"updated_at":  gorm.Expr("NOW(6)"),
			}).Error
	})
}

// finishClaim 是所有“写结果”操作的统一入口：先锁任务行并核对领取
// 资格，再执行调用方的更新。检查和写入必须在同一个事务里。
func (s *Service) finishClaim(
	ctx context.Context,
	claim Claim,
	update func(tx *gorm.DB) error,
) error {
	if claim.TaskID == "" || claim.WorkerID == "" || claim.Attempt <= 0 {
		return ErrClaimLost
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockClaim(tx, claim); err != nil {
			return err
		}
		return update(tx)
	})
}

// lockClaim 锁住任务行，并确认它仍由这次领取持有。
//
// 租约是否有效也交给数据库时钟判断，和领取时写入的基准保持一致。
func lockClaim(tx *gorm.DB, claim Claim) error {
	var record model.Task
	err := tx.Clauses(clause.Locking{Strength: clause.LockingStrengthUpdate}).
		Where(
			"id = ? AND status = ? AND worker_id = ? AND attempts = ? AND lease_until > NOW(6)",
			claim.TaskID, model.TaskStatusRunning, claim.WorkerID, claim.Attempt,
		).
		First(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrClaimLost
	}
	if err != nil {
		return fmt.Errorf("lock task claim %q: %w", claim.TaskID, err)
	}
	return nil
}

// RecoveryResult 是启动恢复遗留 running 任务的统计。
type RecoveryResult struct {
	Completed int64 // 结果已经发布，直接补写 completed
	Requeued  int64 // 保留 phase/state，重新排队
	Failed    int64 // 次数已经用完，明确失败
}

// RecoverRunning 收敛上一次进程留下的 running 任务。
//
// 它必须在开始调度之前调用。单实例部署时，数据库里的 running 记录不可能
// 仍由本进程持有；多实例部署时，其他进程可能正持有有效租约，所以这里
// 只处理 lease_until 为空（旧数据）或已经过期的记录，绝不碰仍然有效的租约。
//
// 顺序是“先核对结果，再决定是否重跑”：
//   - task_results 已有记录：结果已经发布，不能再执行业务，直接补写
//     completed 和结果投影，并保留清理责任；
//   - 没有结果且次数没用完：保留最后已提交的 phase/state，放回 queued；
//   - 没有结果且次数已经用完：明确 failed，不绕过上限。
//
// attempts 不在恢复时增加或清零：每次取得执行资格才加一，崩溃恢复
// 同样受 max_attempts 约束。
func (s *Service) RecoverRunning(ctx context.Context, now time.Time) (RecoveryResult, error) {
	if now.IsZero() {
		now = time.Now()
	}

	var result RecoveryResult
	var cursor string

	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		var tasks []model.Task
		err := s.db.WithContext(ctx).
			Where("status = ? AND id > ?", model.TaskStatusRunning, cursor).
			Where("(lease_until IS NULL OR lease_until <= NOW(6))").
			Order("id ASC").
			Limit(DefaultListLimit).
			Find(&tasks).Error
		if err != nil {
			return result, fmt.Errorf("query interrupted running tasks: %w", err)
		}
		if len(tasks) == 0 {
			return result, nil
		}

		for _, record := range tasks {
			if err := ctx.Err(); err != nil {
				return result, err
			}
			cursor = record.ID
			if err := s.recoverRunningTask(ctx, record, now, &result); err != nil {
				return result, err
			}
		}
	}
}

// publishedTaskResult 返回任务已经提交的发布结果。
//
// task_results 是任务级幂等凭据：只要它存在，逻辑结果就已经发布，
// 无论 tasks.status 现在是什么，都不能再执行业务或重新计费。
func (s *Service) publishedTaskResult(
	ctx context.Context,
	ownerID int,
	taskID string,
) (fileID string, published bool, err error) {
	var result model.TaskResult
	err = s.db.WithContext(ctx).
		Where("task_id = ? AND owner_id = ?", taskID, ownerID).
		First(&result).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("query task result of %q: %w", taskID, err)
	}
	return result.FileID, true, nil
}

// recoverRunningTask 收敛一条 running 记录。更新条件仍带 attempt 围栏，
// 避免覆盖恢复期间被别的执行者改过的行。
func (s *Service) recoverRunningTask(
	ctx context.Context,
	record model.Task,
	now time.Time,
	result *RecoveryResult,
) error {
	publishedFileID, published, err := s.publishedTaskResult(
		ctx, record.OwnerID, record.ID,
	)
	if err != nil {
		return err
	}
	if published {
		// 文件已经发布：只补终态和清理责任，绝不再次执行发布业务。
		updated := s.db.WithContext(ctx).
			Model(&model.Task{}).
			Where("id = ? AND status = ? AND attempts = ?",
				record.ID, model.TaskStatusRunning, record.Attempts).
			Updates(map[string]any{
				"status":          model.TaskStatusCompleted,
				"result_file_id":  publishedFileID,
				"finished_at":     now,
				"last_error":      "",
				"cleanup_pending": true,
				"updated_at":      now,
			})
		if updated.Error != nil {
			return fmt.Errorf("complete recovered task %q: %w", record.ID, updated.Error)
		}
		if updated.RowsAffected == 1 {
			result.Completed++
		}
		return nil
	}

	if record.Attempts >= record.MaxAttempts {
		updated := s.db.WithContext(ctx).
			Model(&model.Task{}).
			Where("id = ? AND status = ? AND attempts = ?",
				record.ID, model.TaskStatusRunning, record.Attempts).
			Updates(map[string]any{
				"status":      model.TaskStatusFailed,
				"finished_at": now,
				"last_error":  "task interrupted after reaching the attempt limit",
				"updated_at":  now,
			})
		if updated.Error != nil {
			return fmt.Errorf("fail exhausted task %q: %w", record.ID, updated.Error)
		}
		if updated.RowsAffected == 1 {
			result.Failed++
		}
		return nil
	}

	// 保留最后已提交的 phase / state_json / attempts，只把调度状态放回
	// queued；扫描器会按 next_run_at 重新发现它。
	updated := s.db.WithContext(ctx).
		Model(&model.Task{}).
		Where("id = ? AND status = ? AND attempts = ?",
			record.ID, model.TaskStatusRunning, record.Attempts).
		Updates(map[string]any{
			"status":      model.TaskStatusQueued,
			"next_run_at": now,
			"updated_at":  now,
		})
	if updated.Error != nil {
		return fmt.Errorf("requeue interrupted task %q: %w", record.ID, updated.Error)
	}
	if updated.RowsAffected == 1 {
		result.Requeued++
	}
	return nil
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
