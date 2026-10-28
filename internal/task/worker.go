package task

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultWorkerLease 是独立 worker 的默认租约时长。
	// 租约必须明显长于心跳间隔，否则一次网络抖动就会被别的实例接管。
	DefaultWorkerLease = 30 * time.Second

	// DefaultWorkerHeartbeat 是默认续租间隔：租约的三分之一左右。
	DefaultWorkerHeartbeat = 10 * time.Second

	// DefaultWorkerPoll 是空闲时重新查询可领取任务的间隔。
	DefaultWorkerPoll = 2 * time.Second

	// heartbeatTimeout 是单次续租的最长等待时间。
	heartbeatTimeout = 5 * time.Second

	// workerFinalizeTimeout 是失败收尾写数据库的超时。执行 context
	// 可能已经取消，收尾要用独立 context。
	workerFinalizeTimeout = 10 * time.Second
)

// WorkerOptions 是独立 worker 的依赖和参数。
type WorkerOptions struct {
	// Service 提供领取、续租和收尾。它只依赖数据库，不依赖 HTTP。
	Service *Service

	// Registry 决定本次实例能执行哪些任务类型。worker 只领取这里
	// 注册过的类型，不会把 ZIP 等本地任务抢走。
	Registry *Registry

	// WorkerID 是本实例的稳定标识，会写进 tasks.worker_id。
	// 必填；不同实例必须不同，否则无法区分谁持有租约。
	WorkerID string

	// Slots 是同时执行的任务数（执行槽位）。每次只领取空闲槽位数量。
	Slots int

	// LeaseDuration / HeartbeatInterval / PollInterval 为零时使用默认值。
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	PollInterval      time.Duration

	// RetryBase 是第一次重试的等待时间；为零时使用 DefaultRetryBase。
	RetryBase time.Duration

	// Logger 为空时使用 slog.Default()。
	Logger *slog.Logger
}

// Worker 是独立进程里的领取循环。
//
// 它不扫描“所有到期任务”再堆进本地队列，而是每次只领当前空闲槽位：
// 领到的任务立刻执行，执行期间靠心跳续租。进程被杀后心跳停止，租约
// 到期后其他实例可以接管同一条任务。
type Worker struct {
	service   *Service
	registry  *Registry
	workerID  string
	slots     int
	lease     time.Duration
	heartbeat time.Duration
	poll      time.Duration
	retryBase time.Duration
	logger    *slog.Logger
}

// NewWorker 校验参数并创建 worker，但不会开始领取；需要显式调用 Run。
func NewWorker(options WorkerOptions) (*Worker, error) {
	if options.Service == nil {
		return nil, errors.New("task worker: service is required")
	}
	if options.Registry == nil {
		return nil, errors.New("task worker: registry is required")
	}
	if options.WorkerID == "" {
		return nil, errors.New("task worker: worker id is required")
	}
	if options.Slots <= 0 {
		return nil, errors.New("task worker: slots must be positive")
	}
	if len(options.Registry.Types()) == 0 {
		return nil, errors.New("task worker: registry has no task types")
	}

	lease := options.LeaseDuration
	if lease <= 0 {
		lease = DefaultWorkerLease
	}
	heartbeat := options.HeartbeatInterval
	if heartbeat <= 0 {
		heartbeat = DefaultWorkerHeartbeat
	}
	if heartbeat >= lease {
		return nil, fmt.Errorf(
			"task worker: heartbeat %s must be shorter than lease %s",
			heartbeat, lease,
		)
	}
	poll := options.PollInterval
	if poll <= 0 {
		poll = DefaultWorkerPoll
	}
	retryBase := options.RetryBase
	if retryBase <= 0 {
		retryBase = DefaultRetryBase
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Worker{
		service:   options.Service,
		registry:  options.Registry,
		workerID:  options.WorkerID,
		slots:     options.Slots,
		lease:     lease,
		heartbeat: heartbeat,
		poll:      poll,
		retryBase: retryBase,
		logger:    logger,
	}, nil
}

// Run 一直领取并执行任务，直到 ctx 被取消。
//
// 退出时不再领取新任务；正在执行的任务随 ctx 取消。它们的租约到期后
// 由其他实例接管，所以这里不把“进程退出”伪装成任务失败。
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("task worker started",
		"worker_id", w.workerID,
		"slots", w.slots,
		"lease", w.lease,
		"heartbeat", w.heartbeat,
		"types", w.registry.Types(),
	)

	ticker := time.NewTicker(w.poll)
	defer ticker.Stop()

	// running 只用来数占用中的槽位；容量就是本实例的并发上限。
	running := make(chan struct{}, w.slots)
	wake := make(chan struct{}, 1)

	var wg sync.WaitGroup
	for {
		free := w.slots - len(running)
		if free > 0 {
			claims, err := w.service.Claim(
				ctx, w.workerID, free, w.lease, w.registry.Types(),
			)
			if err != nil && ctx.Err() == nil {
				w.logger.Error("claim tasks", "worker_id", w.workerID, "error", err)
			}
			for _, claim := range claims {
				running <- struct{}{}
				wg.Add(1)
				go func(claim Claim) {
					defer wg.Done()
					defer func() { <-running }()
					defer func() {
						// 空出一个槽位就立刻再领，不用等下一次轮询。
						select {
						case wake <- struct{}{}:
						default:
						}
					}()
					w.execute(ctx, claim)
				}(claim)
			}
		}

		select {
		case <-ctx.Done():
			wg.Wait()
			w.logger.Info("task worker stopped", "worker_id", w.workerID)
			return nil
		case <-ticker.C:
		case <-wake:
		}
	}
}

// execute 执行一条已经领取的任务。
//
// 成功的任务由执行器自己的事务收敛（例如内容校验调用 Complete），
// 因为业务结果必须和“标记完成”在同一个事务里提交。worker 只负责
// 失败分类：暂时性错误放回 retry_wait，其余明确失败。
func (w *Worker) execute(parent context.Context, claim Claim) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var leaseLost atomic.Bool
	heartbeatDone := make(chan struct{})
	go w.heartbeatLoop(ctx, &leaseLost, claim, heartbeatDone)

	w.logger.Info("task claimed",
		"task_id", claim.TaskID,
		"task_type", claim.Record.Type,
		"worker_id", claim.WorkerID,
		"attempt", claim.Attempt,
		"lease_until", claim.LeaseUntil,
	)

	result, err := w.run(ctx, claim)

	// 先停止续租，再决定收尾，避免续租和终态写入互相覆盖。
	cancel()
	<-heartbeatDone

	if err == nil {
		// 执行器已经通过带围栏的事务标记完成；续租可能在此之后才发现
		// 租约被清空，那种情况不影响这次成功的结论。
		w.logger.Info("task completed",
			"task_id", claim.TaskID,
			"worker_id", claim.WorkerID,
			"attempt", claim.Attempt,
			"result", result,
		)
		return
	}
	if leaseLost.Load() {
		// 续租已经失败。执行器继续跑到收尾，就是为了让最后那次写库
		// 真正撞上围栏并被拒绝；这里只记录被丢弃的结果。
		w.logger.Warn("discard result after losing task lease",
			"task_id", claim.TaskID,
			"worker_id", claim.WorkerID,
			"attempt", claim.Attempt,
			"error", err,
		)
		return
	}
	w.finishFailure(parent, claim, err)
}

// heartbeatLoop 每 heartbeat 续租一次。
//
// 续租返回 ErrClaimLost 说明任务已经不属于本次执行：停止续租并记下
// 这个事实。执行器不会被强杀，它会继续跑到最后一次写库——由数据库
// 围栏拒绝旧结果，这比“以为取消一定生效”更可靠。
func (w *Worker) heartbeatLoop(
	ctx context.Context,
	leaseLost *atomic.Bool,
	claim Claim,
	done chan<- struct{},
) {
	defer close(done)

	ticker := time.NewTicker(w.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeatCtx, heartbeatCancel := context.WithTimeout(
				context.WithoutCancel(ctx), heartbeatTimeout,
			)
			err := w.service.Heartbeat(heartbeatCtx, claim)
			heartbeatCancel()
			if err == nil {
				continue
			}
			if errors.Is(err, ErrClaimLost) {
				leaseLost.Store(true)
				w.logger.Warn("task lease lost; result will be rejected",
					"task_id", claim.TaskID,
					"worker_id", claim.WorkerID,
					"attempt", claim.Attempt,
				)
				return
			}
			if ctx.Err() == nil {
				w.logger.Warn("renew task lease",
					"task_id", claim.TaskID,
					"worker_id", claim.WorkerID,
					"error", err,
				)
			}
		}
	}
}

// run 把记录交给注册表重建并执行，同时把 panic 转成普通错误。
func (w *Worker) run(ctx context.Context, claim Claim) (result string, err error) {
	defer func() {
		if p := recover(); p != nil {
			w.logger.Error("task panicked",
				"task_id", claim.TaskID,
				"task_type", claim.Record.Type,
				"panic", p,
			)
			result = ""
			err = &panicError{value: p}
		}
	}()
	return w.registry.Run(ctx, claim.Record, nil)
}

// finishFailure 收尾一次失败执行。
//
// 进程退出或任务已被接管时不再写终态：前者交给租约过期后的接管，
// 后者由新执行者负责。收尾使用独立 context，执行期的取消不会影响
// 写数据库。
func (w *Worker) finishFailure(parent context.Context, claim Claim, cause error) {
	if parent.Err() != nil {
		w.logger.Warn("task abandoned during shutdown",
			"task_id", claim.TaskID,
			"worker_id", claim.WorkerID,
			"error", cause,
		)
		return
	}
	if errors.Is(cause, ErrClaimLost) {
		w.logger.Warn("discard result after losing task claim",
			"task_id", claim.TaskID,
			"worker_id", claim.WorkerID,
			"attempt", claim.Attempt,
		)
		return
	}

	finalizeCtx, cancel := context.WithTimeout(
		context.WithoutCancel(parent), workerFinalizeTimeout,
	)
	defer cancel()

	if !IsRetryable(cause) || claim.Attempt >= claim.Record.MaxAttempts {
		if err := w.service.Fail(finalizeCtx, claim, cause); err != nil {
			w.logger.Error("mark task failed",
				"task_id", claim.TaskID, "error", err,
			)
			return
		}
		w.logger.Warn("task failed",
			"task_id", claim.TaskID,
			"worker_id", claim.WorkerID,
			"attempt", claim.Attempt,
			"error", cause,
		)
		return
	}

	nextRunAt := time.Now().Add(retryDelay(w.retryBase, claim.Attempt))
	if err := w.service.Retry(finalizeCtx, claim, nextRunAt, cause); err != nil {
		w.logger.Error("mark task retry", "task_id", claim.TaskID, "error", err)
		return
	}
	w.logger.Warn("task will retry",
		"task_id", claim.TaskID,
		"worker_id", claim.WorkerID,
		"attempt", claim.Attempt,
		"next_run_at", nextRunAt,
		"error", cause,
	)
}
