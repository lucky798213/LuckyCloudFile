package task

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/queue"
)

const (
	// DefaultScanInterval 是周期扫描的默认间隔。
	// 即使一次唤醒通知完全丢失，任务最多等这么久也会被重新发现。
	DefaultScanInterval = 5 * time.Second

	// DefaultScanBatchSize 是单次扫描最多取回多少条到期记录。
	DefaultScanBatchSize = 50

	// finalizeTimeout 是任务结束后写终态的独立超时。
	// 执行 context 可能已经超时或取消，不能用它去写数据库。
	finalizeTimeout = 10 * time.Second
)

// DispatcherOptions 是调度器的依赖和参数。
type DispatcherOptions struct {
	// Service 提供到期查询、TryStart 和终态写入。
	Service *Service

	// Queue 是有界执行队列。内存满时任务留在数据库，等下一次扫描。
	Queue *queue.Queue

	// Runner 从数据库记录执行具体业务，例如 ZIP 打包。
	Runner Runner

	// ScanInterval / BatchSize 为零时使用上面的默认值。
	ScanInterval time.Duration
	BatchSize    int

	// Logger 为空时使用 slog.Default()。
	Logger *slog.Logger
}

// Dispatcher 把数据库里到期的任务送进有界执行队列。
//
// 它只负责“发现并排队”，不直接执行任务：执行发生在 queue 的 worker 里，
// 出队后还要先 TryStart 才能运行。因此扫描重复或通知重复都不会让同一
// 任务并发执行——数据库条件更新是最后一道闸。
type Dispatcher struct {
	service      *Service
	queue        *queue.Queue
	runner       Runner
	scanInterval time.Duration
	batchSize    int
	logger       *slog.Logger

	// inFlight 是当前进程已经放进队列、但还没有执行结束的任务 ID。
	// 它在扫描和重启之间不持久化；重启后由数据库里的状态重新决定。
	mu       sync.Mutex
	inFlight map[string]struct{}

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	startOnce sync.Once
	stopOnce  sync.Once
}

// NewDispatcher 创建调度器，但不会开始扫描；需要显式调用 Start。
func NewDispatcher(options DispatcherOptions) (*Dispatcher, error) {
	if options.Service == nil {
		return nil, errors.New("task dispatcher: service is required")
	}
	if options.Queue == nil {
		return nil, errors.New("task dispatcher: queue is required")
	}
	if options.Runner == nil {
		return nil, errors.New("task dispatcher: runner is required")
	}

	scanInterval := options.ScanInterval
	if scanInterval <= 0 {
		scanInterval = DefaultScanInterval
	}
	batchSize := options.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultScanBatchSize
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Dispatcher{
		service:      options.Service,
		queue:        options.Queue,
		runner:       options.Runner,
		scanInterval: scanInterval,
		batchSize:    batchSize,
		logger:       logger,
		inFlight:     make(map[string]struct{}),
		ctx:          ctx,
		cancel:       cancel,
	}, nil
}

// Start 启动后台扫描，先立即扫一次，之后按周期扫描并响应提交唤醒。
func (d *Dispatcher) Start() {
	d.startOnce.Do(func() {
		d.wg.Add(1)
		go d.loop()
	})
}

// Shutdown 停止扫描并等待扫描协程退出。
//
// 它不等待正在执行的任务：那些任务属于 queue 的生命周期，应该由
// queue.Shutdown 负责取消并等待。调用顺序应为 Dispatcher → Queue。
func (d *Dispatcher) Shutdown() {
	d.stopOnce.Do(func() { d.cancel() })
	d.wg.Wait()
}

// loop 是唯一的扫描协程：ticker 保证通知丢失时仍能找回任务，
// wake 让正常提交几乎立即被处理。
func (d *Dispatcher) loop() {
	defer d.wg.Done()

	ticker := time.NewTicker(d.scanInterval)
	defer ticker.Stop()

	d.scan()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-d.service.wake:
			d.scan()
		case <-ticker.C:
			d.scan()
		}
	}
}

// scan 取一批到期任务并尝试放入队列。
//
// 查询失败只记录日志：数据库暂时不可用时下一轮还会扫，不能清任务，
// 也不能把已经落库的任务当成失败。
func (d *Dispatcher) scan() {
	if d.ctx.Err() != nil {
		return
	}

	due, err := d.service.ListDue(d.ctx, time.Now(), d.batchSize)
	if err != nil {
		if d.ctx.Err() == nil {
			d.logger.Error("scan due tasks", "error", err)
		}
		return
	}

	for _, record := range due {
		if d.ctx.Err() != nil {
			return
		}
		d.enqueue(record)
	}
}

// enqueue 把一个到期记录按原 ID 放进执行队列。
//
// 本地 inFlight 集合挡在入队之前：同一轮或下一轮扫描再次看到它时直接跳过。
// 即使两个进程同时扫描，worker 里的 TryStart 条件更新也只会放行一个。
func (d *Dispatcher) enqueue(record model.Task) {
	d.mu.Lock()
	if _, ok := d.inFlight[record.ID]; ok {
		d.mu.Unlock()
		return
	}
	d.inFlight[record.ID] = struct{}{}
	d.mu.Unlock()

	job := &dispatchedJob{dispatcher: d, record: record}
	if err := d.queue.Enqueue(record.ID, record.OwnerID, job); err != nil {
		d.release(record.ID)
		switch {
		case errors.Is(err, queue.ErrQueueFull):
			// 内存队列暂时满不是任务失败：行还在数据库里，等下一轮扫描。
		case errors.Is(err, queue.ErrDuplicate), errors.Is(err, queue.ErrQueueClosed):
			// 重复入队或正在关闭：本轮跳过即可。
		default:
			d.logger.Warn("enqueue task", "task_id", record.ID, "error", err)
		}
	}
}

func (d *Dispatcher) release(id string) {
	d.mu.Lock()
	delete(d.inFlight, id)
	d.mu.Unlock()
}

// dispatchedJob 是“已取得数据库执行资格之后才运行”的包装。
//
// queue 只保证 worker 有界和超时；真正能否执行由 TryStart 决定。
// 这样出队后才发现任务已被别的执行者拿走、已终态或未到期时，不会误跑。
type dispatchedJob struct {
	dispatcher *Dispatcher
	record     model.Task
}

func (j *dispatchedJob) Run(ctx context.Context) (string, error) {
	return j.RunWithProgress(ctx, nil)
}

func (j *dispatchedJob) RunWithProgress(
	ctx context.Context,
	report queue.ProgressFunc,
) (string, error) {
	// 无论成功、失败还是 TryStart 失败，都要把本地 inFlight 名额还回去，
	// 否则等待重试的任务永远不会被下一轮扫描再次入队。
	defer j.dispatcher.release(j.record.ID)

	started, err := j.dispatcher.service.TryStart(ctx, j.record.ID, time.Now())
	if err != nil {
		return "", err
	}

	result, runErr := j.dispatcher.runner.Run(ctx, *started, report)
	if runErr != nil {
		if ctx.Err() != nil {
			// 取消或超时：数据库先保持 running。启动恢复会重新处理残留
			// running 记录（后续步骤）；不能在取消之后草率改成 failed，
			// 否则正常关闭会把可继续的任务错误地终结。
			return "", runErr
		}
		if markErr := j.dispatcher.finalize(ctx, func(finalizeCtx context.Context) error {
			return j.dispatcher.service.MarkFailed(
				finalizeCtx, started.ID, started.Attempts, runErr,
			)
		}); markErr != nil {
			j.dispatcher.logger.Warn(
				"mark task failed", "task_id", started.ID, "error", markErr,
			)
		}
		return "", runErr
	}

	if markErr := j.dispatcher.finalize(ctx, func(finalizeCtx context.Context) error {
		return j.dispatcher.service.MarkCompleted(
			finalizeCtx, started.ID, started.Attempts, result,
		)
	}); markErr != nil {
		// 业务已经跑完，但完成状态没有写进去。不能返回成功让调用方以为
		// 状态一致；数据库里仍是 running，后续恢复要核对结果再收敛。
		j.dispatcher.logger.Warn(
			"mark task completed", "task_id", started.ID, "error", markErr,
		)
		return "", markErr
	}

	return result, nil
}

// finalize 用独立超时写数据库。执行 context 此时可能已经超时，
// 但终态写入不应该跟着一起失败；同时仍然限制最长等待，避免卡住 worker。
func (d *Dispatcher) finalize(ctx context.Context, update func(context.Context) error) error {
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeTimeout)
	defer cancel()
	return update(finalizeCtx)
}
