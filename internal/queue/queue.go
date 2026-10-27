// Package queue 是一个带固定 worker 数的内存任务队列。
//
// 队列只负责调用任务、限制并发和记录结果，不认识文件、ZIP 等业务概念。
// channel 保存"要执行什么"，map 保存"执行到哪了"。
//
// 内存边界由三部分组成：
//
//   - worker 数和等待队列容量限制同时存在多少个任务；
//   - timeout 限制单个任务最多执行多久；
//   - retention / maxTerminalRecords 限制已完成、已失败记录保留多久、多少条。
//
// 前两项限制“正在处理的任务”，第三项限制“处理完之后还留在内存里的记录”。
// 只限制前两项并不足以保证长期运行的内存有界：任务可以很快完成，终态快照
// 却会一直留在 map 里。
//
// 生命周期：New 创建队列自己的 context，每个任务从它派生出带超时的 context。
// 因此任务使用的 context 不来自任何 HTTP 请求。Shutdown 停止接收、取消执行、
// 等待所有 worker 和回收协程退出。接入应用时必须按下面的顺序收尾：
//
//	停止接收新请求（http.Server.Shutdown）
//	→ queue.Shutdown()（取消任务并等待清理）
//	→ database.Close()
//
// 顺序不能反：任务可能还在读写数据库。
//
// 注意：context 超时/取消是协作式的，不会强制杀死 goroutine。worker 会一直等
// Job.Run 返回才领取下一项，否则实际并发会超过 worker 数。因此不响应 context
// 的任务会让 Shutdown 一直等待。
package queue

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"luckyCloudFile/internal/idgen"
)

// 默认值。Options 里的零值表示“使用对应默认值”；负数仍然报错。
//
// 应用层通过 [Task] 配置显式传入这些参数，这里的默认值主要用于直接
// 构造队列的测试和库调用方，保证不写参数时仍然是一个有界队列。
const (
	DefaultWorkers            = 2
	DefaultCapacity           = 64
	DefaultTimeout            = 10 * time.Minute
	DefaultRetention          = time.Hour
	DefaultMaxTerminalRecords = 10000
)

// Status 是任务的当前状态。
type Status string

const (
	Queued    Status = "queued"
	Running   Status = "running"
	Completed Status = "completed"
	Failed    Status = "failed"
)

var (
	ErrQueueFull   = errors.New("queue is full")
	ErrQueueClosed = errors.New("queue is closed")
	ErrNotFound    = errors.New("task not found")
)

// Job 是业务与队列的边界。
// result 暂时用字符串；以后 ZIP 任务可以返回结果文件 ID。
//
// Run 必须响应 ctx 的取消和超时，并在返回前完成自己的清理。
// 返回 nil 表示成功；放弃执行时应返回 ctx.Err()。队列不会用 deadline
// 推断失败：只要 Run 返回 nil，即使已经超时也记为成功。
type Job interface {
	Run(ctx context.Context) (result string, err error)
}

// Progress 是一次可选的阶段/进度更新。
// Processed 只用于展示，不代表任务已经成功。
type Progress struct {
	Stage     string
	Processed int
	Total     int
}

// ProgressFunc 接收任务执行过程中的进度更新。
type ProgressFunc func(Progress)

// ProgressReporter 是 Job 的可选能力。实现了它的任务会通过 report
// 汇报阶段和已处理数量；worker 仍然同步等待 RunWithProgress 返回。
type ProgressReporter interface {
	RunWithProgress(ctx context.Context, report ProgressFunc) (string, error)
}

// JobFunc 方便用普通函数构造假任务。
type JobFunc func(context.Context) (string, error)

func (f JobFunc) Run(ctx context.Context) (string, error) {
	return f(ctx)
}

// Snapshot 是查询返回的值，不包含待执行的业务对象。
type Snapshot struct {
	ID        string
	OwnerID   int
	Status    Status
	Stage     string
	Processed int
	Total     int
	Result    string
	Error     string
}

// Options 是队列的容量和回收参数。零值使用对应默认值。
//
// Workers 和 Capacity 只限制排队和执行中的任务；Retention 和
// MaxTerminalRecords 才限制完成后的记录。两者必须同时有界：
// TTL 让记录在空闲时也会被回收，数量上限则在短时间大量任务完成时
// 给内存一个硬上限。
type Options struct {
	Workers   int
	Capacity  int
	Timeout   time.Duration
	Retention time.Duration

	// MaxTerminalRecords 是 completed + failed 记录的总数上限。
	// 超出时优先回收最早结束的记录；正在执行和排队中的任务不受影响。
	MaxTerminalRecords int
}

// item 只在排队和执行期间保留 Job。
type item struct {
	id  string
	job Job
}

// record 是任务状态和回收时间。finishedAt 只在进入终态时设置。
type record struct {
	snapshot   Snapshot
	finishedAt time.Time
}

// Queue 是一个固定 worker 数的任务队列。
type Queue struct {
	mu          sync.Mutex
	jobs        chan item
	tasks       map[string]*record
	closed      bool
	ctx         context.Context
	cancel      context.CancelFunc
	timeout     time.Duration
	retention   time.Duration
	maxTerminal int
	wg          sync.WaitGroup
}

// New 创建队列并启动固定数量的 worker 和终态记录回收协程。
//
// Capacity 只限制等待队列，不包含正在执行的任务；Timeout 是单个任务的执行上限。
// 已完成、已失败的记录保留 Retention，并且总数不超过 MaxTerminalRecords。
func New(options Options) (*Queue, error) {
	options, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		jobs:        make(chan item, options.Capacity),
		tasks:       make(map[string]*record),
		ctx:         ctx,
		cancel:      cancel,
		timeout:     options.Timeout,
		retention:   options.Retention,
		maxTerminal: options.MaxTerminalRecords,
	}

	q.wg.Add(options.Workers + 1)
	for i := 0; i < options.Workers; i++ {
		go q.worker()
	}
	// 终态记录必须有独立的回收者：只在 Submit/Get 时顺手清理的话，
	// 服务空闲下来以后旧记录会一直留在 map 里。
	go q.sweep()

	return q, nil
}

func normalizeOptions(options Options) (Options, error) {
	if options.Workers == 0 {
		options.Workers = DefaultWorkers
	}
	if options.Capacity == 0 {
		options.Capacity = DefaultCapacity
	}
	if options.Timeout == 0 {
		options.Timeout = DefaultTimeout
	}
	if options.Retention == 0 {
		options.Retention = DefaultRetention
	}
	if options.MaxTerminalRecords == 0 {
		options.MaxTerminalRecords = DefaultMaxTerminalRecords
	}

	if options.Workers < 0 {
		return Options{}, errors.New("workers must not be negative")
	}
	if options.Capacity < 0 {
		return Options{}, errors.New("capacity must not be negative")
	}
	if options.Timeout < 0 {
		return Options{}, errors.New("timeout must not be negative")
	}
	if options.Retention < 0 {
		return Options{}, errors.New("retention must not be negative")
	}
	if options.MaxTerminalRecords < 0 {
		return Options{}, errors.New("max terminal records must not be negative")
	}
	return options, nil
}

// Submit 登记任务并放入等待队列，返回任务 ID。
// 任务的执行 context 由队列生命周期和 timeout 生成，调用方不需要传。
func (q *Queue) Submit(ownerID int, job Job) (string, error) {
	if job == nil {
		return "", errors.New("job is required")
	}

	id, err := idgen.New()
	if err != nil {
		return "", err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return "", ErrQueueClosed
	}

	// 必须先登记，再让 worker 有机会取到任务。
	q.tasks[id] = &record{snapshot: Snapshot{
		ID:      id,
		OwnerID: ownerID,
		Status:  Queued,
		Stage:   string(Queued),
	}}

	select {
	case q.jobs <- item{id: id, job: job}:
		return id, nil
	default:
		// 没有接收成功，不得留下永久 queued 的记录。
		delete(q.tasks, id)
		return "", ErrQueueFull
	}
}

// Get 返回任务的快照。不存在、不属于该 owner 或已经超过保留期
// 统一返回 ErrNotFound。
func (q *Queue) Get(ownerID int, id string) (Snapshot, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.tasks[id]
	if !ok || task.snapshot.OwnerID != ownerID {
		return Snapshot{}, ErrNotFound
	}

	// 不依赖回收协程的调度：TTL 一到，下一次查询就应该看不到记录。
	// 这里顺手删除，回收协程的清理由 sweep 继续完成。
	if q.expired(task, time.Now()) {
		delete(q.tasks, id)
		return Snapshot{}, ErrNotFound
	}

	// map 保存指针，这里返回快照副本。
	return task.snapshot, nil
}

func (q *Queue) worker() {
	defer q.wg.Done()

	// channel 关闭后，仍会消费完已经缓冲的任务。
	for task := range q.jobs {
		// 任务开始（queued→running）与关闭判断在同一把锁内完成。
		// Shutdown 先拿到锁时这里返回 false，任务不会被执行。
		if !q.start(task.id) {
			q.update(task.id, Failed, "", context.Canceled)
			continue
		}

		// 执行 context 来自队列生命周期，而不是某个 HTTP 请求。
		ctx, cancel := context.WithTimeout(q.ctx, q.timeout)
		result, err := q.execute(ctx, task.id, task.job)
		cancel()

		// 返回 nil 就是成功。任务应自己响应 ctx：放弃时返回 ctx.Err()。
		// 不能在事后仅凭 deadline 已到就把成功改成失败，否则已经发布的结果会被丢弃。
		if err != nil {
			q.update(task.id, Failed, "", err)
			continue
		}

		q.update(task.id, Completed, result, nil)
	}
}

// start 是任务开始的唯一边界：在同一把锁内检查关闭并完成 queued→running。
//
// Shutdown 也在同一把锁内设置 closed 并取消，因此只有两种顺序：
//   - start 先拿到锁：任务算已开始，Shutdown 随后取消它的 context；
//   - Shutdown 先拿到锁：start 返回 false，任务不会执行。
func (q *Queue) start(id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return false
	}

	task, ok := q.tasks[id]
	if !ok {
		return false
	}
	task.snapshot.Status = Running
	task.snapshot.Stage = string(Running)
	return true
}

// execute 在单个任务边界内 recover，panic 不会让整个 worker 退出。
// 实现了 ProgressReporter 的任务会收到进度回调。
func (q *Queue) execute(ctx context.Context, id string, job Job) (result string, err error) {
	defer func() {
		if p := recover(); p != nil {
			result = ""
			err = fmt.Errorf("task panic: %v", p)
		}
	}()

	if reporter, ok := job.(ProgressReporter); ok {
		return reporter.RunWithProgress(ctx, func(p Progress) {
			q.report(id, p)
		})
	}
	return job.Run(ctx)
}

// report 只在任务仍在执行时记录进度，避免终态被迟到的回调覆盖。
func (q *Queue) report(id string, p Progress) {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.tasks[id]
	if !ok || task.snapshot.Status != Running {
		return
	}
	task.snapshot.Stage = p.Stage
	task.snapshot.Processed = p.Processed
	task.snapshot.Total = p.Total
}

func (q *Queue) update(id string, status Status, result string, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.tasks[id]
	if !ok {
		return
	}
	task.snapshot.Status = status
	task.snapshot.Stage = string(status)
	task.snapshot.Result = result
	task.snapshot.Error = ""
	if err != nil {
		task.snapshot.Error = err.Error()
	}

	// 终态记录从这里开始计算保留期，并进行一次数量裁剪。
	if isTerminal(status) {
		task.finishedAt = time.Now()
		q.trimTerminalLocked(id)
	}
}

// trimTerminalLocked 把终态记录裁剪到 maxTerminal 条，优先删除最早结束的。
// keepID 是刚刚结束的任务，数量相同时不能把它删掉。
//
// 这里按条扫描 map。ZIP 任务不是高频接口，maxTerminalRecords 又有明确上限；
// 用额外的 FIFO 索引换取 O(log n) 的回收不值得增加状态和并发复杂度。
func (q *Queue) trimTerminalLocked(keepID string) {
	if q.maxTerminal <= 0 {
		return
	}

	type candidate struct {
		id         string
		finishedAt time.Time
	}
	terminal := make([]candidate, 0, len(q.tasks))
	for id, task := range q.tasks {
		if isTerminal(task.snapshot.Status) {
			terminal = append(terminal, candidate{id: id, finishedAt: task.finishedAt})
		}
	}
	excess := len(terminal) - q.maxTerminal
	if excess <= 0 {
		return
	}

	sort.Slice(terminal, func(i, j int) bool {
		if terminal[i].finishedAt.Equal(terminal[j].finishedAt) {
			return terminal[i].id < terminal[j].id
		}
		return terminal[i].finishedAt.Before(terminal[j].finishedAt)
	})
	for _, task := range terminal {
		if excess == 0 {
			return
		}
		if task.id == keepID {
			continue
		}
		delete(q.tasks, task.id)
		excess--
	}
}

// sweep 定期回收超过 retention 的终态记录，直到 Shutdown 取消队列 context。
//
// 扫描间隔取 retention 的一半并限制在 10ms～1min：短 retention 的测试
// 不用等待太久，长 retention 也不会频繁唤醒。
func (q *Queue) sweep() {
	defer q.wg.Done()

	interval := q.retention / 2
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	if interval > time.Minute {
		interval = time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-q.ctx.Done():
			return
		case now := <-ticker.C:
			q.purgeExpired(now)
		}
	}
}

func (q *Queue) purgeExpired(now time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for id, task := range q.tasks {
		if q.expired(task, now) {
			delete(q.tasks, id)
		}
	}
}

func (q *Queue) expired(task *record, now time.Time) bool {
	return isTerminal(task.snapshot.Status) &&
		!task.finishedAt.IsZero() &&
		!now.Before(task.finishedAt.Add(q.retention))
}

func isTerminal(status Status) bool {
	return status == Completed || status == Failed
}

// Shutdown 停止接收，取消正在执行和排队中的任务，等待所有 worker 和回收协程退出。
// 已开始的任务会收到 ctx 取消，但 worker 必须等它真正返回（完成清理）才退出。
// 可以重复调用；只能由应用生命周期调用，不应在 Job 内调用。
func (q *Queue) Shutdown() {
	q.mu.Lock()
	if !q.closed {
		// closed 与 start 共用同一把锁：关闭之后不再允许 queued→running。
		q.closed = true
		q.cancel()
		// 再关闭 channel，让 worker 排空缓冲后退出。
		close(q.jobs)
	}
	q.mu.Unlock()

	// 不能持有 mu 等待，否则 worker 无法更新终态。
	q.wg.Wait()
}
