// Package queue 是一个带固定 worker 数的内存任务队列。
//
// 队列只负责调用任务、限制并发和记录结果，不认识文件、ZIP 等业务概念。
// channel 保存"要执行什么"，map 保存"执行到哪了"，任务出队后记录不会消失。
//
// 生命周期：New 创建队列自己的 context，每个任务从它派生出带超时的 context。
// 因此任务使用的 context 不来自任何 HTTP 请求。Shutdown 停止接收、取消执行、
// 等待所有 worker 退出。接入应用时必须按下面的顺序收尾：
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
	"sync"
	"time"

	"luckyCloudFile/internal/idgen"
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

// item 只在排队和执行期间保留 Job。
type item struct {
	id  string
	job Job
}

// Queue 是一个固定 worker 数的任务队列。
type Queue struct {
	mu      sync.RWMutex
	jobs    chan item
	tasks   map[string]Snapshot
	closed  bool
	ctx     context.Context
	cancel  context.CancelFunc
	timeout time.Duration
	wg      sync.WaitGroup
}

// New 创建队列并启动 workers 个 worker。
// capacity 只限制等待队列，不包含正在执行的任务；timeout 是单个任务的执行上限。
func New(workers, capacity int, timeout time.Duration) (*Queue, error) {
	if workers <= 0 || capacity <= 0 {
		return nil, errors.New("workers and capacity must be positive")
	}
	if timeout <= 0 {
		return nil, errors.New("timeout must be positive")
	}

	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		jobs:    make(chan item, capacity),
		tasks:   make(map[string]Snapshot),
		ctx:     ctx,
		cancel:  cancel,
		timeout: timeout,
	}

	q.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go q.worker()
	}

	return q, nil
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
	q.tasks[id] = Snapshot{
		ID:      id,
		OwnerID: ownerID,
		Status:  Queued,
		Stage:   string(Queued),
	}

	select {
	case q.jobs <- item{id: id, job: job}:
		return id, nil
	default:
		// 没有接收成功，不得留下永久 queued 的记录。
		delete(q.tasks, id)
		return "", ErrQueueFull
	}
}

// Get 返回任务的快照。不存在与不属于该 owner 统一返回 ErrNotFound。
func (q *Queue) Get(ownerID int, id string) (Snapshot, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	task, ok := q.tasks[id]
	if !ok || task.OwnerID != ownerID {
		return Snapshot{}, ErrNotFound
	}

	// map 保存值类型，这里返回副本。
	return task, nil
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

	task := q.tasks[id]
	task.Status = Running
	task.Stage = string(Running)
	q.tasks[id] = task
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
	if !ok || task.Status != Running {
		return
	}
	task.Stage = p.Stage
	task.Processed = p.Processed
	task.Total = p.Total
	q.tasks[id] = task
}

func (q *Queue) update(id string, status Status, result string, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	task := q.tasks[id]
	task.Status = status
	task.Stage = string(status)
	task.Result = result
	task.Error = ""
	if err != nil {
		task.Error = err.Error()
	}
	q.tasks[id] = task
}

// Shutdown 停止接收，取消正在执行和排队中的任务，等待所有 worker 退出。
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
