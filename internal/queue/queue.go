// Package queue 是一个带固定 worker 数的内存任务队列。
//
// 队列只负责调用任务、限制并发和记录结果，不认识文件、ZIP 等业务概念。
// channel 保存"要执行什么"，map 保存"执行到哪了"，任务出队后记录不会消失。
package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"

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
type Job interface {
	Run(ctx context.Context) (result string, err error)
}

// JobFunc 方便用普通函数构造假任务。
type JobFunc func(context.Context) (string, error)

func (f JobFunc) Run(ctx context.Context) (string, error) {
	return f(ctx)
}

// Snapshot 是查询返回的值，不包含待执行的业务对象。
type Snapshot struct {
	ID      string
	OwnerID int
	Status  Status
	Result  string
	Error   string
}

// item 只在排队和执行期间保留 Job。
type item struct {
	id  string
	job Job
}

// Queue 是一个固定 worker 数的任务队列。
type Queue struct {
	mu     sync.RWMutex
	jobs   chan item
	tasks  map[string]Snapshot
	closed bool
	wg     sync.WaitGroup
}

// New 创建队列并启动 workers 个 worker。
// capacity 只限制等待队列，不包含正在执行的任务。
func New(workers, capacity int) (*Queue, error) {
	if workers <= 0 || capacity <= 0 {
		return nil, errors.New("workers and capacity must be positive")
	}

	q := &Queue{
		jobs:  make(chan item, capacity),
		tasks: make(map[string]Snapshot),
	}

	q.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go q.worker()
	}

	return q, nil
}

// Submit 登记任务并放入等待队列，返回任务 ID。
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
		q.update(task.id, Running, "", nil)

		// 同步执行：当前任务返回之前，不领取下一项。
		result, err := run(context.Background(), task.job)

		if err != nil {
			q.update(task.id, Failed, "", err)
			continue
		}

		q.update(task.id, Completed, result, nil)
	}
}

// run 在单个任务边界内 recover，panic 不会让整个 worker 退出。
func run(ctx context.Context, job Job) (result string, err error) {
	defer func() {
		if p := recover(); p != nil {
			result = ""
			err = fmt.Errorf("task panic: %v", p)
		}
	}()

	return job.Run(ctx)
}

func (q *Queue) update(id string, status Status, result string, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	task := q.tasks[id]
	task.Status = status
	task.Result = result
	task.Error = ""
	if err != nil {
		task.Error = err.Error()
	}
	q.tasks[id] = task
}

// Shutdown 停止接收，排空已接收任务，等待所有 worker 退出。
// 只能由应用生命周期调用，不应在 Job 内调用。
func (q *Queue) Shutdown() {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		close(q.jobs)
	}
	q.mu.Unlock()

	// 不能持有 mu 等待，否则 worker 无法更新终态。
	q.wg.Wait()
}
