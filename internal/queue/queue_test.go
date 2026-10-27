package queue

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestQueue(t *testing.T, workers, capacity int, timeout time.Duration) *Queue {
	t.Helper()

	q, err := New(workers, capacity, timeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Shutdown)
	return q
}

func TestBoundedQueue(t *testing.T) {
	q, err := New(2, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Shutdown)

	started := make(chan struct{}, 4)
	release := make(chan struct{})

	var once sync.Once
	unblock := func() {
		once.Do(func() { close(release) })
	}
	t.Cleanup(unblock)
	t.Cleanup(func() {
		unblock()
		q.Shutdown()
	})

	var active atomic.Int32

	job := JobFunc(func(context.Context) (string, error) {
		if n := active.Add(1); n > 2 {
			t.Errorf("too many running tasks: %d", n)
		}
		defer active.Add(-1)

		started <- struct{}{}
		<-release // 阻塞在这里，测试显式放行。
		return "ok", nil
	})

	submit := func() string {
		t.Helper()
		id, err := q.Submit(1, job)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	ids := []string{submit(), submit()}

	// 确定两个 worker 都已进入任务，而不是靠 sleep 猜。
	<-started
	<-started

	// 两个 worker 被占住，下面两个只能进入等待队列。
	ids = append(ids, submit(), submit())

	waiting, err := q.Get(1, ids[2])
	if err != nil {
		t.Fatal(err)
	}
	if waiting.Status != Queued {
		t.Fatalf("want queued, got %s", waiting.Status)
	}

	// 执行槽和等待槽都满了，第五个任务必须立即被拒绝。
	if _, err := q.Submit(1, job); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want queue full, got %v", err)
	}

	unblock()

	// 等四个任务都真正开始，避免 Shutdown 把排队中的任务当成取消。
	<-started
	<-started

	q.Shutdown() // 等四个已接收任务全部执行结束。

	for _, id := range ids {
		task, err := q.Get(1, id)
		if err != nil {
			t.Fatal(err)
		}
		if task.Status != Completed || task.Result != "ok" {
			t.Fatalf("unexpected result: %+v", task)
		}
	}

	if _, err := q.Get(2, ids[0]); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another owner must not access task: %v", err)
	}

	if _, err := q.Submit(1, job); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("want queue closed, got %v", err)
	}
}

func TestFailedJobRecordsErrorAndWorkerContinues(t *testing.T) {
	q := newTestQueue(t, 1, 2, time.Minute)

	started := make(chan struct{})
	failed, err := q.Submit(1, JobFunc(func(context.Context) (string, error) {
		close(started)
		return "", errors.New("boom")
	}))
	if err != nil {
		t.Fatal(err)
	}

	nextRan := make(chan struct{})
	next, err := q.Submit(1, JobFunc(func(context.Context) (string, error) {
		close(nextRan)
		return "ok", nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	<-started
	<-nextRan

	q.Shutdown()

	task, err := q.Get(1, failed)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != Failed || task.Error != "boom" || task.Result != "" {
		t.Fatalf("unexpected failed result: %+v", task)
	}

	task, err = q.Get(1, next)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != Completed || task.Result != "ok" {
		t.Fatalf("worker must continue after error: %+v", task)
	}
}

func TestPanickingJobDoesNotStopWorker(t *testing.T) {
	q := newTestQueue(t, 1, 1, time.Minute)

	started := make(chan struct{})
	panicked, err := q.Submit(1, JobFunc(func(context.Context) (string, error) {
		close(started)
		panic("boom")
	}))
	if err != nil {
		t.Fatal(err)
	}

	// 等 worker 取走第一个任务，否则容量 1 的队列会拒绝第二个任务。
	<-started

	nextStarted := make(chan struct{})
	next, err := q.Submit(1, JobFunc(func(context.Context) (string, error) {
		close(nextStarted)
		return "ok", nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	<-nextStarted
	q.Shutdown()

	task, err := q.Get(1, panicked)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != Failed || task.Error != "task panic: boom" {
		t.Fatalf("unexpected panic result: %+v", task)
	}

	task, err = q.Get(1, next)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != Completed || task.Result != "ok" {
		t.Fatalf("worker must keep running after panic: %+v", task)
	}
}

func TestTimeoutFailsJobAndWorkerContinues(t *testing.T) {
	q := newTestQueue(t, 1, 2, 50*time.Millisecond)

	slow, err := q.Submit(1, JobFunc(func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}))
	if err != nil {
		t.Fatal(err)
	}

	nextRan := make(chan struct{})
	next, err := q.Submit(1, JobFunc(func(context.Context) (string, error) {
		close(nextRan)
		return "ok", nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-nextRan:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not continue after timeout")
	}

	q.Shutdown()

	task, err := q.Get(1, slow)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != Failed || !strings.Contains(task.Error, "deadline exceeded") {
		t.Fatalf("unexpected timeout result: %+v", task)
	}

	task, err = q.Get(1, next)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != Completed || task.Result != "ok" {
		t.Fatalf("worker must continue after timeout: %+v", task)
	}
}

// 超时是协作式的：即使 context 已到期，只要 Run 没返回，worker 就不能领取下一项。
// 同时，Run 最终返回成功时，队列不能仅凭 deadline 已到就把它改成失败。
func TestTimeoutDoesNotReleaseWorkerSlot(t *testing.T) {
	q := newTestQueue(t, 1, 1, 20*time.Millisecond)

	release := make(chan struct{})
	firstStarted := make(chan struct{})
	first, err := q.Submit(1, JobFunc(func(context.Context) (string, error) {
		close(firstStarted)
		<-release // 故意不响应 context。
		return "late", nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	<-firstStarted

	secondStarted := make(chan struct{})
	second, err := q.Submit(1, JobFunc(func(context.Context) (string, error) {
		close(secondStarted)
		return "ok", nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-secondStarted:
		t.Fatal("worker started next job before timed-out job returned")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case <-secondStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not continue after the late job returned")
	}

	q.Shutdown()

	// 任务自己返回成功，即使已经超过 deadline 也不能丢结果。
	task, err := q.Get(1, first)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != Completed || task.Result != "late" {
		t.Fatalf("successful late result must be kept: %+v", task)
	}

	task, err = q.Get(1, second)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != Completed || task.Result != "ok" {
		t.Fatalf("second job should complete: %+v", task)
	}
}

func TestShutdownCancelsRunningJobAndWaitsForCleanup(t *testing.T) {
	q := newTestQueue(t, 1, 1, time.Minute)

	started := make(chan struct{})
	var cleaned atomic.Bool

	id, err := q.Submit(1, JobFunc(func(ctx context.Context) (string, error) {
		close(started)
		<-ctx.Done()
		cleaned.Store(true) // 模拟业务在返回前完成的清理。
		return "", ctx.Err()
	}))
	if err != nil {
		t.Fatal(err)
	}
	<-started

	shutdownDone := make(chan struct{})
	go func() {
		q.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not wait for job cleanup")
	}

	if !cleaned.Load() {
		t.Fatal("Shutdown returned before the job cleaned up")
	}

	task, err := q.Get(1, id)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != Failed || task.Error != context.Canceled.Error() {
		t.Fatalf("running job should be canceled: %+v", task)
	}
}

func TestShutdownCancelsQueuedJobsWithoutStartingThem(t *testing.T) {
	q := newTestQueue(t, 1, 2, time.Minute)

	started := make(chan struct{})
	running, err := q.Submit(1, JobFunc(func(ctx context.Context) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	}))
	if err != nil {
		t.Fatal(err)
	}
	<-started

	var ran atomic.Bool
	queued, err := q.Submit(1, JobFunc(func(context.Context) (string, error) {
		ran.Store(true)
		return "ok", nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	q.Shutdown()

	if ran.Load() {
		t.Fatal("queued job must not start after Shutdown")
	}

	for _, id := range []string{running, queued} {
		task, err := q.Get(1, id)
		if err != nil {
			t.Fatal(err)
		}
		if task.Status != Failed || task.Error != context.Canceled.Error() {
			t.Fatalf("unexpected canceled result: %+v", task)
		}
	}

	if _, err := q.Submit(1, JobFunc(func(context.Context) (string, error) {
		return "ok", nil
	})); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("want queue closed, got %v", err)
	}
}

// 任务返回成功时，不能仅因 deadline 已到就把结果改成失败。
func TestSuccessAfterDeadlineIsKept(t *testing.T) {
	q := newTestQueue(t, 1, 1, 10*time.Millisecond)

	started := make(chan struct{})
	id, err := q.Submit(1, JobFunc(func(context.Context) (string, error) {
		close(started)
		time.Sleep(30 * time.Millisecond) // 故意超过 deadline，但最终成功。
		return "published", nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	<-started

	q.Shutdown()

	task, err := q.Get(1, id)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != Completed || task.Result != "published" {
		t.Fatalf("successful result must be kept: %+v", task)
	}
}

// start 是任务开始的唯一边界：Shutdown 之后不能再把 queued 改成 running。
func TestStartIsRejectedAfterShutdown(t *testing.T) {
	q, err := New(1, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Shutdown)

	// 模拟 worker 已取到任务、但还没执行 start 时的排队记录。
	q.mu.Lock()
	q.tasks["queued"] = Snapshot{ID: "queued", OwnerID: 1, Status: Queued}
	q.mu.Unlock()

	q.Shutdown()

	if q.start("queued") {
		t.Fatal("task started after shutdown")
	}

	task, err := q.Get(1, "queued")
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != Queued {
		t.Fatalf("start must not change status after shutdown: %+v", task)
	}
}

// Submit 与 Shutdown 并发时不能 panic，已接收的任务必须全部落到终态。
func TestConcurrentSubmitAndShutdown(t *testing.T) {
	for iter := 0; iter < 30; iter++ {
		q, err := New(4, 16, time.Minute)
		if err != nil {
			t.Fatal(err)
		}

		var mu sync.Mutex
		accepted := make(map[string]int) // task ID -> owner
		var wg sync.WaitGroup

		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func(owner int) {
				defer wg.Done()
				for i := 0; i < 25; i++ {
					id, err := q.Submit(owner, JobFunc(func(ctx context.Context) (string, error) {
						<-ctx.Done()
						return "", ctx.Err()
					}))
					switch {
					case err == nil:
						mu.Lock()
						accepted[id] = owner
						mu.Unlock()
					case errors.Is(err, ErrQueueClosed), errors.Is(err, ErrQueueFull):
					default:
						t.Errorf("unexpected submit error: %v", err)
					}
				}
			}(g)
		}

		// 先接收一部分任务，再与提交并发关闭。
		time.Sleep(time.Millisecond)
		q.Shutdown()
		wg.Wait()

		for id, owner := range accepted {
			task, err := q.Get(owner, id)
			if err != nil {
				t.Errorf("accepted task lost: %v", err)
				continue
			}
			if task.Status != Completed && task.Status != Failed {
				t.Errorf("task left non-terminal: %+v", task)
			}
		}
	}
}

// start 与 Shutdown 并发时只有两种合法结果：
// start 先拿到锁则为 running；Shutdown 先拿到锁则保持 queued，绝不启动。
func TestStartAndShutdownRace(t *testing.T) {
	for iter := 0; iter < 200; iter++ {
		q, err := New(1, 1, time.Minute)
		if err != nil {
			t.Fatal(err)
		}

		const id = "queued"
		q.mu.Lock()
		q.tasks[id] = Snapshot{ID: id, OwnerID: 1, Status: Queued}
		q.mu.Unlock()

		started := make(chan bool, 1)
		go func() {
			started <- q.start(id)
		}()

		q.Shutdown()
		ok := <-started

		task, err := q.Get(1, id)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if task.Status != Running {
				t.Fatalf("started task lost running status: %+v", task)
			}
			continue
		}
		if task.Status != Queued {
			t.Fatalf("task must not start after shutdown: %+v", task)
		}
	}
}
