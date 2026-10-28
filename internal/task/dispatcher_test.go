package task

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/queue"
)

// newTaskTestQueue 创建测试用有界队列，并在测试结束时收尾。
// 注册顺序保证 Dispatcher 先停止，再由 queue 等待正在执行的任务退出。
func newTaskTestQueue(t *testing.T, workers, capacity int) *queue.Queue {
	t.Helper()
	q, err := queue.New(queue.Options{
		Workers:  workers,
		Capacity: capacity,
		Timeout:  time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Shutdown)
	return q
}

// startTestDispatcher 启动调度器；t.Cleanup 在队列收尾之后（后进先出）
// 才执行，所以调度器会先停止扫描。
func startTestDispatcher(
	t *testing.T,
	service *Service,
	q *queue.Queue,
	runner Runner,
	interval time.Duration,
) *Dispatcher {
	t.Helper()
	return startTestDispatcherWithRetry(t, service, q, runner, interval, 50*time.Millisecond)
}

// startTestDispatcherWithRetry 允许测试指定退避基数。需要观察 retry_wait
// 的测试用更长的基数，避免状态在断言之前就到期。
func startTestDispatcherWithRetry(
	t *testing.T,
	service *Service,
	q *queue.Queue,
	runner Runner,
	interval, retryBase time.Duration,
) *Dispatcher {
	t.Helper()

	// 测试库可能同时留有其他包创建的 queued 任务（例如清理任务）。
	// 调度器只负责自己注册的类型，这里把测试限定在 fake_echo 上，
	// 避免扫描到别的任务而让断言看到错误的记录。
	dispatcher, err := NewDispatcher(DispatcherOptions{
		Service:      service,
		Queue:        q,
		Runner:       runner,
		Types:        []string{"fake_echo"},
		ScanInterval: interval,
		BatchSize:    10,
		RetryBase:    retryBase,
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.Start()
	t.Cleanup(dispatcher.Shutdown)
	return dispatcher
}

// waitTask 轮询数据库直到任务满足条件。数据库写入发生在任务执行之后，
// 这里用带截止时间的条件轮询等待，而不是假定某个固定耗时。
func waitTask(
	t *testing.T,
	service *Service,
	ownerID int,
	id string,
	want func(model.Task) bool,
) model.Task {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	var last model.Task
	var lastErr error
	for {
		record, err := service.GetOwned(context.Background(), ownerID, id)
		if err == nil {
			last = *record
			if want(last) {
				return last
			}
		}
		lastErr = err
		if time.Now().After(deadline) {
			t.Fatalf("task %s did not reach the expected state: %+v (err=%v)", id, last, lastErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func pendingCount(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var count int64
	err := db.Model(&model.Task{}).
		Where("status IN ?", []string{
			model.TaskStatusQueued,
			model.TaskStatusRunning,
			model.TaskStatusRetryWait,
		}).
		Count(&count).Error
	if err != nil {
		t.Fatal(err)
	}
	return count
}

// TestDispatcherWakesOnSubmit 验证正常路径：落库后的唤醒让任务几乎立即
// 执行；扫描间隔设成一小时，确保它不可能是周期扫描立功。
func TestDispatcherWakesOnSubmit(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "wake")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	service := NewService(db).WithMaxPending(100)
	q := newTaskTestQueue(t, 1, 4)

	started := make(chan string, 4)
	runner := RunnerFunc(func(
		ctx context.Context,
		record model.Task,
		report queue.ProgressFunc,
	) (string, error) {
		if report != nil {
			report(queue.Progress{Stage: "compressing", Processed: 1, Total: 2})
		}
		started <- record.ID
		return "result-file", nil
	})
	startTestDispatcher(t, service, q, runner, time.Hour)

	submitted, err := service.Submit(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
		StateJSON: `{"input":"hello"}`,
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case id := <-started:
		if id != submitted.ID {
			t.Fatalf("runner started task %q, want stable id %q", id, submitted.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("submitted task was not woken up for execution")
	}

	finished := waitTask(t, service, owner.ID, submitted.ID, func(record model.Task) bool {
		return record.Status == model.TaskStatusCompleted
	})
	if finished.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", finished.Attempts)
	}
	if finished.ResultFileID == nil || *finished.ResultFileID != "result-file" {
		t.Fatalf("result file = %v, want result-file", finished.ResultFileID)
	}
	if finished.FinishedAt == nil {
		t.Fatal("completed task has no finished_at")
	}

	// 内存快照（展示用）也应该记录这次执行的结果。
	snapshot, err := q.Get(owner.ID, submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != queue.Completed || snapshot.Result != "result-file" {
		t.Fatalf("queue snapshot = %+v", snapshot)
	}
}

// TestDispatcherPeriodicScanRecoversLostWake 验证通知丢失的边界：
// 用 Create 直接落库（不触发唤醒），只有周期扫描能发现它。
func TestDispatcherPeriodicScanRecoversLostWake(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "lost-wake")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	service := NewService(db).WithMaxPending(100)
	q := newTaskTestQueue(t, 1, 4)

	started := make(chan string, 4)
	runner := RunnerFunc(func(
		ctx context.Context,
		record model.Task,
		report queue.ProgressFunc,
	) (string, error) {
		started <- record.ID
		return "result-file", nil
	})
	// 先启动调度器，再创建任务：启动时的那一次扫描看不到它，
	// 只有 ticker 能把任务找回来。
	startTestDispatcher(t, service, q, runner, 20*time.Millisecond)

	created, err := service.Create(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
		StateJSON: `{}`,
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case id := <-started:
		if id != created.ID {
			t.Fatalf("runner started task %q, want %q", id, created.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("periodic scan did not recover the task whose wake was lost")
	}

	waitTask(t, service, owner.ID, created.ID, func(record model.Task) bool {
		return record.Status == model.TaskStatusCompleted
	})
}

// TestDispatcherIgnoresDuplicateScans 验证重复扫描边界：
// 任务执行期间即使被扫描很多轮，本地集合和 TryStart 也只放行一次。
func TestDispatcherIgnoresDuplicateScans(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "duplicate")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	service := NewService(db).WithMaxPending(100)
	q := newTaskTestQueue(t, 1, 4)

	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	started := make(chan struct{}, 8)
	var starts atomic.Int32
	runner := RunnerFunc(func(
		ctx context.Context,
		record model.Task,
		report queue.ProgressFunc,
	) (string, error) {
		starts.Add(1)
		started <- struct{}{}
		<-release
		return "result-file", nil
	})
	startTestDispatcher(t, service, q, runner, 10*time.Millisecond)

	submitted, err := service.Submit(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("task never started")
	}

	// 让调度器空转若干轮；任务仍在执行，重复扫描不能再次执行它。
	time.Sleep(100 * time.Millisecond)
	if got := starts.Load(); got != 1 {
		t.Fatalf("runner started %d times, want 1", got)
	}
	running, err := service.GetOwned(ctx, owner.ID, submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if running.Status != model.TaskStatusRunning || running.Attempts != 1 {
		t.Fatalf("running task = %+v, want running with attempts 1", running)
	}

	releaseOnce.Do(func() { close(release) })
	waitTask(t, service, owner.ID, submitted.ID, func(record model.Task) bool {
		return record.Status == model.TaskStatusCompleted
	})
	if got := starts.Load(); got != 1 {
		t.Fatalf("runner started %d times after completion, want 1", got)
	}
}

// TestDispatcherQueueFullKeepsTaskForLaterScan 验证内存队列满的边界：
// 已经落库的任务仍算接收成功，暂时留在 queued，等队列腾出位置后再执行。
func TestDispatcherQueueFullKeepsTaskForLaterScan(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "queue-full")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	service := NewService(db).WithMaxPending(100)
	q := newTaskTestQueue(t, 1, 1)

	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	// 占住唯一的 worker。
	blockerStarted := make(chan struct{})
	if err := q.Enqueue("blocker", owner.ID, queue.JobFunc(func(context.Context) (string, error) {
		close(blockerStarted)
		<-release
		return "", nil
	})); err != nil {
		t.Fatal(err)
	}
	<-blockerStarted
	// 再占满唯一的等待槽。
	if err := q.Enqueue("filler", owner.ID, queue.JobFunc(func(context.Context) (string, error) {
		<-release
		return "", nil
	})); err != nil {
		t.Fatal(err)
	}

	ran := make(chan string, 4)
	runner := RunnerFunc(func(
		ctx context.Context,
		record model.Task,
		report queue.ProgressFunc,
	) (string, error) {
		ran <- record.ID
		return "result-file", nil
	})
	startTestDispatcher(t, service, q, runner, 10*time.Millisecond)

	submitted, err := service.Submit(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
	})
	if err != nil {
		t.Fatalf("submit must accept a task even when the memory queue is full: %v", err)
	}

	// 队列一直是满的，任务应该保持 queued，不能被当成提交失败丢掉。
	time.Sleep(50 * time.Millisecond)
	pending, err := service.GetOwned(ctx, owner.ID, submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if pending.Status != model.TaskStatusQueued {
		t.Fatalf("task status = %q while queue is full, want queued", pending.Status)
	}

	// 腾出队列后，下一次扫描必须把它找回来执行。
	releaseOnce.Do(func() { close(release) })
	select {
	case id := <-ran:
		if id != submitted.ID {
			t.Fatalf("runner started task %q, want %q", id, submitted.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("task was never executed after the queue drained")
	}
	waitTask(t, service, owner.ID, submitted.ID, func(record model.Task) bool {
		return record.Status == model.TaskStatusCompleted
	})
}

// TestDispatcherMarksFailedTask 验证执行错误会写回数据库终态。
func TestDispatcherMarksFailedTask(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "failure")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	service := NewService(db).WithMaxPending(100)
	q := newTaskTestQueue(t, 1, 4)
	runner := RunnerFunc(func(
		ctx context.Context,
		record model.Task,
		report queue.ProgressFunc,
	) (string, error) {
		return "", errors.New("archive failed")
	})
	startTestDispatcher(t, service, q, runner, 20*time.Millisecond)

	submitted, err := service.Submit(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
	})
	if err != nil {
		t.Fatal(err)
	}

	failed := waitTask(t, service, owner.ID, submitted.ID, func(record model.Task) bool {
		return record.Status == model.TaskStatusFailed
	})
	if failed.LastError != "archive failed" {
		t.Fatalf("last_error = %q, want %q", failed.LastError, "archive failed")
	}
	if failed.Attempts != 1 || failed.FinishedAt == nil {
		t.Fatalf("failed task = %+v", failed)
	}
	if failed.ResultFileID != nil {
		t.Fatalf("failed task has result file %v", failed.ResultFileID)
	}
}

// TestSubmitRejectsAtPendingLimitBeforeInsert 验证数据库积压上限：
// 达到上限后拒绝发生在插入之前，不会留下一条已落库却返回失败的记录。
func TestSubmitRejectsAtPendingLimitBeforeInsert(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "admission")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	// 用当前库里的待执行量 +1 作为上限，避免测试库里的其它记录影响断言。
	limit := pendingCount(t, db) + 1
	service := NewService(db).WithMaxPending(int(limit))

	first, err := service.Create(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := pendingCount(t, db); got != limit {
		t.Fatalf("pending tasks = %d, want %d", got, limit)
	}

	if _, err := service.Submit(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
	}); !errors.Is(err, ErrTooManyPending) {
		t.Fatalf("submit at limit error = %v, want ErrTooManyPending", err)
	}
	if got := pendingCount(t, db); got != limit {
		t.Fatalf("rejected submit changed pending tasks to %d, want %d", got, limit)
	}

	// 把第一条任务收尾，腾出名额后提交应该成功。
	if _, err := service.TryStart(ctx, first.ID, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := service.MarkFailed(ctx, first.ID, 1, errors.New("done for test")); err != nil {
		t.Fatal(err)
	}
	if got := pendingCount(t, db); got != limit-1 {
		t.Fatalf("pending tasks after finishing = %d, want %d", got, limit-1)
	}

	accepted, err := service.Submit(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
	})
	if err != nil {
		t.Fatalf("submit after freeing capacity: %v", err)
	}
	if accepted.ID == "" || accepted.Status != model.TaskStatusQueued {
		t.Fatalf("accepted task = %+v", accepted)
	}
}

// TestDispatcherRunsDueRetryWait 验证 retry_wait 也是调度候选：
// 退避到期后按原 ID 重新入队，用新的 attempts 再次取得执行资格。
func TestDispatcherRunsDueRetryWait(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "retry-scan")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	service := NewService(db).WithMaxPending(100)
	q := newTaskTestQueue(t, 1, 4)

	created, err := service.Create(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 模拟第一次执行失败后进入退避，并且退避时间已经过去。
	started, err := service.TryStart(ctx, created.ID, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if started.Attempts != 1 {
		t.Fatalf("first attempts = %d, want 1", started.Attempts)
	}
	if err := service.MarkRetry(
		ctx, created.ID, started.Attempts,
		time.Now().Add(-time.Second), errors.New("temporary"),
	); err != nil {
		t.Fatal(err)
	}

	ran := make(chan model.Task, 4)
	runner := RunnerFunc(func(
		ctx context.Context,
		record model.Task,
		report queue.ProgressFunc,
	) (string, error) {
		ran <- record
		return "result-file", nil
	})
	startTestDispatcher(t, service, q, runner, 20*time.Millisecond)

	select {
	case record := <-ran:
		if record.ID != created.ID {
			t.Fatalf("runner started task %q, want %q", record.ID, created.ID)
		}
		if record.Attempts != 2 {
			t.Fatalf("retry attempt = %d, want 2", record.Attempts)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("due retry_wait task was not scanned and executed")
	}

	finished := waitTask(t, service, owner.ID, created.ID, func(record model.Task) bool {
		return record.Status == model.TaskStatusCompleted
	})
	if finished.Attempts != 2 {
		t.Fatalf("final attempts = %d, want 2", finished.Attempts)
	}
	if finished.LastError != "" {
		t.Fatalf("completed task kept last_error %q", finished.LastError)
	}
}

// TestDispatcherRetriesMarkedTemporaryFailure 验证带 task.Retryable 标记的
// 失败会进入 retry_wait：next_run_at 已经写好，worker 立即释放；到期后
// 用新的 attempts 再执行并成功。
func TestDispatcherRetriesMarkedTemporaryFailure(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "retryable")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	service := NewService(db).WithMaxPending(100)
	q := newTaskTestQueue(t, 1, 4)

	var attempts atomic.Int32
	firstFailed := make(chan struct{})
	runner := RunnerFunc(func(
		ctx context.Context,
		record model.Task,
		report queue.ProgressFunc,
	) (string, error) {
		if attempts.Add(1) == 1 {
			close(firstFailed)
			return "", Retryable(errors.New("temporary database failure"))
		}
		return "result-file", nil
	})
	startTestDispatcherWithRetry(t, service, q, runner, 10*time.Millisecond, time.Second)

	submitted, err := service.Submit(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstFailed:
	case <-time.After(5 * time.Second):
		t.Fatal("task never started")
	}

	waiting := waitTask(t, service, owner.ID, submitted.ID, func(record model.Task) bool {
		return record.Status == model.TaskStatusRetryWait
	})
	if waiting.Attempts != 1 {
		t.Fatalf("retry_wait attempts = %d, want 1", waiting.Attempts)
	}
	if !waiting.NextRunAt.After(time.Now()) {
		t.Fatalf("retry_wait next_run_at = %s, want a future time", waiting.NextRunAt)
	}
	if waiting.LastError == "" {
		t.Fatal("retry_wait task has no error")
	}

	finished := waitTask(t, service, owner.ID, submitted.ID, func(record model.Task) bool {
		return record.Status == model.TaskStatusCompleted
	})
	if finished.Attempts != 2 {
		t.Fatalf("completed attempts = %d, want 2", finished.Attempts)
	}
	if finished.ResultFileID == nil || *finished.ResultFileID != "result-file" {
		t.Fatalf("completed result = %v", finished.ResultFileID)
	}
}

// TestDispatcherFailsWhenRetriesExhausted 验证临时错误也受 max_attempts
// 约束：最后一次执行失败后必须明确 failed，不能永远停在 retry_wait。
func TestDispatcherFailsWhenRetriesExhausted(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "retry-exhausted")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	service := NewService(db).WithMaxPending(100)
	q := newTaskTestQueue(t, 1, 4)
	runner := RunnerFunc(func(
		ctx context.Context,
		record model.Task,
		report queue.ProgressFunc,
	) (string, error) {
		return "", Retryable(errors.New("still down"))
	})
	startTestDispatcher(t, service, q, runner, 10*time.Millisecond)

	submitted, err := service.Submit(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	failed := waitTask(t, service, owner.ID, submitted.ID, func(record model.Task) bool {
		return record.Status == model.TaskStatusFailed
	})
	if failed.Attempts != 1 {
		t.Fatalf("failed attempts = %d, want 1", failed.Attempts)
	}
	if failed.LastError == "" {
		t.Fatal("failed task has no error")
	}
}

// TestDispatcherTimeoutBecomesRetryWait 验证执行超时不会把可恢复业务
// 直接判死：worker 返回后写 retry_wait，下一次调度继续。
func TestDispatcherTimeoutBecomesRetryWait(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	owner := createOwner(t, db, "timeout-retry")
	t.Cleanup(func() { cleanupOwners(t, db, owner.ID) })

	q, err := queue.New(queue.Options{
		Workers: 1, Capacity: 4, Timeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Shutdown)

	service := NewService(db).WithMaxPending(100)
	runner := RunnerFunc(func(
		ctx context.Context,
		record model.Task,
		report queue.ProgressFunc,
	) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	startTestDispatcher(t, service, q, runner, 10*time.Millisecond)

	submitted, err := service.Submit(ctx, CreateParams{
		Type: "fake_echo", OwnerID: owner.ID, Phase: "prepare",
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	waiting := waitTask(t, service, owner.ID, submitted.ID, func(record model.Task) bool {
		return record.Status == model.TaskStatusRetryWait
	})
	if waiting.Attempts != 1 {
		t.Fatalf("timeout attempts = %d, want 1", waiting.Attempts)
	}
	if waiting.LastError == "" {
		t.Fatal("timeout task has no error")
	}
}

// TestRetryDelayIsCappedBackoff 验证退避按执行次数增长并有上限。
func TestRetryDelayIsCappedBackoff(t *testing.T) {
	for attempt, want := range map[int]time.Duration{
		1:  10 * time.Second,
		2:  20 * time.Second,
		3:  40 * time.Second,
		4:  time.Minute,
		10: time.Minute,
	} {
		if got := retryDelay(10*time.Second, attempt); got != want {
			t.Fatalf("retryDelay(%d) = %s, want %s", attempt, got, want)
		}
	}
}
