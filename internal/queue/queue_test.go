package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestBoundedQueue(t *testing.T) {
	q, err := New(2, 2)
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{}, 4)
	release := make(chan struct{})

	var once sync.Once
	unblock := func() {
		once.Do(func() { close(release) })
	}
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

func TestFailedJobRecordsError(t *testing.T) {
	q, err := New(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Shutdown()

	id, err := q.Submit(1, JobFunc(func(context.Context) (string, error) {
		return "", errors.New("boom")
	}))
	if err != nil {
		t.Fatal(err)
	}

	q.Shutdown()

	task, err := q.Get(1, id)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != Failed || task.Error != "boom" || task.Result != "" {
		t.Fatalf("unexpected result: %+v", task)
	}
}

func TestPanickingJobDoesNotStopWorker(t *testing.T) {
	q, err := New(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Shutdown()

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

	next, err := q.Submit(1, JobFunc(func(context.Context) (string, error) {
		return "ok", nil
	}))
	if err != nil {
		t.Fatal(err)
	}

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
