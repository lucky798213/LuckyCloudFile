package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"luckyCloudFile/internal/archive"
	"luckyCloudFile/internal/queue"
)

// fakeTaskQueue 只记录提交并返回预设快照，用来测试 HTTP 映射。
type fakeTaskQueue struct {
	mu        sync.Mutex
	next      int
	jobs      []queue.Job
	owners    []int
	tasks     map[string]queue.Snapshot
	submitErr error
}

func newFakeTaskQueue() *fakeTaskQueue {
	return &fakeTaskQueue{tasks: make(map[string]queue.Snapshot)}
}

func (f *fakeTaskQueue) Submit(ownerID int, job queue.Job) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.submitErr != nil {
		return "", f.submitErr
	}
	f.next++
	id := fmt.Sprintf("task-%d", f.next)
	f.jobs = append(f.jobs, job)
	f.owners = append(f.owners, ownerID)
	f.tasks[id] = queue.Snapshot{
		ID:      id,
		OwnerID: ownerID,
		Status:  queue.Queued,
		Stage:   "queued",
	}
	return id, nil
}

func (f *fakeTaskQueue) Get(ownerID int, id string) (queue.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	task, ok := f.tasks[id]
	if !ok || task.OwnerID != ownerID {
		return queue.Snapshot{}, queue.ErrNotFound
	}
	return task, nil
}

func (f *fakeTaskQueue) setTask(task queue.Snapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks[task.ID] = task
}

func newTaskRouter(t *testing.T, tasks *fakeTaskQueue) http.Handler {
	t.Helper()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewRouterWithTasks(logger, newFakeFileService(), newFakeAuthService(), tasks)
}

func taskRequest(method, path, body, token string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return authorize(req, token)
}

func TestCreateArchiveTask(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		submitErr  error
		wantStatus int
		wantCode   string
	}{
		{
			name:       "accepted",
			body:       `{"file_ids":["file-a","file-b"],"parent_id":"dir-1","name":"bundle.zip"}`,
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "invalid json",
			body:       `{"file_ids":`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_json",
		},
		{
			name:       "no sources",
			body:       `{"file_ids":[],"name":"bundle.zip"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "empty file ID",
			body:       `{"file_ids":[""],"name":"bundle.zip"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "invalid result name",
			body:       `{"file_ids":["file-a"],"name":"../bundle.zip"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "queue full",
			body:       `{"file_ids":["file-a"],"name":"bundle.zip"}`,
			submitErr:  queue.ErrQueueFull,
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "queue_full",
		},
		{
			name:       "queue closed",
			body:       `{"file_ids":["file-a"],"name":"bundle.zip"}`,
			submitErr:  queue.ErrQueueClosed,
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "queue_shutdown",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks := newFakeTaskQueue()
			tasks.submitErr = tc.submitErr
			router := newTaskRouter(t, tasks)

			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, taskRequest(http.MethodPost, "/api/archive-tasks", tc.body, testTokenA))

			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
			if tc.wantCode != "" {
				assertErrorCode(t, recorder, tc.wantCode)
				return
			}

			var body archiveTaskResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.TaskID != "task-1" {
				t.Fatalf("task_id = %q, want task-1", body.TaskID)
			}
			if len(tasks.jobs) != 1 || tasks.owners[0] != testUserAID {
				t.Fatalf("submitted owners = %v, want one task for user %d", tasks.owners, testUserAID)
			}
			if _, ok := tasks.jobs[0].(*archive.Job); !ok {
				t.Fatalf("submitted job is %T, want *archive.Job", tasks.jobs[0])
			}
		})
	}
}

func TestGetTask(t *testing.T) {
	tasks := newFakeTaskQueue()
	tasks.setTask(queue.Snapshot{
		ID:        "task-running",
		OwnerID:   testUserAID,
		Status:    queue.Running,
		Stage:     "compressing",
		Processed: 1,
		Total:     3,
	})
	tasks.setTask(queue.Snapshot{
		ID:        "task-done",
		OwnerID:   testUserAID,
		Status:    queue.Completed,
		Stage:     "completed",
		Processed: 3,
		Total:     3,
		Result:    "file-9",
	})
	tasks.setTask(queue.Snapshot{
		ID:      "task-other",
		OwnerID: testUserBID,
		Status:  queue.Queued,
		Stage:   "queued",
	})

	router := newTaskRouter(t, tasks)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, taskRequest(http.MethodGet, "/api/tasks/task-running", "", testTokenA))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var running taskStatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &running); err != nil {
		t.Fatal(err)
	}
	if running.TaskID != "task-running" || running.Status != "running" ||
		running.Stage != "compressing" || running.ProcessedFiles != 1 ||
		running.TotalFiles != 3 || running.ResultFileID != "" {
		t.Fatalf("unexpected running task: %+v", running)
	}

	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, taskRequest(http.MethodGet, "/api/tasks/task-done", "", testTokenA))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var done taskStatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &done); err != nil {
		t.Fatal(err)
	}
	if done.Status != "completed" || done.ResultFileID != "file-9" {
		t.Fatalf("unexpected completed task: %+v", done)
	}

	// 别人的任务和不存在的任务必须同样返回 404。
	for _, id := range []string{"task-other", "missing"} {
		recorder = httptest.NewRecorder()
		router.ServeHTTP(recorder, taskRequest(http.MethodGet, "/api/tasks/"+id, "", testTokenA))
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("%s: status = %d, want 404: %s", id, recorder.Code, recorder.Body.String())
		}
		assertErrorCode(t, recorder, "task_not_found")
	}
}
