package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"luckyCloudFile/internal/archive"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/queue"
	"luckyCloudFile/internal/task"
	"luckyCloudFile/internal/verify"
)

// fakeTaskService 模拟 task.Service 的持久化接口：
// 提交时生成稳定 ID 并保存记录，查询时按 owner 隔离。
type fakeTaskService struct {
	mu        sync.Mutex
	next      int
	submitted []task.CreateParams
	tasks     map[string]*model.Task
	submitErr error
}

func newFakeTaskService() *fakeTaskService {
	return &fakeTaskService{tasks: make(map[string]*model.Task)}
}

func (f *fakeTaskService) Submit(
	ctx context.Context,
	params task.CreateParams,
) (*model.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.submitErr != nil {
		return nil, f.submitErr
	}
	f.next++
	params.ID = fmt.Sprintf("task-%d", f.next)
	f.submitted = append(f.submitted, params)

	record := &model.Task{
		ID:           params.ID,
		Type:         params.Type,
		OwnerID:      params.OwnerID,
		Status:       model.TaskStatusQueued,
		Phase:        params.Phase,
		StateVersion: params.StateVersion,
		StateJSON:    params.StateJSON,
	}
	f.tasks[record.ID] = record
	return record, nil
}

func (f *fakeTaskService) GetOwned(
	ctx context.Context,
	ownerID int,
	id string,
) (*model.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	record, ok := f.tasks[id]
	if !ok || record.OwnerID != ownerID {
		return nil, task.ErrNotFound
	}
	copied := *record
	return &copied, nil
}

func (f *fakeTaskService) setTask(record model.Task) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks[record.ID] = &record
}

// fakeTaskSnapshots 提供运行期进度；没有条目时返回 ErrNotFound。
type fakeTaskSnapshots struct {
	tasks map[string]queue.Snapshot
}

func newFakeTaskSnapshots() *fakeTaskSnapshots {
	return &fakeTaskSnapshots{tasks: make(map[string]queue.Snapshot)}
}

func (f *fakeTaskSnapshots) Get(ownerID int, id string) (queue.Snapshot, error) {
	snapshot, ok := f.tasks[id]
	if !ok || snapshot.OwnerID != ownerID {
		return queue.Snapshot{}, queue.ErrNotFound
	}
	return snapshot, nil
}

func (f *fakeTaskSnapshots) setTask(snapshot queue.Snapshot) {
	f.tasks[snapshot.ID] = snapshot
}

func newTaskRouter(
	t *testing.T,
	tasks *fakeTaskService,
	snapshots *fakeTaskSnapshots,
) http.Handler {
	t.Helper()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewRouterWithTasks(
		logger, newFakeFileService(), newFakeAuthService(), tasks, snapshots,
	)
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
			name:       "pending limit",
			body:       `{"file_ids":["file-a"],"name":"bundle.zip"}`,
			submitErr:  task.ErrTooManyPending,
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "task_limit",
		},
		{
			name:       "database error",
			body:       `{"file_ids":["file-a"],"name":"bundle.zip"}`,
			submitErr:  errors.New("database down"),
			wantStatus: http.StatusInternalServerError,
			wantCode:   "internal_error",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks := newFakeTaskService()
			tasks.submitErr = tc.submitErr
			router := newTaskRouter(t, tasks, newFakeTaskSnapshots())

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

			// 提交给持久化层的是可重建的输入参数，不是 Job 指针。
			if len(tasks.submitted) != 1 {
				t.Fatalf("submitted %d tasks, want 1", len(tasks.submitted))
			}
			submitted := tasks.submitted[0]
			if submitted.OwnerID != testUserAID ||
				submitted.Type != archive.TaskType ||
				submitted.Phase != "prepare" ||
				submitted.StateVersion != archive.TaskStateVersion {
				t.Fatalf("unexpected submit params: %+v", submitted)
			}
			var state archive.TaskState
			if err := json.Unmarshal([]byte(submitted.StateJSON), &state); err != nil {
				t.Fatal(err)
			}
			if len(state.SourceIDs) != 2 ||
				state.SourceIDs[0] != "file-a" ||
				state.SourceIDs[1] != "file-b" ||
				state.ParentID != "dir-1" ||
				state.Name != "bundle.zip" {
				t.Fatalf("unexpected archive state: %+v", state)
			}
		})
	}
}

func TestGetTask(t *testing.T) {
	tasks := newFakeTaskService()
	tasks.setTask(model.Task{
		ID:      "task-running",
		OwnerID: testUserAID,
		Status:  model.TaskStatusRunning,
		Phase:   "compress",
	})
	result := "file-9"
	tasks.setTask(model.Task{
		ID:           "task-done",
		OwnerID:      testUserAID,
		Status:       model.TaskStatusCompleted,
		Phase:        "publish",
		ResultFileID: &result,
	})
	tasks.setTask(model.Task{
		ID:        "task-retry",
		OwnerID:   testUserAID,
		Status:    model.TaskStatusRetryWait,
		Phase:     "prepare",
		LastError: "temporary",
	})
	tasks.setTask(model.Task{
		ID:      "task-queued",
		OwnerID: testUserAID,
		Status:  model.TaskStatusQueued,
		Phase:   "prepare",
	})
	tasks.setTask(model.Task{
		ID:      "task-other",
		OwnerID: testUserBID,
		Status:  model.TaskStatusQueued,
	})

	snapshots := newFakeTaskSnapshots()
	snapshots.setTask(queue.Snapshot{
		ID:        "task-running",
		OwnerID:   testUserAID,
		Status:    queue.Running,
		Stage:     "compressing",
		Processed: 1,
		Total:     3,
	})
	snapshots.setTask(queue.Snapshot{
		ID:        "task-done",
		OwnerID:   testUserAID,
		Status:    queue.Completed,
		Stage:     "completed",
		Processed: 3,
		Total:     3,
		Result:    "file-9",
	})
	// 等待重试时内存快照可能还是上一轮失败，状态必须以数据库为准。
	snapshots.setTask(queue.Snapshot{
		ID:      "task-retry",
		OwnerID: testUserAID,
		Status:  queue.Failed,
		Stage:   "failed",
	})
	// 数据库已经重新排队，但内存里还是上一轮的 failed 快照：
	// 展示阶段不能跟着显示 failed。
	snapshots.setTask(queue.Snapshot{
		ID:      "task-queued",
		OwnerID: testUserAID,
		Status:  queue.Failed,
		Stage:   "failed",
	})

	router := newTaskRouter(t, tasks, snapshots)

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
	if done.Status != "completed" || done.Stage != "completed" ||
		done.ResultFileID != "file-9" || done.ProcessedFiles != 3 || done.TotalFiles != 3 {
		t.Fatalf("unexpected completed task: %+v", done)
	}

	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, taskRequest(http.MethodGet, "/api/tasks/task-retry", "", testTokenA))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var retry taskStatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &retry); err != nil {
		t.Fatal(err)
	}
	if retry.Status != "retry_wait" || retry.Stage != "prepare" || retry.Error != "temporary" {
		t.Fatalf("database status must win over stale snapshot: %+v", retry)
	}

	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, taskRequest(http.MethodGet, "/api/tasks/task-queued", "", testTokenA))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var queued taskStatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &queued); err != nil {
		t.Fatal(err)
	}
	if queued.Status != "queued" || queued.Stage != "prepare" {
		t.Fatalf("stale failed snapshot must not override queued stage: %+v", queued)
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

func TestCreateContentVerifyTasks(t *testing.T) {
	ctx := context.Background()
	files := newFakeFileService()
	tasks := newFakeTaskService()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	router := NewRouterWithTasks(
		logger, files, newFakeAuthService(), tasks, newFakeTaskSnapshots(),
	)

	uploaded, err := files.Upload(ctx, testUserAID, "", "legacy.bin", strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	directory, err := files.CreateDirectory(ctx, testUserAID, "", "folder")
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "accepted",
			body:       fmt.Sprintf(`{"file_ids":["%s"]}`, uploaded.ID),
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "empty file ids",
			body:       `{"file_ids":[]}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "missing file",
			body:       `{"file_ids":["missing"]}`,
			wantStatus: http.StatusNotFound,
			wantCode:   "file_not_found",
		},
		{
			name:       "directory",
			body:       fmt.Sprintf(`{"file_ids":["%s"]}`, directory.ID),
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "pending limit",
			body:       fmt.Sprintf(`{"file_ids":["%s"]}`, uploaded.ID),
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "task_limit",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tasks.mu.Lock()
			tasks.submitErr = nil
			if c.name == "pending limit" {
				tasks.submitErr = task.ErrTooManyPending
			}
			tasks.mu.Unlock()

			recorder := httptest.NewRecorder()
			req := taskRequest(http.MethodPost, "/api/content-verify-tasks", c.body, testTokenA)
			router.ServeHTTP(recorder, req)

			if recorder.Code != c.wantStatus {
				t.Fatalf("status = %d, want %d: %s",
					recorder.Code, c.wantStatus, recorder.Body.String())
			}
			if c.wantCode != "" {
				assertErrorCode(t, recorder, c.wantCode)
			}
		})
	}

	// 成功提交时每个文件一条任务，类型和状态参数都必须正确。
	tasks.mu.Lock()
	submitted := append([]task.CreateParams(nil), tasks.submitted...)
	tasks.mu.Unlock()
	if len(submitted) != 1 {
		t.Fatalf("submitted %d tasks, want 1", len(submitted))
	}
	if submitted[0].Type != verify.TaskType ||
		submitted[0].OwnerID != testUserAID ||
		submitted[0].StateVersion != verify.StateVersion ||
		!strings.Contains(submitted[0].StateJSON, uploaded.ID) {
		t.Fatalf("submitted task params = %+v", submitted[0])
	}
}

func TestGetTaskExposesWorkerAttemptAndResult(t *testing.T) {
	tasks := newFakeTaskService()
	snapshots := newFakeTaskSnapshots()
	lease := time.Now().Add(time.Minute)
	tasks.setTask(model.Task{
		ID:         "task-lease",
		OwnerID:    testUserAID,
		Status:     model.TaskStatusRunning,
		Phase:      "verify",
		WorkerID:   "worker-a",
		Attempts:   2,
		LeaseUntil: &lease,
		ResultJSON: `{"sha256":"abc","size":7}`,
	})

	router := newTaskRouter(t, tasks, snapshots)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, taskRequest(
		http.MethodGet, "/api/tasks/task-lease", "", testTokenA,
	))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}

	var record taskStatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record.WorkerID != "worker-a" || record.Attempt != 2 ||
		record.LeaseUntil == nil || !record.LeaseUntil.Equal(lease) {
		t.Fatalf("worker fields = %+v", record)
	}
	result, ok := record.Result.(map[string]any)
	if !ok || result["sha256"] != "abc" {
		t.Fatalf("result = %#v", record.Result)
	}
}
