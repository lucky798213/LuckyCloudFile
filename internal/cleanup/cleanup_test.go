package cleanup

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/task"
)

type fakeStore struct {
	tasks   []model.Task
	cleared []string
	err     error
}

func (s *fakeStore) ListCleanupPending(context.Context, int) ([]model.Task, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.tasks, nil
}

func (s *fakeStore) ClearCleanupPending(_ context.Context, id string) error {
	s.cleared = append(s.cleared, id)
	return nil
}

type fakeFiles struct {
	result file.CleanupResult
	err    error
	calls  int
}

func (f *fakeFiles) CleanupPending(context.Context, int) (file.CleanupResult, error) {
	f.calls++
	return f.result, f.err
}

// TestCleanupTaskRemovesTaskDirAndClearsResponsibility 验证清理任务复用
// 已有内容回收，并在工作目录删除成功后才收回数据库里的责任标记。
func TestCleanupTaskRemovesTaskDirAndClearsResponsibility(t *testing.T) {
	workDir := t.TempDir()
	taskID := "task-1"
	dir := filepath.Join(workDir, "task-"+taskID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "archive.zip"), []byte("zip"), 0o600); err != nil {
		t.Fatal(err)
	}

	store := &fakeStore{tasks: []model.Task{{
		ID:             taskID,
		Status:         model.TaskStatusCompleted,
		CleanupPending: true,
	}}}
	files := &fakeFiles{}

	factory := NewTaskFactory(files, store, Options{WorkDir: workDir})
	runner, err := factory.Rebuild(model.Task{Type: TaskType, StateVersion: StateVersion})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(context.Background(), model.Task{}, nil); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("task directory still exists: %v", err)
	}
	if len(store.cleared) != 1 || store.cleared[0] != taskID {
		t.Fatalf("cleared = %v, want [%s]", store.cleared, taskID)
	}
	if files.calls != 1 {
		t.Fatalf("CleanupPending calls = %d, want 1", files.calls)
	}
}

// TestCleanupTaskRetriesDeferredContent 验证内容回收还有失败项时任务会
// 进入可重试状态，而不是把清理责任当成已经完成。
func TestCleanupTaskRetriesDeferredContent(t *testing.T) {
	workDir := t.TempDir()
	store := &fakeStore{}
	files := &fakeFiles{result: file.CleanupResult{Scanned: 2, Deferred: 1}}

	factory := NewTaskFactory(files, store, Options{WorkDir: workDir})
	runner, err := factory.Rebuild(model.Task{Type: TaskType, StateVersion: StateVersion})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(context.Background(), model.Task{}, nil); !task.IsRetryable(err) {
		t.Fatalf("deferred cleanup error = %v, want retryable", err)
	}
}

// TestCleanupTaskKeepsResponsibilityWhenDirectoryRemovalFails 验证删除失败
// 时保留数据库责任，后续任务还能继续重试。
func TestCleanupTaskKeepsResponsibilityWhenDirectoryRemovalFails(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	store := &fakeStore{tasks: []model.Task{{
		ID:             "task-1",
		Status:         model.TaskStatusCompleted,
		CleanupPending: true,
	}}}
	files := &fakeFiles{}

	// WorkDir 指向普通文件，task-1 子路径无法删除。
	factory := NewTaskFactory(files, store, Options{WorkDir: blocker})
	runner, err := factory.Rebuild(model.Task{Type: TaskType, StateVersion: StateVersion})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(context.Background(), model.Task{}, nil); !task.IsRetryable(err) {
		t.Fatalf("directory removal error = %v, want retryable", err)
	}
	if len(store.cleared) != 0 {
		t.Fatalf("responsibility was cleared despite removal failure: %v", store.cleared)
	}
}

// TestTaskDirRejectsEscapingID 验证数据库里的异常任务 ID 不能把删除动作
// 带出工作根目录。
func TestTaskDirRejectsEscapingID(t *testing.T) {
	for _, id := range []string{"", ".", "..", "../x", "a/b", `a\b`} {
		if _, err := taskDir(t.TempDir(), id); err == nil {
			t.Fatalf("taskDir accepted unsafe ID %q", id)
		}
	}
}
