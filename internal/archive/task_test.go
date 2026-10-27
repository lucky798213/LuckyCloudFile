package archive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/task"
)

// recordingCheckpoints 模拟 task.Service 的阶段检查点入口。
// failAt 用来在指定阶段提交后制造失败，等价于“产物已生成、检查点没提交”。
type recordingCheckpoints struct {
	mu         sync.Mutex
	saved      []task.Checkpoint
	failAt     string
	cleanup    []bool
	cleanupErr error
}

func (r *recordingCheckpoints) SaveCheckpoint(
	_ context.Context,
	_ string,
	_ int,
	checkpoint task.Checkpoint,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.saved = append(r.saved, checkpoint)
	if r.failAt != "" && checkpoint.Phase == r.failAt {
		return fmt.Errorf("checkpoint %q rejected", checkpoint.Phase)
	}
	return nil
}

func (r *recordingCheckpoints) SetCleanupPending(
	_ context.Context,
	_ string,
	_ int,
	pending bool,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanup = append(r.cleanup, pending)
	return r.cleanupErr
}

func (r *recordingCheckpoints) cleanupMarked() []bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bool(nil), r.cleanup...)
}

func (r *recordingCheckpoints) phases() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	phases := make([]string, 0, len(r.saved))
	for _, checkpoint := range r.saved {
		phases = append(phases, checkpoint.Phase)
	}
	return phases
}

func (r *recordingCheckpoints) forPhase(t *testing.T, phase string) task.Checkpoint {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, checkpoint := range r.saved {
		if checkpoint.Phase == phase {
			return checkpoint
		}
	}
	t.Fatalf("no checkpoint for phase %q: %+v", phase, r.saved)
	return task.Checkpoint{}
}

// newTaskRunner 注册真实的 archive 工厂，再用注册表按记录重建。
func newTaskRunner(
	t *testing.T,
	files *fakeService,
	checkpoints TaskStore,
	workDir string,
) task.Runner {
	t.Helper()

	registry := task.NewRegistry()
	if err := registry.Register(
		TaskType,
		NewTaskFactory(files, checkpoints, Options{WorkDir: workDir}),
	); err != nil {
		t.Fatal(err)
	}
	return registry
}

// newTaskRecord 只把输入、阶段和 state 放进记录，不放 Job 或闭包。
func newTaskRecord(
	t *testing.T,
	id string,
	ownerID int,
	phase string,
	state TaskState,
) model.Task {
	t.Helper()

	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return model.Task{
		ID:           id,
		Type:         TaskType,
		OwnerID:      ownerID,
		Phase:        phase,
		StateVersion: TaskStateVersion,
		StateJSON:    string(encoded),
		Attempts:     1,
	}
}

// TestNewTaskStateReusesSubmitValidation 验证提交时和执行时使用同一套
// 静态校验，避免“提交成功、执行才发现参数非法”。
func TestNewTaskStateReusesSubmitValidation(t *testing.T) {
	cases := []struct {
		name      string
		sourceIDs []string
		sizeName  string
		opts      Options
		want      error
	}{
		{
			name:     "no sources",
			sizeName: "bundle.zip",
			want:     ErrNoSources,
		},
		{
			name:      "too many files",
			sourceIDs: []string{"file-a", "file-b"},
			sizeName:  "bundle.zip",
			opts:      Options{MaxFiles: 1},
			want:      ErrTooManyFiles,
		},
		{
			name:      "duplicate file",
			sourceIDs: []string{"file-a", "file-a"},
			sizeName:  "bundle.zip",
			want:      ErrDuplicateFile,
		},
		{
			name:      "empty file ID",
			sourceIDs: []string{""},
			sizeName:  "bundle.zip",
			want:      ErrInvalidFileID,
		},
		{
			name:      "invalid result name",
			sourceIDs: []string{"file-a"},
			sizeName:  "../bundle.zip",
			want:      ErrInvalidName,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewTaskState(tc.sourceIDs, "parent", tc.sizeName, tc.opts); !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}

	state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(state.SourceIDs) != 1 || state.SourceIDs[0] != "file-a" ||
		state.ParentID != "parent" || state.Name != "bundle.zip" {
		t.Fatalf("unexpected state: %+v", state)
	}
	if state.Archive != nil || state.ResultFileID != "" {
		t.Fatalf("new task state must not contain results: %+v", state)
	}
}
