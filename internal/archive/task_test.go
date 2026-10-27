package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/task"
)

// recordingCheckpoints 模拟 task.Service 的阶段检查点入口。
// failAt 用来在指定阶段提交后制造失败，等价于“产物已生成、检查点没提交”。
type recordingCheckpoints struct {
	mu     sync.Mutex
	saved  []task.Checkpoint
	failAt string
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
	checkpoints task.Checkpointer,
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

// TestTaskFactoryRunsPhasesFromRecord 验证只凭 type / owner / phase / state
// 就能重建任务，并按 prepare → compress → publish → finalize 逐步提交检查点。
func TestTaskFactoryRunsPhasesFromRecord(t *testing.T) {
	workDir := t.TempDir()
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, name: "a.txt", data: []byte("alpha")},
		"file-b": {owner: 1, name: "b.txt", data: []byte("beta")},
	})
	state, err := NewTaskState([]string{"file-a", "file-b"}, "parent", "bundle.zip", Options{})
	if err != nil {
		t.Fatal(err)
	}

	checkpoints := &recordingCheckpoints{}
	runner := newTaskRunner(t, files, checkpoints, workDir)
	record := newTaskRecord(t, "task-1", 1, PhasePrepare, state)

	result, err := runner.Run(context.Background(), record, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != "result-file" {
		t.Fatalf("result = %q, want result-file", result)
	}
	if files.uploadCount() != 1 {
		t.Fatalf("uploaded %d archive(s), want 1", files.uploadCount())
	}

	// 检查点的 phase 是“下一步做什么”，所以最后一条应该是 finalize。
	wantPhases := []string{PhaseCompress, PhasePublish, PhaseFinalize}
	gotPhases := checkpoints.phases()
	if len(gotPhases) != len(wantPhases) {
		t.Fatalf("checkpoints = %v, want %v", gotPhases, wantPhases)
	}
	for i := range wantPhases {
		if gotPhases[i] != wantPhases[i] {
			t.Fatalf("checkpoint[%d] = %q, want %q", i, gotPhases[i], wantPhases[i])
		}
	}

	// phase=publish 的检查点必须带着完整产物的路径、大小和摘要。
	var published TaskState
	if err := json.Unmarshal(
		[]byte(checkpoints.forPhase(t, PhasePublish).StateJSON), &published,
	); err != nil {
		t.Fatal(err)
	}
	if published.Archive == nil {
		t.Fatal("publish checkpoint has no artifact metadata")
	}
	if published.Archive.Path != filepath.Join("task-task-1", "archive.zip") {
		t.Fatalf("artifact path = %q", published.Archive.Path)
	}

	files.mu.Lock()
	uploaded := append([]byte(nil), files.uploads[0]...)
	files.mu.Unlock()
	sum := sha256.Sum256(uploaded)
	if published.Archive.Size != int64(len(uploaded)) {
		t.Fatalf("artifact size = %d, want %d", published.Archive.Size, len(uploaded))
	}
	if published.Archive.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("artifact digest = %q, want %q",
			published.Archive.SHA256, hex.EncodeToString(sum[:]))
	}

	// finalize 检查点之后，结果文件 ID 也已保存，临时目录被清理。
	var finalized TaskState
	if err := json.Unmarshal(
		[]byte(checkpoints.forPhase(t, PhaseFinalize).StateJSON), &finalized,
	); err != nil {
		t.Fatal(err)
	}
	if finalized.ResultFileID != "result-file" {
		t.Fatalf("finalize checkpoint result = %q", finalized.ResultFileID)
	}
	entries, err := os.ReadDir(workDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("task work directory was not cleaned: %v", entries)
	}

	// owner 必须取自记录；来源不属于它时执行失败，且不会发布任何东西。
	other := newTaskRecord(t, "task-other", 2, PhasePrepare, state)
	if _, err := runner.Run(context.Background(), other, nil); err == nil {
		t.Fatal("runner accepted a record whose owner does not own the sources")
	}
	if files.uploadCount() != 1 {
		t.Fatalf("owner mismatch published an archive: uploads = %d", files.uploadCount())
	}
}

// TestTaskFactoryResumesFromPublish 验证恢复只依赖已提交的 phase/state：
// 从 publish 开始时不再读取来源文件，也不重新压缩。
func TestTaskFactoryResumesFromPublish(t *testing.T) {
	workDir := t.TempDir()
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, name: "a.txt", data: []byte("alpha")},
	})
	state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
	if err != nil {
		t.Fatal(err)
	}

	// 第一次执行完成 compress 后，故意让 publish 检查点提交失败：
	// 这等价于“完整 ZIP 已生成，但数据库里的 phase 还没有推进”。
	crashed := &recordingCheckpoints{failAt: PhasePublish}
	runner := newTaskRunner(t, files, crashed, workDir)
	record := newTaskRecord(t, "task-1", 1, PhasePrepare, state)
	if _, err := runner.Run(context.Background(), record, nil); err == nil {
		t.Fatal("checkpoint failure must fail the attempt")
	}
	if files.uploadCount() != 0 {
		t.Fatalf("crashed attempt published %d archive(s)", files.uploadCount())
	}

	committed := crashed.forPhase(t, PhasePublish)
	if committed.Phase != PhasePublish {
		t.Fatalf("committed phase = %q", committed.Phase)
	}

	// 来源文件已经消失：phase=publish 不允许再读它，只能使用已提交产物。
	files.mu.Lock()
	delete(files.sources, "file-a")
	files.mu.Unlock()

	resumedCheckpoints := &recordingCheckpoints{}
	resumedRunner := newTaskRunner(t, files, resumedCheckpoints, workDir)
	resumed := model.Task{
		ID:           record.ID,
		Type:         TaskType,
		OwnerID:      record.OwnerID,
		Phase:        PhasePublish,
		StateVersion: TaskStateVersion,
		StateJSON:    committed.StateJSON,
		Attempts:     2,
	}

	result, err := resumedRunner.Run(context.Background(), resumed, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != "result-file" {
		t.Fatalf("result = %q, want result-file", result)
	}
	if files.uploadCount() != 1 {
		t.Fatalf("resumed run uploaded %d archive(s), want 1", files.uploadCount())
	}
	gotPhases := resumedCheckpoints.phases()
	if len(gotPhases) != 1 || gotPhases[0] != PhaseFinalize {
		t.Fatalf("resumed checkpoints = %v, want only finalize", gotPhases)
	}
}

// TestTaskFactoryRejectsUncommittedArtifact 验证 phase=publish 不是进度标记：
// 没有产物元数据或产物被改动时必须失败，不能发布半成品或错误内容。
func TestTaskFactoryRejectsUncommittedArtifact(t *testing.T) {
	t.Run("missing metadata", func(t *testing.T) {
		files := newFakeService(map[string]fakeSource{
			"file-a": {owner: 1, name: "a.txt", data: []byte("alpha")},
		})
		state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
		if err != nil {
			t.Fatal(err)
		}
		runner := newTaskRunner(t, files, &recordingCheckpoints{}, t.TempDir())
		record := newTaskRecord(t, "task-1", 1, PhasePublish, state)

		if _, err := runner.Run(context.Background(), record, nil); err == nil {
			t.Fatal("phase=publish without artifact must fail")
		}
		if files.uploadCount() != 0 {
			t.Fatalf("published %d archive(s) without an artifact", files.uploadCount())
		}
	})

	t.Run("tampered artifact", func(t *testing.T) {
		workDir := t.TempDir()
		files := newFakeService(map[string]fakeSource{
			"file-a": {owner: 1, name: "a.txt", data: []byte("alpha")},
		})
		state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
		if err != nil {
			t.Fatal(err)
		}

		// 先得到一条合法的 publish 检查点，再篡改产物内容。
		crashed := &recordingCheckpoints{failAt: PhasePublish}
		runner := newTaskRunner(t, files, crashed, workDir)
		record := newTaskRecord(t, "task-1", 1, PhasePrepare, state)
		if _, err := runner.Run(context.Background(), record, nil); err == nil {
			t.Fatal("checkpoint failure must fail the attempt")
		}
		committed := crashed.forPhase(t, PhasePublish)

		var committedState TaskState
		if err := json.Unmarshal([]byte(committed.StateJSON), &committedState); err != nil {
			t.Fatal(err)
		}
		artifactPath := filepath.Join(workDir, committedState.Archive.Path)
		if err := os.WriteFile(artifactPath, []byte("not the committed zip"), 0o600); err != nil {
			t.Fatal(err)
		}

		resumed := model.Task{
			ID:           record.ID,
			Type:         TaskType,
			OwnerID:      record.OwnerID,
			Phase:        PhasePublish,
			StateVersion: TaskStateVersion,
			StateJSON:    committed.StateJSON,
			Attempts:     2,
		}
		if _, err := runner.Run(context.Background(), resumed, nil); err == nil {
			t.Fatal("tampered artifact must not be published")
		}
		if files.uploadCount() != 0 {
			t.Fatalf("tampered artifact was published: uploads = %d", files.uploadCount())
		}
	})
}

// TestTaskFactoryRejectsInvalidRecords 验证未知类型/版本/阶段和逃逸路径
// 都明确失败，不能按零值猜参数继续执行。
func TestTaskFactoryRejectsInvalidRecords(t *testing.T) {
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, name: "a.txt", data: []byte("alpha")},
	})
	state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	runner := newTaskRunner(t, files, &recordingCheckpoints{}, t.TempDir())

	cases := []struct {
		name   string
		record model.Task
		want   error
	}{
		{
			name: "unknown type",
			record: model.Task{
				ID: "task-1", Type: "fake_echo", OwnerID: 1,
				StateVersion: TaskStateVersion, StateJSON: string(encoded),
			},
			want: task.ErrUnknownTaskType,
		},
		{
			name: "unknown version",
			record: model.Task{
				ID: "task-1", Type: TaskType, OwnerID: 1,
				StateVersion: TaskStateVersion + 1, StateJSON: string(encoded),
			},
		},
		{
			name: "invalid json",
			record: model.Task{
				ID: "task-1", Type: TaskType, OwnerID: 1,
				StateVersion: TaskStateVersion, StateJSON: `{`,
			},
		},
		{
			name: "unknown phase",
			record: model.Task{
				ID: "task-1", Type: TaskType, OwnerID: 1, Phase: "verify",
				StateVersion: TaskStateVersion, StateJSON: string(encoded),
			},
			want: ErrUnknownPhase,
		},
		{
			name: "unsafe task id",
			record: model.Task{
				ID: "../escape", Type: TaskType, OwnerID: 1, Phase: PhasePrepare,
				StateVersion: TaskStateVersion, StateJSON: string(encoded),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runner.Run(context.Background(), tc.record, nil)
			if err == nil {
				t.Fatal("runner accepted an invalid record")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}

	// 检查点里的产物路径也必须留在任务工作目录内。
	t.Run("unsafe artifact path", func(t *testing.T) {
		escaped := state
		escaped.Archive = &ArchiveArtifact{Path: "../escape.zip", Size: 1, SHA256: "00"}
		record := newTaskRecord(t, "task-1", 1, PhasePublish, escaped)
		if _, err := runner.Run(context.Background(), record, nil); err == nil {
			t.Fatal("runner accepted an artifact path outside the work directory")
		}
	})
}
