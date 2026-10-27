package archive

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/task"
)

// runUntilCheckpoint 执行到指定阶段检查点，并让该检查点提交失败。
// 返回的 checkpoint 就是“产物已经生成、但数据库阶段还没推进”时应该提交的内容。
func runUntilCheckpoint(
	t *testing.T,
	files *fakeService,
	workDir, failAt string,
	state TaskState,
) (model.Task, task.Checkpoint) {
	t.Helper()

	checkpoints := &recordingCheckpoints{failAt: failAt}
	runner := newTaskRunner(t, files, checkpoints, workDir)
	record := newTaskRecord(t, "task-1", 1, PhasePrepare, state)
	if _, err := runner.Run(context.Background(), record, nil); err == nil {
		t.Fatal("checkpoint failure must fail the attempt")
	}
	return record, checkpoints.forPhase(t, failAt)
}

func resumeTask(record model.Task, phase string, stateJSON string, attempt int) model.Task {
	return model.Task{
		ID:           record.ID,
		Type:         TaskType,
		OwnerID:      record.OwnerID,
		Phase:        phase,
		StateVersion: TaskStateVersion,
		StateJSON:    stateJSON,
		Attempts:     attempt,
	}
}

// TestTaskFactoryRunsPhasesFromRecord 验证只凭 type / owner / phase / state
// 就能重建任务，并按 prepare → compress → publish → finalize 逐步提交检查点。
func TestTaskFactoryRunsPhasesFromRecord(t *testing.T) {
	workDir := t.TempDir()
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, entityID: 101, name: "a.txt", data: []byte("alpha")},
		"file-b": {owner: 1, entityID: 102, name: "b.txt", data: []byte("beta")},
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
	if got := checkpoints.phases(); !equalStrings(got, wantPhases) {
		t.Fatalf("checkpoints = %v, want %v", got, wantPhases)
	}

	// phase=publish 的检查点必须带着来源快照、完整产物的路径、大小和摘要。
	var published TaskState
	if err := json.Unmarshal(
		[]byte(checkpoints.forPhase(t, PhasePublish).StateJSON), &published,
	); err != nil {
		t.Fatal(err)
	}
	if len(published.Sources) != 2 ||
		published.Sources[0].FileID != "file-a" ||
		published.Sources[0].EntityID != 101 ||
		published.Sources[1].FileID != "file-b" {
		t.Fatalf("publish checkpoint sources = %+v", published.Sources)
	}
	if published.Archive == nil {
		t.Fatal("publish checkpoint has no artifact metadata")
	}
	if published.Archive.Path != filepath.Join("task-task-1", "archive.zip") {
		t.Fatalf("artifact path = %q", published.Archive.Path)
	}

	files.mu.Lock()
	uploaded := append([]byte(nil), files.uploads[0]...)
	protectCalls, releases := files.protectCalls, files.releases
	files.mu.Unlock()
	sum := sha256.Sum256(uploaded)
	if published.Archive.Size != int64(len(uploaded)) {
		t.Fatalf("artifact size = %d, want %d", published.Archive.Size, len(uploaded))
	}
	if published.Archive.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("artifact digest = %q, want %q",
			published.Archive.SHA256, hex.EncodeToString(sum[:]))
	}
	// 每次执行只取得一次来源保护，并在压缩结束/函数返回时释放。
	if protectCalls != 1 || releases != 1 {
		t.Fatalf("source protection calls = %d, releases = %d, want 1/1",
			protectCalls, releases)
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
	// 责任在进入工作目录前写一次，finalize 删除成功后再清零。
	if got := checkpoints.cleanupMarked(); !equalBools(got, []bool{true, true, false}) {
		t.Fatalf("cleanup responsibility = %v, want [true true false]", got)
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
		"file-a": {owner: 1, entityID: 11, name: "a.txt", data: []byte("alpha")},
	})
	state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
	if err != nil {
		t.Fatal(err)
	}

	// 第一次执行完成 compress 后，故意让 publish 检查点提交失败：
	// 这等价于“完整 ZIP 已生成，但数据库里的 phase 还没有推进”。
	record, committed := runUntilCheckpoint(t, files, workDir, PhasePublish, state)
	if committed.Phase != PhasePublish {
		t.Fatalf("committed phase = %q", committed.Phase)
	}
	if files.uploadCount() != 0 {
		t.Fatalf("crashed attempt published %d archive(s)", files.uploadCount())
	}

	// 来源文件已经消失：phase=publish 不允许再读它，只能使用已提交产物。
	files.mu.Lock()
	delete(files.sources, "file-a")
	files.mu.Unlock()

	resumedCheckpoints := &recordingCheckpoints{}
	resumedRunner := newTaskRunner(t, files, resumedCheckpoints, workDir)
	resumed := resumeTask(record, PhasePublish, committed.StateJSON, 2)

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
	if got := resumedCheckpoints.phases(); !equalStrings(got, []string{PhaseFinalize}) {
		t.Fatalf("resumed checkpoints = %v, want only finalize", got)
	}
}

// TestTaskFactoryRebuildsMissingOrCorruptArtifact 验证 phase=publish 指向的
// 完整产物丢失、被篡改或只剩 .partial 时，退回 compress 重建，而不是发布
// 半成品或错误内容。
func TestTaskFactoryRebuildsMissingOrCorruptArtifact(t *testing.T) {
	t.Run("missing metadata", func(t *testing.T) {
		files := newFakeService(map[string]fakeSource{
			"file-a": {owner: 1, entityID: 21, name: "a.txt", data: []byte("alpha")},
		})
		state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
		if err != nil {
			t.Fatal(err)
		}
		checkpoints := &recordingCheckpoints{}
		runner := newTaskRunner(t, files, checkpoints, t.TempDir())
		record := newTaskRecord(t, "task-1", 1, PhasePublish, state)

		result, err := runner.Run(context.Background(), record, nil)
		if err != nil {
			t.Fatal(err)
		}
		if result != "result-file" || files.uploadCount() != 1 {
			t.Fatalf("rebuild result = %q, uploads = %d", result, files.uploadCount())
		}
		// 这次执行从 publish 退回 compress，只提交重建后的 publish 和 finalize。
		want := []string{PhasePublish, PhaseFinalize}
		if got := checkpoints.phases(); !equalStrings(got, want) {
			t.Fatalf("checkpoints = %v, want %v", got, want)
		}
	})

	t.Run("tampered artifact", func(t *testing.T) {
		workDir := t.TempDir()
		files := newFakeService(map[string]fakeSource{
			"file-a": {owner: 1, entityID: 22, name: "a.txt", data: []byte("alpha")},
		})
		state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
		if err != nil {
			t.Fatal(err)
		}

		record, committed := runUntilCheckpoint(t, files, workDir, PhasePublish, state)
		var committedState TaskState
		if err := json.Unmarshal([]byte(committed.StateJSON), &committedState); err != nil {
			t.Fatal(err)
		}
		artifactPath := filepath.Join(workDir, committedState.Archive.Path)
		if err := os.WriteFile(artifactPath, []byte("not the committed zip"), 0o600); err != nil {
			t.Fatal(err)
		}

		checkpoints := &recordingCheckpoints{}
		runner := newTaskRunner(t, files, checkpoints, workDir)
		result, err := runner.Run(
			context.Background(),
			resumeTask(record, PhasePublish, committed.StateJSON, 2),
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		if result != "result-file" {
			t.Fatalf("result = %q", result)
		}
		want := []string{PhasePublish, PhaseFinalize}
		if got := checkpoints.phases(); !equalStrings(got, want) {
			t.Fatalf("checkpoints = %v, want %v", got, want)
		}
		assertUploadIsZip(t, files)
	})

	t.Run("partial only", func(t *testing.T) {
		workDir := t.TempDir()
		files := newFakeService(map[string]fakeSource{
			"file-a": {owner: 1, entityID: 23, name: "a.txt", data: []byte("alpha")},
		})
		state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
		if err != nil {
			t.Fatal(err)
		}

		record, committed := runUntilCheckpoint(t, files, workDir, PhasePublish, state)
		var committedState TaskState
		if err := json.Unmarshal([]byte(committed.StateJSON), &committedState); err != nil {
			t.Fatal(err)
		}
		artifactPath := filepath.Join(workDir, committedState.Archive.Path)
		if err := os.Remove(artifactPath); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(artifactPath+".partial", []byte("half written"), 0o600); err != nil {
			t.Fatal(err)
		}

		runner := newTaskRunner(t, files, &recordingCheckpoints{}, workDir)
		if _, err := runner.Run(
			context.Background(),
			resumeTask(record, PhasePublish, committed.StateJSON, 2),
			nil,
		); err != nil {
			t.Fatal(err)
		}
		// 发布的是重建后的完整 ZIP；.partial 从来没有被上传。
		assertUploadIsZip(t, files)
	})
}

// TestTaskFactoryFailsWhenSourceChanged 验证来源在快照之后被改名或换内容时
// 明确失败，不会悄悄压缩另一份内容。
func TestTaskFactoryFailsWhenSourceChanged(t *testing.T) {
	workDir := t.TempDir()
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, entityID: 31, name: "a.txt", data: []byte("alpha")},
	})
	state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
	if err != nil {
		t.Fatal(err)
	}

	// prepare 成功后停在 compress 检查点之前，得到一份已提交的来源快照。
	record, committed := runUntilCheckpoint(t, files, workDir, PhaseCompress, state)

	files.mu.Lock()
	changed := files.sources["file-a"]
	changed.entityID = 99
	changed.name = "renamed.txt"
	files.sources["file-a"] = changed
	files.mu.Unlock()

	runner := newTaskRunner(t, files, &recordingCheckpoints{}, workDir)
	_, err = runner.Run(
		context.Background(),
		resumeTask(record, PhaseCompress, committed.StateJSON, 2),
		nil,
	)
	if !errors.Is(err, file.ErrSourceChanged) {
		t.Fatalf("error = %v, want ErrSourceChanged", err)
	}
	if files.uploadCount() != 0 {
		t.Fatalf("changed source published %d archive(s)", files.uploadCount())
	}
}

// TestTaskFactoryFailsToRebuildChangedSource 验证“产物损坏要重建，但来源
// 也变了”时仍然失败：重建不能成为绕过来源快照的入口。
func TestTaskFactoryFailsToRebuildChangedSource(t *testing.T) {
	workDir := t.TempDir()
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, entityID: 41, name: "a.txt", data: []byte("alpha")},
	})
	state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
	if err != nil {
		t.Fatal(err)
	}

	record, committed := runUntilCheckpoint(t, files, workDir, PhasePublish, state)
	var committedState TaskState
	if err := json.Unmarshal([]byte(committed.StateJSON), &committedState); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(workDir, committedState.Archive.Path)); err != nil {
		t.Fatal(err)
	}

	files.mu.Lock()
	changed := files.sources["file-a"]
	changed.entityID = 42
	files.sources["file-a"] = changed
	files.mu.Unlock()

	runner := newTaskRunner(t, files, &recordingCheckpoints{}, workDir)
	_, err = runner.Run(
		context.Background(),
		resumeTask(record, PhasePublish, committed.StateJSON, 2),
		nil,
	)
	if !errors.Is(err, file.ErrSourceChanged) {
		t.Fatalf("error = %v, want ErrSourceChanged", err)
	}
	if files.uploadCount() != 0 {
		t.Fatalf("changed source published %d archive(s)", files.uploadCount())
	}
}

// TestTaskFactoryReusesTaskResult 验证发布事务提交后、finalize 检查点尚未
// 提交时崩溃：恢复只补 finalize，不再上传，也不依赖已经删掉的产物。
func TestTaskFactoryReusesTaskResult(t *testing.T) {
	workDir := t.TempDir()
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, entityID: 51, name: "a.txt", data: []byte("alpha")},
	})
	state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
	if err != nil {
		t.Fatal(err)
	}

	record, publishCheckpoint := runUntilCheckpoint(t, files, workDir, PhaseFinalize, state)
	if files.uploadCount() != 1 {
		t.Fatalf("first run uploaded %d archive(s), want 1", files.uploadCount())
	}

	// 模拟崩溃后的现场：产物和来源都不在了，但 task_results 已提交。
	if err := os.RemoveAll(filepath.Join(workDir, "task-"+record.ID)); err != nil {
		t.Fatal(err)
	}
	files.mu.Lock()
	delete(files.sources, "file-a")
	files.mu.Unlock()

	checkpoints := &recordingCheckpoints{}
	runner := newTaskRunner(t, files, checkpoints, workDir)
	result, err := runner.Run(
		context.Background(),
		resumeTask(record, PhasePublish, publishCheckpoint.StateJSON, 2),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result != "result-file" {
		t.Fatalf("result = %q, want result-file", result)
	}
	if files.uploadCount() != 1 {
		t.Fatalf("resumed run published a second result: uploads = %d", files.uploadCount())
	}
	if got := checkpoints.phases(); !equalStrings(got, []string{PhaseFinalize}) {
		t.Fatalf("resumed checkpoints = %v, want only finalize", got)
	}
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

	t.Run("unsafe artifact path", func(t *testing.T) {
		job := &Job{workDir: t.TempDir()}
		if _, err := job.artifactPath("../escape.zip"); err == nil {
			t.Fatal("accepted an artifact path outside the work directory")
		}
	})
	t.Run("target validation", func(t *testing.T) {
		broken := newFakeService(map[string]fakeSource{
			"file-a": {owner: 1, name: "a.txt", data: []byte("alpha")},
		})
		broken.validateErr = errors.New("parent is gone")
		brokenRunner := newTaskRunner(t, broken, &recordingCheckpoints{}, t.TempDir())
		record := newTaskRecord(t, "task-1", 1, PhasePrepare, state)
		if _, err := brokenRunner.Run(context.Background(), record, nil); err == nil {
			t.Fatal("runner accepted a missing target directory")
		}
	})
}

func assertUploadIsZip(t *testing.T, files *fakeService) {
	t.Helper()

	files.mu.Lock()
	if len(files.uploads) == 0 {
		files.mu.Unlock()
		t.Fatal("no archive was uploaded")
	}
	uploaded := append([]byte(nil), files.uploads[len(files.uploads)-1]...)
	files.mu.Unlock()

	if _, err := zip.NewReader(bytes.NewReader(uploaded), int64(len(uploaded))); err != nil {
		t.Fatalf("published archive is not a valid ZIP: %v", err)
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestTaskFactoryFailsWhenSourceMoved 验证来源在 prepare 之后被移动也会失败：
// 名字、内容、大小都没变，只有父目录变了。
func TestTaskFactoryFailsWhenSourceMoved(t *testing.T) {
	workDir := t.TempDir()
	files := newFakeService(map[string]fakeSource{
		"file-a": {
			owner:    1,
			entityID: 61,
			parentID: "dir-a",
			name:     "a.txt",
			data:     []byte("alpha"),
		},
	})
	state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
	if err != nil {
		t.Fatal(err)
	}

	record, committed := runUntilCheckpoint(t, files, workDir, PhaseCompress, state)

	// 只改位置：实体、名字和大小都保持原样。
	files.mu.Lock()
	moved := files.sources["file-a"]
	moved.parentID = "dir-b"
	files.sources["file-a"] = moved
	files.mu.Unlock()

	runner := newTaskRunner(t, files, &recordingCheckpoints{}, workDir)
	_, err = runner.Run(
		context.Background(),
		resumeTask(record, PhaseCompress, committed.StateJSON, 2),
		nil,
	)
	if !errors.Is(err, file.ErrSourceChanged) {
		t.Fatalf("error = %v, want ErrSourceChanged", err)
	}
	if files.uploadCount() != 0 {
		t.Fatalf("moved source published %d archive(s)", files.uploadCount())
	}
}

// TestTaskFactoryPersistsCleanupResponsibility 验证清理责任的持久化顺序：
//   - 责任写不进去时不能删除目录并静默完成；
//   - 责任已写、删除失败时任务仍可完成，标记保留给后续清理。
func TestTaskFactoryPersistsCleanupResponsibility(t *testing.T) {
	t.Run("persist failure prevents work directory", func(t *testing.T) {
		workDir := t.TempDir()
		files := newFakeService(map[string]fakeSource{
			"file-a": {owner: 1, entityID: 71, name: "a.txt", data: []byte("alpha")},
		})
		state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
		if err != nil {
			t.Fatal(err)
		}

		checkpoints := &recordingCheckpoints{
			cleanupErr: errors.New("database is unavailable"),
		}
		runner := newTaskRunner(t, files, checkpoints, workDir)
		record := newTaskRecord(t, "task-1", 1, PhasePrepare, state)
		if _, err := runner.Run(context.Background(), record, nil); err == nil {
			t.Fatal("execution continued without persisting cleanup responsibility")
		}
		if got := checkpoints.cleanupMarked(); !equalBools(got, []bool{true}) {
			t.Fatalf("cleanup attempts = %v, want [true]", got)
		}
		// 责任写不进去时不能创建工作目录，否则这个目录以后没人知道要删。
		if _, err := os.Stat(filepath.Join(workDir, "task-task-1")); !os.IsNotExist(err) {
			t.Fatalf("work directory was created before responsibility was saved: %v", err)
		}
	})

	t.Run("delete failure keeps responsibility", func(t *testing.T) {
		workDir := t.TempDir()
		taskDir := filepath.Join(workDir, "task-task-1")
		if err := os.MkdirAll(taskDir, 0o750); err != nil {
			t.Fatal(err)
		}
		files := newFakeService(map[string]fakeSource{})
		files.taskResults["task-1"] = fakeTaskResult{owner: 1, fileID: "result-file"}
		checkpoints := &recordingCheckpoints{}

		// 去掉工作根的写权限，让 RemoveAll 无法删除任务目录。
		if err := os.Chmod(workDir, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(workDir, 0o750) })

		job := &Job{
			files:   files,
			ownerID: 1,
			logger:  testDiscardLogger(),
		}
		record := model.Task{ID: "task-1", OwnerID: 1, Attempts: 1}
		state := TaskState{ResultFileID: "result-file"}
		if err := job.finalize(
			context.Background(), taskDir, state, record, checkpoints,
		); err != nil {
			t.Fatal(err)
		}
		if got := checkpoints.cleanupMarked(); !equalBools(got, []bool{true}) {
			t.Fatalf("cleanup responsibility = %v, want [true] left behind", got)
		}
		if _, err := os.Stat(taskDir); err != nil {
			t.Fatalf("task directory should remain for later cleanup: %v", err)
		}
	})
}

// TestTaskFactoryKeepsCleanupAfterPublishFailure 覆盖“ZIP 已生成、发布失败、
// 没有进入 finalize”的路径：任务会以 failed 结束，但工作目录和清理责任
// 必须已经留在数据库里，供 cleanup_pending 扫描回收。
func TestTaskFactoryKeepsCleanupAfterPublishFailure(t *testing.T) {
	workDir := t.TempDir()
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, entityID: 71, name: "a.txt", data: []byte("alpha")},
	})
	// 同名冲突/配额不足都发生在创建发布会话时，属于永久失败。
	files.createErr = file.ErrAlreadyExists
	state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
	if err != nil {
		t.Fatal(err)
	}

	checkpoints := &recordingCheckpoints{}
	runner := newTaskRunner(t, files, checkpoints, workDir)
	record := newTaskRecord(t, "task-1", 1, PhasePrepare, state)
	_, runErr := runner.Run(context.Background(), record, nil)
	if !errors.Is(runErr, file.ErrAlreadyExists) {
		t.Fatalf("publish error = %v, want ErrAlreadyExists", runErr)
	}
	if task.IsRetryable(runErr) {
		t.Fatal("permanent publish failure was marked retryable")
	}
	if got := checkpoints.cleanupMarked(); !equalBools(got, []bool{true}) {
		t.Fatalf("cleanup responsibility = %v, want [true] left behind", got)
	}
	if _, err := os.Stat(filepath.Join(workDir, "task-task-1", "archive.zip")); err != nil {
		t.Fatalf("generated ZIP should remain for cleanup: %v", err)
	}
}

// TestTaskFactoryKeepsCleanupAfterTransientFailure 覆盖重试前的暂时性失败：
// 本次执行没有完成，清理责任已经落库，工作目录留给重试或最终清理。
func TestTaskFactoryKeepsCleanupAfterTransientFailure(t *testing.T) {
	workDir := t.TempDir()
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, entityID: 71, name: "a.txt", data: []byte("alpha")},
	})
	files.createErr = errors.New("storage temporarily unavailable")
	state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
	if err != nil {
		t.Fatal(err)
	}

	checkpoints := &recordingCheckpoints{}
	runner := newTaskRunner(t, files, checkpoints, workDir)
	record := newTaskRecord(t, "task-1", 1, PhasePrepare, state)
	_, runErr := runner.Run(context.Background(), record, nil)
	if !task.IsRetryable(runErr) {
		t.Fatalf("transient error = %v, want retryable", runErr)
	}
	if got := checkpoints.cleanupMarked(); !equalBools(got, []bool{true}) {
		t.Fatalf("cleanup responsibility = %v, want [true] left behind", got)
	}
	if _, err := os.Stat(filepath.Join(workDir, "task-task-1", "archive.zip")); err != nil {
		t.Fatalf("generated ZIP should remain for retry and cleanup: %v", err)
	}
}

func equalBools(got, want []bool) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func testDiscardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}
