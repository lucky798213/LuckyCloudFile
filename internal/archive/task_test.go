package archive

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"luckyCloudFile/internal/model"
)

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
}

// TestTaskRunnerRebuildsFromRecord 验证 Runner 只依靠 tasks 记录里的
// type / version / owner / state 就能重新构造并执行 ZIP 任务。
func TestTaskRunnerRebuildsFromRecord(t *testing.T) {
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, name: "a.txt", data: []byte("a")},
	})
	state, err := NewTaskState([]string{"file-a"}, "parent", "bundle.zip", Options{})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}

	record := model.Task{
		ID:           "task-1",
		Type:         TaskType,
		OwnerID:      1,
		StateVersion: TaskStateVersion,
		StateJSON:    string(encoded),
	}
	result, err := NewTaskRunner(files, Options{}).Run(context.Background(), record, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != "result-file" {
		t.Fatalf("result = %q, want result-file", result)
	}
	if files.uploadCount() != 1 {
		t.Fatalf("uploaded %d archive(s), want 1", files.uploadCount())
	}

	// owner 必须取自记录，不能相信 state_json 里的任何“所有者”字段。
	otherOwner := record
	otherOwner.OwnerID = 2
	if _, err := NewTaskRunner(files, Options{}).Run(context.Background(), otherOwner, nil); err == nil {
		t.Fatal("runner accepted a record whose owner does not own the sources")
	}

	// 未知类型和未知版本必须明确失败，不能按零值猜参数继续执行。
	for _, tc := range []struct {
		name   string
		record model.Task
	}{
		{"unknown type", model.Task{
			Type: "other_type", OwnerID: 1,
			StateVersion: TaskStateVersion, StateJSON: string(encoded),
		}},
		{"unknown version", model.Task{
			Type: TaskType, OwnerID: 1,
			StateVersion: TaskStateVersion + 1, StateJSON: string(encoded),
		}},
		{"invalid json", model.Task{
			Type: TaskType, OwnerID: 1,
			StateVersion: TaskStateVersion, StateJSON: `{`,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewTaskRunner(files, Options{}).Run(context.Background(), tc.record, nil); err == nil {
				t.Fatal("runner accepted an invalid record")
			}
		})
	}
}
