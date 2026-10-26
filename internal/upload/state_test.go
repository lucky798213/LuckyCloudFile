package upload

import (
	"errors"
	"testing"

	"luckyCloudFile/internal/model"
)

// TestCanTransition 覆盖完整的状态转换表：
// 合法路径必须放行，跨越状态或从终态离开必须拒绝。
func TestCanTransition(t *testing.T) {
	tests := []struct {
		from string
		to   string
		want bool
	}{
		// 正常上传路径。
		{model.UploadStatusPending, model.UploadStatusReceiving, true},
		{model.UploadStatusReceiving, model.UploadStatusReady, true},
		{model.UploadStatusReady, model.UploadStatusCompleted, true},

		// 写入失败且已经停止：receiving 回到 pending，可以重新上传。
		{model.UploadStatusReceiving, model.UploadStatusPending, true},

		// 未完成状态都可以取消或过期。
		{model.UploadStatusPending, model.UploadStatusCanceled, true},
		{model.UploadStatusPending, model.UploadStatusExpired, true},
		{model.UploadStatusReceiving, model.UploadStatusCanceled, true},
		{model.UploadStatusReceiving, model.UploadStatusExpired, true},
		{model.UploadStatusReady, model.UploadStatusCanceled, true},
		{model.UploadStatusReady, model.UploadStatusExpired, true},

		// 重复确认完成是幂等重放，状态不变。
		{model.UploadStatusCompleted, model.UploadStatusCompleted, true},

		// 不能跳过 receiving / ready。
		{model.UploadStatusPending, model.UploadStatusReady, false},
		{model.UploadStatusPending, model.UploadStatusCompleted, false},
		{model.UploadStatusReceiving, model.UploadStatusCompleted, false},

		// 不能从 ready 回退到接收中或 pending。
		{model.UploadStatusReady, model.UploadStatusPending, false},
		{model.UploadStatusReady, model.UploadStatusReceiving, false},

		// completed 不能再取消或过期。
		{model.UploadStatusCompleted, model.UploadStatusCanceled, false},
		{model.UploadStatusCompleted, model.UploadStatusExpired, false},
		{model.UploadStatusCompleted, model.UploadStatusReady, false},

		// canceled / expired 是终态，不能再变回任何状态。
		{model.UploadStatusCanceled, model.UploadStatusPending, false},
		{model.UploadStatusCanceled, model.UploadStatusReceiving, false},
		{model.UploadStatusCanceled, model.UploadStatusCompleted, false},
		{model.UploadStatusExpired, model.UploadStatusPending, false},
		{model.UploadStatusExpired, model.UploadStatusReceiving, false},
		{model.UploadStatusExpired, model.UploadStatusCompleted, false},

		// 未知状态不放行。
		{"", model.UploadStatusPending, false},
		{"unknown", model.UploadStatusReceiving, false},
	}

	for _, test := range tests {
		test := test
		t.Run(test.from+"_to_"+test.to, func(t *testing.T) {
			if got := CanTransition(test.from, test.to); got != test.want {
				t.Fatalf("CanTransition(%q, %q) = %v, want %v", test.from, test.to, got, test.want)
			}
		})
	}
}

// TestTransitionHappyPath 走一遍完整上传路径，并确认每一步后状态正确。
func TestTransitionHappyPath(t *testing.T) {
	session := &model.UploadSession{Status: model.UploadStatusPending}

	for _, next := range []string{
		model.UploadStatusReceiving,
		model.UploadStatusReady,
	} {
		if err := Transition(session, next); err != nil {
			t.Fatalf("transition to %q: %v", next, err)
		}
		if session.Status != next {
			t.Fatalf("status = %q, want %q", session.Status, next)
		}
	}
}

// TestTransitionRejectsIllegalMoves 确认非法转换返回错误，而且不留下半改状态。
func TestTransitionRejectsIllegalMoves(t *testing.T) {
	tests := []struct {
		status string
		to     string
	}{
		{model.UploadStatusPending, model.UploadStatusCompleted},
		{model.UploadStatusReceiving, model.UploadStatusCompleted},
		{model.UploadStatusReady, model.UploadStatusPending},
		{model.UploadStatusCompleted, model.UploadStatusCanceled},
		{model.UploadStatusCompleted, model.UploadStatusExpired},
		{model.UploadStatusCanceled, model.UploadStatusPending},
		{model.UploadStatusExpired, model.UploadStatusPending},
	}

	for _, test := range tests {
		session := &model.UploadSession{Status: test.status}
		err := Transition(session, test.to)
		if !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("Transition(%q, %q) error = %v, want ErrInvalidTransition",
				test.status, test.to, err)
		}
		if session.Status != test.status {
			t.Fatalf("failed transition changed status: %q -> %q", test.status, session.Status)
		}
	}
}

// TestCompletedSessionCannotBeCanceledOrExpired 单独覆盖最重要的一条规则：
// 上传已经完成后，取消和过期都不能再改变状态。
func TestCompletedSessionCannotBeCanceledOrExpired(t *testing.T) {
	resultID := "file-id"
	session := &model.UploadSession{
		Status:       model.UploadStatusCompleted,
		ResultFileID: &resultID,
	}

	for _, next := range []string{model.UploadStatusCanceled, model.UploadStatusExpired} {
		if err := Transition(session, next); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("completed -> %s: error = %v, want ErrInvalidTransition", next, err)
		}
		if session.Status != model.UploadStatusCompleted {
			t.Fatalf("completed session changed to %q", session.Status)
		}
		if session.ResultFileID == nil || *session.ResultFileID != resultID {
			t.Fatalf("completed session lost its result file: %+v", session.ResultFileID)
		}
	}
}

// TestPlanCompletionIsIdempotent 验证“只有 ready 可以完成”和
// “重复完成返回已有结果，不创建新文件”这两条规则。
func TestPlanCompletionIsIdempotent(t *testing.T) {
	// 还没 ready：不能创建文件。
	for _, status := range []string{
		model.UploadStatusPending,
		model.UploadStatusReceiving,
		model.UploadStatusCanceled,
		model.UploadStatusExpired,
	} {
		session := &model.UploadSession{Status: status}
		if _, err := PlanCompletion(session); !errors.Is(err, ErrNotCompletable) {
			t.Fatalf("status %q: error = %v, want ErrNotCompletable", status, err)
		}
	}

	// ready：需要创建文件，并允许标记完成。
	ready := &model.UploadSession{Status: model.UploadStatusReady}
	plan, err := PlanCompletion(ready)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.CreateFile || !plan.MarkCompleted || plan.ResultFileID != "" {
		t.Fatalf("unexpected plan for ready session: %+v", plan)
	}

	// 模拟创建完文件、记录结果并进入 completed。
	resultID := "result-file-id"
	ready.ResultFileID = &resultID
	if err := Transition(ready, model.UploadStatusCompleted); err != nil {
		t.Fatal(err)
	}

	// 重复确认完成：返回已有文件 ID，并且明确要求不要再创建文件。
	plan, err = PlanCompletion(ready)
	if err != nil {
		t.Fatal(err)
	}
	if plan.CreateFile || plan.MarkCompleted {
		t.Fatalf("repeated completion must not create a file: %+v", plan)
	}
	if plan.ResultFileID != resultID {
		t.Fatalf("repeated completion result = %q, want %q", plan.ResultFileID, resultID)
	}

	// completed 但没有结果文件说明数据不一致，不能静默当成成功。
	broken := &model.UploadSession{Status: model.UploadStatusCompleted}
	if _, err := PlanCompletion(broken); !errors.Is(err, ErrMissingResultFile) {
		t.Fatalf("completed without result: error = %v, want ErrMissingResultFile", err)
	}

	// nil 会话也按不能完成处理，避免调用方解引用空指针。
	if _, err := PlanCompletion(nil); !errors.Is(err, ErrNotCompletable) {
		t.Fatalf("nil session: error = %v, want ErrNotCompletable", err)
	}
}
