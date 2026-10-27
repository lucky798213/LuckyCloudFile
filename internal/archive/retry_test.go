package archive

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/task"
)

// TestClassifyRunError 固定重试边界：永久错误保持原样，未识别的暂时性
// 错误才带 task.Retryable 标记。调度器只认这个标记，不猜错误文本。
func TestClassifyRunError(t *testing.T) {
	permanent := []error{
		fmt.Errorf("snapshot: %w", file.ErrNotFound),
		fmt.Errorf("source: %w", file.ErrSourceChanged),
		fmt.Errorf("quota: %w", file.ErrQuotaExceeded),
		fmt.Errorf("name: %w", file.ErrAlreadyExists),
		ErrUnknownPhase,
	}
	for _, err := range permanent {
		if task.IsRetryable(classifyRunError(err)) {
			t.Fatalf("permanent error %v was marked retryable", err)
		}
	}

	transient := []error{
		errors.New("dial tcp: connection reset"),
		fmt.Errorf("commit: %w", context.DeadlineExceeded),
		ErrArchiveCorrupt,
	}
	for _, err := range transient {
		if !task.IsRetryable(classifyRunError(err)) {
			t.Fatalf("transient error %v was not marked retryable", err)
		}
	}
}
