package file

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

// waitForFinalizeGate 等到 beginFinalize 真正安装好门禁。
//
// 测试用轮询 + Gosched 而不是固定 sleep：只等待状态出现，不猜执行顺序。
func waitForFinalizeGate(t *testing.T, c *uploadController, sessionID string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		_, installed := c.finalizing[sessionID]
		c.mu.Unlock()
		if installed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("finalize gate was not installed")
		}
		runtime.Gosched()
	}
}

// TestChunkControllerParallelSlots 验证：
//   - 不同编号可以同时登记（并行度上限内）；
//   - 同一编号的第二个请求被拒绝；
//   - 超过并行度的请求被拒绝；
//   - 一个请求结束后名额释放给下一个编号。
func TestChunkControllerParallelSlots(t *testing.T) {
	controller := newUploadController()
	ctx := context.Background()

	_, end0, err := controller.beginChunk(ctx, "s1", 0)
	if err != nil {
		t.Fatalf("begin chunk 0: %v", err)
	}
	_, end1, err := controller.beginChunk(ctx, "s1", 1)
	if err != nil {
		t.Fatalf("begin chunk 1: %v", err)
	}

	if _, _, err := controller.beginChunk(ctx, "s1", 0); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("same index error = %v, want ErrUploadSessionConflict", err)
	}
	if _, _, err := controller.beginChunk(ctx, "s1", 2); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("over-limit error = %v, want ErrUploadSessionConflict", err)
	}
	if !controller.isActive("s1") {
		t.Fatal("chunk uploads were not registered as active")
	}

	// 释放一个名额后，另一个编号可以登记。
	end0()
	_, end2, err := controller.beginChunk(ctx, "s1", 2)
	if err != nil {
		t.Fatalf("begin chunk 2 after a slot was released: %v", err)
	}

	end1()
	end2()
	if controller.isActive("s1") {
		t.Fatal("session still active after all chunks ended")
	}
}

// TestChunkControllerCancelWaitsForEveryRequest 验证取消只发通知：
// 所有在途请求都收到取消，但 drained 必须等最后一个 end 才关闭。
func TestChunkControllerCancelWaitsForEveryRequest(t *testing.T) {
	controller := newUploadController()
	ctx := context.Background()

	uploadCtx0, end0, err := controller.beginChunk(ctx, "s1", 0)
	if err != nil {
		t.Fatal(err)
	}
	uploadCtx1, end1, err := controller.beginChunk(ctx, "s1", 1)
	if err != nil {
		t.Fatal(err)
	}

	done := controller.cancel("s1")
	if uploadCtx0.Err() == nil || uploadCtx1.Err() == nil {
		t.Fatal("cancel did not notify every in-flight chunk")
	}
	select {
	case <-done:
		t.Fatal("drained closed before the chunks exited")
	default:
	}

	// stopping 已经打开：取消之后不能再登记新请求。
	if _, _, err := controller.beginChunk(ctx, "s1", 2); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("begin after cancel error = %v, want ErrUploadSessionConflict", err)
	}

	end0()
	select {
	case <-done:
		t.Fatal("drained closed before the last chunk exited")
	default:
	}

	end1()
	select {
	case <-done:
	default:
		t.Fatal("drained was not closed after the last chunk exited")
	}
	if controller.isActive("s1") {
		t.Fatal("session still active after the last chunk exited")
	}
	// stopping 保留到终态清理：忘记之前仍然拒绝登记。
	if _, _, err := controller.beginChunk(ctx, "s1", 2); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("begin before forget error = %v, want ErrUploadSessionConflict", err)
	}
	controller.forget("s1")

	_, endAgain, err := controller.beginChunk(ctx, "s1", 0)
	if err != nil {
		t.Fatalf("begin after forget: %v", err)
	}
	endAgain()
}

// TestChunkControllerGateWaitsWithoutCanceling 验证完成门禁对分片的行为：
// 新请求被拒绝，但在途分片不会被取消，门禁等它们自然结束。
func TestChunkControllerGateWaitsWithoutCanceling(t *testing.T) {
	controller := newUploadController()
	ctx := context.Background()

	uploadCtx, endUpload, err := controller.beginChunk(ctx, "s1", 0)
	if err != nil {
		t.Fatal(err)
	}

	type gateResult struct {
		release func()
		err     error
	}
	gateReady := make(chan gateResult, 1)
	go func() {
		release, err := controller.beginFinalize(ctx, "s1")
		gateReady <- gateResult{release: release, err: err}
	}()
	waitForFinalizeGate(t, controller, "s1")

	// 门禁不能取消在途分片：它要等分片自己写完。
	select {
	case <-uploadCtx.Done():
		t.Fatal("finalize gate canceled an in-flight chunk")
	default:
	}
	select {
	case result := <-gateReady:
		if result.release != nil {
			result.release()
		}
		t.Fatal("finalize gate was released before the chunk exited")
	default:
	}
	// 门禁安装后，新写入（包括不同编号）都被拒绝。
	if _, _, err := controller.beginChunk(ctx, "s1", 1); !errors.Is(err, ErrUploadSessionConflict) {
		t.Fatalf("begin while gate is held error = %v, want ErrUploadSessionConflict", err)
	}

	endUpload()
	var result gateResult
	select {
	case result = <-gateReady:
	case <-time.After(5 * time.Second):
		t.Fatal("finalize gate did not proceed after the chunk exited")
	}
	if result.err != nil {
		t.Fatalf("beginFinalize: %v", result.err)
	}
	if !controller.isActive("s1") {
		t.Fatal("session with a finalize gate must count as active for cleanup")
	}

	result.release()
	if controller.isActive("s1") {
		t.Fatal("session still active after releasing the finalize gate")
	}
}

// TestWithChunkParallelKeepsDefaultValues 验证配置注入方法：
// 默认是 2；非正数不会把并行度改成 0（启动校验已经挡住显式 0/负数，
// 这里只保证直接构造 Service 的测试和旧调用方仍然安全）。
func TestWithChunkParallelKeepsDefaultValues(t *testing.T) {
	service := NewService(nil, nil)
	if service.uploads.maxParallel != defaultChunkParallel {
		t.Fatalf("default parallel = %d, want %d",
			service.uploads.maxParallel, defaultChunkParallel)
	}

	service.WithChunkParallel(5)
	if service.uploads.maxParallel != 5 {
		t.Fatalf("parallel after WithChunkParallel(5) = %d, want 5",
			service.uploads.maxParallel)
	}

	service.WithChunkParallel(0)
	service.WithChunkParallel(-3)
	if service.uploads.maxParallel != 5 {
		t.Fatalf("non-positive value changed parallel to %d, want 5",
			service.uploads.maxParallel)
	}
}
