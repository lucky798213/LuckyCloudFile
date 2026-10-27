package task

import (
	"context"
	"errors"
	"testing"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/queue"
)

// TestRegistryRebuildsForEveryExecution 验证注册表只保存工厂，不缓存 Job：
// 同一条记录执行两次必须重建两次，恢复不会复用上一次的 runner。
func TestRegistryRebuildsForEveryExecution(t *testing.T) {
	registry := NewRegistry()

	rebuilds := 0
	err := registry.Register("fake_echo", FactoryFunc(func(record model.Task) (Runner, error) {
		rebuilds++
		return RunnerFunc(func(
			_ context.Context,
			_ model.Task,
			_ queue.ProgressFunc,
		) (string, error) {
			return record.ID, nil
		}), nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	record := model.Task{ID: "task-1", Type: "fake_echo", OwnerID: 1}
	for i := 0; i < 2; i++ {
		result, err := registry.Run(context.Background(), record, nil)
		if err != nil {
			t.Fatal(err)
		}
		if result != "task-1" {
			t.Fatalf("result = %q, want task-1", result)
		}
	}
	if rebuilds != 2 {
		t.Fatalf("factory rebuilt %d time(s), want 2", rebuilds)
	}
}

// TestRegistryRejectsBadRegistrations 验证空类型、空工厂和重复注册都报错。
func TestRegistryRejectsBadRegistrations(t *testing.T) {
	registry := NewRegistry()

	if err := registry.Register("", FactoryFunc(func(model.Task) (Runner, error) {
		return nil, nil
	})); err == nil {
		t.Fatal("empty task type was registered")
	}
	if err := registry.Register("fake_echo", nil); err == nil {
		t.Fatal("nil factory was registered")
	}

	factory := FactoryFunc(func(model.Task) (Runner, error) {
		return RunnerFunc(func(context.Context, model.Task, queue.ProgressFunc) (string, error) {
			return "", nil
		}), nil
	})
	if err := registry.Register("fake_echo", factory); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("fake_echo", factory); err == nil {
		t.Fatal("duplicate task type was registered")
	}
}

// TestRegistryUnknownType 验证未注册类型是明确的永久错误。
func TestRegistryUnknownType(t *testing.T) {
	registry := NewRegistry()
	_, err := registry.Run(context.Background(), model.Task{Type: "missing"}, nil)
	if !errors.Is(err, ErrUnknownTaskType) {
		t.Fatalf("error = %v, want ErrUnknownTaskType", err)
	}
}

// TestRegistryPropagatesFactoryErrors 验证工厂自身的错误和空 runner 不能被吞掉。
func TestRegistryPropagatesFactoryErrors(t *testing.T) {
	registry := NewRegistry()
	factoryErr := errors.New("bad state")
	if err := registry.Register("broken", FactoryFunc(func(model.Task) (Runner, error) {
		return nil, factoryErr
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Run(context.Background(), model.Task{Type: "broken"}, nil); !errors.Is(err, factoryErr) {
		t.Fatalf("error = %v, want %v", err, factoryErr)
	}

	if err := registry.Register("empty", FactoryFunc(func(model.Task) (Runner, error) {
		return nil, nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Run(context.Background(), model.Task{Type: "empty"}, nil); err == nil {
		t.Fatal("factory returning nil runner was accepted")
	}
}
