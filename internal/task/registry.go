package task

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/queue"
)

// ErrUnknownTaskType 表示 tasks.type 没有注册重建工厂。
// 未知类型的任务必须明确失败，不能按零值猜参数执行。
var ErrUnknownTaskType = errors.New("unknown task type")

// Checkpointer 是业务在阶段边界提交检查点的能力。
//
// 生产实现是 *Service：它用 attempts 做围栏，只允许当前这一轮执行推进
// phase。业务（例如 archive）只依赖这个小接口，不需要持有数据库连接。
type Checkpointer interface {
	SaveCheckpoint(ctx context.Context, id string, attempt int, checkpoint Checkpoint) error
}

var _ Checkpointer = (*Service)(nil)

// Factory 从一条 tasks 记录重建并执行一项业务。
//
// 依赖（文件服务、工作目录、限制等）在应用启动时注入工厂，记录里只允许
// 有 type、owner、phase 和 state_json。Factory 不能在内存里保留 Job 指针
// 供下一次执行复用：每次 Run 都必须重新构造。
type Factory interface {
	Rebuild(record model.Task) (Runner, error)
}

// FactoryFunc 方便用普通函数实现一个 Factory。
type FactoryFunc func(record model.Task) (Runner, error)

func (f FactoryFunc) Rebuild(record model.Task) (Runner, error) {
	if f == nil {
		return nil, errors.New("task factory is nil")
	}
	return f(record)
}

// Registry 是 type → 重建工厂的注册表，本身也实现 Runner。
//
// 它只保存工厂，不保存 Job、context、reader 或任何一次执行的状态；
// 每次调用 Run 都先按记录里的 type 找到工厂，再从记录重建任务。
type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

var _ Runner = (*Registry)(nil)

// NewRegistry 创建空注册表。注册应在开始调度之前完成。
func NewRegistry() *Registry {
	return &Registry{factories: make(map[string]Factory)}
}

// Register 为一种任务类型登记重建工厂。重复登记、空类型或空工厂都报错。
func (r *Registry) Register(taskType string, factory Factory) error {
	if taskType == "" {
		return errors.New("task type is required")
	}
	if factory == nil {
		return errors.New("task factory is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.factories[taskType]; ok {
		return fmt.Errorf("task type %q is already registered", taskType)
	}
	r.factories[taskType] = factory
	return nil
}

// Runner 按记录里的 type 重建一个执行器。它不执行任务，方便调用方在
// 开始前先发现“未知类型”这类永久错误。
func (r *Registry) Runner(record model.Task) (Runner, error) {
	r.mu.RLock()
	factory, ok := r.factories[record.Type]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownTaskType, record.Type)
	}
	runner, err := factory.Rebuild(record)
	if err != nil {
		return nil, err
	}
	if runner == nil {
		return nil, fmt.Errorf("task factory %q returned no runner", record.Type)
	}
	return runner, nil
}

// Run 实现 Runner：重建后立即执行一次。
//
// 重建和执行紧挨着发生，中间没有常驻对象；所以进程重启、队列重建或
// 同一条记录再次到期时，拿到的都是根据最新记录构造的新任务。
func (r *Registry) Run(
	ctx context.Context,
	record model.Task,
	report queue.ProgressFunc,
) (string, error) {
	runner, err := r.Runner(record)
	if err != nil {
		return "", err
	}
	return runner.Run(ctx, record, report)
}
