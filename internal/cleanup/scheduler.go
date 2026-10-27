package cleanup

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/task"
)

// DefaultInterval 是两次周期触发之间的默认间隔。清理任务本身的执行
// 由调度器按 next_run_at 决定，这里只负责“确保有一条待执行任务”。
const DefaultInterval = time.Minute

// Scheduler 负责周期触发清理任务。
//
// 它不直接删除任何东西，也不在 tick 里执行长任务；只调用 EnsurePeriodic，
// 由 tasks 表去重，再由有界 worker 执行。重复 tick、重复唤醒都不会产生
// 第二条未终态清理任务。
type Scheduler struct {
	tasks    *task.Service
	ownerID  int
	interval time.Duration
	logger   *slog.Logger
}

// NewScheduler 创建周期清理触发器。ownerID 必须是内部清理账号。
func NewScheduler(
	tasks *task.Service,
	ownerID int,
	interval time.Duration,
	logger *slog.Logger,
) (*Scheduler, error) {
	if tasks == nil {
		return nil, fmt.Errorf("cleanup scheduler: task service is required")
	}
	if ownerID <= 0 {
		return nil, fmt.Errorf("cleanup scheduler: owner ID must be positive")
	}
	if interval <= 0 {
		interval = DefaultInterval
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{
		tasks:    tasks,
		ownerID:  ownerID,
		interval: interval,
		logger:   logger,
	}, nil
}

// Params 返回内部清理任务的创建参数。
func Params(ownerID int) task.CreateParams {
	return task.CreateParams{
		Type:         TaskType,
		OwnerID:      ownerID,
		Phase:        "scan",
		StateVersion: StateVersion,
		StateJSON:    `{}`,
	}
}

// Ensure 确保至少有一条未终态清理任务。已存在时返回它，不插入新行。
func (s *Scheduler) Ensure(ctx context.Context) (*model.Task, error) {
	return s.tasks.EnsurePeriodic(ctx, Params(s.ownerID))
}

// Run 立即触发一次，之后按 interval 周期触发，直到 ctx 被取消。
//
// 触发失败只记录日志：数据库暂时不可用时下一轮还会尝试，不能因为一次
// 周期任务创建失败就让进程退出或丢掉清理责任。
func (s *Scheduler) Run(ctx context.Context) {
	if _, err := s.Ensure(ctx); err != nil && ctx.Err() == nil {
		s.logger.Warn("schedule cleanup task", "error", err)
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.Ensure(ctx); err != nil && ctx.Err() == nil {
				s.logger.Warn("schedule cleanup task", "error", err)
			}
		}
	}
}
