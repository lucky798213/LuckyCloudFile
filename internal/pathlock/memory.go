package pathlock

import (
	"fmt"
	"sync"
	"time"

	"luckyCloudFile/internal/idgen"
)

// entry 是内存里的一把锁。
//
// holds 和 closing 留给后续的保护区进入/退出；第一版的 Create 和 Unlock
// 只关心 lease，不修改这两个字段。
type entry struct {
	lease   Lease
	holds   int  // 当前有多少个正在执行的保护区
	closing bool // 禁止再进入，等待已有保护区退出
}

// Manager 是纯内存锁管理器，零值可以直接使用。
//
// mu 只保护 byToken 的读取和修改。像生成令牌、读取时钟这类操作都在
// 进入临界区之前完成；Create 和 Unlock 内部不会查询数据库或写磁盘。
type Manager struct {
	mu      sync.Mutex
	byToken map[string]*entry
	now     func() time.Time
}

// NewManager 创建锁管理器。
func NewManager() *Manager {
	return &Manager{
		byToken: make(map[string]*entry),
		now:     time.Now,
	}
}

// Create 申请一把锁。
//
// 参数校验通过、且与同一命名空间中已有的锁不冲突时，生成随机令牌并保存锁。
// 成功返回的 Lease 里带规范化之后的 Request；失败时不会留下任何锁。
func (m *Manager) Create(req Request) (Lease, error) {
	if req.Namespace == "" {
		return Lease{}, ErrInvalidNamespace
	}
	if req.Scope != Node && req.Scope != Subtree {
		return Lease{}, fmt.Errorf("%w: %d", ErrInvalidScope, req.Scope)
	}

	normalized, err := normalizePath(req.Path)
	if err != nil {
		return Lease{}, err
	}
	req.Path = normalized

	now := m.currentTime()
	if !req.ExpiresAt.IsZero() && !req.ExpiresAt.After(now) {
		return Lease{}, fmt.Errorf("%w: %s", ErrInvalidExpiry, req.ExpiresAt)
	}

	// 令牌在进入临界区之前生成，mu 内只做内存操作。
	token, err := idgen.New()
	if err != nil {
		return Lease{}, fmt.Errorf("pathlock: generate token: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.byToken == nil {
		m.byToken = make(map[string]*entry)
	}
	m.removeExpiredLocked(now)

	for _, held := range m.byToken {
		if !conflicts(held.lease.Request, req) {
			continue
		}
		return Lease{}, fmt.Errorf(
			"%w: %s %s is covered by %s %s",
			ErrConflict, req.Scope, req.Path, held.lease.Request.Scope, held.lease.Request.Path,
		)
	}

	lease := Lease{Token: token, Request: req}
	m.byToken[token] = &entry{lease: lease}
	return lease, nil
}

// Unlock 按令牌移除锁。
//
// 令牌不存在、已经解锁或已经过期时返回 ErrNoSuchLock。
// 移除只按令牌匹配，所以旧令牌不会影响后来创建的新锁。
func (m *Manager) Unlock(token string) error {
	now := m.currentTime()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.removeExpiredLocked(now)
	if _, ok := m.byToken[token]; !ok {
		return fmt.Errorf("%w: %q", ErrNoSuchLock, token)
	}
	delete(m.byToken, token)
	return nil
}

// removeExpiredLocked 惰性清理过期的锁，不维护额外的过期堆。
func (m *Manager) removeExpiredLocked(now time.Time) {
	for token, held := range m.byToken {
		expiresAt := held.lease.Request.ExpiresAt
		if expiresAt.IsZero() || now.Before(expiresAt) {
			continue
		}
		delete(m.byToken, token)
	}
}

func (m *Manager) currentTime() time.Time {
	if m.now == nil {
		return time.Now()
	}
	return m.now()
}
