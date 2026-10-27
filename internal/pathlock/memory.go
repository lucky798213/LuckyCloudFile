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

// Create 原子地申请一批锁：要么全部成功，要么一把也不留下。
//
// 同一命名空间、同一路径的重复申请会合并；Node 与 Subtree 同时出现时
// 合并为 Subtree。返回值按合并后首次出现的顺序排列。
//
// 批次内不同路径之间如果存在覆盖关系（例如 Subtree /docs 和 Node /docs/a），
// 视为不合理的重叠申请，返回 ErrConflict。全部检查通过后才统一写入。
func (m *Manager) Create(requests ...Request) ([]Lease, error) {
	now := m.currentTime()

	normalized, err := normalizeRequests(requests, now)
	if err != nil {
		return nil, err
	}
	if len(normalized) == 0 {
		return []Lease{}, nil
	}

	// 令牌在进入临界区之前生成，mu 内只做内存操作。
	// 只要有一个令牌生成失败，整批申请就失败，此时还没有写入任何锁。
	tokens := make([]string, len(normalized))
	for i := range normalized {
		token, err := idgen.New()
		if err != nil {
			return nil, fmt.Errorf("pathlock: generate token: %w", err)
		}
		tokens[i] = token
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.byToken == nil {
		m.byToken = make(map[string]*entry)
	}
	m.removeExpiredLocked(now)

	// 先检查整批与已有锁的冲突。
	for _, requested := range normalized {
		for _, held := range m.byToken {
			if !conflicts(held.lease.Request, requested) {
				continue
			}
			return nil, fmt.Errorf(
				"%w: %s %s is covered by %s %s",
				ErrConflict,
				requested.Scope, requested.Path,
				held.lease.Request.Scope, held.lease.Request.Path,
			)
		}
	}

	// 再检查这一批内部是否存在不合理的重叠。
	if err := checkBatchOverlap(normalized); err != nil {
		return nil, err
	}

	// 全部通过后才统一写入，失败路径不需要回滚半套锁。
	leases := make([]Lease, len(normalized))
	for i, requested := range normalized {
		lease := Lease{Token: tokens[i], Request: requested}
		m.byToken[lease.Token] = &entry{lease: lease}
		leases[i] = lease
	}
	return leases, nil
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

type requestKey struct {
	namespace string
	path      string
}

// normalizeRequests 校验并规范化整批申请，同时合并重复的键。
//
// 合并后的过期时间取两者中较晚的；任一方为零值（永不过期）时结果也为零值。
func normalizeRequests(requests []Request, now time.Time) ([]Request, error) {
	merged := make([]Request, 0, len(requests))
	positions := make(map[requestKey]int, len(requests))

	for _, req := range requests {
		if req.Namespace == "" {
			return nil, ErrInvalidNamespace
		}
		if req.Scope != Node && req.Scope != Subtree {
			return nil, fmt.Errorf("%w: %d", ErrInvalidScope, req.Scope)
		}

		normalized, err := normalizePath(req.Path)
		if err != nil {
			return nil, err
		}
		req.Path = normalized

		if !req.ExpiresAt.IsZero() && !req.ExpiresAt.After(now) {
			return nil, fmt.Errorf("%w: %s", ErrInvalidExpiry, req.ExpiresAt)
		}

		key := requestKey{namespace: req.Namespace, path: req.Path}
		index, ok := positions[key]
		if !ok {
			positions[key] = len(merged)
			merged = append(merged, req)
			continue
		}

		existing := &merged[index]
		if req.Scope == Subtree {
			existing.Scope = Subtree
		}
		existing.ExpiresAt = laterExpiry(existing.ExpiresAt, req.ExpiresAt)
	}

	return merged, nil
}

// checkBatchOverlap 检查同一批申请之间是否存在覆盖关系。
//
// 同一路径的申请已经在 normalizeRequests 里合并，这里只处理不同路径。
func checkBatchOverlap(requests []Request) error {
	for i := 0; i < len(requests); i++ {
		for j := i + 1; j < len(requests); j++ {
			if !conflicts(requests[i], requests[j]) {
				continue
			}
			return fmt.Errorf(
				"%w: batch requests %s %s and %s %s overlap",
				ErrConflict,
				requests[i].Scope, requests[i].Path,
				requests[j].Scope, requests[j].Path,
			)
		}
	}
	return nil
}

func laterExpiry(a, b time.Time) time.Time {
	switch {
	case a.IsZero() || b.IsZero():
		return time.Time{}
	case b.After(a):
		return b
	default:
		return a
	}
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
