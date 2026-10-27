package pathlock

import (
	"fmt"
	"sync"
	"time"

	"luckyCloudFile/internal/idgen"
)

// entry 是内存里的一把锁。
//
// holds 记录当前已经 Confirm、但还没有释放的保护区。只要 holds > 0，
// 即使锁已经过期或开始关闭，也要保留冲突占位，不能提前把它从 byToken 移除。
type entry struct {
	lease   Lease
	holds   int  // 当前有多少个正在执行的保护区
	closing bool // 禁止再进入，等待已有保护区退出
}

// Manager 是纯内存锁管理器，零值可以直接使用。
//
// mu 只保护 byToken 的读取和修改。生成令牌在进入临界区之前完成；
// 用于过期判断的时钟在取得 mu 之后读取。Create 会先做一次预校验，
// 但入锁后会重新读取时钟并复核，避免等待锁期间跨过过期时间。
// 时钟不是数据库或磁盘 I/O，不会扩大临界区的实际开销。
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

	// 令牌生成期间可能已经跨过申请的有效期，入锁后必须重新校验。
	now = m.currentTime()
	for _, requested := range normalized {
		if err := ensureRequestNotExpired(requested, now); err != nil {
			return nil, err
		}
	}

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

// Confirm 在令牌覆盖的路径上进入一次短时保护区。
//
// 同一令牌可以同时有多个保护区，它们只增加 holds，不串行化请求。
// 返回的释放函数用 sync.Once 保证只扣减一次；最后一次释放时，如果锁已经
// 过期或正在关闭，才真正从 byToken 移除。
func (m *Manager) Confirm(namespace, p, token string) (func(), error) {
	if namespace == "" {
		return nil, ErrInvalidNamespace
	}
	normalized, err := normalizePath(p)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.currentTime()

	held, ok := m.byToken[token]
	if !ok || held.lease.Request.Namespace != namespace {
		return nil, fmt.Errorf("%w: %q", ErrNoSuchLock, token)
	}
	if !requestCovers(held.lease.Request, normalized) {
		return nil, fmt.Errorf(
			"%w: %s %s does not cover %s",
			ErrNotCovered,
			held.lease.Request.Scope, held.lease.Request.Path, normalized,
		)
	}
	if expiredLocked(held, now) {
		return nil, fmt.Errorf("%w: %q", ErrExpired, token)
	}
	if held.closing {
		return nil, fmt.Errorf("%w: %q", ErrClosing, token)
	}

	held.holds++

	var once sync.Once
	return func() {
		once.Do(func() {
			m.release(held)
		})
	}, nil
}

// Refresh 修改令牌的过期时间。
//
// 过期的锁不能通过 Refresh 复活；正在关闭的锁也不能续期。
// until 为零值表示改为永不过期，否则必须是 now 之后的时间。
func (m *Manager) Refresh(namespace, token string, until time.Time) error {
	if namespace == "" {
		return ErrInvalidNamespace
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.currentTime()

	held, ok := m.byToken[token]
	if !ok || held.lease.Request.Namespace != namespace {
		return fmt.Errorf("%w: %q", ErrNoSuchLock, token)
	}
	if expiredLocked(held, now) {
		return fmt.Errorf("%w: %q", ErrExpired, token)
	}
	if held.closing {
		return fmt.Errorf("%w: %q", ErrClosing, token)
	}
	if !until.IsZero() && !until.After(now) {
		return fmt.Errorf("%w: %s", ErrInvalidExpiry, until)
	}

	held.lease.Request.ExpiresAt = until
	return nil
}

// Unlock 按命名空间和令牌移除锁。
//
// 令牌立即失效，新的 Confirm 和 Refresh 都会被拒绝。如果还有保护区没有退出，
// 先保留冲突占位，等最后一个 release 执行时再真正移除。整批令牌要么全部
// 标记关闭，要么全部保持不变。
func (m *Manager) Unlock(namespace string, tokens ...string) error {
	if namespace == "" {
		return ErrInvalidNamespace
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.currentTime()

	m.removeExpiredLocked(now)

	entries := make([]*entry, 0, len(tokens))
	seen := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}

		held, ok := m.byToken[token]
		if !ok || held.lease.Request.Namespace != namespace {
			return fmt.Errorf("%w: %q", ErrNoSuchLock, token)
		}
		entries = append(entries, held)
	}

	for _, held := range entries {
		held.closing = true
		if held.holds == 0 {
			m.removeEntryLocked(held)
		}
	}
	return nil
}

// release 扣减一次保护区；只有最后一个保护区退出，并且锁已经过期或关闭，
// 才真正移除锁。
func (m *Manager) release(held *entry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.currentTime()

	if held.holds == 0 {
		return
	}
	held.holds--
	if held.holds > 0 {
		return
	}
	if !held.closing && !expiredLocked(held, now) {
		return
	}
	m.removeEntryLocked(held)
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

		if err := ensureRequestNotExpired(req, now); err != nil {
			return nil, err
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

func ensureRequestNotExpired(req Request, now time.Time) error {
	if req.ExpiresAt.IsZero() || req.ExpiresAt.After(now) {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrInvalidExpiry, req.ExpiresAt)
}

// removeExpiredLocked 惰性清理过期的锁，不维护额外的过期堆。
// 还有保护区的锁即使过期也要保留，等最后一个 release 再移除。
func (m *Manager) removeExpiredLocked(now time.Time) {
	for _, held := range m.byToken {
		if held.holds > 0 || !expiredLocked(held, now) {
			continue
		}
		m.removeEntryLocked(held)
	}
}

func (m *Manager) removeEntryLocked(held *entry) {
	if current, ok := m.byToken[held.lease.Token]; ok && current == held {
		delete(m.byToken, held.lease.Token)
	}
}

func expiredLocked(held *entry, now time.Time) bool {
	expiresAt := held.lease.Request.ExpiresAt
	return !expiresAt.IsZero() && !now.Before(expiresAt)
}

func (m *Manager) currentTime() time.Time {
	if m.now == nil {
		return time.Now()
	}
	return m.now()
}
