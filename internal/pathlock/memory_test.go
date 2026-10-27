package pathlock

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func mustCreate(t *testing.T, m *Manager, req Request) Lease {
	t.Helper()

	leases := mustCreateBatch(t, m, req)
	if len(leases) != 1 {
		t.Fatalf("Create(%+v) returned %d leases, want 1", req, len(leases))
	}
	return leases[0]
}

func mustCreateBatch(t *testing.T, m *Manager, requests ...Request) []Lease {
	t.Helper()

	leases, err := m.Create(requests...)
	if err != nil {
		t.Fatalf("Create(%+v) error = %v", requests, err)
	}
	return leases
}

func mustConflict(t *testing.T, m *Manager, req Request) {
	t.Helper()

	leases, err := m.Create(req)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("Create(%+v) error = %v, want ErrConflict", req, err)
	}
	if len(leases) != 0 {
		t.Fatalf("conflicting Create(%+v) returned %d leases, want 0", req, len(leases))
	}
}

func mustUnlock(t *testing.T, m *Manager, lease Lease) {
	t.Helper()

	if err := m.Unlock(lease.Request.Namespace, lease.Token); err != nil {
		t.Fatalf("Unlock(%q) error = %v", lease.Token, err)
	}
}

func mustConfirm(t *testing.T, m *Manager, namespace, p, token string) func() {
	t.Helper()

	release, err := m.Confirm(namespace, p, token)
	if err != nil {
		t.Fatalf("Confirm(%q, %q) error = %v", p, token, err)
	}
	return release
}

// TestCreateTable 覆盖虚拟路径锁的冲突表。
func TestCreateTable(t *testing.T) {
	user1 := Namespace(1)
	user2 := Namespace(2)

	tests := []struct {
		name     string
		held     Request
		request  Request
		conflict bool
	}{
		{
			name:     "same node",
			held:     Request{Namespace: user1, Path: "/docs/a.txt", Scope: Node},
			request:  Request{Namespace: user1, Path: "/docs/a.txt", Scope: Node},
			conflict: true,
		},
		{
			name:    "sibling nodes",
			held:    Request{Namespace: user1, Path: "/docs/a.txt", Scope: Node},
			request: Request{Namespace: user1, Path: "/docs/b.txt", Scope: Node},
		},
		{
			name:     "held subtree covers requested node",
			held:     Request{Namespace: user1, Path: "/docs", Scope: Subtree},
			request:  Request{Namespace: user1, Path: "/docs/a.txt", Scope: Node},
			conflict: true,
		},
		{
			name:     "requested subtree covers held node",
			held:     Request{Namespace: user1, Path: "/docs/a.txt", Scope: Node},
			request:  Request{Namespace: user1, Path: "/docs", Scope: Subtree},
			conflict: true,
		},
		{
			name:    "node does not cover its child",
			held:    Request{Namespace: user1, Path: "/docs", Scope: Node},
			request: Request{Namespace: user1, Path: "/docs/a.txt", Scope: Node},
		},
		{
			name:    "prefix is not a path boundary",
			held:    Request{Namespace: user1, Path: "/a/b", Scope: Subtree},
			request: Request{Namespace: user1, Path: "/a/bb/file", Scope: Node},
		},
		{
			name:    "different user namespaces",
			held:    Request{Namespace: user1, Path: "/docs", Scope: Subtree},
			request: Request{Namespace: user2, Path: "/docs", Scope: Subtree},
		},
		{
			name:     "root subtree covers everything",
			held:     Request{Namespace: user1, Path: "/", Scope: Subtree},
			request:  Request{Namespace: user1, Path: "/docs/a.txt", Scope: Node},
			conflict: true,
		},
		{
			name:    "root node only covers itself",
			held:    Request{Namespace: user1, Path: "/", Scope: Node},
			request: Request{Namespace: user1, Path: "/docs", Scope: Node},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewManager()
			held := mustCreate(t, m, tt.held)

			leases, err := m.Create(tt.request)
			if tt.conflict {
				if !errors.Is(err, ErrConflict) {
					t.Fatalf("Create() error = %v, want ErrConflict", err)
				}
				if len(leases) != 0 {
					t.Fatalf("conflicting Create() returned %d leases, want 0", len(leases))
				}

				// 冲突申请不能留下锁：释放已有锁后，同一申请应当成功。
				mustUnlock(t, m, held)
				retry := mustCreate(t, m, tt.request)
				mustUnlock(t, m, retry)
				return
			}

			if err != nil {
				t.Fatalf("Create() error = %v, want success", err)
			}
			if len(leases) != 1 {
				t.Fatalf("Create() returned %d leases, want 1", len(leases))
			}
			lease := leases[0]
			if lease.Token == "" {
				t.Fatal("Create() returned an empty token")
			}

			// 成功后锁确实生效。
			mustConflict(t, m, tt.request)

			// 解锁后可以再次申请。
			mustUnlock(t, m, lease)
			again := mustCreate(t, m, tt.request)
			mustUnlock(t, m, again)
			mustUnlock(t, m, held)
		})
	}
}

func TestUnlockUnknownToken(t *testing.T) {
	m := NewManager()

	if err := m.Unlock(Namespace(1), "does-not-exist"); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("Unlock() error = %v, want ErrNoSuchLock", err)
	}

	lease := mustCreate(t, m, Request{
		Namespace: Namespace(1),
		Path:      "/docs/a.txt",
		Scope:     Node,
	})
	mustUnlock(t, m, lease)
	if err := m.Unlock(Namespace(1), lease.Token); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("second Unlock() error = %v, want ErrNoSuchLock", err)
	}
}

func TestOldTokenCannotUnlockNewLock(t *testing.T) {
	m := NewManager()
	user := Namespace(1)
	req := Request{
		Namespace: user,
		Path:      "/docs/a.txt",
		Scope:     Node,
	}

	first := mustCreate(t, m, req)
	mustUnlock(t, m, first)

	second := mustCreate(t, m, req)
	if second.Token == first.Token {
		t.Fatal("re-created lock reused the old token")
	}

	if err := m.Unlock(user, first.Token); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("Unlock(old token) error = %v, want ErrNoSuchLock", err)
	}

	// 旧令牌没有影响新锁，新锁仍然有效。
	mustConflict(t, m, req)
	mustUnlock(t, m, second)
}

func TestPathNormalization(t *testing.T) {
	m := NewManager()
	held := mustCreate(t, m, Request{
		Namespace: Namespace(1),
		Path:      "/docs/",
		Scope:     Subtree,
	})
	if held.Request.Path != "/docs" {
		t.Fatalf("normalized path = %q, want /docs", held.Request.Path)
	}

	for _, p := range []string{"/docs/a.txt", "//docs//b.txt", "/docs/a.txt/"} {
		mustConflict(t, m, Request{Namespace: Namespace(1), Path: p, Scope: Node})
	}

	mustUnlock(t, m, held)
}

func TestCreateRejectsInvalidRequest(t *testing.T) {
	now := time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)
	m := NewManager()
	m.now = func() time.Time { return now }

	tests := []struct {
		name string
		req  Request
		want error
	}{
		{
			name: "empty namespace",
			req:  Request{Path: "/docs", Scope: Node},
			want: ErrInvalidNamespace,
		},
		{
			name: "relative path",
			req:  Request{Namespace: Namespace(1), Path: "docs/a.txt", Scope: Node},
			want: ErrInvalidPath,
		},
		{
			name: "dot segment",
			req:  Request{Namespace: Namespace(1), Path: "/docs/../a.txt", Scope: Node},
			want: ErrInvalidPath,
		},
		{
			name: "nul byte",
			req:  Request{Namespace: Namespace(1), Path: "/docs\x00a.txt", Scope: Node},
			want: ErrInvalidPath,
		},
		{
			name: "invalid scope",
			req:  Request{Namespace: Namespace(1), Path: "/docs", Scope: Scope(9)},
			want: ErrInvalidScope,
		},
		{
			name: "expired request",
			req: Request{
				Namespace: Namespace(1),
				Path:      "/docs",
				Scope:     Node,
				ExpiresAt: now,
			},
			want: ErrInvalidExpiry,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := m.Create(tt.req); !errors.Is(err, tt.want) {
				t.Fatalf("Create() error = %v, want %v", err, tt.want)
			}
		})
	}

	// 非法申请不能挡住后来的合法申请。
	lease := mustCreate(t, m, Request{
		Namespace: Namespace(1),
		Path:      "/docs",
		Scope:     Node,
	})
	mustUnlock(t, m, lease)
}

func TestExpiredLockAllowsCreate(t *testing.T) {
	now := time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)
	m := NewManager()
	m.now = func() time.Time { return now }

	req := Request{
		Namespace: Namespace(1),
		Path:      "/docs/a.txt",
		Scope:     Node,
		ExpiresAt: now.Add(time.Minute),
	}
	expiring := mustCreate(t, m, req)
	mustConflict(t, m, req)

	now = now.Add(2 * time.Minute)
	if err := m.Unlock(req.Namespace, expiring.Token); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("Unlock(expired token) error = %v, want ErrNoSuchLock", err)
	}

	// 过期锁不再阻塞；零值 ExpiresAt 的锁不会被清理。
	req.ExpiresAt = time.Time{}
	fresh := mustCreate(t, m, req)
	mustUnlock(t, m, fresh)

	forever := mustCreate(t, m, Request{
		Namespace: Namespace(1),
		Path:      "/forever",
		Scope:     Node,
	})
	now = now.Add(100 * 365 * 24 * time.Hour)
	mustConflict(t, m, forever.Request)
	mustUnlock(t, m, forever)
}

// TestCreateBatchAtomicOnExistingConflict 验证批量申请失败时不留半套锁：
// 先占用目标，批量申请源和目标失败，源路径仍然可以单独申请。
func TestCreateBatchAtomicOnExistingConflict(t *testing.T) {
	m := NewManager()
	user := Namespace(1)
	source := Request{Namespace: user, Path: "/docs/report.txt", Scope: Node}
	target := Request{Namespace: user, Path: "/archive/report.txt", Scope: Node}

	targetLease := mustCreate(t, m, target)

	leases, err := m.Create(source, target)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("batch Create() error = %v, want ErrConflict", err)
	}
	if len(leases) != 0 {
		t.Fatalf("failed batch returned %d leases, want 0", len(leases))
	}

	// 目标仍然被占用，但源没有留下任何锁，所以可以单独申请。
	sourceLease := mustCreate(t, m, source)

	mustUnlock(t, m, sourceLease)
	mustUnlock(t, m, targetLease)
}

func TestCreateBatchSuccess(t *testing.T) {
	m := NewManager()
	user := Namespace(1)
	source := Request{Namespace: user, Path: "/docs/report.txt", Scope: Node}
	target := Request{Namespace: user, Path: "/archive/report.txt", Scope: Node}

	leases := mustCreateBatch(t, m, source, target)
	if len(leases) != 2 {
		t.Fatalf("batch Create() returned %d leases, want 2", len(leases))
	}
	if leases[0].Request.Path != source.Path || leases[1].Request.Path != target.Path {
		t.Fatalf("batch leases are out of order: %+v", leases)
	}

	mustConflict(t, m, source)
	mustConflict(t, m, target)

	for _, lease := range leases {
		mustUnlock(t, m, lease)
	}

	again := mustCreateBatch(t, m, source, target)
	for _, lease := range again {
		mustUnlock(t, m, lease)
	}
}

func TestCreateBatchMergesDuplicatePath(t *testing.T) {
	m := NewManager()
	user := Namespace(1)

	leases := mustCreateBatch(t, m,
		Request{Namespace: user, Path: "/docs/a.txt", Scope: Node},
		Request{Namespace: user, Path: "/docs/a.txt/", Scope: Subtree},
		Request{Namespace: user, Path: "//docs//a.txt", Scope: Node},
	)
	if len(leases) != 1 {
		t.Fatalf("batch Create() returned %d leases, want 1 merged lease", len(leases))
	}
	lease := leases[0]
	if lease.Request.Path != "/docs/a.txt" {
		t.Fatalf("merged path = %q, want /docs/a.txt", lease.Request.Path)
	}
	if lease.Request.Scope != Subtree {
		t.Fatalf("merged scope = %s, want subtree", lease.Request.Scope)
	}

	// 合并为子树锁后，后代也被覆盖。
	mustConflict(t, m, Request{Namespace: user, Path: "/docs/a.txt/child", Scope: Node})
	mustUnlock(t, m, lease)
}

func TestCreateBatchKeepsNamespacesSeparate(t *testing.T) {
	m := NewManager()

	leases := mustCreateBatch(t, m,
		Request{Namespace: Namespace(1), Path: "/docs", Scope: Subtree},
		Request{Namespace: Namespace(2), Path: "/docs", Scope: Subtree},
	)
	if len(leases) != 2 {
		t.Fatalf("batch Create() returned %d leases, want 2", len(leases))
	}

	for _, lease := range leases {
		mustUnlock(t, m, lease)
	}
}

func TestCreateBatchRejectsInternalOverlap(t *testing.T) {
	m := NewManager()
	user := Namespace(1)
	node := Request{Namespace: user, Path: "/docs/a.txt", Scope: Node}
	subtree := Request{Namespace: user, Path: "/docs", Scope: Subtree}

	leases, err := m.Create(subtree, node)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("batch Create() error = %v, want ErrConflict", err)
	}
	if len(leases) != 0 {
		t.Fatalf("overlapping batch returned %d leases, want 0", len(leases))
	}

	// 两个申请都没有被写入：分别单独申请都能成功。
	nodeLease := mustCreate(t, m, node)
	mustUnlock(t, m, nodeLease)
	subtreeLease := mustCreate(t, m, subtree)
	mustUnlock(t, m, subtreeLease)
}

func TestCreateBatchAtomicOnInvalidRequest(t *testing.T) {
	m := NewManager()
	user := Namespace(1)
	valid := Request{Namespace: user, Path: "/docs/report.txt", Scope: Node}
	invalid := Request{Namespace: user, Path: "archive/report.txt", Scope: Node}

	leases, err := m.Create(valid, invalid)
	if !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("batch Create() error = %v, want ErrInvalidPath", err)
	}
	if len(leases) != 0 {
		t.Fatalf("invalid batch returned %d leases, want 0", len(leases))
	}

	lease := mustCreate(t, m, valid)
	mustUnlock(t, m, lease)
}

func TestCreateBatchMergesExpiry(t *testing.T) {
	now := time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)
	m := NewManager()
	m.now = func() time.Time { return now }
	user := Namespace(1)
	later := now.Add(time.Hour)

	leases := mustCreateBatch(t, m,
		Request{
			Namespace: user,
			Path:      "/docs/a.txt",
			Scope:     Node,
			ExpiresAt: now.Add(time.Minute),
		},
		Request{
			Namespace: user,
			Path:      "/docs/a.txt",
			Scope:     Subtree,
			ExpiresAt: later,
		},
	)
	if len(leases) != 1 {
		t.Fatalf("batch Create() returned %d leases, want 1", len(leases))
	}
	if !leases[0].Request.ExpiresAt.Equal(later) {
		t.Fatalf("merged expiry = %s, want %s", leases[0].Request.ExpiresAt, later)
	}

	now = later.Add(-time.Minute)
	mustConflict(t, m, Request{Namespace: user, Path: "/docs/a.txt/child", Scope: Node})

	now = later.Add(time.Minute)
	if err := m.Unlock(user, leases[0].Token); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("Unlock(expired token) error = %v, want ErrNoSuchLock", err)
	}
}

func TestConfirmChecksNamespaceAndCoverage(t *testing.T) {
	m := NewManager()
	user1 := Namespace(1)
	user2 := Namespace(2)
	node := mustCreate(t, m, Request{
		Namespace: user1,
		Path:      "/node.txt",
		Scope:     Node,
	})
	subtree := mustCreate(t, m, Request{
		Namespace: user1,
		Path:      "/docs",
		Scope:     Subtree,
	})

	release := mustConfirm(t, m, user1, "/node.txt", node.Token)
	release()

	// release 只结束这一次短时保护，不会释放 Lease。
	release = mustConfirm(t, m, user1, "/node.txt", node.Token)
	release()

	if _, err := m.Confirm(user1, "/node.txt/child", node.Token); !errors.Is(err, ErrNotCovered) {
		t.Fatalf("node Confirm(child) error = %v, want ErrNotCovered", err)
	}

	release = mustConfirm(t, m, user1, "/docs/a.txt", subtree.Token)
	release()

	if _, err := m.Confirm(user2, "/node.txt", node.Token); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("wrong namespace error = %v, want ErrNoSuchLock", err)
	}
	if _, err := m.Confirm(user1, "/node.txt", "missing"); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("unknown token error = %v, want ErrNoSuchLock", err)
	}
	if _, err := m.Confirm(user1, "relative", node.Token); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("invalid path error = %v, want ErrInvalidPath", err)
	}

	mustUnlock(t, m, node)
	mustUnlock(t, m, subtree)
}

func TestExpiredLeaseKeepsProtectionUntilRelease(t *testing.T) {
	now := time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)
	m := NewManager()
	m.now = func() time.Time { return now }
	user := Namespace(1)

	lease := mustCreate(t, m, Request{
		Namespace: user,
		Path:      "/docs",
		Scope:     Subtree,
		ExpiresAt: now.Add(time.Hour),
	})
	release := mustConfirm(t, m, user, "/docs/a.txt", lease.Token)

	now = now.Add(2 * time.Hour)

	// 过期后不能再次 Confirm，也不能通过 Refresh 复活。
	if _, err := m.Confirm(user, "/docs/b.txt", lease.Token); !errors.Is(err, ErrExpired) {
		t.Fatalf("Confirm(expired) error = %v, want ErrExpired", err)
	}
	if err := m.Refresh(user, lease.Token, now.Add(time.Hour)); !errors.Is(err, ErrExpired) {
		t.Fatalf("Refresh(expired) error = %v, want ErrExpired", err)
	}

	// 保护区还没有退出，冲突资源仍然不能抢走占位。
	conflicting := Request{
		Namespace: user,
		Path:      "/docs/new.txt",
		Scope:     Node,
		ExpiresAt: now.Add(time.Hour),
	}
	if _, err := m.Create(conflicting); !errors.Is(err, ErrConflict) {
		t.Fatalf("Create(conflicting) error = %v, want ErrConflict", err)
	}

	release()

	// 最后一个保护区退出后，过期锁才被真正移除。
	newLease := mustCreate(t, m, conflicting)
	if _, err := m.Confirm(user, "/docs/a.txt", lease.Token); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("Confirm(after release) error = %v, want ErrNoSuchLock", err)
	}
	mustUnlock(t, m, newLease)
}

func TestConfirmMultipleHolds(t *testing.T) {
	m := NewManager()
	user := Namespace(1)
	lease := mustCreate(t, m, Request{
		Namespace: user,
		Path:      "/docs/a.txt",
		Scope:     Node,
	})

	release1 := mustConfirm(t, m, user, "/docs/a.txt", lease.Token)
	release2 := mustConfirm(t, m, user, "/docs/a.txt", lease.Token)

	// 重复释放只扣减一次；此时 release2 仍然保护着资源。
	release1()
	release1()

	// Unlock 立即禁止新的 Confirm，但因为有 holds，冲突占位必须保留。
	if err := m.Unlock(user, lease.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Confirm(user, "/docs/a.txt", lease.Token); !errors.Is(err, ErrClosing) {
		t.Fatalf("Confirm(closing) error = %v, want ErrClosing", err)
	}
	if err := m.Refresh(user, lease.Token, time.Now().Add(time.Hour)); !errors.Is(err, ErrClosing) {
		t.Fatalf("Refresh(closing) error = %v, want ErrClosing", err)
	}
	if _, err := m.Create(Request{
		Namespace: user,
		Path:      "/docs/a.txt",
		Scope:     Node,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("Create(during last hold) error = %v, want ErrConflict", err)
	}

	// 最后一个保护区退出后，冲突资源才能申请。
	release2()
	release2()

	newLease := mustCreate(t, m, Request{
		Namespace: user,
		Path:      "/docs/a.txt",
		Scope:     Node,
	})
	mustUnlock(t, m, newLease)
}

func TestRefreshExtendsLease(t *testing.T) {
	now := time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)
	m := NewManager()
	m.now = func() time.Time { return now }
	user := Namespace(1)

	lease := mustCreate(t, m, Request{
		Namespace: user,
		Path:      "/docs/a.txt",
		Scope:     Node,
		ExpiresAt: now.Add(time.Minute),
	})

	now = now.Add(30 * time.Second)
	extended := now.Add(time.Hour)
	if err := m.Refresh(user, lease.Token, extended); err != nil {
		t.Fatal(err)
	}

	// 越过原来的过期时间，但仍然在新过期时间之前。
	now = now.Add(2 * time.Minute)
	release := mustConfirm(t, m, user, "/docs/a.txt", lease.Token)
	release()

	if err := m.Refresh(user, lease.Token, now); !errors.Is(err, ErrInvalidExpiry) {
		t.Fatalf("Refresh(past) error = %v, want ErrInvalidExpiry", err)
	}
	if err := m.Refresh(Namespace(2), lease.Token, now.Add(time.Hour)); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("Refresh(wrong namespace) error = %v, want ErrNoSuchLock", err)
	}
	if err := m.Refresh(user, "missing", now.Add(time.Hour)); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("Refresh(unknown token) error = %v, want ErrNoSuchLock", err)
	}

	// 零值表示改为永不过期。
	if err := m.Refresh(user, lease.Token, time.Time{}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(100 * time.Hour)
	release = mustConfirm(t, m, user, "/docs/a.txt", lease.Token)
	release()

	mustUnlock(t, m, lease)
}

func TestUnlockRequiresNamespace(t *testing.T) {
	m := NewManager()
	user1 := Namespace(1)
	user2 := Namespace(2)

	lease := mustCreate(t, m, Request{
		Namespace: user1,
		Path:      "/docs/a.txt",
		Scope:     Node,
	})

	if err := m.Unlock(user2, lease.Token); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("Unlock(wrong namespace) error = %v, want ErrNoSuchLock", err)
	}

	// 命名空间不匹配时锁不受影响。
	release := mustConfirm(t, m, user1, "/docs/a.txt", lease.Token)
	release()

	if err := m.Unlock("", lease.Token); !errors.Is(err, ErrInvalidNamespace) {
		t.Fatalf("Unlock(empty namespace) error = %v, want ErrInvalidNamespace", err)
	}
	mustUnlock(t, m, lease)

	// 有一个令牌无效时，整批都不改变。
	second := mustCreate(t, m, Request{
		Namespace: user1,
		Path:      "/docs/b.txt",
		Scope:     Node,
	})
	if err := m.Unlock(user1, second.Token, "missing"); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("Unlock(mixed batch) error = %v, want ErrNoSuchLock", err)
	}
	release = mustConfirm(t, m, user1, "/docs/b.txt", second.Token)
	release()
	mustUnlock(t, m, second)
}

// blockingClock 在每次读取时先记下当时的时间，然后等待 unblock。
// 用来模拟“请求还在等 mu 时，时间已经越过过期点”。
func blockingClock(now *time.Time) (func() time.Time, <-chan struct{}, chan struct{}) {
	called := make(chan struct{}, 1)
	unblock := make(chan struct{})

	clock := func() time.Time {
		read := *now
		select {
		case called <- struct{}{}:
		default:
		}
		<-unblock
		return read
	}
	return clock, called, unblock
}

func TestLockedTimeUsedForExpiryChecks(t *testing.T) {
	user := Namespace(1)
	base := time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)
	const lockWait = 200 * time.Millisecond

	t.Run("confirm", func(t *testing.T) {
		now := base
		m := NewManager()
		m.now = func() time.Time { return now }
		lease := mustCreate(t, m, Request{
			Namespace: user,
			Path:      "/docs/a.txt",
			Scope:     Node,
			ExpiresAt: base.Add(time.Minute),
		})

		clock, called, unblock := blockingClock(&now)
		m.now = clock

		// 先占住 mu，让 Confirm 在拿到它之前跨过过期时间。
		m.mu.Lock()
		result := make(chan error, 1)
		go func() {
			_, err := m.Confirm(user, "/docs/a.txt", lease.Token)
			result <- err
		}()

		select {
		case <-called:
			// 旧时序会在这里就读取时钟；新时序还在等 mu。
		case <-time.After(lockWait):
		}

		now = base.Add(2 * time.Minute)
		close(unblock)
		m.mu.Unlock()

		if err := <-result; !errors.Is(err, ErrExpired) {
			t.Fatalf("Confirm() error = %v, want ErrExpired", err)
		}
	})

	t.Run("refresh", func(t *testing.T) {
		now := base
		m := NewManager()
		m.now = func() time.Time { return now }
		lease := mustCreate(t, m, Request{
			Namespace: user,
			Path:      "/docs/a.txt",
			Scope:     Node,
			ExpiresAt: base.Add(time.Minute),
		})

		clock, called, unblock := blockingClock(&now)
		m.now = clock

		m.mu.Lock()
		result := make(chan error, 1)
		go func() {
			result <- m.Refresh(user, lease.Token, base.Add(time.Hour))
		}()

		select {
		case <-called:
		case <-time.After(lockWait):
		}

		now = base.Add(2 * time.Minute)
		close(unblock)
		m.mu.Unlock()

		if err := <-result; !errors.Is(err, ErrExpired) {
			t.Fatalf("Refresh() error = %v, want ErrExpired", err)
		}
	})

	t.Run("release", func(t *testing.T) {
		now := base
		m := NewManager()
		m.now = func() time.Time { return now }
		lease := mustCreate(t, m, Request{
			Namespace: user,
			Path:      "/docs/a.txt",
			Scope:     Node,
			ExpiresAt: base.Add(time.Minute),
		})
		release := mustConfirm(t, m, user, "/docs/a.txt", lease.Token)

		clock, called, unblock := blockingClock(&now)
		m.now = clock

		m.mu.Lock()
		done := make(chan struct{})
		go func() {
			release()
			close(done)
		}()

		select {
		case <-called:
		case <-time.After(lockWait):
		}

		now = base.Add(2 * time.Minute)
		close(unblock)
		m.mu.Unlock()
		<-done

		// release 在锁内看到锁已过期，最后一个保护区退出时应直接移除。
		m.now = func() time.Time { return now }
		if _, err := m.Confirm(user, "/docs/a.txt", lease.Token); !errors.Is(err, ErrNoSuchLock) {
			t.Fatalf("Confirm(after expired release) error = %v, want ErrNoSuchLock", err)
		}
	})
}

func TestCreateRechecksExpiryAfterMutexWait(t *testing.T) {
	base := time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)
	now := base
	m := NewManager()

	firstRead := make(chan struct{})
	first := true
	m.now = func() time.Time {
		if first {
			first = false
			close(firstRead)
			return base
		}
		return now
	}

	request := Request{
		Namespace: Namespace(1),
		Path:      "/docs/a.txt",
		Scope:     Node,
		ExpiresAt: base.Add(time.Minute),
	}

	m.mu.Lock()
	result := make(chan error, 1)
	go func() {
		_, err := m.Create(request)
		result <- err
	}()

	// 等 Create 用旧时间完成预校验并开始等待 mu。
	<-firstRead
	now = base.Add(2 * time.Minute)
	m.mu.Unlock()

	if err := <-result; !errors.Is(err, ErrInvalidExpiry) {
		t.Fatalf("Create() error = %v, want ErrInvalidExpiry", err)
	}

	// 失败的批次不能留下任何锁。
	m.now = func() time.Time { return now }
	lease := mustCreate(t, m, Request{
		Namespace: Namespace(1),
		Path:      "/docs/a.txt",
		Scope:     Node,
	})
	mustUnlock(t, m, lease)
}

func TestConcurrentCreateOnlyOneWins(t *testing.T) {
	m := NewManager()
	req := Request{
		Namespace: Namespace(1),
		Path:      "/docs/a.txt",
		Scope:     Node,
	}

	const goroutines = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var acquired []Lease

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			leases, err := m.Create(req)
			if err != nil {
				if !errors.Is(err, ErrConflict) {
					t.Errorf("Create() error = %v, want nil or ErrConflict", err)
				}
				return
			}
			mu.Lock()
			acquired = append(acquired, leases...)
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	if len(acquired) != 1 {
		t.Fatalf("%d goroutines acquired the same lock, want 1", len(acquired))
	}

	mustUnlock(t, m, acquired[0])
	again := mustCreate(t, m, req)
	mustUnlock(t, m, again)
}
