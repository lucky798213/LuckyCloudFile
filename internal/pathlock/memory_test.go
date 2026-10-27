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
				if err := m.Unlock(held.Token); err != nil {
					t.Fatal(err)
				}
				retry := mustCreate(t, m, tt.request)
				if err := m.Unlock(retry.Token); err != nil {
					t.Fatal(err)
				}
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
			if err := m.Unlock(lease.Token); err != nil {
				t.Fatal(err)
			}
			again := mustCreate(t, m, tt.request)
			if err := m.Unlock(again.Token); err != nil {
				t.Fatal(err)
			}
			if err := m.Unlock(held.Token); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestUnlockUnknownToken(t *testing.T) {
	m := NewManager()

	if err := m.Unlock("does-not-exist"); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("Unlock() error = %v, want ErrNoSuchLock", err)
	}

	lease := mustCreate(t, m, Request{
		Namespace: Namespace(1),
		Path:      "/docs/a.txt",
		Scope:     Node,
	})
	if err := m.Unlock(lease.Token); err != nil {
		t.Fatal(err)
	}
	if err := m.Unlock(lease.Token); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("second Unlock() error = %v, want ErrNoSuchLock", err)
	}
}

func TestOldTokenCannotUnlockNewLock(t *testing.T) {
	m := NewManager()
	req := Request{
		Namespace: Namespace(1),
		Path:      "/docs/a.txt",
		Scope:     Node,
	}

	first := mustCreate(t, m, req)
	if err := m.Unlock(first.Token); err != nil {
		t.Fatal(err)
	}

	second := mustCreate(t, m, req)
	if second.Token == first.Token {
		t.Fatal("re-created lock reused the old token")
	}

	if err := m.Unlock(first.Token); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("Unlock(old token) error = %v, want ErrNoSuchLock", err)
	}

	// 旧令牌没有影响新锁，新锁仍然有效。
	mustConflict(t, m, req)
	if err := m.Unlock(second.Token); err != nil {
		t.Fatal(err)
	}
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

	if err := m.Unlock(held.Token); err != nil {
		t.Fatal(err)
	}
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
	if err := m.Unlock(lease.Token); err != nil {
		t.Fatal(err)
	}
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
	if err := m.Unlock(expiring.Token); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("Unlock(expired token) error = %v, want ErrNoSuchLock", err)
	}

	// 过期锁不再阻塞；零值 ExpiresAt 的锁不会被清理。
	req.ExpiresAt = time.Time{}
	fresh := mustCreate(t, m, req)
	if err := m.Unlock(fresh.Token); err != nil {
		t.Fatal(err)
	}

	forever := mustCreate(t, m, Request{
		Namespace: Namespace(1),
		Path:      "/forever",
		Scope:     Node,
	})
	now = now.Add(100 * 365 * 24 * time.Hour)
	mustConflict(t, m, forever.Request)
	if err := m.Unlock(forever.Token); err != nil {
		t.Fatal(err)
	}
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

	if err := m.Unlock(sourceLease.Token); err != nil {
		t.Fatal(err)
	}
	if err := m.Unlock(targetLease.Token); err != nil {
		t.Fatal(err)
	}
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
		if err := m.Unlock(lease.Token); err != nil {
			t.Fatal(err)
		}
	}

	again := mustCreateBatch(t, m, source, target)
	for _, lease := range again {
		if err := m.Unlock(lease.Token); err != nil {
			t.Fatal(err)
		}
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
	if err := m.Unlock(lease.Token); err != nil {
		t.Fatal(err)
	}
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
		if err := m.Unlock(lease.Token); err != nil {
			t.Fatal(err)
		}
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
	if err := m.Unlock(nodeLease.Token); err != nil {
		t.Fatal(err)
	}
	subtreeLease := mustCreate(t, m, subtree)
	if err := m.Unlock(subtreeLease.Token); err != nil {
		t.Fatal(err)
	}
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
	if err := m.Unlock(lease.Token); err != nil {
		t.Fatal(err)
	}
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
	if err := m.Unlock(leases[0].Token); !errors.Is(err, ErrNoSuchLock) {
		t.Fatalf("Unlock(expired token) error = %v, want ErrNoSuchLock", err)
	}
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

	if err := m.Unlock(acquired[0].Token); err != nil {
		t.Fatal(err)
	}
	again := mustCreate(t, m, req)
	if err := m.Unlock(again.Token); err != nil {
		t.Fatal(err)
	}
}
