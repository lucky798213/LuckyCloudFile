package pathlock

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func mustCreate(t *testing.T, m *Manager, req Request) Lease {
	t.Helper()

	lease, err := m.Create(req)
	if err != nil {
		t.Fatalf("Create(%+v) error = %v", req, err)
	}
	return lease
}

func mustConflict(t *testing.T, m *Manager, req Request) {
	t.Helper()

	if _, err := m.Create(req); !errors.Is(err, ErrConflict) {
		t.Fatalf("Create(%+v) error = %v, want ErrConflict", req, err)
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

			lease, err := m.Create(tt.request)
			if tt.conflict {
				if !errors.Is(err, ErrConflict) {
					t.Fatalf("Create() error = %v, want ErrConflict", err)
				}
				if lease.Token != "" {
					t.Fatalf("conflicting Create() returned token %q", lease.Token)
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
	var leases []Lease

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			lease, err := m.Create(req)
			if err != nil {
				if !errors.Is(err, ErrConflict) {
					t.Errorf("Create() error = %v, want nil or ErrConflict", err)
				}
				return
			}
			mu.Lock()
			leases = append(leases, lease)
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	if len(leases) != 1 {
		t.Fatalf("%d goroutines acquired the same lock, want 1", len(leases))
	}

	if err := m.Unlock(leases[0].Token); err != nil {
		t.Fatal(err)
	}
	again := mustCreate(t, m, req)
	if err := m.Unlock(again.Token); err != nil {
		t.Fatal(err)
	}
}
