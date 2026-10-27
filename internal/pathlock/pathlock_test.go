package pathlock

import (
	"errors"
	"sync"
	"testing"
)

func TestNamespace(t *testing.T) {
	if got := Namespace(1); got != "user:1" {
		t.Fatalf("Namespace(1) = %q, want user:1", got)
	}
	if got := Namespace(2); got != "user:2" {
		t.Fatalf("Namespace(2) = %q, want user:2", got)
	}
}

// TestAcquireTable 覆盖需求里的冲突表，路径全部是用户文件树里的虚拟路径。
func TestAcquireTable(t *testing.T) {
	user1 := Namespace(1)
	user2 := Namespace(2)

	tests := []struct {
		name     string
		held     lock
		request  lock
		conflict bool
	}{
		{
			name:     "same node",
			held:     lock{user1, "/docs/a.txt", Node},
			request:  lock{user1, "/docs/a.txt", Node},
			conflict: true,
		},
		{
			name:    "sibling nodes",
			held:    lock{user1, "/docs/a.txt", Node},
			request: lock{user1, "/docs/b.txt", Node},
		},
		{
			name:     "held subtree covers node",
			held:     lock{user1, "/docs", Subtree},
			request:  lock{user1, "/docs/a.txt", Node},
			conflict: true,
		},
		{
			name:     "requested subtree covers node",
			held:     lock{user1, "/docs/a.txt", Node},
			request:  lock{user1, "/docs", Subtree},
			conflict: true,
		},
		{
			name:    "node does not cover child",
			held:    lock{user1, "/docs", Node},
			request: lock{user1, "/docs/a.txt", Node},
		},
		{
			name:    "prefix is not a path boundary",
			held:    lock{user1, "/a/b", Subtree},
			request: lock{user1, "/a/bb/file", Node},
		},
		{
			name:    "different namespaces",
			held:    lock{user1, "/docs", Node},
			request: lock{user2, "/docs", Node},
		},
		{
			name:    "different namespaces with subtrees",
			held:    lock{user1, "/docs", Subtree},
			request: lock{user2, "/docs/a.txt", Subtree},
		},
		{
			name:     "root subtree covers everything",
			held:     lock{user1, "/", Subtree},
			request:  lock{user1, "/docs/a.txt", Node},
			conflict: true,
		},
		{
			name:    "root node only covers itself",
			held:    lock{user1, "/", Node},
			request: lock{user1, "/docs", Node},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m Manager
			releaseHeld, err := m.Acquire(tt.held.namespace, tt.held.path, tt.held.kind)
			if err != nil {
				t.Fatalf("acquire held lock: %v", err)
			}

			release, err := m.Acquire(tt.request.namespace, tt.request.path, tt.request.kind)
			if tt.conflict {
				if !errors.Is(err, ErrConflict) {
					t.Fatalf("Acquire() error = %v, want ErrConflict", err)
				}
				if release != nil {
					t.Fatal("conflicting Acquire() returned a release function")
				}

				// 冲突申请不能留下任何锁：释放已有锁后，同一申请应当成功。
				releaseHeld()
				release, err = m.Acquire(tt.request.namespace, tt.request.path, tt.request.kind)
				if err != nil {
					t.Fatalf("rejected lock was stored: %v", err)
				}
				release()
				return
			}

			if err != nil {
				t.Fatalf("Acquire() error = %v, want success", err)
			}

			// 成功后锁确实生效，再申请同一路径会冲突。
			if _, err := m.Acquire(tt.request.namespace, tt.request.path, tt.request.kind); !errors.Is(err, ErrConflict) {
				t.Fatalf("reacquire success lock error = %v, want ErrConflict", err)
			}

			// 释放后可以重新申请。
			release()
			releaseAgain, err := m.Acquire(tt.request.namespace, tt.request.path, tt.request.kind)
			if err != nil {
				t.Fatalf("acquire after release: %v", err)
			}
			releaseAgain()
			releaseHeld()
		})
	}
}

func TestAcquireNormalizesPath(t *testing.T) {
	user := Namespace(1)
	var m Manager

	release, err := m.Acquire(user, "/docs", Subtree)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	for _, p := range []string{"/docs/", "//docs//a.txt", "/docs/a.txt/"} {
		if _, err := m.Acquire(user, p, Node); !errors.Is(err, ErrConflict) {
			t.Fatalf("Acquire(%q) error = %v, want ErrConflict after normalization", p, err)
		}
	}
}

func TestAcquireRejectsInvalidInput(t *testing.T) {
	user := Namespace(1)
	var m Manager

	for _, p := range []string{
		"",
		"docs",
		"docs/a.txt",
		"/docs/../a.txt",
		"/docs/./a.txt",
		"/..",
		"/.",
		"/docs\x00a.txt",
	} {
		if _, err := m.Acquire(user, p, Node); !errors.Is(err, ErrInvalidPath) {
			t.Fatalf("Acquire(%q) error = %v, want ErrInvalidPath", p, err)
		}
	}

	if _, err := m.Acquire("", "/docs", Node); !errors.Is(err, ErrInvalidNamespace) {
		t.Fatalf("empty namespace error = %v, want ErrInvalidNamespace", err)
	}

	if _, err := m.Acquire(user, "/docs", Kind(99)); !errors.Is(err, ErrInvalidKind) {
		t.Fatalf("invalid kind error = %v, want ErrInvalidKind", err)
	}

	// 非法申请不能挡住后来的合法申请。
	release, err := m.Acquire(user, "/docs", Node)
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestAcquireOnlyOneConcurrentWinner(t *testing.T) {
	user := Namespace(1)
	var m Manager

	const goroutines = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var releases []func()

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			release, err := m.Acquire(user, "/docs/a.txt", Node)
			if err != nil {
				if !errors.Is(err, ErrConflict) {
					t.Errorf("Acquire() error = %v, want nil or ErrConflict", err)
				}
				return
			}
			mu.Lock()
			releases = append(releases, release)
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	if len(releases) != 1 {
		t.Fatalf("%d goroutines acquired the same lock, want 1", len(releases))
	}
	releases[0]()
	if release, err := m.Acquire(user, "/docs/a.txt", Node); err != nil {
		t.Fatalf("acquire after release: %v", err)
	} else {
		release()
	}
}

func TestReleaseCanBeCalledTwice(t *testing.T) {
	user := Namespace(1)
	var m Manager

	release, err := m.Acquire(user, "/docs/a.txt", Node)
	if err != nil {
		t.Fatal(err)
	}
	release()
	release()

	other, err := m.Acquire(user, "/docs/b.txt", Node)
	if err != nil {
		t.Fatalf("double release removed another lock: %v", err)
	}
	other()
}
