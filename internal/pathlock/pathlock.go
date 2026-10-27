// Package pathlock 管理用户文件树里的虚拟路径锁。
//
// 锁按命名空间隔离，命名空间由用户 ID 生成（user:1、user:2）。
// 路径是用户文件树里的绝对虚拟路径，不是磁盘路径，也不是 Entity.StorageKey。
//
// 节点锁只锁指定节点，不覆盖它的后代；子树锁覆盖节点本身和全部后代。
// 因此移动、改名或删除目录时，必须申请目录的子树锁。
package pathlock

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
)

// Kind 是锁的粒度。
type Kind int

const (
	// Node 只锁指定节点，不覆盖它的后代。
	Node Kind = iota
	// Subtree 锁指定节点及其全部后代。
	Subtree
)

var (
	// ErrInvalidNamespace 表示没有提供命名空间。
	ErrInvalidNamespace = errors.New("pathlock: namespace is required")

	// ErrInvalidPath 表示路径不是合法的绝对虚拟路径。
	ErrInvalidPath = errors.New("pathlock: invalid virtual path")

	// ErrInvalidKind 表示锁的粒度未知。
	ErrInvalidKind = errors.New("pathlock: invalid lock kind")

	// ErrConflict 表示申请与同一命名空间里已有的锁冲突。
	ErrConflict = errors.New("pathlock: lock conflicts with a held lock")
)

// Namespace 返回用户对应的锁命名空间。
func Namespace(userID int) string {
	return fmt.Sprintf("user:%d", userID)
}

// Manager 保存当前进程内持有的锁，零值可以直接使用。
type Manager struct {
	mu    sync.Mutex
	locks []*lock
}

type lock struct {
	namespace string
	path      string
	kind      Kind
}

// Acquire 在 namespace 中申请 path 上的锁。
//
// 成功返回释放函数；释放函数可以重复调用。与已有锁冲突时返回 ErrConflict，
// 且不会保存这次申请。
func (m *Manager) Acquire(namespace, p string, kind Kind) (func(), error) {
	if namespace == "" {
		return nil, ErrInvalidNamespace
	}
	if kind != Node && kind != Subtree {
		return nil, ErrInvalidKind
	}

	normalized, err := normalizePath(p)
	if err != nil {
		return nil, err
	}
	requested := &lock{namespace: namespace, path: normalized, kind: kind}

	m.mu.Lock()
	for _, held := range m.locks {
		if !conflicts(held, requested) {
			continue
		}
		m.mu.Unlock()
		return nil, fmt.Errorf(
			"%w: %s %s is covered by %s %s",
			ErrConflict, requested.kind, requested.path, held.kind, held.path,
		)
	}
	m.locks = append(m.locks, requested)
	m.mu.Unlock()

	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for i, held := range m.locks {
			if held != requested {
				continue
			}
			m.locks = append(m.locks[:i], m.locks[i+1:]...)
			return
		}
	}, nil
}

// conflicts 判断两把锁是否冲突。调用方保证两者路径都已经规范化。
func conflicts(held, requested *lock) bool {
	if held.namespace != requested.namespace {
		return false
	}
	if held.path == requested.path {
		return true
	}
	if held.kind == Subtree && covers(held.path, requested.path) {
		return true
	}
	return requested.kind == Subtree && covers(requested.path, held.path)
}

// covers 判断 root 是否覆盖 target。root 是已经规范化的绝对路径。
func covers(root, target string) bool {
	return root == "/" ||
		target == root ||
		strings.HasPrefix(target, root+"/")
}

// normalizePath 先拒绝非法输入，再用 path 包规范化。
// 这里使用 path 而不是 filepath，因为虚拟路径不依赖操作系统的分隔符。
func normalizePath(p string) (string, error) {
	if p == "" || p[0] != '/' || strings.IndexByte(p, 0) >= 0 {
		return "", fmt.Errorf("%w: %q", ErrInvalidPath, p)
	}

	// 先拒绝 "." 和 ".."，避免它们被 Clean 折叠成另一条路径。
	for _, element := range strings.Split(p, "/") {
		if element == "." || element == ".." {
			return "", fmt.Errorf("%w: %q", ErrInvalidPath, p)
		}
	}

	return path.Clean(p), nil
}

func (k Kind) String() string {
	switch k {
	case Node:
		return "node"
	case Subtree:
		return "subtree"
	default:
		return "unknown"
	}
}
