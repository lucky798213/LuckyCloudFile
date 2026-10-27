// Package pathlock 管理用户文件树里的纯内存虚拟路径锁。
//
// 锁按命名空间隔离，命名空间由用户 ID 生成（user:1、user:2）。
// 路径是用户文件树里的绝对虚拟路径，不是磁盘路径，也不是 Entity.StorageKey。
//
// Node 只锁指定节点，不覆盖它的后代；Subtree 锁节点及全部后代。
// 因此移动、改名或删除目录时，必须申请目录的子树锁。
//
// 本包只做内存操作，不依赖 HTTP、数据库或存储驱动。
package pathlock

import (
	"errors"
	"fmt"
	"time"
)

// Scope 是锁的粒度。
type Scope uint8

const (
	// Node 只锁指定节点，不覆盖它的后代。
	Node Scope = iota
	// Subtree 锁指定节点及其全部后代。
	Subtree
)

// String 返回 Scope 的可读名字。
func (s Scope) String() string {
	switch s {
	case Node:
		return "node"
	case Subtree:
		return "subtree"
	default:
		return "unknown"
	}
}

// Request 是一次锁申请。
type Request struct {
	// Namespace 隔离不同用户，用 Namespace 生成。
	Namespace string

	// Path 是用户文件树里的绝对虚拟路径，例如 /docs/a.txt。
	Path string

	// Scope 决定这把锁是否覆盖后代。
	Scope Scope

	// ExpiresAt 是过期时间。零值表示永不过期。
	ExpiresAt time.Time
}

// Lease 是申请成功后返回的锁。Token 由服务端生成，调用方只需保存它。
type Lease struct {
	Token   string
	Request Request
}

var (
	// ErrInvalidNamespace 表示没有提供命名空间。
	ErrInvalidNamespace = errors.New("pathlock: namespace is required")

	// ErrInvalidPath 表示路径不是合法的绝对虚拟路径。
	ErrInvalidPath = errors.New("pathlock: invalid virtual path")

	// ErrInvalidScope 表示锁的粒度未知。
	ErrInvalidScope = errors.New("pathlock: invalid scope")

	// ErrInvalidExpiry 表示申请时过期时间已经不在未来。
	ErrInvalidExpiry = errors.New("pathlock: expiration is not in the future")

	// ErrConflict 表示申请与同一命名空间里已有的锁冲突。
	ErrConflict = errors.New("pathlock: lock conflicts with a held lock")

	// ErrNoSuchLock 表示令牌不存在，或者对应的锁已经过期、已经解锁。
	ErrNoSuchLock = errors.New("pathlock: no such lock")
)

// Namespace 返回用户对应的锁命名空间。
func Namespace(userID int) string {
	return fmt.Sprintf("user:%d", userID)
}
