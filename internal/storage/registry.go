package storage

import (
	"errors"
	"fmt"
)

// ErrPolicyNotFound 表示请求的策略没有对应的 Store。
// 调用方必须把它当作明确失败，不能回退到“第一个”或“默认”位置。
var ErrPolicyNotFound = errors.New("storage policy not found")

// Registry 把数据库里的策略 ID 映射到具体的 Store。
//
// 它只做这一件事：不查数据库、不判断用户权限、不选默认策略。
// 启动时由 app 按照数据库中的策略逐条注册，之后业务层只拿 ID 来取 Store。
type Registry struct {
	stores map[int]Store
}

// NewRegistry 创建一个空 Registry。
func NewRegistry() *Registry {
	return &Registry{stores: make(map[int]Store)}
}

// Register 把策略 ID 绑定到一个 Store。
//
// 同一个 ID 重复注册会返回错误，避免后注册的实现悄悄覆盖前一个位置。
func (r *Registry) Register(policyID int, store Store) error {
	if policyID <= 0 {
		return fmt.Errorf("storage policy id must be positive, got %d", policyID)
	}
	if store == nil {
		return fmt.Errorf("storage policy %d has a nil store", policyID)
	}
	if r.stores == nil {
		// 允许直接使用零值 Registry，不必强制先调 NewRegistry。
		r.stores = make(map[int]Store)
	}
	if _, exists := r.stores[policyID]; exists {
		return fmt.Errorf("storage policy %d is already registered", policyID)
	}
	r.stores[policyID] = store
	return nil
}

// Get 返回策略对应的 Store。
//
// 策略不存在时返回 ErrPolicyNotFound，绝不偷偷换成别的 Store：
// 旧内容必须从它实际写入的位置读取，猜错位置比报错更危险。
func (r *Registry) Get(policyID int) (Store, error) {
	store, ok := r.stores[policyID]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrPolicyNotFound, policyID)
	}
	return store, nil
}
