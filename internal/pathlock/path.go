package pathlock

import (
	"fmt"
	"path"
	"strings"
)

// conflicts 判断两把锁是否冲突。调用方保证路径都已经规范化。
func conflicts(held, requested Request) bool {
	if held.Namespace != requested.Namespace {
		return false
	}
	if held.Path == requested.Path {
		return true
	}
	if held.Scope == Subtree && covers(held.Path, requested.Path) {
		return true
	}
	return requested.Scope == Subtree && covers(requested.Path, held.Path)
}

// requestCovers 判断一把锁是否覆盖 target。
// Node 只覆盖完全相同的路径；Subtree 还覆盖全部后代。
func requestCovers(req Request, target string) bool {
	return req.Path == target ||
		(req.Scope == Subtree && covers(req.Path, target))
}

// covers 判断 root 是否覆盖 target。两者都是已经规范化的绝对路径。
//
// 不能写成 strings.HasPrefix(target, root)：那样会把 /a/bb 误判为 /a/b 的后代。
func covers(root, target string) bool {
	return root == "/" ||
		target == root ||
		strings.HasPrefix(target, root+"/")
}

// normalizePath 先拒绝非法输入，再用 path 包规范化。
// 使用 path 而不是 filepath，因为虚拟路径不依赖操作系统的分隔符。
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
