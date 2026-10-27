// Package failpoint 提供受控的崩溃点，只用于恢复测试和故障演示。
//
// 生产运行不设置 CLOUDREVE_FAILPOINT 时，Hit 是空操作。测试进程设置它
// 之后，命中点会写一个标记文件并永久阻塞；外部测试随后向进程发送
// SIGKILL，确保 defer 和正常关闭逻辑都不会执行。这正是“杀进程”与
// “取消 context”的区别。
package failpoint

import (
	"fmt"
	"os"
	"time"
)

// Hit 在 CLOUDREVE_FAILPOINT 等于 name 时记录标记并阻塞进程。
//
// 标记路径来自 CLOUDREVE_FAILPOINT_MARKER；为空时只阻塞，不写标记，
// 便于手工演示。正常启动不设置这两个环境变量，因此没有运行时影响。
func Hit(name string) {
	if os.Getenv("CLOUDREVE_FAILPOINT") != name {
		return
	}

	if marker := os.Getenv("CLOUDREVE_FAILPOINT_MARKER"); marker != "" {
		if err := os.WriteFile(marker, []byte(name+"\n"), 0o600); err != nil {
			fmt.Fprintf(os.Stderr, "failpoint %s: write marker: %v\n", name, err)
		}
	}

	// 不使用 select{}，避免在没有其他协程运行时被 runtime 判定为
	// 全局死锁而直接退出。外部测试会发送 SIGKILL。
	for {
		time.Sleep(time.Hour)
	}
}
