package file

import (
	"context"
	"sync"
)

// activeUpload 是一次正在进行的会话内容写入。
//
// cancel 通知传输停止；done 在写入和收尾都退出、并且从登记表移除后关闭。
// 取消方只依赖 done 判断“现在能不能安全清理”，不直接碰存储。
type activeUpload struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// uploadController 是单进程共享的活跃上传登记表。
//
// 它只保护“哪个会话正在写”这一小段内存状态。文件传输和数据库操作
// 都在锁外执行，绝不能拿着全局锁去读写内容。
type uploadController struct {
	mu     sync.Mutex
	active map[string]*activeUpload
}

func newUploadController() *uploadController {
	return &uploadController{active: make(map[string]*activeUpload)}
}

// begin 登记一个会话的写入请求，并返回可被取消的子 context。
//
// 同一个会话只允许一个活跃写入：重复登记返回 ErrUploadSessionConflict。
// 调用方必须在 pending → receiving 的数据库抢占之前调用它，
// 这样取消方不会在“数据库已经 receiving、但请求还没登记”的窗口里误判。
//
// 返回的 end 必须由调用方在退出时执行，且只执行一次。
func (c *uploadController) begin(parent context.Context, sessionID string) (context.Context, func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.active[sessionID]; ok {
		return nil, nil, ErrUploadSessionConflict
	}

	ctx, cancel := context.WithCancel(parent)
	entry := &activeUpload{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	c.active[sessionID] = entry

	var once sync.Once
	end := func() {
		once.Do(func() {
			// 先取消本次传输，再移除登记，最后关闭 done。
			// 只要 done 关闭，取消方就能确定写入已经退出。
			cancel()
			c.mu.Lock()
			if c.active[sessionID] == entry {
				delete(c.active, sessionID)
			}
			c.mu.Unlock()
			close(entry.done)
		})
	}
	return ctx, end, nil
}

// cancel 通知会话的活跃写入停止，并返回一个在它退出时关闭的 channel。
// 没有活跃写入时返回一个已经关闭的 channel，调用方可以立即继续清理。
func (c *uploadController) cancel(sessionID string) <-chan struct{} {
	c.mu.Lock()
	entry := c.active[sessionID]
	if entry != nil {
		entry.cancel()
	}
	c.mu.Unlock()

	if entry == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return entry.done
}

// isActive 判断会话当前是否有活跃写入。
func (c *uploadController) isActive(sessionID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.active[sessionID]
	return ok
}
