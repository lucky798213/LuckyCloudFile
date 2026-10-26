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

// finalizeGate 是一次会话级收尾的登记。
//
// done 在收尾结束、门禁释放后关闭。并发的完成请求等待它，而不是
// 拿到同一份账本再做一次 FinalizeChunks。
type finalizeGate struct {
	done chan struct{}
}

// uploadController 是单进程共享的活跃上传登记表。
//
// 它只保护“哪个会话正在写/收尾”这一小段内存状态。文件传输和数据库操作
// 都在锁外执行，绝不能拿着全局锁去读写内容。
type uploadController struct {
	mu         sync.Mutex
	active     map[string]*activeUpload
	finalizing map[string]*finalizeGate
}

func newUploadController() *uploadController {
	return &uploadController{
		active:     make(map[string]*activeUpload),
		finalizing: make(map[string]*finalizeGate),
	}
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
	// 收尾期间禁止登记新的写入。完成接口拿到门禁后会等待在途写入退出，
	// 此时再放行一次写入，FinalizeChunks 之后就可能又出现新的分片。
	if _, ok := c.finalizing[sessionID]; ok {
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

// beginFinalize 取得一个会话的收尾门禁。
//
// 取得门禁后：
//   - 新的写入登记（begin）会被拒绝，分片请求不会在收尾过程中再写盘；
//   - 已经在途的写入会被取消，并且本方法会等到它完全退出后才返回。
//
// 调用方拿到 release 后才能核对账本、调用 FinalizeChunks；结束时必须调用
// release。同一会话已经有收尾在运行时，这里等待它结束再重试，这样并发
// /complete 最终都会读到 completed 并返回同一个结果文件。
func (c *uploadController) beginFinalize(
	ctx context.Context,
	sessionID string,
) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		c.mu.Lock()
		if gate, ok := c.finalizing[sessionID]; ok {
			c.mu.Unlock()
			select {
			case <-gate.done:
				// 上一轮收尾已经结束：重新读取会话，决定是继续还是直接返回。
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		gate := &finalizeGate{done: make(chan struct{})}
		c.finalizing[sessionID] = gate
		entry := c.active[sessionID]
		if entry != nil {
			// 通知在途写入停止；不持锁等待它退出。
			entry.cancel()
		}
		c.mu.Unlock()

		if entry == nil {
			return func() { c.endFinalize(sessionID, gate) }, nil
		}

		select {
		case <-entry.done:
			// 写入已经退出，而且不会再登记新的写入（门禁还在）。
			return func() { c.endFinalize(sessionID, gate) }, nil
		case <-ctx.Done():
			c.endFinalize(sessionID, gate)
			return nil, ctx.Err()
		}
	}
}

// beginIdleGate 只在会话完全空闲时取得独占门禁，不会取消或等待写入。
//
// 它用于“数据库还是 receiving、但内存里已经没有写入”的核对恢复：
// 拿不到门禁说明还有请求在写，或者另一个收尾正在进行，调用方必须按冲突
// 处理，不能擅自改数据库状态。
func (c *uploadController) beginIdleGate(sessionID string) (func(), bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.active[sessionID]; ok {
		return nil, false
	}
	if _, ok := c.finalizing[sessionID]; ok {
		return nil, false
	}

	gate := &finalizeGate{done: make(chan struct{})}
	c.finalizing[sessionID] = gate
	return func() { c.endFinalize(sessionID, gate) }, true
}

// endFinalize 释放收尾门禁，唤醒等待中的完成请求。
func (c *uploadController) endFinalize(sessionID string, gate *finalizeGate) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 只释放本次登记的门禁，避免误删后来者的登记。
	if c.finalizing[sessionID] != gate {
		return
	}
	delete(c.finalizing, sessionID)
	close(gate.done)
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

// isActive 判断会话当前是否有活跃写入或正在收尾。
//
// 收尾也算“活跃”：完成接口正在核对账本、FinalizeChunks 或发布文件，
// 取消/过期方此时不能清理物理内容。门禁释放后，完成接口会补一次移交。
func (c *uploadController) isActive(sessionID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.active[sessionID]; ok {
		return true
	}
	_, ok := c.finalizing[sessionID]
	return ok
}
