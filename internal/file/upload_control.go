package file

import (
	"context"
	"sync"
)

// defaultChunkParallel 是单个会话同时处理多少个分片请求。
//
// 第一版只做进程内限流：超过上限的请求直接返回冲突（HTTP 409），
// 不在服务端排队。配置化留到有性能数据之后。
const defaultChunkParallel = 2

// activeUpload 是一次正在进行的会话写入请求。
//
// 整文件上传用 uploadController.active 登记；分片上传按编号登记在
// chunkRun.active 里。cancel 只通知传输停止；done 在请求真正退出、
// 并且从登记表移除后关闭。取消方只依赖 done 判断“现在能不能安全清理”。
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

// chunkRun 是一个分片会话当前的运行状态。
//
// 它和分片账本（chunkLedger）是两件事：
//   - active 说明“这个编号现在有没有人正在处理”；
//   - 账本说明“这个编号之前有没有成功写入”。
//
// 账本挡不住两个请求同时处理同一片，所以两者必须同时存在。
type chunkRun struct {
	// active 由 uploadController.mu 保护：编号 → 当前处理它的请求。
	active map[int]*activeUpload

	// drained 在 active 从“有人”变成“没人”时关闭。只有 active 非空时
	// 才非 nil；beginFinalize 在同一把锁内取得它，避免“看到没人、又有
	// 新请求登记”的窗口。
	drained chan struct{}

	// stopping 表示会话正在取消或已经失效：禁止继续登记新请求，
	// 但不会把已经登记请求的 done 提前关闭。
	stopping bool

	// progressMu 只串行化本会话的“读账本 + 落库进度”，
	// 不覆盖读请求体、写盘等耗时操作。
	progressMu sync.Mutex
}

// uploadController 是单进程共享的活跃上传登记表。
//
// 它只保护“哪个会话正在写/收尾”这一小段内存状态。文件传输和数据库操作
// 都在锁外执行，绝不能拿着全局锁去读写内容。
type uploadController struct {
	mu sync.Mutex

	// active 是整文件上传的独占登记：一个会话同时只允许一个请求。
	active map[string]*activeUpload

	// chunks 是分片上传的登记：每个会话允许多个编号并行，
	// 同一编号仍然只允许一个请求。
	chunks map[string]*chunkRun

	// finalizing 是会话级收尾门禁。整文件、分片共用：
	// 门禁存在时，两种写入都不能再登记。
	finalizing map[string]*finalizeGate

	// maxParallel 是单个分片会话的并行度上限。
	maxParallel int
}

func newUploadController() *uploadController {
	return &uploadController{
		active:      make(map[string]*activeUpload),
		chunks:      make(map[string]*chunkRun),
		finalizing:  make(map[string]*finalizeGate),
		maxParallel: defaultChunkParallel,
	}
}

// begin 登记一个整文件会话的写入请求，并返回可被取消的子 context。
//
// 同一个会话只允许一个活跃整文件写入：重复登记、或者存在活跃分片请求时
// 返回 ErrUploadSessionConflict。调用方必须在 pending → receiving 的数据库
// 抢占之前调用它，这样取消方不会在“数据库已经 receiving、但请求还没登记”
// 的窗口里误判。
//
// 返回的 end 必须由调用方在退出时执行，且只执行一次。
func (c *uploadController) begin(parent context.Context, sessionID string) (context.Context, func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.active[sessionID]; ok {
		return nil, nil, ErrUploadSessionConflict
	}
	if run, ok := c.chunks[sessionID]; ok && len(run.active) > 0 {
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

// beginChunk 登记一个分片请求。
//
// 检查、占编号、占名额在同一把锁内完成，所以：
//   - 同一编号不会有两个请求同时进入处理阶段；
//   - 超过并行度上限的请求在读请求体之前就被拒绝；
//   - 收尾门禁安装之后不会再有新的分片登记。
//
// 返回的 end 必须由调用方在读取、写盘、登记账本和进度同步都结束后执行，
// 并且只执行一次。
func (c *uploadController) beginChunk(
	parent context.Context,
	sessionID string,
	index int,
) (context.Context, func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	run, ok := c.chunks[sessionID]
	if !ok {
		run = &chunkRun{active: make(map[int]*activeUpload)}
		c.chunks[sessionID] = run
	}

	// 拒绝时不要把只是为了检查而创建的空运行状态留在表里；
	// 已经 stopping 的运行状态要保留，继续挡住所后续登记。
	reject := func() (context.Context, func(), error) {
		if len(run.active) == 0 && !run.stopping {
			delete(c.chunks, sessionID)
		}
		return nil, nil, ErrUploadSessionConflict
	}

	switch {
	case run.stopping:
		// 正在取消或已经失效：不再接受新请求。
		return reject()
	case c.finalizing[sessionID] != nil:
		// 正在收尾：FinalizeChunks 之前不能再出现新的写入。
		return reject()
	case c.active[sessionID] != nil:
		// 同会话的整文件写入还在进行：模式不匹配，按冲突处理。
		return reject()
	case run.active[index] != nil:
		// 同一编号现在有人处理：第二个请求在读 body 之前就被挡住。
		return reject()
	case len(run.active) >= c.maxParallel:
		// 达到并行度上限。第一版直接拒绝，不在服务端排队。
		return reject()
	}

	if len(run.active) == 0 {
		// 新一批活跃请求：为等待方准备一个“全部退出”的信号。
		run.drained = make(chan struct{})
	}

	ctx, cancel := context.WithCancel(parent)
	entry := &activeUpload{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	run.active[index] = entry

	var once sync.Once
	end := func() {
		once.Do(func() {
			cancel()

			c.mu.Lock()
			// 核对身份，不能误删后来者。
			if run.active[index] == entry {
				delete(run.active, index)
				if len(run.active) == 0 {
					close(run.drained)
					run.drained = nil
					// 正常结束的分片会话不保留空的运行状态：
					// 下一次请求会重新创建，账本仍然独立保留。
					if !run.stopping {
						delete(c.chunks, sessionID)
					}
				}
			}
			c.mu.Unlock()

			close(entry.done)
		})
	}
	return ctx, end, nil
}

// lockChunkProgress 取得本会话的进度同步锁，返回解锁函数。
//
// 调用方必须持有本会话的一个活跃分片登记，这样 chunkRun 不会被删除。
func (c *uploadController) lockChunkProgress(sessionID string) func() {
	c.mu.Lock()
	run := c.chunks[sessionID]
	c.mu.Unlock()
	if run == nil {
		return func() {}
	}

	run.progressMu.Lock()
	return run.progressMu.Unlock
}

// beginFinalize 取得一个会话的收尾门禁。
//
// 取得门禁后：
//   - 新的写入登记（begin / beginChunk）会被拒绝；
//   - 已经在途的分片写入不会被取消，本方法会等它们自然退出；
//   - 已经在途的整文件写入仍然会被取消（整文件请求不是分片，等它传完
//     没有意义）。
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

		// 安装门禁和取得 drained 必须在同一把锁内：
		// 否则完成请求可能看到“没有写入”，然后新分片在 Finalize 之前登记成功。
		var wait <-chan struct{}
		if entry := c.active[sessionID]; entry != nil {
			// 整文件上传：通知停止，并等它真正退出。
			entry.cancel()
			wait = entry.done
		} else if run := c.chunks[sessionID]; run != nil && len(run.active) > 0 {
			// 分片上传：不取消，等在途分片自然结束。
			wait = run.drained
		}
		c.mu.Unlock()

		release := func() { c.endFinalize(sessionID, gate) }
		if wait == nil {
			return release, nil
		}

		select {
		case <-wait:
			return release, nil
		case <-ctx.Done():
			c.endFinalize(sessionID, gate)
			return nil, ctx.Err()
		}
	}
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

	// 已经停止、而且没有在途请求的分片会话，不再需要保留运行状态。
	if run := c.chunks[sessionID]; run != nil && run.stopping && len(run.active) == 0 {
		delete(c.chunks, sessionID)
	}
}

// cancel 通知会话的活跃写入停止，并返回一个在它们全部退出时关闭的 channel。
//
// 取消信号不等于请求已经退出：分片请求可能还阻塞在 Read 里。
// 因此：
//   - 只是把 stopping 打开，禁止新请求；
//   - 逐个 cancel 在途请求；
//   - 不关闭 drained，等每个请求实际执行 end。
//
// 没有活跃写入时返回一个已经关闭的 channel。
func (c *uploadController) cancel(sessionID string) <-chan struct{} {
	c.mu.Lock()

	var (
		wait    <-chan struct{}
		cancels []context.CancelFunc
	)
	if entry := c.active[sessionID]; entry != nil {
		cancels = append(cancels, entry.cancel)
		wait = entry.done
	}

	if run := c.chunks[sessionID]; run != nil {
		run.stopping = true
		for _, entry := range run.active {
			cancels = append(cancels, entry.cancel)
		}
		if len(run.active) > 0 {
			// 有多个在途请求时，drained 在最后一个 end 时关闭。
			if wait == nil {
				wait = run.drained
			}
		}
		// 没有在途请求时保留 stopping 标记，后续登记仍然会被拒绝。
		// 终态清理完成后由 forget 删除。
	}
	c.mu.Unlock()

	// 取消通知在锁外发出：它只通知传输停止，不代表请求已经退出。
	for _, cancel := range cancels {
		cancel()
	}

	if wait == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return wait
}

// forget 删除一个会话的分片运行状态。
//
// 只在会话已经进入终态、物理清理也确认过没有在途写入之后调用。
// 保留 stopping 标记直到这一刻，避免取消后又有请求登记成功。
func (c *uploadController) forget(sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if run := c.chunks[sessionID]; run != nil && len(run.active) == 0 {
		delete(c.chunks, sessionID)
	}
}

// isActive 判断会话当前是否有活跃写入或正在收尾。
//
// 收尾也算“活跃”：完成接口正在核对账本、FinalizeChunks 或发布文件，
// 取消/过期方此时不能清理物理内容。门禁释放后，完成接口会补一次移交。
//
// 只有 stopping、没有在途请求不算活跃：此时已经没有写入会碰磁盘，
// 可以开始清理。
func (c *uploadController) isActive(sessionID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.active[sessionID]; ok {
		return true
	}
	if run, ok := c.chunks[sessionID]; ok && len(run.active) > 0 {
		return true
	}
	_, ok := c.finalizing[sessionID]
	return ok
}
