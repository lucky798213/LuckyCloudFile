package file

import "sync"

// ChunkReceipt 是服务端对一片成功写入的确认。
//
// Size 是这一片必须收到的长度；SHA256 是服务端实际读到的内容摘要。
// 重复编号再次到达时，用它判断“同一个分片的重试”还是“同编号不同内容”。
type ChunkReceipt struct {
	Size   int64
	SHA256 [32]byte
}

// ChunkProgress 是一个上传会话的分片账本。
//
// 它跨请求存在：每个分片请求结束时 activeUpload 会被注销，但这里记录
// 哪些编号已经成功、累计收到了多少有效字节。字段只能通过 chunkLedger
// 的方法访问，调用方不要直接修改。
type ChunkProgress struct {
	Uploaded      map[int]ChunkReceipt
	ReceivedBytes int64
}

// chunkLedger 是单进程内的分片账本。
//
// P5 不支持跨进程续传，所以不需要落库；服务重启后旧会话本来就会被启动
// 恢复标记为 expired。这里只保护内存 map 的并发访问，不承担活跃请求登记
// 的职责——那是 uploadController 的事。
type chunkLedger struct {
	mu       sync.Mutex
	sessions map[string]*ChunkProgress
}

func newChunkLedger() *chunkLedger {
	return &chunkLedger{sessions: make(map[string]*ChunkProgress)}
}

// create 为一个会话建立空账本；重复调用不会清空已有进度。
func (l *chunkLedger) create(sessionID string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.sessions[sessionID]; !ok {
		l.sessions[sessionID] = &ChunkProgress{
			Uploaded: make(map[int]ChunkReceipt),
		}
	}
}

// remove 删除一个会话的账本；会话完成、取消或过期后调用。
func (l *chunkLedger) remove(sessionID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.sessions, sessionID)
}

// received 返回已经确认的累计字节数，以及账本是否存在。
func (l *chunkLedger) received(sessionID string) (int64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	progress, ok := l.sessions[sessionID]
	if !ok {
		return 0, false
	}
	return progress.ReceivedBytes, true
}

// lookup 返回指定编号的确认记录。
func (l *chunkLedger) lookup(sessionID string, index int) (ChunkReceipt, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	progress, ok := l.sessions[sessionID]
	if !ok {
		return ChunkReceipt{}, false
	}
	receipt, ok := progress.Uploaded[index]
	return receipt, ok
}

// snapshot 返回账本的一份只读副本、累计字节数，以及账本是否存在。
//
// 完成接口用它核对“每个合法编号都有成功记录”。返回副本是为了让核对
// 过程不持有账本锁：核对期间的写入已经被会话级收尾门禁挡住了。
func (l *chunkLedger) snapshot(sessionID string) (map[int]ChunkReceipt, int64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	progress, ok := l.sessions[sessionID]
	if !ok {
		return nil, 0, false
	}
	uploaded := make(map[int]ChunkReceipt, len(progress.Uploaded))
	for index, receipt := range progress.Uploaded {
		uploaded[index] = receipt
	}
	return uploaded, progress.ReceivedBytes, true
}

// record 在写入成功后登记一片。
//
// 返回累计有效字节数和是否真的登记成功。编号已经存在时返回 false，
// 调用方必须把它当作冲突处理：不能重复累计字节，也不能让第二份内容
// 覆盖已经确认过的分片。
func (l *chunkLedger) record(
	sessionID string,
	index int,
	receipt ChunkReceipt,
) (int64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	progress, ok := l.sessions[sessionID]
	if !ok {
		return 0, false
	}
	if _, exists := progress.Uploaded[index]; exists {
		return progress.ReceivedBytes, false
	}

	progress.Uploaded[index] = receipt
	progress.ReceivedBytes += receipt.Size
	return progress.ReceivedBytes, true
}
