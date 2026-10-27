package file

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/pathlock"
	"luckyCloudFile/internal/storage"
)

// blockingChunkWriteStore 在第一次 WriteChunkAt 时关闭 entered，并停住，
// 直到测试关闭 release。用它把“分片请求正在写盘”变成一个确定的同步点，
// 不依赖 sleep 猜测请求有没有进入保护区。
type blockingChunkWriteStore struct {
	storage.ChunkStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingChunkWriteStore) WriteChunkAt(
	ctx context.Context,
	key string,
	offset int64,
	data []byte,
) error {
	s.once.Do(func() { close(s.entered) })
	// 故意不监听 ctx.Done：取消/过期也必须等测试放行，
	// 这样才能验证“锁已经关闭，但在途保护区还没退出”的中间状态。
	<-s.release
	return s.ChunkStore.WriteChunkAt(ctx, key, offset, data)
}

// chunkMoveScenario 是一次“分片上传 + 移动父目录”的测试环境。
type chunkMoveScenario struct {
	env     *testEnv
	svc     *Service
	store   *blockingChunkWriteStore
	release func()
	info    UploadSessionInfo
	docs    File
	dest    File
}

func newChunkMoveScenario(t *testing.T) *chunkMoveScenario {
	t.Helper()

	env := newTestEnv(t)
	store := &blockingChunkWriteStore{
		ChunkStore: storage.NewLocal(env.rootA),
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	registry := storage.NewRegistry()
	if err := registry.Register(env.policyA.ID, store); err != nil {
		t.Fatal(err)
	}
	svc := NewService(env.db, registry)

	ctx := context.Background()
	docs, err := svc.CreateDirectory(ctx, env.ownerA.ID, "", "docs")
	if err != nil {
		t.Fatal(err)
	}
	dest, err := svc.CreateDirectory(ctx, env.ownerA.ID, "", "dest")
	if err != nil {
		t.Fatal(err)
	}
	info, err := svc.CreateUploadSessionWithMode(
		ctx, env.ownerA.ID, docs.ID, "report.bin",
		UploadChunkSize, model.UploadModeChunked,
	)
	if err != nil {
		t.Fatal(err)
	}

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(store.release) })
	}
	t.Cleanup(release)

	return &chunkMoveScenario{
		env:     env,
		svc:     svc,
		store:   store,
		release: release,
		info:    info,
		docs:    docs,
		dest:    dest,
	}
}

// startChunk 启动一个分片请求，并等到它真正进入 WriteChunkAt。
func (s *chunkMoveScenario) startChunk(t *testing.T) <-chan error {
	t.Helper()

	chunkDone := make(chan error, 1)
	go func() {
		_, err := s.svc.UploadSessionChunk(
			context.Background(),
			s.env.ownerA.ID,
			s.info.ID,
			0,
			bytes.NewReader(bytes.Repeat([]byte("a"), int(UploadChunkSize))),
		)
		chunkDone <- err
	}()

	select {
	case <-s.store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("chunk did not reach WriteChunkAt")
	}
	return chunkDone
}

// assertParentMoveBlocked 在分片请求停住时移动父目录，必须被会话资源锁挡住。
func (s *chunkMoveScenario) assertParentMoveBlocked(t *testing.T) {
	t.Helper()

	if _, err := s.svc.Move(
		context.Background(), s.env.ownerA.ID, s.docs.ID, s.dest.ID,
	); !errors.Is(err, ErrResourceLocked) {
		t.Fatalf("move parent while chunk is in flight error = %v, want ErrResourceLocked", err)
	}
}

// assertLockReleasedAndMoveSucceeds 在会话进入终态、在途请求退出后，
// 校验绑定已删除、目标锁已释放，父目录可以正常移动。
func (s *chunkMoveScenario) assertLockReleasedAndMoveSucceeds(t *testing.T) {
	t.Helper()

	if _, ok := s.svc.uploadLocks.lookup(s.info.ID); ok {
		t.Fatal("session resource lock binding still exists after terminal state")
	}
	moved, err := s.svc.Move(
		context.Background(), s.env.ownerA.ID, s.docs.ID, s.dest.ID,
	)
	if err != nil {
		t.Fatalf("move parent after terminal state: %v", err)
	}
	if moved.ParentID == nil || *moved.ParentID != s.dest.ID {
		t.Fatalf("moved directory parent = %v, want %s", moved.ParentID, s.dest.ID)
	}
}

// TestChunkedUploadParentMoveBlockedUntilTerminal 验证分片上传期间移动父目录
// 会被资源锁挡住；取消、过期、完成后锁释放，父目录可以移动。
//
// 取消/过期还额外验证“锁已关闭但保护区未退出”时仍然不能被抢走：
// 只有等分片请求真正退出，锁才从管理器移除。
func TestChunkedUploadParentMoveBlockedUntilTerminal(t *testing.T) {
	cases := []struct {
		name string
		run  func(t *testing.T, sc *chunkMoveScenario, chunkDone <-chan error)
	}{
		{
			name: "cancel",
			run: func(t *testing.T, sc *chunkMoveScenario, chunkDone <-chan error) {
				if _, err := sc.svc.CancelUploadSession(
					context.Background(), sc.env.ownerA.ID, sc.info.ID,
				); err != nil {
					t.Fatal(err)
				}
				// 取消只标记 closing；在途分片还持有保护区，Move 仍然必须失败。
				sc.assertParentMoveBlocked(t)
				sc.release()
				if err := <-chunkDone; err == nil {
					t.Fatal("chunk succeeded after cancel")
				}
			},
		},
		{
			name: "expire",
			run: func(t *testing.T, sc *chunkMoveScenario, chunkDone <-chan error) {
				session := loadSessionOnly(t, sc.env, sc.info.ID)
				if err := sc.svc.ExpireUploadSessions(
					context.Background(), expiryTestTimeAfter(session), 10,
				); err != nil {
					t.Fatal(err)
				}
				// 过期同样只关闭锁；保护区退出前 Move 仍然必须失败。
				sc.assertParentMoveBlocked(t)
				sc.release()
				if err := <-chunkDone; err == nil {
					t.Fatal("chunk succeeded after expiry")
				}
			},
		},
		{
			name: "complete",
			run: func(t *testing.T, sc *chunkMoveScenario, chunkDone <-chan error) {
				// 完成必须等分片自然结束并进入 ready，再发布。
				sc.release()
				if err := <-chunkDone; err != nil {
					t.Fatalf("chunk before complete: %v", err)
				}
				// ready 还不是终态：分片已经收齐，但完成事务还没发布，
				// 会话锁必须继续挡住父目录移动。
				sc.assertParentMoveBlocked(t)
				if _, err := sc.svc.CompleteUploadSession(
					context.Background(), sc.env.ownerA.ID, sc.info.ID,
				); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sc := newChunkMoveScenario(t)
			chunkDone := sc.startChunk(t)
			sc.assertParentMoveBlocked(t)
			tc.run(t, sc, chunkDone)
			sc.assertLockReleasedAndMoveSucceeds(t)
		})
	}
}

// TestConcurrentCrossDirectoryMovesDoNotCycleOrLeakLocks 让两个目录同时尝试
// 互相移动：一个成功，另一个因为目标父目录已经成为源目录的后代而被拒绝；
// 两边都必须返回，最终树不能成环，失败方不能遗留路径锁。
func TestConcurrentCrossDirectoryMovesDoNotCycleOrLeakLocks(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	a, err := env.service.CreateDirectory(ctx, env.ownerA.ID, "", "a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := env.service.CreateDirectory(ctx, env.ownerA.ID, "", "b")
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)

	go func() {
		<-start
		_, moveErr := env.service.Move(ctx, env.ownerA.ID, a.ID, b.ID)
		results <- moveErr
	}()
	go func() {
		<-start
		_, moveErr := env.service.Move(ctx, env.ownerA.ID, b.ID, a.ID)
		results <- moveErr
	}()
	close(start)

	var (
		successes int
		failures  int
		failure   error
	)
	for i := 0; i < 2; i++ {
		select {
		case moveErr := <-results:
			if moveErr == nil {
				successes++
				continue
			}
			failures++
			failure = moveErr
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent cross directory moves deadlocked")
		}
	}

	if successes != 1 || failures != 1 {
		t.Fatalf("cross moves: successes=%d failures=%d, want 1/1", successes, failures)
	}
	if !errors.Is(failure, ErrInvalidMove) &&
		!errors.Is(failure, ErrResourceLocked) &&
		!errors.Is(failure, ErrResourceChanged) {
		t.Fatalf("unexpected cross move failure: %v", failure)
	}

	// 不成环：两个目录都还能沿 parent_id 走到根目录。另一种可能的最终
	// 形状是 B 移入 A：a=/a、b=/a/b；或者 A 移入 B：b=/b、a=/b/a。
	aSnapshot, err := env.service.resolvePath(ctx, env.ownerA.ID, a.ID)
	if err != nil {
		t.Fatalf("resolve a after cross moves: %v", err)
	}
	bSnapshot, err := env.service.resolvePath(ctx, env.ownerA.ID, b.ID)
	if err != nil {
		t.Fatalf("resolve b after cross moves: %v", err)
	}
	got := map[string]bool{aSnapshot.Path: true, bSnapshot.Path: true}
	switch {
	case got["/a"] && got["/a/b"]:
	case got["/b"] && got["/b/a"]:
	default:
		t.Fatalf("unexpected paths after cross moves: a=%q b=%q",
			aSnapshot.Path, bSnapshot.Path)
	}

	// 失败不遗留锁：把两个移动可能占用的路径逐把重新申请一遍。
	// 任何一把申请失败都说明失败方没有释放资源锁。
	for _, p := range []string{"/a", "/b", "/a/b", "/b/a"} {
		leases, err := env.service.pathLocks.Create(pathlock.Request{
			Namespace: pathlock.Namespace(env.ownerA.ID),
			Path:      p,
			Scope:     pathlock.Subtree,
		})
		if err != nil {
			t.Fatalf("path lock leaked at %s: %v", p, err)
		}
		for _, lease := range leases {
			if err := env.service.pathLocks.Unlock(
				lease.Request.Namespace, lease.Token,
			); err != nil {
				t.Fatalf("release probe lock at %s: %v", p, err)
			}
		}
	}
}
