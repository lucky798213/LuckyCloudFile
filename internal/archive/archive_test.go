package archive

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/queue"
)

// fakeSource 是测试里的一个源文件。
type fakeSource struct {
	owner   int
	name    string
	data    []byte
	readErr error
	reader  io.ReadCloser // 非空时直接使用，用于模拟阻塞读取。
}

type fakeService struct {
	mu      sync.Mutex
	sources map[string]fakeSource
	closed  map[string]int
	uploads [][]byte

	uploadErr    error
	uploadOwner  int
	uploadParent string
	uploadName   string
}

func newFakeService(sources map[string]fakeSource) *fakeService {
	return &fakeService{
		sources: sources,
		closed:  make(map[string]int),
	}
}

func (f *fakeService) Open(_ context.Context, ownerID int, id string) (file.File, io.ReadCloser, error) {
	f.mu.Lock()
	src, ok := f.sources[id]
	f.mu.Unlock()

	if !ok || src.owner != ownerID {
		return file.File{}, nil, file.ErrNotFound
	}
	if src.reader != nil {
		return file.File{ID: id, Name: src.name, Size: int64(len(src.data)), Kind: "file"}, src.reader, nil
	}
	return file.File{ID: id, Name: src.name, Size: int64(len(src.data)), Kind: "file"},
		&fakeReader{service: f, id: id, data: src.data, readErr: src.readErr}, nil
}

func (f *fakeService) Upload(_ context.Context, ownerID int, parentID, name string, r io.Reader) (file.File, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return file.File{}, err
	}

	f.mu.Lock()
	f.uploads = append(f.uploads, data)
	f.uploadOwner = ownerID
	f.uploadParent = parentID
	f.uploadName = name
	uploadErr := f.uploadErr
	f.mu.Unlock()

	if uploadErr != nil {
		return file.File{}, uploadErr
	}
	return file.File{ID: "result-file", Name: name, Size: int64(len(data))}, nil
}

func (f *fakeService) closeCount(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed[id]
}

func (f *fakeService) uploadCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.uploads)
}

type fakeReader struct {
	service *fakeService
	id      string
	data    []byte
	pos     int
	readErr error
	closed  bool
}

func (r *fakeReader) Read(p []byte) (int, error) {
	if r.pos < len(r.data) {
		n := copy(p, r.data[r.pos:])
		r.pos += n
		return n, nil
	}
	if r.readErr != nil {
		return 0, r.readErr
	}
	return 0, io.EOF
}

func (r *fakeReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.service.mu.Lock()
	r.service.closed[r.id]++
	r.service.mu.Unlock()
	return nil
}

func newTestJob(t *testing.T, files *fakeService, ids []string, opts Options) (*Job, string) {
	t.Helper()

	if opts.TempDir == "" {
		opts.TempDir = t.TempDir()
	}
	job, err := New(files, 1, ids, "parent", "bundle.zip", opts)
	if err != nil {
		t.Fatal(err)
	}
	return job, opts.TempDir
}

func assertDirEmpty(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("temporary files left behind: %v", entries)
	}
}

func TestRunBuildsAndPublishesZip(t *testing.T) {
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, name: "alpha.txt", data: []byte("alpha")},
		"file-b": {owner: 1, name: "中文 名称.txt", data: []byte("中文内容")},
		"file-c": {owner: 1, name: "empty.bin"},
	})
	job, dir := newTestJob(t, files, []string{"file-a", "file-b", "file-c"}, Options{})

	id, err := job.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if id != "result-file" {
		t.Fatalf("result ID = %q, want result-file", id)
	}

	if files.uploadCount() != 1 {
		t.Fatalf("upload calls = %d, want 1", files.uploadCount())
	}
	if files.uploadOwner != 1 || files.uploadParent != "parent" || files.uploadName != "bundle.zip" {
		t.Fatalf("unexpected publish target: owner=%d parent=%q name=%q",
			files.uploadOwner, files.uploadParent, files.uploadName)
	}

	files.mu.Lock()
	archiveFile := files.uploads[0]
	files.mu.Unlock()

	zr, err := zip.NewReader(bytes.NewReader(archiveFile), int64(len(archiveFile)))
	if err != nil {
		t.Fatalf("published content is not a valid zip: %v", err)
	}

	want := map[string]string{
		"alpha.txt": "alpha",
		"中文 名称.txt": "中文内容",
		"empty.bin": "",
	}
	got := make(map[string]string, len(zr.File))
	for _, entry := range zr.File {
		rc, err := entry.Open()
		if err != nil {
			t.Fatal(err)
		}
		content, err := io.ReadAll(rc)
		if err != nil {
			rc.Close()
			t.Fatal(err)
		}
		if err := rc.Close(); err != nil {
			t.Fatal(err)
		}
		got[entry.Name] = string(content)
	}
	if len(got) != len(want) {
		t.Fatalf("entries = %d, want %d: %v", len(got), len(want), got)
	}
	for name, content := range want {
		if got[name] != content {
			t.Fatalf("entry %q = %q, want %q", name, got[name], content)
		}
	}

	for _, id := range []string{"file-a", "file-b", "file-c"} {
		if n := files.closeCount(id); n != 1 {
			t.Fatalf("source %s closed %d times, want 1", id, n)
		}
	}
	assertDirEmpty(t, dir)
}

func TestRunRejectsDuplicateEntryNames(t *testing.T) {
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, name: "same.txt", data: []byte("a")},
		"file-b": {owner: 1, name: "same.txt", data: []byte("b")},
	})
	job, dir := newTestJob(t, files, []string{"file-a", "file-b"}, Options{})

	if _, err := job.Run(context.Background()); !errors.Is(err, ErrDuplicateName) {
		t.Fatalf("want duplicate name error, got %v", err)
	}
	if files.uploadCount() != 0 {
		t.Fatal("must not publish after a duplicate entry name")
	}
	for _, id := range []string{"file-a", "file-b"} {
		if n := files.closeCount(id); n != 1 {
			t.Fatalf("source %s closed %d times, want 1", id, n)
		}
	}
	assertDirEmpty(t, dir)
}

func TestRunRejectsUnsafeEntryNames(t *testing.T) {
	names := []string{"", ".", "..", "dir/file.txt", `dir\file.txt`, "bad\x00name", "\xff"}

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			files := newFakeService(map[string]fakeSource{
				"file-a": {owner: 1, name: name, data: []byte("x")},
			})
			job, dir := newTestJob(t, files, []string{"file-a"}, Options{})

			if _, err := job.Run(context.Background()); !errors.Is(err, ErrUnsafeName) {
				t.Fatalf("want unsafe name error, got %v", err)
			}
			if files.uploadCount() != 0 {
				t.Fatal("must not publish after an unsafe entry name")
			}
			if n := files.closeCount("file-a"); n != 1 {
				t.Fatalf("source closed %d times, want 1", n)
			}
			assertDirEmpty(t, dir)
		})
	}
}

func TestRunFailsWhenSourceReadFails(t *testing.T) {
	readErr := errors.New("disk on fire")
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, name: "a.txt", data: []byte("partial"), readErr: readErr},
	})
	job, dir := newTestJob(t, files, []string{"file-a"}, Options{})

	if _, err := job.Run(context.Background()); !errors.Is(err, readErr) {
		t.Fatalf("want read error, got %v", err)
	}
	if files.uploadCount() != 0 {
		t.Fatal("must not publish a partial archive")
	}
	if n := files.closeCount("file-a"); n != 1 {
		t.Fatalf("source closed %d times, want 1", n)
	}
	assertDirEmpty(t, dir)
}

// panicReader 在读取时 panic，用来验证 defer 关闭句柄。
type panicReader struct {
	closed atomic.Bool
}

func (r *panicReader) Read([]byte) (int, error) {
	panic("source reader boom")
}

func (r *panicReader) Close() error {
	r.closed.Store(true)
	return nil
}

// 来源 reader panic 时：源句柄要关闭、临时文件要清理、队列 worker 要继续。
func TestPanickingReaderClosesHandlesAndWorkerContinues(t *testing.T) {
	reader := &panicReader{}
	files := newFakeService(map[string]fakeSource{
		"boom": {owner: 1, name: "boom.bin", reader: reader},
	})
	tempDir := t.TempDir()

	badJob, err := New(files, 1, []string{"boom"}, "parent", "bad.zip", Options{TempDir: tempDir})
	if err != nil {
		t.Fatal(err)
	}

	q, err := queue.New(1, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(q.Shutdown)

	badID, err := q.Submit(1, badJob)
	if err != nil {
		t.Fatal(err)
	}

	secondRan := make(chan struct{})
	goodID, err := q.Submit(1, queue.JobFunc(func(context.Context) (string, error) {
		close(secondRan)
		return "ok", nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-secondRan:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not continue after the archive job panicked")
	}
	q.Shutdown()

	bad, err := q.Get(1, badID)
	if err != nil {
		t.Fatal(err)
	}
	if bad.Status != queue.Failed || !strings.Contains(bad.Error, "task panic: source reader boom") {
		t.Fatalf("unexpected panicked task: %+v", bad)
	}

	good, err := q.Get(1, goodID)
	if err != nil {
		t.Fatal(err)
	}
	if good.Status != queue.Completed || good.Result != "ok" {
		t.Fatalf("worker did not process the next task: %+v", good)
	}

	if !reader.closed.Load() {
		t.Fatal("source reader was not closed on panic")
	}
	if files.uploadCount() != 0 {
		t.Fatal("must not publish after a panicking reader")
	}
	assertDirEmpty(t, tempDir)
}

// blockingReader 先返回固定前缀，然后阻塞到 context 取消。
type blockingReader struct {
	ctx     context.Context
	prefix  []byte
	blocked chan struct{}
	once    sync.Once
	closed  atomic.Bool
}

func (r *blockingReader) Read(p []byte) (int, error) {
	if len(r.prefix) > 0 {
		n := copy(p, r.prefix)
		r.prefix = r.prefix[n:]
		return n, nil
	}
	r.once.Do(func() { close(r.blocked) })
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func (r *blockingReader) Close() error {
	r.closed.Store(true)
	return nil
}

func TestRunStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &blockingReader{
		ctx:     ctx,
		prefix:  []byte("partial"),
		blocked: make(chan struct{}),
	}
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, name: "a.txt", reader: reader},
	})
	job, dir := newTestJob(t, files, []string{"file-a"}, Options{})

	done := make(chan error, 1)
	go func() {
		_, err := job.Run(ctx)
		done <- err
	}()

	<-reader.blocked
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("job did not stop after cancellation")
	}

	if files.uploadCount() != 0 {
		t.Fatal("must not publish a canceled archive")
	}
	if !reader.closed.Load() {
		t.Fatal("source reader was not closed")
	}
	assertDirEmpty(t, dir)
}

func TestNewRejectsInvalidInput(t *testing.T) {
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, name: "a.txt", data: []byte("a")},
		"file-b": {owner: 1, name: "b.txt", data: []byte("b")},
	})

	cases := []struct {
		name string
		job  func() (*Job, error)
		want error
	}{
		{
			name: "no sources",
			job: func() (*Job, error) {
				return New(files, 1, nil, "parent", "bundle.zip", Options{})
			},
			want: ErrNoSources,
		},
		{
			name: "too many files",
			job: func() (*Job, error) {
				return New(files, 1, []string{"file-a", "file-b"}, "parent", "bundle.zip", Options{MaxFiles: 1})
			},
			want: ErrTooManyFiles,
		},
		{
			name: "duplicate file",
			job: func() (*Job, error) {
				return New(files, 1, []string{"file-a", "file-a"}, "parent", "bundle.zip", Options{})
			},
			want: ErrDuplicateFile,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.job(); !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}

	if _, err := New(nil, 1, []string{"file-a"}, "parent", "bundle.zip", Options{}); err == nil {
		t.Fatal("want error for nil file service")
	}
}

func TestRunRejectsSourceOverInputLimit(t *testing.T) {
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, name: "a.txt", data: []byte("123456")},
	})
	job, dir := newTestJob(t, files, []string{"file-a"}, Options{MaxSourceBytes: 5})

	if _, err := job.Run(context.Background()); !errors.Is(err, ErrSourceTooLarge) {
		t.Fatalf("want source too large, got %v", err)
	}
	if files.uploadCount() != 0 {
		t.Fatal("must not publish an oversized source archive")
	}
	assertDirEmpty(t, dir)
}

func TestRunRejectsArchiveOverOutputLimit(t *testing.T) {
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, name: "a.txt", data: []byte("hello")},
	})
	job, dir := newTestJob(t, files, []string{"file-a"}, Options{MaxArchiveBytes: 1})

	if _, err := job.Run(context.Background()); !errors.Is(err, ErrArchiveTooLarge) {
		t.Fatalf("want archive too large, got %v", err)
	}
	if files.uploadCount() != 0 {
		t.Fatal("must not publish an oversized archive")
	}
	assertDirEmpty(t, dir)
}

func TestRunFailsWhenPublishFailsAndCleansUp(t *testing.T) {
	publishErr := errors.New("quota exceeded")
	files := newFakeService(map[string]fakeSource{
		"file-a": {owner: 1, name: "a.txt", data: []byte("hello")},
	})
	files.uploadErr = publishErr
	job, dir := newTestJob(t, files, []string{"file-a"}, Options{})

	if _, err := job.Run(context.Background()); !errors.Is(err, publishErr) {
		t.Fatalf("want publish error, got %v", err)
	}
	if n := files.closeCount("file-a"); n != 1 {
		t.Fatalf("source closed %d times, want 1", n)
	}
	assertDirEmpty(t, dir)
}
