package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"luckyCloudFile/internal/config"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func testListener(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func waitError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server or request")
		return nil
	}
}

func waitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for synchronization point")
	}
}

type observedListener struct {
	net.Listener
	closed chan struct{}
	once   sync.Once
}

func (l *observedListener) Close() error {
	err := l.Listener.Close()
	l.once.Do(func() { close(l.closed) })
	return err
}

func TestShutdownWaitsForInflightRequestAndReleasesPort(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		_, _ = io.WriteString(w, "finished")
	})}
	t.Cleanup(func() { _ = server.Close() })
	listener := &observedListener{Listener: testListener(t), closed: make(chan struct{})}
	address := listener.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, server, listener, time.Second, testLogger()) }()
	requestDone := make(chan error, 1)
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	go func() {
		response, err := client.Get("http://" + address)
		if err != nil {
			requestDone <- err
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err == nil && string(body) != "finished" {
			err = errors.New("inflight response was interrupted")
		}
		requestDone <- err
	}()
	waitSignal(t, started)
	cancel()
	// 确认服务已开始关闭监听后，才放行在途请求，避免仅靠调度碰巧通过。
	waitSignal(t, listener.closed)
	select {
	case err := <-done:
		t.Fatalf("server exited before inflight request finished: %v", err)
	default:
	}
	unblock()
	if err := waitError(t, requestDone); err != nil {
		t.Fatal(err)
	}
	if err := waitError(t, done); err != nil {
		t.Fatal(err)
	}
	rebound, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("port not released: %v", err)
	}
	_ = rebound.Close()
}

func TestShutdownTimeoutClosesRemainingConnections(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(stopped)
	})}
	t.Cleanup(func() { _ = server.Close() })
	listener := testListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, server, listener, 30*time.Millisecond, testLogger()) }()
	requestDone := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 3 * time.Second}
		defer client.CloseIdleConnections()
		response, err := client.Get("http://" + listener.Addr().String())
		if response != nil {
			_ = response.Body.Close()
		}
		requestDone <- err
	}()
	waitSignal(t, started)
	cancel()
	if err := waitError(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wanted shutdown deadline error, got %v", err)
	}
	waitSignal(t, stopped)
	_ = waitError(t, requestDone)
}

func TestRunRejectsOccupiedPort(t *testing.T) {
	listener := testListener(t)
	cfg := config.Config{HTTP: config.HTTP{Listen: listener.Addr().String(), ShutdownTimeoutSeconds: 1},
		Storage: config.Storage{LocalRoot: t.TempDir()}}
	if err := Run(context.Background(), cfg, testLogger()); err == nil || !strings.Contains(err.Error(), "listen HTTP") {
		t.Fatalf("wanted occupied-port error, got %v", err)
	}
}

func TestCheckStorage(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	if err := checkStorage(root); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("write probe was not removed: %v", err)
	}
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkStorage(file); err == nil {
		t.Fatal("regular file accepted as storage directory")
	}
}
