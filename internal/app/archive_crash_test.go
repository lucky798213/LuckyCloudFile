package app

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/archive"
	"luckyCloudFile/internal/auth"
	"luckyCloudFile/internal/config"
	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/idgen"
	"luckyCloudFile/internal/model"
	"luckyCloudFile/internal/task"
)

// TestArchiveCrashRecovery 是真正的进程崩溃验收：每个场景都启动真实的
// app.run 子进程，在指定故障点写标记后由父进程发送 SIGKILL，再启动新进程
// 验证恢复结果。只取消 context 会执行 defer，不能替代这里。
//
// 默认跳过；需要真实 MySQL 和显式开关：
//
//	CLOUDREVE_ARCHIVE_CRASH=1 \
//	CLOUDREVE_TEST_MYSQL_DSN='lucky:lucky@tcp(127.0.0.1:3306)/luckycloudfile_test?parseTime=true' \
//	go test ./internal/app -run TestArchiveCrashRecovery -v
func TestArchiveCrashRecovery(t *testing.T) {
	if os.Getenv("CLOUDREVE_ARCHIVE_CRASH") != "1" {
		t.Skip("set CLOUDREVE_ARCHIVE_CRASH=1 to run the SIGKILL recovery suite")
	}
	if os.Getenv("CLOUDREVE_TEST_MYSQL_DSN") == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run the SIGKILL recovery suite")
	}

	t.Run("tasks persisted without wake are found by scan", func(t *testing.T) {
		h := newCrashHarness(t)
		taskID := h.newTask(t)
		app := h.start(t, "")
		h.waitCompleted(t, app, taskID, false, "")
		h.assertSingleResult(t, taskID)
	})

	t.Run("half-written zip is rebuilt", func(t *testing.T) {
		h := newCrashHarness(t)
		taskID := h.newTask(t)

		app := h.start(t, "archive.compress.partial")
		h.waitMarker(t, app, "archive.compress.partial")
		partial := filepath.Join(h.workDir, "task-"+taskID, "archive.zip.partial")
		if info, err := os.Stat(partial); err != nil || info.Size() == 0 {
			t.Fatalf("partial ZIP was not written before crash: %v", err)
		}
		app.stop()

		if stored := h.task(t, taskID); stored.Phase != archive.PhaseCompress {
			t.Fatalf("crashed task phase = %q, want %q", stored.Phase, archive.PhaseCompress)
		}

		restarted := h.start(t, "")
		h.waitCompleted(t, restarted, taskID, false, "")
		h.assertSingleResult(t, taskID)
		if _, err := os.Stat(partial); !os.IsNotExist(err) {
			t.Fatalf("partial ZIP survived successful recovery: %v", err)
		}
	})

	t.Run("committed zip checkpoint is reused", func(t *testing.T) {
		h := newCrashHarness(t)
		taskID := h.newTask(t)

		app := h.start(t, "archive.publish.after_checkpoint")
		h.waitMarker(t, app, "archive.publish.after_checkpoint")

		stored := h.task(t, taskID)
		if stored.Phase != archive.PhasePublish {
			t.Fatalf("crashed task phase = %q, want %q", stored.Phase, archive.PhasePublish)
		}
		var state archive.TaskState
		if err := json.Unmarshal([]byte(stored.StateJSON), &state); err != nil {
			t.Fatal(err)
		}
		if state.Archive == nil {
			t.Fatal("publish checkpoint has no archive metadata")
		}
		archivePath := filepath.Join(h.workDir, "task-"+taskID, "archive.zip")
		if _, err := os.Stat(archivePath); err != nil {
			t.Fatalf("committed archive is missing: %v", err)
		}
		app.stop()

		// 重启时把“重新压缩”设成故障点：如果恢复没有复用检查点，
		// 进程会在这里阻塞并写标记，测试能明确失败。
		restarted := h.start(t, "archive.compress.partial")
		h.waitCompleted(t, restarted, taskID, false, restarted.markerPath)
		if _, err := os.Stat(restarted.markerPath); err == nil {
			t.Fatal("recovery recompressed instead of reusing the committed ZIP")
		}
		h.assertSingleResult(t, taskID)
	})

	t.Run("uploaded content without publish is retried", func(t *testing.T) {
		h := newCrashHarness(t)
		taskID := h.newTask(t)

		app := h.start(t, "archive.publish.content_uploaded")
		h.waitMarker(t, app, "archive.publish.content_uploaded")

		var session model.UploadSession
		if err := h.db.Where("task_id = ?", taskID).First(&session).Error; err != nil {
			t.Fatalf("task upload session not found: %v", err)
		}
		if session.Status != model.UploadStatusReady {
			t.Fatalf("session status = %q, want ready", session.Status)
		}
		app.stop()

		restarted := h.start(t, "")
		h.waitCompleted(t, restarted, taskID, false, "")

		var expired model.UploadSession
		if err := h.db.First(&expired, "id = ?", session.ID).Error; err != nil {
			t.Fatal(err)
		}
		if expired.Status != model.UploadStatusExpired {
			t.Fatalf("old upload session status = %q, want expired", expired.Status)
		}
		h.assertSingleResult(t, taskID)
	})

	t.Run("published result is reused without double billing", func(t *testing.T) {
		h := newCrashHarness(t)
		taskID := h.newTask(t)

		app := h.start(t, "archive.publish.completed")
		h.waitMarker(t, app, "archive.publish.completed")

		results := h.taskResults(t, taskID)
		if len(results) != 1 {
			t.Fatalf("task results before crash = %d, want 1", len(results))
		}
		publishedFileID := results[0].FileID
		usedBefore := h.userRecord(t).UsedBytes
		filesBefore := h.countFiles(t)
		app.stop()

		restarted := h.start(t, "")
		h.waitCompleted(t, restarted, taskID, true, "")

		resultsAfter := h.taskResults(t, taskID)
		if len(resultsAfter) != 1 || resultsAfter[0].FileID != publishedFileID {
			t.Fatalf("task results after recovery = %+v, want one %s", resultsAfter, publishedFileID)
		}
		if got := h.userRecord(t).UsedBytes; got != usedBefore {
			t.Fatalf("used bytes = %d after recovery, want %d", got, usedBefore)
		}
		if got := h.countFiles(t); got != filesBefore {
			t.Fatalf("file rows = %d after recovery, want %d", got, filesBefore)
		}
		h.assertZip(t, publishedFileID)
	})

	t.Run("cleanup resumes after restart", func(t *testing.T) {
		h := newCrashHarness(t)
		taskID := h.newTask(t)

		app := h.start(t, "archive.finalize.cleanup_pending")
		h.waitMarker(t, app, "archive.finalize.cleanup_pending")

		dir := filepath.Join(h.workDir, "task-"+taskID)
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("task directory should remain before cleanup: %v", err)
		}
		if len(h.taskResults(t, taskID)) != 1 {
			t.Fatal("result was not published before the cleanup crash")
		}
		app.stop()

		restarted := h.start(t, "")
		h.waitCompleted(t, restarted, taskID, true, "")

		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("task directory was not cleaned up: %v", err)
		}
		if len(h.taskResults(t, taskID)) != 1 {
			t.Fatal("cleanup recovery published another result")
		}
		if got := h.countFiles(t); got != int64(len(h.sourceIDs)+1) {
			t.Fatalf("file rows = %d, want %d", got, len(h.sourceIDs)+1)
		}
	})

	t.Run("completed task is not executed again", func(t *testing.T) {
		h := newCrashHarness(t)
		taskID := h.newTask(t)

		finished := time.Now()
		if err := h.db.Model(&model.Task{}).Where("id = ?", taskID).
			Updates(map[string]any{
				"status":      model.TaskStatusCompleted,
				"phase":       archive.PhaseFinalize,
				"attempts":    1,
				"finished_at": finished,
				"last_error":  "",
			}).Error; err != nil {
			t.Fatal(err)
		}

		app := h.start(t, "archive.prepare")
		time.Sleep(2500 * time.Millisecond)
		if _, err := os.Stat(app.markerPath); err == nil {
			t.Fatal("completed task was executed again")
		}

		stored := h.task(t, taskID)
		if stored.Status != model.TaskStatusCompleted || stored.Attempts != 1 {
			t.Fatalf("completed task changed: %+v", stored)
		}
	})
}

// TestArchiveCrashHelper 是子进程入口。父测试用 -test.run 只运行它，
// 通过环境变量传入完整配置，然后启动真实的 app.run 并等待被 SIGKILL。
func TestArchiveCrashHelper(t *testing.T) {
	if os.Getenv("CLOUDREVE_CRASH_HELPER") != "1" {
		t.Skip("crash helper process only")
	}

	var cfg config.Config
	if err := json.Unmarshal([]byte(os.Getenv("CLOUDREVE_CRASH_CONFIG")), &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "decode crash helper config: %v\n", err)
		os.Exit(2)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(context.Background(), cfg, logger, recoverUnfinishedUploads); err != nil {
		fmt.Fprintf(os.Stderr, "crash helper app exited: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

type crashHarness struct {
	db             *gorm.DB
	fileService    *file.Service
	user           *model.User
	sourceIDs      []string
	sourceContents map[string][]byte
	storageCfg     config.Storage
	databaseCfg    config.Database
	workDir        string
}

func newCrashHarness(t *testing.T) *crashHarness {
	t.Helper()

	db, err := database.OpenDSN(context.Background(), os.Getenv("CLOUDREVE_TEST_MYSQL_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close(db) })

	storageCfg := testStorageConfig(t, db)
	stores, err := newStorageRegistry(db, storageCfg)
	if err != nil {
		t.Fatal(err)
	}
	fileService := file.NewService(db, stores)

	user, err := auth.CreateUserWithQuota(
		context.Background(), db,
		fmt.Sprintf("crash-%d", time.Now().UnixNano()),
		"crash-test-password", 8<<20,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupE2EUser(t, db, user) })

	// 大文件放在第一个条目：压缩器只有在累计足够输入后才会把字节
	// 真正写到 .partial，这样“写到一半”的故障点是可观察的。
	large := make([]byte, 256<<10)
	var x uint32 = 0x12345678
	for i := range large {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		large[i] = byte(x)
	}
	contents := map[string][]byte{
		"large.bin": large,
		"alpha.txt": []byte("alpha crash content\n"),
		"beta.txt":  []byte("beta crash content\n"),
	}
	names := []string{"large.bin", "alpha.txt", "beta.txt"}
	sourceIDs := make([]string, 0, len(names))
	for _, name := range names {
		record, err := fileService.Upload(
			context.Background(), user.ID, "", name, bytes.NewReader(contents[name]),
		)
		if err != nil {
			t.Fatal(err)
		}
		sourceIDs = append(sourceIDs, record.ID)
	}

	return &crashHarness{
		db:             db,
		fileService:    fileService,
		user:           user,
		sourceIDs:      sourceIDs,
		sourceContents: contents,
		storageCfg:     storageCfg,
		databaseCfg:    testDatabaseConfig(t),
		workDir:        t.TempDir(),
	}
}

func (h *crashHarness) newTask(t *testing.T) string {
	t.Helper()

	state, err := archive.NewTaskState(h.sourceIDs, "", "bundle.zip", archive.Options{})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	id, err := idgen.New()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := task.NewService(h.db).Create(context.Background(), task.CreateParams{
		ID:           id,
		Type:         archive.TaskType,
		OwnerID:      h.user.ID,
		Phase:        archive.PhasePrepare,
		StateVersion: archive.TaskStateVersion,
		StateJSON:    string(encoded),
		MaxAttempts:  3,
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

type crashApp struct {
	cmd        *exec.Cmd
	address    string
	markerPath string
	logPath    string
	logFile    *os.File
	done       chan struct{}
	waitErr    error
	stopOnce   sync.Once
}

func (h *crashHarness) start(t *testing.T, failpointName string) *crashApp {
	t.Helper()

	cfg := config.Config{
		HTTP:   config.HTTP{Listen: freeAddress(t), ShutdownTimeoutSeconds: 1},
		Upload: config.Upload{MaxParallelChunks: 2},
		Task: config.Task{
			Workers: 1, QueueCapacity: 8, TimeoutSeconds: 30,
			RetentionSeconds: 60, MaxTerminalRecords: 64,
			MaxPendingTasks:     100,
			ScanIntervalSeconds: 1, ScanBatchSize: 50,
			WorkDir: h.workDir,
		},
		Storage:  h.storageCfg,
		Database: h.databaseCfg,
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	markerPath := filepath.Join(dir, "marker")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(exe, "-test.run=^TestArchiveCrashHelper$", "-test.timeout=2m")
	cmd.Env = append(os.Environ(),
		"CLOUDREVE_CRASH_HELPER=1",
		"CLOUDREVE_CRASH_CONFIG="+string(raw),
		"CLOUDREVE_FAILPOINT="+failpointName,
		"CLOUDREVE_FAILPOINT_MARKER="+markerPath,
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}

	app := &crashApp{
		cmd:        cmd,
		address:    cfg.HTTP.Listen,
		markerPath: markerPath,
		logPath:    logPath,
		logFile:    logFile,
		done:       make(chan struct{}),
	}
	go func() {
		app.waitErr = cmd.Wait()
		close(app.done)
	}()
	t.Cleanup(app.stop)

	h.waitReady(t, app)
	return app
}

func (h *crashHarness) waitReady(t *testing.T, app *crashApp) {
	t.Helper()

	client := &http.Client{Timeout: 500 * time.Millisecond}
	defer client.CloseIdleConnections()

	deadline := time.Now().Add(30 * time.Second)
	for {
		select {
		case <-app.done:
			t.Fatalf("crash helper exited before ready: %v\n%s", app.waitErr, app.log())
		default:
		}

		response, err := client.Get("http://" + app.address + "/healthz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("crash helper did not become ready\n%s", app.log())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (h *crashHarness) waitMarker(t *testing.T, app *crashApp, name string) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(app.markerPath); err == nil {
			return
		}
		select {
		case <-app.done:
			t.Fatalf("crash helper exited before failpoint %s: %v\n%s",
				name, app.waitErr, app.log())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("failpoint %s was not reached\n%s", name, app.log())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (h *crashHarness) waitCompleted(
	t *testing.T,
	app *crashApp,
	taskID string,
	waitCleanup bool,
	forbiddenMarker string,
) model.Task {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for {
		if forbiddenMarker != "" {
			if _, err := os.Stat(forbiddenMarker); err == nil {
				t.Fatalf("recovery hit forbidden failpoint marker %s", forbiddenMarker)
			}
		}

		var record model.Task
		err := h.db.First(&record, "id = ?", taskID).Error
		if err == nil {
			if record.Status == model.TaskStatusFailed {
				t.Fatalf("recovered task failed: %+v", record)
			}
			if record.Status == model.TaskStatusCompleted &&
				(!waitCleanup || !record.CleanupPending) {
				return record
			}
		}
		select {
		case <-app.done:
			t.Fatalf("crash helper exited before task completed: %v\n%s",
				app.waitErr, app.log())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %s did not complete in time\n%s", taskID, app.log())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (h *crashHarness) task(t *testing.T, id string) model.Task {
	t.Helper()
	var record model.Task
	if err := h.db.First(&record, "id = ?", id).Error; err != nil {
		t.Fatal(err)
	}
	return record
}

func (h *crashHarness) taskResults(t *testing.T, taskID string) []model.TaskResult {
	t.Helper()
	var results []model.TaskResult
	if err := h.db.Where("task_id = ?", taskID).Find(&results).Error; err != nil {
		t.Fatal(err)
	}
	return results
}

func (h *crashHarness) userRecord(t *testing.T) model.User {
	t.Helper()
	var user model.User
	if err := h.db.First(&user, h.user.ID).Error; err != nil {
		t.Fatal(err)
	}
	return user
}

func (h *crashHarness) countFiles(t *testing.T) int64 {
	t.Helper()
	var count int64
	if err := h.db.Model(&model.File{}).
		Where("owner_id = ? AND kind = ?", h.user.ID, model.KindFile).
		Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

func (h *crashHarness) assertSingleResult(t *testing.T, taskID string) {
	t.Helper()

	record := h.task(t, taskID)
	if record.Status != model.TaskStatusCompleted {
		t.Fatalf("task status = %q, want completed", record.Status)
	}
	if record.ResultFileID == nil || *record.ResultFileID == "" {
		t.Fatal("completed task has no result file")
	}
	if record.CleanupPending {
		t.Fatal("completed task still has cleanup pending")
	}

	results := h.taskResults(t, taskID)
	if len(results) != 1 {
		t.Fatalf("task results = %d, want exactly 1", len(results))
	}
	if results[0].FileID != *record.ResultFileID {
		t.Fatalf("task result file = %q, task projection = %q",
			results[0].FileID, *record.ResultFileID)
	}
	if got := h.countFiles(t); got != int64(len(h.sourceIDs)+1) {
		t.Fatalf("file rows = %d, want %d", got, len(h.sourceIDs)+1)
	}
	if user := h.userRecord(t); user.ReservedBytes != 0 {
		t.Fatalf("reserved bytes = %d, want 0", user.ReservedBytes)
	}
	h.assertZip(t, *record.ResultFileID)
}

func (h *crashHarness) assertZip(t *testing.T, fileID string) {
	t.Helper()

	_, rc, err := h.fileService.Open(context.Background(), h.user.ID, fileID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	closeErr := rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}

	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("result is not a valid ZIP: %v", err)
	}
	if len(reader.File) != len(h.sourceContents) {
		t.Fatalf("ZIP entries = %d, want %d", len(reader.File), len(h.sourceContents))
	}
	for _, entry := range reader.File {
		want, ok := h.sourceContents[entry.Name]
		if !ok {
			t.Fatalf("unexpected ZIP entry %q", entry.Name)
		}
		rc, err := entry.Open()
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ZIP entry %q content changed: %q", entry.Name, got)
		}
	}
}

func (a *crashApp) stop() {
	a.stopOnce.Do(func() {
		if a.cmd != nil && a.cmd.Process != nil {
			_ = a.cmd.Process.Kill()
		}
		<-a.done
		if a.logFile != nil {
			_ = a.logFile.Close()
		}
	})
}

func (a *crashApp) log() string {
	data, err := os.ReadFile(a.logPath)
	if err != nil {
		return fmt.Sprintf("<read log: %v>", err)
	}
	return strings.TrimSpace(string(data))
}
