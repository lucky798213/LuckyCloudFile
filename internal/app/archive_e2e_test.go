package app

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"

	"luckyCloudFile/internal/archive"
	"luckyCloudFile/internal/auth"
	"luckyCloudFile/internal/config"
	"luckyCloudFile/internal/database"
	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/model"
)

// TestArchiveTaskEndToEnd 是 ZIP 链路的真实验收：
//
//	真实登录 → 上传源文件 → 提交打包 → 轮询完成
//	→ 下载结果 → 校验 ZIP 内容 → 检查容量与残留
//
// 它启动真实的 app.run（真实 MySQL、真实磁盘、真实 HTTP 路由），不使用
// 假文件服务或假队列。因为会创建账号、写入共享测试库，默认跳过；需要显式：
//
//	CLOUDREVE_ARCHIVE_E2E=1 \
//	CLOUDREVE_TEST_MYSQL_DSN='lucky:lucky@tcp(127.0.0.1:3306)/luckycloudfile_test?parseTime=true' \
//	go test ./internal/app -run TestArchiveTaskEndToEnd -v
func TestArchiveTaskEndToEnd(t *testing.T) {
	if os.Getenv("CLOUDREVE_ARCHIVE_E2E") != "1" {
		t.Skip("set CLOUDREVE_ARCHIVE_E2E=1 to run the real archive chain")
	}
	dsn := os.Getenv("CLOUDREVE_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set CLOUDREVE_TEST_MYSQL_DSN to run the real archive chain")
	}

	ctx := context.Background()
	db, err := database.OpenDSN(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close(db) })

	const quotaBytes = int64(8 << 20)
	username := fmt.Sprintf("archive-e2e-%d", time.Now().UnixNano())
	password := "archive-e2e-password"
	user, err := auth.CreateUserWithQuota(ctx, db, username, password, quotaBytes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupE2EUser(t, db, user) })

	// 先准备好所有临时目录，再改 TMPDIR；否则 t.TempDir 自己也建在新 TMPDIR 里，
	// 后面的“临时 ZIP 已清理”断言就会把测试目录当成残留。
	storageCfg := testStorageConfig(t, db)
	archiveTempDir := t.TempDir()
	t.Setenv("TMPDIR", archiveTempDir)

	cfg := config.Config{
		HTTP:   config.HTTP{Listen: freeAddress(t), ShutdownTimeoutSeconds: 5},
		Upload: config.Upload{MaxParallelChunks: 2},
		Task: config.Task{
			Workers: 2, QueueCapacity: 8, TimeoutSeconds: 60,
			RetentionSeconds: 60, MaxTerminalRecords: 64,
			// 可恢复 ZIP 必须写在配置的任务工作目录，而不是系统临时目录。
			WorkDir: archiveTempDir,
		},
		Storage:  storageCfg,
		Database: testDatabaseConfig(t),
	}

	client := &http.Client{Timeout: 15 * time.Second}
	t.Cleanup(client.CloseIdleConnections)

	serverCtx, stopServer := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- run(serverCtx, cfg, testLogger(), recoverUnfinishedUploads) }()
	serverExited := false
	t.Cleanup(func() {
		stopServer()
		if serverExited {
			return
		}
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("application exited with error: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Errorf("application did not shut down")
		}
	})

	base := "http://" + cfg.HTTP.Listen
	waitE2EServer(t, client, base, runDone, &serverExited)
	t.Log("PASS: real server ready with MySQL, storage and task queue")

	// 1. 真实登录。
	loginBody, _ := json.Marshal(map[string]string{"username": username, "password": password})
	loginResponse := e2eRequest(t, client, http.MethodPost, base+"/api/auth/login", "", loginBody, "application/json", http.StatusOK)
	var login struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(loginResponse.body, &login); err != nil {
		t.Fatal(err)
	}
	if login.Token == "" {
		t.Fatal("login returned an empty token")
	}
	token := login.Token
	t.Log("PASS: logged in over HTTP and received a bearer token")

	// 2. 上传两个源文件。
	sources := map[string][]byte{
		"alpha.txt": []byte("alpha from the real chain\n"),
		"beta.txt":  []byte("beta from the real chain\n"),
	}
	uploadOrder := []string{"alpha.txt", "beta.txt"}
	sourceFiles := make(map[string]file.File, len(sources))
	var sourceBytes int64
	for _, name := range uploadOrder {
		record := e2eUpload(t, client, base, token, name, sources[name])
		if record.ID == "" || record.Name != name || record.Size != int64(len(sources[name])) {
			t.Fatalf("unexpected uploaded file: %+v", record)
		}
		sourceFiles[name] = record
		sourceBytes += record.Size
	}
	t.Logf("PASS: uploaded %d source files (%d bytes) over HTTP", len(sourceFiles), sourceBytes)

	// 3. 提交打包任务。
	fileIDs := make([]string, 0, len(uploadOrder))
	for _, name := range uploadOrder {
		fileIDs = append(fileIDs, sourceFiles[name].ID)
	}
	submitBody, _ := json.Marshal(map[string]any{"file_ids": fileIDs, "name": "bundle.zip"})
	submitResponse := e2eRequest(t, client, http.MethodPost, base+"/api/archive-tasks", token, submitBody, "application/json", http.StatusAccepted)
	var submitted struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(submitResponse.body, &submitted); err != nil {
		t.Fatal(err)
	}
	if submitted.TaskID == "" {
		t.Fatal("archive task submission returned an empty task ID")
	}
	t.Logf("PASS: submitted archive task %s", submitted.TaskID)

	// 4. 轮询到终态。
	task := waitE2ETask(t, client, base, token, submitted.TaskID)
	if task.Status != "completed" {
		t.Fatalf("task failed: status=%s stage=%s error=%s", task.Status, task.Stage, task.Error)
	}
	if task.TotalFiles != len(uploadOrder) || task.ProcessedFiles != len(uploadOrder) {
		t.Fatalf("task progress = %d/%d, want %d/%d",
			task.ProcessedFiles, task.TotalFiles, len(uploadOrder), len(uploadOrder))
	}
	if task.ResultFileID == "" {
		t.Fatal("completed task has no result file ID")
	}
	t.Logf("PASS: task completed with result file %s", task.ResultFileID)

	// 任务表和 task_results 必须一起收敛：phase 是最后一步 finalize，
	// 结果凭据指向同一个文件，且没有遗留清理责任。
	var storedTask model.Task
	if err := db.First(&storedTask, "id = ?", submitted.TaskID).Error; err != nil {
		t.Fatal(err)
	}
	if storedTask.Status != model.TaskStatusCompleted ||
		storedTask.Phase != archive.PhaseFinalize ||
		storedTask.CleanupPending ||
		storedTask.ResultFileID == nil ||
		*storedTask.ResultFileID != task.ResultFileID {
		t.Fatalf("stored task did not converge: %+v", storedTask)
	}
	var taskResult model.TaskResult
	if err := db.First(&taskResult, "task_id = ?", submitted.TaskID).Error; err != nil {
		t.Fatal(err)
	}
	if taskResult.OwnerID != user.ID || taskResult.FileID != task.ResultFileID {
		t.Fatalf("task result = %+v, want owner %d file %s",
			taskResult, user.ID, task.ResultFileID)
	}
	t.Log("PASS: task phase and task_results converged to finalize")

	// 5. 下载 ZIP，校验响应头和字节数。
	download := e2eRequest(t, client, http.MethodGet, base+"/api/files/"+task.ResultFileID, token, nil, "", http.StatusOK)
	if got := download.header.Get("Content-Length"); got != strconv.Itoa(len(download.body)) {
		t.Fatalf("Content-Length = %q, body = %d bytes", got, len(download.body))
	}
	if disposition := download.header.Get("Content-Disposition"); !strings.Contains(disposition, "bundle.zip") {
		t.Fatalf("Content-Disposition = %q, want bundle.zip", disposition)
	}
	t.Logf("PASS: downloaded result ZIP (%d bytes)", len(download.body))

	// 6. 校验 ZIP 内容和源文件一字节不差。
	reader, err := zip.NewReader(bytes.NewReader(download.body), int64(len(download.body)))
	if err != nil {
		t.Fatalf("result is not a valid ZIP: %v", err)
	}
	if len(reader.File) != len(sources) {
		t.Fatalf("ZIP has %d entries, want %d", len(reader.File), len(sources))
	}
	seen := make(map[string]bool, len(reader.File))
	for _, entry := range reader.File {
		want, ok := sources[entry.Name]
		if !ok {
			t.Fatalf("unexpected ZIP entry %q", entry.Name)
		}
		if seen[entry.Name] {
			t.Fatalf("duplicate ZIP entry %q", entry.Name)
		}
		seen[entry.Name] = true
		rc, err := entry.Open()
		if err != nil {
			t.Fatalf("open ZIP entry %q: %v", entry.Name, err)
		}
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		if readErr != nil {
			t.Fatalf("read ZIP entry %q: %v", entry.Name, readErr)
		}
		if closeErr != nil {
			t.Fatalf("close ZIP entry %q: %v", entry.Name, closeErr)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ZIP entry %q content = %q, want %q", entry.Name, got, want)
		}
	}
	t.Log("PASS: ZIP entries match the uploaded source bytes")

	// 7. 检查容量账目：used = 两个源文件 + 结果 ZIP，reserved 必须归零。
	var storedUser model.User
	if err := db.First(&storedUser, user.ID).Error; err != nil {
		t.Fatal(err)
	}
	wantUsed := sourceBytes + int64(len(download.body))
	if storedUser.QuotaBytes != quotaBytes {
		t.Fatalf("quota_bytes = %d, want %d", storedUser.QuotaBytes, quotaBytes)
	}
	if storedUser.UsedBytes != wantUsed {
		t.Fatalf("used_bytes = %d, want %d", storedUser.UsedBytes, wantUsed)
	}
	if storedUser.ReservedBytes != 0 {
		t.Fatalf("reserved_bytes = %d after completion, want 0", storedUser.ReservedBytes)
	}
	if storedUser.UsedBytes+storedUser.ReservedBytes > storedUser.QuotaBytes {
		t.Fatal("used + reserved exceeds quota")
	}
	t.Logf("PASS: quota accounting used=%d reserved=0 quota=%d", storedUser.UsedBytes, storedUser.QuotaBytes)

	// 8. 检查数据库残留：会话已完成且预留归零，实体都是 active/ref=1，文件数符合预期。
	var sessions []model.UploadSession
	if err := db.Where("owner_id = ?", user.ID).Find(&sessions).Error; err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 {
		t.Fatalf("upload sessions = %d, want exactly the completed archive session", len(sessions))
	}
	session := sessions[0]
	if session.Status != model.UploadStatusCompleted || session.ReservedBytes != 0 {
		t.Fatalf("archive session left non-terminal state: %+v", session)
	}
	if session.ResultFileID == nil || *session.ResultFileID != task.ResultFileID {
		t.Fatalf("archive session result = %v, want %s", session.ResultFileID, task.ResultFileID)
	}
	if user.RootFileID == nil || session.ParentID != *user.RootFileID {
		t.Fatalf("archive session parent = %q, want root %v", session.ParentID, user.RootFileID)
	}
	// 任务发布会话必须绑定 task_id，完成事务才能和 task_results 一起提交。
	if session.TaskID == nil || *session.TaskID != submitted.TaskID {
		t.Fatalf("archive session task = %v, want %s", session.TaskID, submitted.TaskID)
	}

	var files []model.File
	if err := db.Where("owner_id = ?", user.ID).Find(&files).Error; err != nil {
		t.Fatal(err)
	}
	if len(files) != 1+len(sources)+1 {
		t.Fatalf("files = %d, want root + %d sources + result", len(files), len(sources))
	}

	var entityIDs []int64
	if err := db.Model(&model.File{}).
		Where("owner_id = ? AND entity_id IS NOT NULL", user.ID).
		Pluck("entity_id", &entityIDs).Error; err != nil {
		t.Fatal(err)
	}
	var entities []model.Entity
	if err := db.Where("id IN ?", entityIDs).Find(&entities).Error; err != nil {
		t.Fatal(err)
	}
	if len(entities) != len(sources)+1 {
		t.Fatalf("entities = %d, want %d source + 1 result", len(entities), len(sources))
	}
	storageKeys := make(map[string]bool, len(entities))
	for _, entity := range entities {
		if entity.Status != model.EntityStatusActive || entity.RefCount != 1 {
			t.Fatalf("unexpected entity state: %+v", entity)
		}
		if entity.StoragePolicyID != user.DefaultStoragePolicyID {
			t.Fatalf("entity storage policy = %d, want %d", entity.StoragePolicyID, user.DefaultStoragePolicyID)
		}
		storageKeys[entity.StorageKey] = true
	}
	t.Log("PASS: no pending/canceled upload sessions and all entities are active")

	// 9. 检查磁盘残留：只有三份 active 内容，没有 .upload- 临时文件。
	entries, err := os.ReadDir(cfg.Storage.LocalRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(storageKeys) {
		t.Fatalf("storage has %d entries, want %d", len(entries), len(storageKeys))
	}
	for _, entry := range entries {
		if entry.IsDir() {
			t.Fatalf("unexpected directory in storage root: %s", entry.Name())
		}
		if strings.HasPrefix(entry.Name(), ".upload-") {
			t.Fatalf("leftover temporary upload file: %s", entry.Name())
		}
		if !storageKeys[entry.Name()] {
			t.Fatalf("unexpected file in storage root: %s", entry.Name())
		}
	}
	if entries, err := os.ReadDir(archiveTempDir); err != nil {
		t.Fatal(err)
	} else if len(entries) != 0 {
		t.Fatalf("archive temporary ZIP was not cleaned up: %d file(s)", len(entries))
	}
	t.Log("PASS: storage contains only the three active objects; no temp residue")

	// 10. 根目录列表只有两个源文件 + 结果 ZIP，没有隐藏的中间文件。
	listing := e2eRequest(t, client, http.MethodGet, base+"/api/files", token, nil, "", http.StatusOK)
	var listed file.ListResult
	if err := json.Unmarshal(listing.body, &listed); err != nil {
		t.Fatal(err)
	}
	if listed.Total != int64(1+len(sources)) || len(listed.Items) != 1+len(sources) {
		t.Fatalf("root listing = %d items (total %d), want %d", len(listed.Items), listed.Total, 1+len(sources))
	}
	listedNames := make(map[string]bool, len(listed.Items))
	for _, item := range listed.Items {
		listedNames[item.Name] = true
	}
	for _, name := range append(uploadOrder, "bundle.zip") {
		if !listedNames[name] {
			t.Fatalf("root listing is missing %q: %+v", name, listed.Items)
		}
	}
	t.Log("PASS: root listing shows exactly the two sources and bundle.zip")

	t.Log("PASS: real archive chain verified end to end")
}

// e2eHTTPResponse 保留状态、头和完整正文，供下载头和 ZIP 校验使用。
type e2eHTTPResponse struct {
	status int
	header http.Header
	body   []byte
}

func e2eRequest(
	t *testing.T,
	client *http.Client,
	method, url, token string,
	body []byte,
	contentType string,
	wantStatus int,
) e2eHTTPResponse {
	t.Helper()

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}

	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("%s %s: read body: %v", method, url, err)
	}
	if response.StatusCode != wantStatus {
		t.Fatalf("%s %s: status %d, want %d, body %s",
			method, url, response.StatusCode, wantStatus, data)
	}
	return e2eHTTPResponse{status: response.StatusCode, header: response.Header, body: data}
}

func e2eUpload(
	t *testing.T,
	client *http.Client,
	base, token, name string,
	content []byte,
) file.File {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	response := e2eRequest(
		t, client, http.MethodPost, base+"/api/files", token,
		body.Bytes(), writer.FormDataContentType(), http.StatusCreated,
	)
	var record file.File
	if err := json.Unmarshal(response.body, &record); err != nil {
		t.Fatal(err)
	}
	return record
}

type e2eTaskStatus struct {
	TaskID         string `json:"task_id"`
	Status         string `json:"status"`
	Stage          string `json:"stage"`
	ProcessedFiles int    `json:"processed_files"`
	TotalFiles     int    `json:"total_files"`
	Error          string `json:"error"`
	ResultFileID   string `json:"result_file_id"`
}

func waitE2ETask(t *testing.T, client *http.Client, base, token, taskID string) e2eTaskStatus {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for {
		response := e2eRequest(
			t, client, http.MethodGet, base+"/api/tasks/"+taskID, token,
			nil, "", http.StatusOK,
		)
		var task e2eTaskStatus
		if err := json.Unmarshal(response.body, &task); err != nil {
			t.Fatal(err)
		}
		switch task.Status {
		case "queued", "running", "retry_wait":
		case "completed", "failed":
			return task
		default:
			t.Fatalf("unexpected task status %q", task.Status)
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %s did not finish in time: %+v", taskID, task)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitE2EServer(
	t *testing.T,
	client *http.Client,
	base string,
	runDone <-chan error,
	serverExited *bool,
) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for {
		response, err := client.Get(base + "/healthz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case runErr := <-runDone:
			*serverExited = true
			t.Fatalf("application exited before becoming ready: %v", runErr)
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("application did not become ready")
		}
	}
}

// cleanupE2EUser 只删除这次测试创建的账号及其数据，不触碰共享测试库里的
// 其他用户。删除顺序必须服从外键：会话/上传会话 → File → Entity → User。
func cleanupE2EUser(t *testing.T, db *gorm.DB, user *model.User) {
	t.Helper()

	if err := db.Where("user_id = ?", user.ID).Delete(&model.AuthSession{}).Error; err != nil {
		t.Logf("cleanup auth sessions for %s: %v", user.Username, err)
	}
	if err := db.Where("owner_id = ?", user.ID).Delete(&model.UploadSession{}).Error; err != nil {
		t.Logf("cleanup upload sessions for %s: %v", user.Username, err)
	}
	if err := db.Where("owner_id = ?", user.ID).Delete(&model.TaskResult{}).Error; err != nil {
		t.Logf("cleanup task results for %s: %v", user.Username, err)
	}

	var entityIDs []int64
	if err := db.Model(&model.File{}).
		Where("owner_id = ? AND entity_id IS NOT NULL", user.ID).
		Pluck("entity_id", &entityIDs).Error; err != nil {
		t.Logf("collect entities for %s: %v", user.Username, err)
	}

	if err := db.Where("owner_id = ? AND parent_id IS NOT NULL", user.ID).
		Delete(&model.File{}).Error; err != nil {
		t.Logf("cleanup child files for %s: %v", user.Username, err)
	}
	if user.RootFileID != nil {
		if err := db.Where("id = ? AND owner_id = ?", *user.RootFileID, user.ID).
			Delete(&model.File{}).Error; err != nil {
			t.Logf("cleanup root file for %s: %v", user.Username, err)
		}
	}
	if len(entityIDs) > 0 {
		if err := db.Where("id IN ?", entityIDs).Delete(&model.Entity{}).Error; err != nil {
			t.Logf("cleanup entities for %s: %v", user.Username, err)
		}
	}
	if err := db.Delete(&model.User{}, user.ID).Error; err != nil {
		t.Logf("cleanup user %s: %v", user.Username, err)
	}
}
