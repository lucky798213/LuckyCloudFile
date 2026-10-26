package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"luckyCloudFile/internal/file"
	"luckyCloudFile/internal/model"
)

// uploadSessionJSONRequest 构造一个带 JSON 请求体的上传会话请求。
func uploadSessionJSONRequest(t *testing.T, method, path, body, token string) *http.Request {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req = authorize(req, token)
	}
	return req
}

// addFakeDirectory 在假文件服务里放一个目录，模拟已存在的目标目录。
func addFakeDirectory(fake *fakeFileService, id string, ownerID int) {
	fake.records[id] = file.File{ID: id, Name: id, Kind: model.KindDirectory}
	fake.owners[id] = ownerID
}

func TestUploadSessionHTTPCreateAndGet(t *testing.T) {
	router, fake := newTestRouter(t)
	addFakeDirectory(fake, "dir-a", testUserAID)

	// 空文件：size=0 必须合法。
	created := httptest.NewRecorder()
	router.ServeHTTP(created, uploadSessionJSONRequest(
		t, http.MethodPost, "/api/upload-sessions",
		`{"parent_id":"dir-a","name":"照片.jpg","size":0}`,
		testTokenA,
	))
	if created.Code != http.StatusCreated {
		t.Fatalf("create: wanted 201, got %d: %s", created.Code, created.Body.String())
	}

	var info file.UploadSessionInfo
	if err := json.Unmarshal(created.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.ID == "" || info.ParentID != "dir-a" || info.Name != "照片.jpg" {
		t.Fatalf("unexpected session info: %+v", info)
	}
	if info.ExpectedSize != 0 || info.ActualSize != 0 {
		t.Fatalf("unexpected sizes: %+v", info)
	}
	if info.Status != model.UploadStatusPending {
		t.Fatalf("new session status = %q, want %q", info.Status, model.UploadStatusPending)
	}
	if info.ResultFileID != nil {
		t.Fatalf("new session already has a result file: %q", *info.ResultFileID)
	}
	if !info.ExpiresAt.After(time.Now()) {
		t.Fatalf("new session expiry is not in the future: %v", info.ExpiresAt)
	}

	// 返回值只能包含前端需要的字段，不能泄露 EntityID / StorageKey 等内部信息。
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(created.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"entity_id", "storage_key", "storage_policy_id", "owner_id"} {
		if _, ok := fields[forbidden]; ok {
			t.Fatalf("response leaked internal field %q: %s", forbidden, created.Body.String())
		}
	}

	// 自己的会话可以查询。
	got := httptest.NewRecorder()
	router.ServeHTTP(got, uploadSessionJSONRequest(
		t, http.MethodGet, "/api/upload-sessions/"+info.ID, "", testTokenA,
	))
	if got.Code != http.StatusOK {
		t.Fatalf("get own session: wanted 200, got %d: %s", got.Code, got.Body.String())
	}
	var fetched file.UploadSessionInfo
	if err := json.Unmarshal(got.Body.Bytes(), &fetched); err != nil {
		t.Fatal(err)
	}
	if fetched.ID != info.ID || fetched.Name != info.Name ||
		fetched.ParentID != info.ParentID || fetched.Status != model.UploadStatusPending {
		t.Fatalf("fetched session = %+v, want %+v", fetched, info)
	}

	// 别人的会话和不存在的会话必须是同一个 404 和同一个 code。
	for _, token := range []string{testTokenB, testTokenA} {
		path := "/api/upload-sessions/" + info.ID
		if token == testTokenA {
			path = "/api/upload-sessions/does-not-exist"
		}
		missing := httptest.NewRecorder()
		router.ServeHTTP(missing, uploadSessionJSONRequest(
			t, http.MethodGet, path, "", token,
		))
		if missing.Code != http.StatusNotFound {
			t.Fatalf("get missing/foreign session: wanted 404, got %d: %s",
				missing.Code, missing.Body.String())
		}
		var body ErrorResponse
		if err := json.Unmarshal(missing.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Code != "upload_session_not_found" {
			t.Fatalf("error code = %q, want upload_session_not_found", body.Code)
		}
	}
}

func TestUploadSessionHTTPRejectsInvalidCreate(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		token    string
		wantCode int
		wantErr  string
	}{
		{
			name:     "missing size",
			body:     `{"parent_id":"dir-a","name":"x.bin"}`,
			token:    testTokenA,
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_size",
		},
		{
			name:     "negative size",
			body:     `{"parent_id":"dir-a","name":"x.bin","size":-1}`,
			token:    testTokenA,
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_size",
		},
		{
			name:     "too large",
			body:     fmt.Sprintf(`{"parent_id":"dir-a","name":"x.bin","size":%d}`, file.MaxUploadBytes+1),
			token:    testTokenA,
			wantCode: http.StatusRequestEntityTooLarge,
			wantErr:  "file_too_large",
		},
		{
			name:     "invalid name",
			body:     `{"parent_id":"dir-a","name":"../escape","size":0}`,
			token:    testTokenA,
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_name",
		},
		{
			name:     "foreign parent",
			body:     `{"parent_id":"dir-b","name":"x.bin","size":0}`,
			token:    testTokenA,
			wantCode: http.StatusNotFound,
			wantErr:  "parent_not_found",
		},
		{
			name:     "non-directory parent",
			body:     `{"parent_id":"file-a","name":"x.bin","size":0}`,
			token:    testTokenA,
			wantCode: http.StatusBadRequest,
			wantErr:  "not_a_directory",
		},
		{
			name:     "invalid json",
			body:     `not json`,
			token:    testTokenA,
			wantCode: http.StatusBadRequest,
			wantErr:  "invalid_json",
		},
		{
			name:     "not logged in",
			body:     `{"parent_id":"dir-a","name":"x.bin","size":0}`,
			token:    "",
			wantCode: http.StatusUnauthorized,
			wantErr:  "unauthorized",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			router, fake := newTestRouter(t)
			addFakeDirectory(fake, "dir-a", testUserAID)
			addFakeDirectory(fake, "dir-b", testUserBID)
			fake.records["file-a"] = file.File{ID: "file-a", Name: "a.txt", Kind: model.KindFile}
			fake.owners["file-a"] = testUserAID

			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, uploadSessionJSONRequest(
				t, http.MethodPost, "/api/upload-sessions", test.body, test.token,
			))
			if recorder.Code != test.wantCode {
				t.Fatalf("wanted %d, got %d: %s",
					test.wantCode, recorder.Code, recorder.Body.String())
			}
			var body ErrorResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Code != test.wantErr {
				t.Fatalf("error code = %q, want %q", body.Code, test.wantErr)
			}
			if got := len(fake.sessions); got != 0 {
				t.Fatalf("invalid create left %d session(s)", got)
			}
		})
	}
}
