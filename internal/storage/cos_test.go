package storage

import (
	"context"
	"fmt"
	cos "github.com/tencentyun/cos-go-sdk-v5"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Exercise the actual SDK transport, including COS's 200-with-error response.
func TestCOSMultipartBoundary(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/uploads/direct-session" {
			t.Errorf("unexpected object: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/xml")
		switch r.Method {
		case http.MethodGet:
			if r.URL.Query().Get("uploadId") != "upload-id" {
				t.Errorf("missing upload ID")
			}
			if r.URL.Query().Get("part-number-marker") == "0" {
				fmt.Fprint(w, `<ListPartsResult><IsTruncated>true</IsTruncated><NextPartNumberMarker>1</NextPartNumberMarker><Part><PartNumber>1</PartNumber><Size>8388608</Size><ETag>first</ETag></Part></ListPartsResult>`)
			} else {
				fmt.Fprint(w, `<ListPartsResult><IsTruncated>false</IsTruncated><Part><PartNumber>2</PartNumber><Size>5</Size><ETag>last</ETag></Part></ListPartsResult>`)
			}
		case http.MethodPost:
			fmt.Fprint(w, `<Error><Code>InvalidPart</Code><Message>bad part</Message></Error>`)
		}
	}))
	defer server.Close()
	s, e := NewCOS(COSOptions{BucketURL: server.URL, Prefix: "uploads", SecretID: "test-id", SecretKey: "test-key"})
	if e != nil {
		t.Fatal(e)
	}
	// Local TLS fixture is the only trusted test endpoint.
	s.client = cos.NewClient(s.client.BaseURL, server.Client())
	parts, e := s.ListParts(context.Background(), "direct-session", "upload-id")
	if e != nil {
		t.Fatal(e)
	}
	if len(parts) != 2 || parts[1].Number != 2 {
		t.Fatalf("pagination: %+v", parts)
	}
	if e = s.Complete(context.Background(), "direct-session", "upload-id", parts); e == nil {
		t.Fatal("embedded merge error was accepted")
	}
	signed, e := s.SignPart(context.Background(), "direct-session", "upload-id", 2, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	for _, want := range []string{"uploadId=upload-id", "partNumber=2", "q-signature="} {
		if !strings.Contains(signed.URL, want) {
			t.Fatalf("signature missing %q", want)
		}
	}
}
