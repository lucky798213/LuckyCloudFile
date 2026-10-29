package storage

import (
	"context"
	"time"
)

type UploadedPart struct {
	Number int    `json:"number"`
	Size   int64  `json:"size"`
	ETag   string `json:"etag"`
}
type SignedPart struct {
	URL       string            `json:"url"`
	Headers   map[string]string `json:"headers"`
	ExpiresAt time.Time         `json:"expires_at"`
}

// MultipartStore is optional: local storage need not emulate cloud uploads.
type MultipartStore interface {
	Initiate(context.Context, string) (string, error)
	SignPart(context.Context, string, string, int, time.Duration) (SignedPart, error)
	ListParts(context.Context, string, string) ([]UploadedPart, error)
	Complete(context.Context, string, string, []UploadedPart) error
	Abort(context.Context, string, string) error
}
