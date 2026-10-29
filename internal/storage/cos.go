package storage

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

type COSOptions struct{ BucketURL, Prefix, SecretID, SecretKey, SessionToken string }
type COS struct {
	client  *cos.Client
	options COSOptions
}

func NewCOS(o COSOptions) (*COS, error) {
	u, e := url.Parse(o.BucketURL)
	if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.Trim(u.Path, "/") != "" {
		return nil, errors.New("COS bucket URL must be an HTTPS origin")
	}
	if o.SecretID == "" || o.SecretKey == "" {
		return nil, errors.New("COS secret ID and key are required")
	}
	for _, p := range strings.Split(o.Prefix, "/") {
		if p == ".." || p == "." || strings.ContainsAny(p, "\\\x00") {
			return nil, errors.New("invalid COS prefix")
		}
	}
	c := cos.NewClient(&cos.BaseURL{BucketURL: u}, &http.Client{Transport: &cos.AuthorizationTransport{SecretID: o.SecretID, SecretKey: o.SecretKey, SessionToken: o.SessionToken}})
	return &COS{c, o}, nil
}
func (s *COS) key(key string) (string, error) {
	if key == "" || key == "." || key == ".." || strings.ContainsAny(key, "/\\\x00") {
		return "", errors.New("invalid storage key")
	}
	if p := strings.Trim(s.options.Prefix, "/"); p != "" {
		return p + "/" + key, nil
	}
	return key, nil
}
func cosError(e error) error {
	var ce *cos.ErrorResponse
	if errors.As(e, &ce) && (ce.Code == "NoSuchKey" || ce.Code == "NoSuchUpload") {
		return fs.ErrNotExist
	}
	return e
}
func (s *COS) Save(ctx context.Context, key string, r io.Reader, max int64) (int64, error) {
	k, e := s.key(key)
	if e != nil {
		return 0, e
	}
	if max < 0 || max == 1<<63-1 {
		return 0, errors.New("invalid size")
	}
	f, e := os.CreateTemp("", "lucky-cos-*")
	if e != nil {
		return 0, e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	n, e := io.Copy(f, io.LimitReader(r, max+1))
	if e != nil {
		return 0, e
	}
	if n > max {
		return 0, ErrTooLarge
	}
	if _, e = f.Seek(0, 0); e != nil {
		return 0, e
	}
	_, e = s.client.Object.Put(ctx, k, f, &cos.ObjectPutOptions{ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{ContentLength: n}})
	return n, e
}
func (s *COS) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	k, e := s.key(key)
	if e != nil {
		return nil, e
	}
	r, e := s.client.Object.Get(ctx, k, nil)
	if e != nil {
		return nil, cosError(e)
	}
	return r.Body, nil
}
func (s *COS) Remove(ctx context.Context, key string) error {
	k, e := s.key(key)
	if e != nil {
		return e
	}
	_, e = s.client.Object.Delete(ctx, k)
	if errors.Is(cosError(e), fs.ErrNotExist) {
		return nil
	}
	return e
}
func (s *COS) Initiate(ctx context.Context, key string) (string, error) {
	k, e := s.key(key)
	if e != nil {
		return "", e
	}
	r, _, e := s.client.Object.InitiateMultipartUpload(ctx, k, nil)
	if e != nil {
		return "", e
	}
	return r.UploadID, nil
}
func (s *COS) SignPart(ctx context.Context, key, id string, n int, ttl time.Duration) (SignedPart, error) {
	k, e := s.key(key)
	if e != nil {
		return SignedPart{}, e
	}
	q := url.Values{}
	q.Set("uploadId", id)
	q.Set("partNumber", strconv.Itoa(n))
	if s.options.SessionToken != "" {
		q.Set("x-cos-security-token", s.options.SessionToken)
	}
	u, e := s.client.Object.GetPresignedURL(ctx, http.MethodPut, k, s.options.SecretID, s.options.SecretKey, ttl, &cos.PresignedURLOptions{Query: &q})
	if e != nil {
		return SignedPart{}, e
	}
	return SignedPart{u.String(), map[string]string{}, time.Now().Add(ttl)}, nil
}
func (s *COS) ListParts(ctx context.Context, key, id string) ([]UploadedPart, error) {
	k, e := s.key(key)
	if e != nil {
		return nil, e
	}
	parts := []UploadedPart{}
	marker := 0
	for {
		r, _, e := s.client.Object.ListParts(ctx, k, id, &cos.ObjectListPartsOptions{PartNumberMarker: strconv.Itoa(marker), MaxParts: "1000"})
		if e != nil {
			return nil, cosError(e)
		}
		for _, p := range r.Parts {
			parts = append(parts, UploadedPart{p.PartNumber, p.Size, p.ETag})
		}
		if !r.IsTruncated {
			break
		}
		next, parseErr := strconv.Atoi(r.NextPartNumberMarker)
		if parseErr != nil || next <= marker {
			return nil, errors.New("COS pagination did not advance")
		}
		marker = next
	}
	return parts, nil
}
func (s *COS) Complete(ctx context.Context, key, id string, parts []UploadedPart) error {
	k, e := s.key(key)
	if e != nil {
		return e
	}
	o := &cos.CompleteMultipartUploadOptions{}
	for _, p := range parts {
		o.Parts = append(o.Parts, cos.Object{PartNumber: p.Number, ETag: p.ETag})
	}
	_, _, e = s.client.Object.CompleteMultipartUpload(ctx, k, id, o)
	return e
}
func (s *COS) Abort(ctx context.Context, key, id string) error {
	k, e := s.key(key)
	if e != nil {
		return e
	}
	_, e = s.client.Object.AbortMultipartUpload(ctx, k, id)
	if errors.Is(cosError(e), fs.ErrNotExist) {
		return nil
	}
	return e
}
