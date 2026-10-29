package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/s3utils"
)

// S3Options 描述一个 S3 兼容存储位置。凭据仅存在于运行时配置中，不入库。
// SpoolDir 是该策略专用的本地暂存目录，不是最终文件的存储位置。
type S3Options struct {
	Endpoint        string
	Region          string
	Bucket          string
	Prefix          string
	SpoolDir        string
	ForcePathStyle  bool
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	// Transport 可注入 HTTP 传输实现；INI 不提供此项，正常运行使用 SDK 默认值。
	Transport http.RoundTripper
}

// Validate 不连接网络、不创建目录，配置加载时也可以调用。
func (o S3Options) Validate() error {
	u, err := url.Parse(o.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("S3 endpoint must be an http(s) origin without credentials, path or query")
	}
	if strings.TrimSpace(o.Region) == "" {
		return errors.New("S3 region is required")
	}
	if err := s3utils.CheckValidBucketName(o.Bucket); err != nil {
		return fmt.Errorf("S3 bucket: %w", err)
	}
	if o.AccessKeyID == "" || o.SecretAccessKey == "" {
		return errors.New("S3 access key ID and secret access key are required")
	}
	if strings.TrimSpace(o.SpoolDir) == "" {
		return errors.New("S3 spool directory is required")
	}
	if strings.Contains(o.Prefix, "\\") {
		return errors.New("S3 prefix must not contain backslashes")
	}
	for _, part := range strings.Split(strings.Trim(o.Prefix, "/"), "/") {
		if part == "." || part == ".." {
			return errors.New("S3 prefix must not contain dot segments")
		}
	}
	return nil
}

// S3 用单次 PutObject 发布完整对象；分片先落本地，收齐后再上传。
// 这不是云端 Multipart，也不是浏览器直传。逻辑发布仍由 file 服务控制。
type S3 struct {
	client *minio.Client
	bucket string
	prefix string
	spool  *Local
}

var _ ChunkStore = (*S3)(nil)

func NewS3(opts S3Options) (*S3, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	u, _ := url.Parse(opts.Endpoint)
	lookup := minio.BucketLookupAuto
	if opts.ForcePathStyle {
		lookup = minio.BucketLookupPath
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(opts.AccessKeyID, opts.SecretAccessKey, opts.SessionToken),
		Secure: u.Scheme == "https", Region: opts.Region, BucketLookup: lookup,
		Transport: opts.Transport,
	})
	if err != nil {
		return nil, fmt.Errorf("create S3 client: %w", err)
	}
	if err := os.MkdirAll(opts.SpoolDir, 0o700); err != nil {
		return nil, fmt.Errorf("create S3 spool directory: %w", err)
	}
	return &S3{client: client, bucket: opts.Bucket,
		prefix: strings.Trim(opts.Prefix, "/"), spool: NewLocal(opts.SpoolDir)}, nil
}

// Check 只检查现有 bucket 是否可访问；不自动建 bucket 或改变公开权限。
func (s *S3) Check(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("check S3 bucket: %w", err)
	}
	if !exists {
		return errors.New("S3 bucket does not exist")
	}
	return nil
}

func (s *S3) objectKey(key string) (string, error) {
	// key 同时用于暂存文件名，只接受单个、不含路径的服务端生成标识。
	if key == "" || key == "." || key == ".." || strings.ContainsAny(key, "/\\\x00") {
		return "", errors.New("invalid storage key")
	}
	if s.prefix == "" {
		return key, nil
	}
	return s.prefix + "/" + key, nil
}

func (s *S3) Save(ctx context.Context, key string, r io.Reader, maxBytes int64) (int64, error) {
	if _, err := s.objectKey(key); err != nil {
		return 0, err
	}
	if maxBytes < 0 || maxBytes == math.MaxInt64 {
		return 0, errors.New("invalid maximum content size")
	}
	// 先流式暂存并检查大小，避免未知长度的 Reader 隐式触发云端 Multipart，
	// 也避免超限后已在 bucket 中发布一个截断对象。内存不随文件大小增长。
	size, err := s.spool.Save(ctx, key, r, maxBytes)
	if err != nil {
		return 0, err
	}
	defer func() { _ = s.spool.Remove(context.Background(), key) }()
	if err := s.putStaged(ctx, key, size); err != nil {
		// PUT 响应丢失时对象可能已存在；清理不能复用已取消的请求 context。
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		object, _ := s.objectKey(key)
		cleanupErr := s.client.RemoveObject(cleanupCtx, s.bucket, object, minio.RemoveObjectOptions{})
		return 0, errors.Join(err, cleanupErr)
	}
	return size, nil
}

func (s *S3) putStaged(ctx context.Context, key string, size int64) error {
	object, err := s.objectKey(key)
	if err != nil {
		return err
	}
	f, err := os.Open(s.spool.Path(key))
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() != size {
		return errors.New("S3 staged content size mismatch")
	}
	_, err = s.client.PutObject(ctx, s.bucket, object, f, size, minio.PutObjectOptions{
		ContentType: "application/octet-stream", DisableMultipart: true,
		DisableContentSha256: true, SendContentMd5: true,
	})
	if err != nil {
		return fmt.Errorf("put S3 object: %w", err)
	}
	return nil
}

func (s *S3) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	object, err := s.objectKey(key)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r, err := s.client.GetObject(ctx, s.bucket, object, minio.GetObjectOptions{})
	if err != nil {
		return nil, s3ObjectError(err)
	}
	// GetObject 是惰性请求；必须在返回前触发 Stat，让 404/403 在 Open 边界暴露。
	if _, err := r.Stat(); err != nil {
		_ = r.Close()
		return nil, s3ObjectError(err)
	}
	return r, nil
}

func (s *S3) Remove(ctx context.Context, key string) error {
	object, err := s.objectKey(key)
	if err != nil {
		return err
	}
	_, statErr := s.client.StatObject(ctx, s.bucket, object, minio.StatObjectOptions{})
	remoteErr := s3ObjectError(statErr)
	if statErr == nil {
		remoteErr = s.client.RemoveObject(ctx, s.bucket, object, minio.RemoveObjectOptions{})
	}
	localErr := s.spool.Remove(ctx, key)
	if remoteErr != nil && !errors.Is(remoteErr, fs.ErrNotExist) {
		return fmt.Errorf("remove S3 object: %w", remoteErr)
	}
	if localErr != nil && !errors.Is(localErr, fs.ErrNotExist) {
		return fmt.Errorf("remove S3 spool content: %w", localErr)
	}
	if errors.Is(remoteErr, fs.ErrNotExist) && errors.Is(localErr, fs.ErrNotExist) {
		return fs.ErrNotExist
	}
	return nil
}

// 不把 NoSuchBucket / AccessDenied 当成对象不存在，否则清理会错误丢掉重试责任。
func s3ObjectError(err error) error {
	if err == nil {
		return nil
	}
	switch minio.ToErrorResponse(err).Code {
	case "NoSuchKey", "NoSuchObject", "NotFound":
		return fmt.Errorf("S3 object: %w", fs.ErrNotExist)
	default:
		return err
	}
}

func (s *S3) PrepareChunks(ctx context.Context, key string, totalSize int64) error {
	if _, err := s.objectKey(key); err != nil {
		return err
	}
	return s.spool.PrepareChunks(ctx, key, totalSize)
}

func (s *S3) WriteChunkAt(ctx context.Context, key string, offset int64, data []byte) error {
	if _, err := s.objectKey(key); err != nil {
		return err
	}
	return s.spool.WriteChunkAt(ctx, key, offset, data)
}

func (s *S3) FinalizeChunks(ctx context.Context, key string, totalSize int64) error {
	object, err := s.objectKey(key)
	if err != nil {
		return err
	}
	if err := s.spool.FinalizeChunks(ctx, key, totalSize); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		// 上一次 PUT 已成功、暂存已清除，但 DB ready 尚未提交：核对云端结果。
		info, err := s.client.StatObject(ctx, s.bucket, object, minio.StatObjectOptions{})
		if err != nil {
			return s3ObjectError(err)
		}
		if info.Size != totalSize {
			return errors.New("finalized S3 content size mismatch")
		}
		return nil
	}
	if err := s.putStaged(ctx, key, totalSize); err != nil {
		// 保留完整暂存，允许显式完成请求重试同一对象，不丢已经收到的分片。
		return err
	}
	if err := s.spool.Remove(ctx, key); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove finalized S3 spool content: %w", err)
	}
	return nil
}
