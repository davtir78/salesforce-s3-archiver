package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/davtir78/salesforce-s3-archiver/internal/log"
)

type S3Options struct {
	Bucket         string
	Prefix         string
	Region         string
	Endpoint       string
	ForcePathStyle bool
	KmsKeyId       string
	StorageClass   string
	MaxAttempts    int
	// Total timeout for one upload call, including the SDK retries inside it.
	// Without it a hung connection blocks the collector indefinitely. Default 5m.
	UploadTimeout time.Duration
}

// S3Sink uploads objects to Amazon S3 (or an S3-compatible endpoint).
type S3Sink struct {
	opts     S3Options
	client   *s3.Client
	uploader *manager.Uploader
}

func NewS3Sink(ctx context.Context, opts S3Options) (*S3Sink, error) {
	if opts.Bucket == "" {
		return nil, errors.New("s3 bucket is required")
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 5
	}
	if opts.UploadTimeout <= 0 {
		opts.UploadTimeout = 5 * time.Minute
	}
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = opts.MaxAttempts
				o.MaxBackoff = 20 * time.Second
			})
		}),
	}
	if opts.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(opts.Region))
	}
	awsConf, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	client := s3.NewFromConfig(awsConf, func(o *s3.Options) {
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		}
		o.UsePathStyle = opts.ForcePathStyle
	})
	return &S3Sink{
		opts:   opts,
		client: client,
		uploader: manager.NewUploader(client, func(u *manager.Uploader) {
			u.PartSize = 16 * 1024 * 1024
			u.Concurrency = 4
			// The manager aborts with the (already expired) upload context, so
			// abort here instead, with a context that survives the timeout.
			u.LeavePartsOnError = true
		}),
	}, nil
}

// Client exposes the underlying S3 client (used by verification tooling).
func (s *S3Sink) Client() *s3.Client { return s.client }

func (s *S3Sink) Write(ctx context.Context, meta ObjectMeta, fill func(w RecordWriter) error) (Manifest, error) {
	return encodeObject(ctx, meta, fill, func(ctx context.Context, body *os.File, m *Manifest) error {
		name := meta.Name
		if name == "" {
			name = UniqueName(m.IngestedAt)
		}
		dataKey, manifestKey := ObjectPaths(meta, name)
		dataKey = joinPrefix(s.opts.Prefix, dataKey)
		manifestKey = joinPrefix(s.opts.Prefix, manifestKey)
		m.ObjectKey = dataKey

		if err := s.put(ctx, dataKey, body, "application/gzip", m); err != nil {
			return fmt.Errorf("uploading data object s3://%s/%s: %w", s.opts.Bucket, dataKey, err)
		}
		mb, err := json.MarshalIndent(m, "", "  ")
		if err != nil {
			return err
		}
		if err := s.put(ctx, manifestKey, bytes.NewReader(mb), "application/json", nil); err != nil {
			return fmt.Errorf("uploading manifest s3://%s/%s: %w", s.opts.Bucket, manifestKey, err)
		}
		return nil
	})
}

func (s *S3Sink) put(ctx context.Context, key string, body io.Reader, contentType string, m *Manifest) error {
	input := &s3.PutObjectInput{
		Bucket:            aws.String(s.opts.Bucket),
		Key:               aws.String(key),
		Body:              body,
		ContentType:       aws.String(contentType),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	}
	if m != nil {
		input.Metadata = map[string]string{
			"record-count": fmt.Sprintf("%d", m.RecordCount),
			"sha256":       m.SHA256,
			"source":       m.Source,
		}
	}
	if s.opts.KmsKeyId != "" {
		input.ServerSideEncryption = types.ServerSideEncryptionAwsKms
		input.SSEKMSKeyId = aws.String(s.opts.KmsKeyId)
		input.BucketKeyEnabled = aws.Bool(true)
	}
	if s.opts.StorageClass != "" {
		input.StorageClass = types.StorageClass(s.opts.StorageClass)
	}
	uploadCtx, cancel := context.WithTimeout(ctx, s.opts.UploadTimeout)
	defer cancel()
	_, err := s.uploader.Upload(uploadCtx, input)
	if err != nil {
		s.abortMultipart(ctx, key, err)
	}
	return err
}

// abortMultipart cleans up a failed multipart upload so its parts are not
// billed indefinitely. It uses a fresh context because the upload context is
// typically the one that just expired.
func (s *S3Sink) abortMultipart(ctx context.Context, key string, cause error) {
	var mu manager.MultiUploadFailure
	if !errors.As(cause, &mu) || mu.UploadID() == "" {
		return
	}
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	_, err := s.client.AbortMultipartUpload(abortCtx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.opts.Bucket),
		Key:      aws.String(key),
		UploadId: aws.String(mu.UploadID()),
	})
	if err != nil {
		log.Warnf("Could not abort multipart upload %s for s3://%s/%s: %v (an AbortIncompleteMultipartUpload lifecycle rule will clean it up)",
			mu.UploadID(), s.opts.Bucket, key, err)
	}
}

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }
