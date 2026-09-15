package archive

import (
	"context"
	"errors"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/config"
)

// FromConfig builds the configured sink.
func FromConfig(ctx context.Context, conf config.ArchiveConfig) (Sink, error) {
	switch {
	case conf.S3 != nil:
		return NewS3Sink(ctx, S3Options{
			Bucket:         conf.S3.Bucket,
			Prefix:         conf.S3.Prefix,
			Region:         conf.S3.Region,
			Endpoint:       conf.S3.Endpoint,
			ForcePathStyle: conf.S3.ForcePathStyle,
			KmsKeyId:       conf.S3.KmsKeyId,
			StorageClass:   conf.S3.StorageClass,
			MaxAttempts:    conf.S3.MaxAttempts,
			UploadTimeout:  time.Duration(conf.S3.UploadTimeoutSeconds) * time.Second,
		})
	case conf.Local != nil:
		return NewLocalSink(conf.Local.Dir, "")
	default:
		return nil, errors.New("no archive destination configured")
	}
}
