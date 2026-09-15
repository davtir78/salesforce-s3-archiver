package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Runs against an S3-compatible endpoint (e.g. MinIO from docker compose):
//
//	S3_TEST_ENDPOINT=http://localhost:9000 S3_TEST_BUCKET=archive \
//	AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 \
//	go test ./internal/archive -run TestS3Sink
func TestS3SinkIntegration(t *testing.T) {
	endpoint := os.Getenv("S3_TEST_ENDPOINT")
	bucket := os.Getenv("S3_TEST_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("S3_TEST_ENDPOINT and S3_TEST_BUCKET not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	prefix := "it-" + UniqueName(time.Now())
	sink, err := NewS3Sink(ctx, S3Options{Bucket: bucket, Prefix: prefix, Endpoint: endpoint, ForcePathStyle: true, Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	m, err := WriteRecords(ctx, sink, ObjectMeta{Source: SourceStream, OrgId: "org", EventType: "LoginEventStream"}, sampleRecords(1000))
	if err != nil {
		t.Fatal(err)
	}

	obj, err := sink.Client().GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(m.ObjectKey)})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(obj.Body)
	obj.Body.Close()
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != m.SHA256 {
		t.Errorf("stored object checksum mismatch")
	}

	manifestKey := strings.Replace(m.ObjectKey, prefix+"/raw/", prefix+"/manifests/", 1)
	manifestKey = strings.TrimSuffix(manifestKey, ".json.gz") + ".manifest.json"
	mo, err := sink.Client().GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(manifestKey)})
	if err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
	var stored Manifest
	if err := json.NewDecoder(mo.Body).Decode(&stored); err != nil {
		t.Fatal(err)
	}
	mo.Body.Close()
	if stored.RecordCount != 1000 || stored.SHA256 != m.SHA256 {
		t.Errorf("manifest mismatch: %+v", stored)
	}
}
