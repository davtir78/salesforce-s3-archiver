package redis

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestGenerateIAMAuthToken(t *testing.T) {
	creds := aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "secret", SessionToken: "session"}
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)

	for _, serverless := range []bool{false, true} {
		conf := IAMAuthConfig{CacheName: "sf-archive", UserId: "archiver", Region: "ap-southeast-2", Serverless: serverless}
		token, err := GenerateIAMAuthToken(context.Background(), conf, creds, now)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if strings.HasPrefix(token, "http") {
			t.Errorf("token must not include a scheme: %s", token)
		}
		if !strings.HasPrefix(token, "sf-archive/?") {
			t.Errorf("token must start with the cache name: %s", token)
		}
		u, err := url.Parse("https://" + token)
		if err != nil {
			t.Fatalf("token is not a valid URL: %v", err)
		}
		q := u.Query()
		expect := map[string]string{
			"Action":               "connect",
			"User":                 "archiver",
			"X-Amz-Algorithm":      "AWS4-HMAC-SHA256",
			"X-Amz-Expires":        "900",
			"X-Amz-Security-Token": "session",
			"X-Amz-Date":           "20260914T120000Z",
		}
		for k, v := range expect {
			if got := q.Get(k); got != v {
				t.Errorf("query %s = %q, want %q", k, got, v)
			}
		}
		if !strings.Contains(q.Get("X-Amz-Credential"), "/ap-southeast-2/elasticache/aws4_request") {
			t.Errorf("unexpected credential scope: %s", q.Get("X-Amz-Credential"))
		}
		if q.Get("X-Amz-Signature") == "" {
			t.Errorf("missing signature")
		}
		if serverless != (q.Get("ResourceType") == "ServerlessCache") {
			t.Errorf("ResourceType mismatch for serverless=%v: %q", serverless, q.Get("ResourceType"))
		}
	}
}

func TestBuildTLSConfig(t *testing.T) {
	if c, err := buildTLSConfig(TLSConfig{}, "h"); err != nil || c != nil {
		t.Fatalf("disabled TLS must return nil config, got %v, %v", c, err)
	}

	c, err := buildTLSConfig(TLSConfig{Enabled: true}, "master.cache.amazonaws.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.ServerName != "master.cache.amazonaws.com" {
		t.Errorf("ServerName should default to host, got %q", c.ServerName)
	}

	bad := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(bad, []byte("not a cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := buildTLSConfig(TLSConfig{Enabled: true, CAFile: bad}, "h"); err == nil {
		t.Errorf("expected error for CA file without certificates")
	}
}

func TestNewClientValidation(t *testing.T) {
	if _, err := NewClient(RedisConfig{}); err == nil {
		t.Errorf("expected error for empty host")
	}
	if _, err := NewClient(RedisConfig{Host: "h", Mode: "cluster", DbNumber: 2}); err == nil {
		t.Errorf("expected error for cluster mode with non-zero db")
	}
	if _, err := NewClient(RedisConfig{Host: "h", IAMAuth: IAMAuthConfig{Enabled: true}}); err == nil {
		t.Errorf("expected error for IAM auth without TLS")
	}
	c, err := NewClient(RedisConfig{Host: "h", Mode: "cluster", TLS: TLSConfig{Enabled: true}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c.Close()
}
