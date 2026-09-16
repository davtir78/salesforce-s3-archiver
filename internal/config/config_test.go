package config

import (
	"os"
	"path/filepath"
	"testing"
)

func rightConfig() Config {
	return Config{
		Version:    "3.0",
		IsTemplate: false,
		Archive:    ArchiveConfig{S3: &S3Config{Bucket: "archive"}},
	}
}

func TestIntegrityCheck(t *testing.T) {
	config := Config{}
	err := integrityCheck(&config)
	if err == nil {
		t.Errorf("Integrity check didn't catch an empty config")
	}

	config = rightConfig()

	err = integrityCheck(&config)
	if err != nil {
		t.Errorf("Integrity check failed with a correct config: %v", err)
	}

	config.Version = "3.0.0"
	err = integrityCheck(&config)
	if err == nil {
		t.Errorf("Integrity check didn't catch a wrong version format (3.0.0)")
	}

	config.Version = "hello"
	err = integrityCheck(&config)
	if err == nil {
		t.Errorf("Integrity check didn't catch a wrong version format (hello)")
	}

	config.Version = "2.0"
	err = integrityCheck(&config)
	if err == nil {
		t.Errorf("Integrity check didn't catch a wrong version value (2.0)")
	}

	config = rightConfig()

	config.IsTemplate = true
	err = integrityCheck(&config)
	if err == nil {
		t.Errorf("Integrity check didn't catch the template flag")
	}

	config = rightConfig()
	config.Archive = ArchiveConfig{}
	if integrityCheck(&config) == nil {
		t.Errorf("Integrity check didn't catch a missing archive destination")
	}

	config.Archive = ArchiveConfig{S3: &S3Config{Bucket: "b"}, Local: &LocalArchiveConfig{Dir: "d"}}
	if integrityCheck(&config) == nil {
		t.Errorf("Integrity check didn't catch two archive destinations")
	}

	config.Archive = ArchiveConfig{S3: &S3Config{}}
	if integrityCheck(&config) == nil {
		t.Errorf("Integrity check didn't catch an empty bucket")
	}
}

func TestCheckCache(t *testing.T) {
	if err := CheckCache(nil); err != nil {
		t.Errorf("nil cache must be accepted: %v", err)
	}
	cases := []struct {
		name  string
		redis RedisConfig
		ok    bool
	}{
		{"plain", RedisConfig{Host: "h"}, true},
		{"no host", RedisConfig{}, false},
		{"bad mode", RedisConfig{Host: "h", Mode: "sentinel"}, false},
		{"cluster tls", RedisConfig{Host: "h", Mode: "cluster", TLS: RedisTLSConfig{Enabled: true}}, true},
		{"iam without tls", RedisConfig{Host: "h", IAMAuth: RedisIAMAuthConfig{Enabled: true, CacheName: "c", UserId: "u"}}, false},
		{"iam missing user", RedisConfig{Host: "h", TLS: RedisTLSConfig{Enabled: true}, IAMAuth: RedisIAMAuthConfig{Enabled: true, CacheName: "c"}}, false},
		{"iam with password", RedisConfig{Host: "h", Password: "p", TLS: RedisTLSConfig{Enabled: true}, IAMAuth: RedisIAMAuthConfig{Enabled: true, CacheName: "c", UserId: "u"}}, false},
		{"iam ok", RedisConfig{Host: "h", TLS: RedisTLSConfig{Enabled: true}, IAMAuth: RedisIAMAuthConfig{Enabled: true, CacheName: "c", UserId: "u"}}, true},
	}
	for _, c := range cases {
		r := c.redis
		err := CheckCache(&CacheConfig{Redis: &r})
		if (err == nil) != c.ok {
			t.Errorf("%s: ok=%v, err=%v", c.name, c.ok, err)
		}
	}
}

func TestReadConfigFileEnvVars(t *testing.T) {
	t.Setenv("SF_ARCHIVE_TEST_BUCKET", "from-env")
	path := filepath.Join(t.TempDir(), "config.yml")
	content := "version: \"3.0\"\narchive:\n  s3:\n    bucket: $SF_ARCHIVE_TEST_BUCKET\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	conf, err := ReadConfigFile(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if conf.Archive.S3.Bucket != "from-env" {
		t.Errorf("bucket = %q, want from-env", conf.Archive.S3.Bucket)
	}
}

func TestReadConfigFileEnvVarsInLists(t *testing.T) {
	t.Setenv("SF_ARCHIVE_TEST_TOPIC", "/event/LoginEventStream")
	path := filepath.Join(t.TempDir(), "config.yml")
	content := "version: \"3.0\"\narchive:\n  local:\n    dir: out\neventStream:\n  topics:\n    - $SF_ARCHIVE_TEST_TOPIC\n    - /event/ApiEventStream\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	conf, err := ReadConfigFile(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	topics := conf.EventStream.Topics
	if len(topics) != 2 || topics[0] != "/event/LoginEventStream" || topics[1] != "/event/ApiEventStream" {
		t.Errorf("topics = %v, want the env var expanded inside the list", topics)
	}
}

func TestCheckAuthRequiresHttps(t *testing.T) {
	auth := func(url string, allowHttp bool) *AuthConfig {
		return &AuthConfig{TokenUrl: url, AllowInsecureHttp: allowHttp,
			ClientCred: &ClientCredAuth{ClientId: "id", ClientSecret: "secret"}}
	}
	cases := []struct {
		url       string
		allowHttp bool
		ok        bool
	}{
		{"https://example.my.salesforce.com", false, true},
		{"http://example.my.salesforce.com", false, false},
		{"http://mock-salesforce:8080", true, true},
		{"HTTPS://example.my.salesforce.com", false, true},
		{"ftp://example.com", true, false},
		{"https://", false, false},
	}
	for _, c := range cases {
		err := CheckAuth(auth(c.url, c.allowHttp))
		if (err == nil) != c.ok {
			t.Errorf("tokenUrl %q allowInsecureHttp=%v: err=%v, want ok=%v", c.url, c.allowHttp, err, c.ok)
		}
	}
}
