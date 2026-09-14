package redis

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/redis/go-redis/v9"
)

const (
	ModeStandalone = "standalone"
	ModeCluster    = "cluster"

	iamTokenTTL     = 15 * time.Minute
	iamTokenRefresh = 10 * time.Minute
)

// SHA-256 of an empty payload, required by SigV4 presigning.
var emptyPayloadHash = func() string {
	sum := sha256.Sum256(nil)
	return hex.EncodeToString(sum[:])
}()

type TLSConfig struct {
	Enabled            bool
	InsecureSkipVerify bool
	CAFile             string
	ServerName         string
}

// IAMAuthConfig enables AWS ElastiCache IAM authentication. When enabled the
// password is replaced by a short-lived SigV4 token generated for UserId.
// See https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/auth-iam.html
type IAMAuthConfig struct {
	Enabled bool
	// Replication group ID or serverless cache name.
	CacheName  string
	UserId     string
	Region     string
	Serverless bool
}

type RedisConfig struct {
	Host       string
	Port       int
	DbNumber   int
	Username   string
	Password   string
	ExpireDays int
	Mode       string
	TLS        TLSConfig
	IAMAuth    IAMAuthConfig

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	MaxRetries   int
}

// NewClient builds a go-redis client suitable for local Redis/Valkey and for
// AWS ElastiCache (TLS in transit, AUTH token / RBAC user, IAM auth, cluster mode).
func NewClient(conf RedisConfig) (redis.UniversalClient, error) {
	if conf.Host == "" {
		return nil, errors.New("redis host must be defined")
	}
	conf = conf.WithDefaults()

	tlsConf, err := buildTLSConfig(conf.TLS, conf.Host)
	if err != nil {
		return nil, err
	}

	opts := &redis.UniversalOptions{
		Addrs:        []string{conf.Host + ":" + strconv.Itoa(conf.Port)},
		DB:           conf.DbNumber,
		Username:     conf.Username,
		Password:     conf.Password,
		TLSConfig:    tlsConf,
		DialTimeout:  conf.DialTimeout,
		ReadTimeout:  conf.ReadTimeout,
		WriteTimeout: conf.WriteTimeout,
		MaxRetries:   conf.MaxRetries,
	}

	switch strings.ToLower(conf.Mode) {
	case "", ModeStandalone:
		// UniversalClient returns a single-node client for one address.
	case ModeCluster:
		if conf.DbNumber != 0 {
			return nil, errors.New("redis cluster mode only supports dbNumber 0")
		}
		opts.IsClusterMode = true
	default:
		return nil, fmt.Errorf("unknown redis mode '%s' (expected '%s' or '%s')", conf.Mode, ModeStandalone, ModeCluster)
	}

	if conf.IAMAuth.Enabled {
		if tlsConf == nil {
			return nil, errors.New("redis IAM auth requires TLS to be enabled")
		}
		provider, err := newIAMTokenProvider(conf.IAMAuth)
		if err != nil {
			return nil, err
		}
		opts.Username = ""
		opts.Password = ""
		opts.CredentialsProviderContext = provider.credentials
	}

	return redis.NewUniversalClient(opts), nil
}

// WithDefaults fills in ports, timeouts and retries that were left unset.
func (conf RedisConfig) WithDefaults() RedisConfig {
	if conf.Port == 0 {
		conf.Port = 6379
	}
	if conf.DialTimeout == 0 {
		conf.DialTimeout = 5 * time.Second
	}
	if conf.ReadTimeout == 0 {
		conf.ReadTimeout = 3 * time.Second
	}
	if conf.WriteTimeout == 0 {
		conf.WriteTimeout = 3 * time.Second
	}
	if conf.MaxRetries == 0 {
		conf.MaxRetries = 3
	}
	return conf
}

func buildTLSConfig(conf TLSConfig, host string) (*tls.Config, error) {
	if !conf.Enabled {
		return nil, nil
	}
	tlsConf := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: conf.InsecureSkipVerify, // #nosec G402 -- opt-in for local testing only
		ServerName:         conf.ServerName,
	}
	if tlsConf.ServerName == "" {
		tlsConf.ServerName = host
	}
	if conf.CAFile != "" {
		pem, err := os.ReadFile(conf.CAFile)
		if err != nil {
			return nil, fmt.Errorf("reading redis CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("redis CA file '%s' contains no certificates", conf.CAFile)
		}
		tlsConf.RootCAs = pool
	}
	return tlsConf, nil
}

type iamTokenProvider struct {
	conf  IAMAuthConfig
	creds aws.CredentialsProvider
	mu    sync.Mutex
	token string
	at    time.Time
}

func newIAMTokenProvider(conf IAMAuthConfig) (*iamTokenProvider, error) {
	if conf.CacheName == "" || conf.UserId == "" {
		return nil, errors.New("redis IAM auth requires cacheName and userId")
	}
	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if conf.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(conf.Region))
	}
	awsConf, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config for redis IAM auth: %w", err)
	}
	if conf.Region == "" {
		conf.Region = awsConf.Region
	}
	if conf.Region == "" {
		return nil, errors.New("redis IAM auth requires a region")
	}
	return &iamTokenProvider{conf: conf, creds: awsConf.Credentials}, nil
}

func (p *iamTokenProvider) credentials(ctx context.Context) (string, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token != "" && time.Since(p.at) < iamTokenRefresh {
		return p.conf.UserId, p.token, nil
	}
	creds, err := p.creds.Retrieve(ctx)
	if err != nil {
		return "", "", fmt.Errorf("retrieving AWS credentials for redis IAM auth: %w", err)
	}
	token, err := GenerateIAMAuthToken(ctx, p.conf, creds, time.Now())
	if err != nil {
		return "", "", err
	}
	p.token = token
	p.at = time.Now()
	return p.conf.UserId, token, nil
}

// GenerateIAMAuthToken builds an ElastiCache IAM auth token: a SigV4-presigned
// "connect" request for the cache, with the scheme stripped.
func GenerateIAMAuthToken(ctx context.Context, conf IAMAuthConfig, creds aws.Credentials, now time.Time) (string, error) {
	query := url.Values{}
	query.Set("Action", "connect")
	query.Set("User", conf.UserId)
	if conf.Serverless {
		query.Set("ResourceType", "ServerlessCache")
	}
	query.Set("X-Amz-Expires", strconv.Itoa(int(iamTokenTTL.Seconds())))

	req, err := http.NewRequest(http.MethodGet, "http://"+conf.CacheName+"/?"+query.Encode(), nil)
	if err != nil {
		return "", err
	}
	signed, _, err := v4.NewSigner().PresignHTTP(ctx, creds, req, emptyPayloadHash, "elasticache", conf.Region, now)
	if err != nil {
		return "", fmt.Errorf("presigning redis IAM auth token: %w", err)
	}
	return strings.TrimPrefix(signed, "http://"), nil
}
