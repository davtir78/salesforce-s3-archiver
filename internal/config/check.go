package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/davtir78/salesforce-s3-archiver/internal/log"
)

func CheckUrl(urlStr string) bool {
	if _, err := url.ParseRequestURI(urlStr); err != nil {
		return false
	}
	return true
}

func CheckAuth(auth *AuthConfig) error {
	if auth.TokenUrl == "" {
		return errors.New("Empty 'auth.tokenUrl'")
	}
	if !CheckUrl(auth.TokenUrl) {
		return errors.New("Invalid URL 'auth.tokenUrl'")
	}
	u, err := url.Parse(auth.TokenUrl)
	if err != nil || u.Host == "" {
		return errors.New("Invalid URL 'auth.tokenUrl': expected e.g. https://example.my.salesforce.com")
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
	case "http":
		// The client secret, password or JWT assertion and every access token
		// would cross the network unencrypted.
		if !auth.AllowInsecureHttp {
			return errors.New("'auth.tokenUrl' must use https (set 'auth.allowInsecureHttp: true' only for a local mock org)")
		}
		log.Warnf("'auth.tokenUrl' uses http: credentials and tokens are sent unencrypted (allowInsecureHttp)")
	default:
		return fmt.Errorf("'auth.tokenUrl' must use https, got scheme '%s'", u.Scheme)
	}
	definedAuthMethods := 0
	if auth.Jwt != nil {
		definedAuthMethods += 1
	}
	if auth.ClientCred != nil {
		definedAuthMethods += 1
	}
	if auth.UserPass != nil {
		definedAuthMethods += 1
	}
	if definedAuthMethods != 1 {
		return errors.New("Exactly one auth method must be defined")
	}
	if auth.Jwt != nil {
		err := CheckJwtCredentials(auth.Jwt)
		if err != nil {
			return err
		}
	}
	if auth.ClientCred != nil {
		err := CheckClientCredCredentials(auth.ClientCred)
		if err != nil {
			return err
		}
	}
	if auth.UserPass != nil {
		log.Warnf("Using the Username-Password auth flow, which is DEPRECATED. Please use an alternative: Client Credentials or JWT.")
		err := CheckUserPassCredentials(auth.UserPass)
		if err != nil {
			return err
		}
	}
	return nil
}

func CheckUserPassCredentials(userPassAuth *UserPassAuth) error {
	if userPassAuth == nil {
		return errors.New("Undefined userPass credentials")
	}
	if userPassAuth.ClientId == "" {
		return errors.New("Empty 'userPass.clientId'")
	}
	if userPassAuth.ClientSecret == "" {
		return errors.New("Empty 'userPass.clientSecret'")
	}
	if userPassAuth.Username == "" {
		return errors.New("Empty 'userPass.username'")
	}
	if userPassAuth.Password == "" {
		return errors.New("Empty 'userPass.password'")
	}
	return nil
}

func CheckJwtCredentials(jwtAuth *JwtAuth) error {
	if jwtAuth == nil {
		return errors.New("Undefined jwt credentials")
	}
	if jwtAuth.ClientId == "" {
		return errors.New("Empty 'jwt.clientId'")
	}
	if jwtAuth.PrivateKey == "" {
		return errors.New("Empty 'jwt.privateKey'")
	}
	if jwtAuth.Username == "" {
		return errors.New("Empty 'jwt.username'")
	}
	return nil
}

func CheckClientCredCredentials(clientCredAuth *ClientCredAuth) error {
	if clientCredAuth == nil {
		return errors.New("Undefined clientCred credentials")
	}
	if clientCredAuth.ClientId == "" {
		return errors.New("Empty 'clientCred.clientId'")
	}
	if clientCredAuth.ClientSecret == "" {
		return errors.New("Empty 'clientCred.clientSecret'")
	}
	return nil
}

func CheckCache(cache *CacheConfig) error {
	if cache == nil {
		log.Warnf("Cache not defined.")
		return nil
	}
	if cache.Redis == nil {
		log.Warnf("Redis DB not defined.")
		return nil
	}
	r := cache.Redis
	if r.Host == "" {
		return errors.New("Empty 'cache.redis.host'")
	}
	switch r.Mode {
	case "", "standalone", "cluster":
	default:
		return fmt.Errorf("Invalid 'cache.redis.mode' '%s': expected 'standalone' or 'cluster'", r.Mode)
	}
	if r.IAMAuth.Enabled {
		if !r.TLS.Enabled {
			return errors.New("'cache.redis.iamAuth' requires 'cache.redis.tls.enabled: true'")
		}
		if r.IAMAuth.CacheName == "" || r.IAMAuth.UserId == "" {
			return errors.New("'cache.redis.iamAuth' requires 'cacheName' and 'userId'")
		}
		if r.Password != "" {
			return errors.New("'cache.redis.password' must be empty when 'iamAuth' is enabled")
		}
	}
	if r.TLS.InsecureSkipVerify {
		log.Warnf("'cache.redis.tls.insecureSkipVerify' is enabled; use only for local testing.")
	}
	return nil
}

func CheckArchive(archive *ArchiveConfig) error {
	if archive.S3 == nil && archive.Local == nil {
		return errors.New("An archive destination must be defined: 'archive.s3' or 'archive.local'")
	}
	if archive.S3 != nil && archive.Local != nil {
		return errors.New("Only one archive destination may be defined: 'archive.s3' or 'archive.local'")
	}
	if archive.S3 != nil && archive.S3.Bucket == "" {
		return errors.New("Empty 'archive.s3.bucket'")
	}
	if archive.Local != nil && archive.Local.Dir == "" {
		return errors.New("Empty 'archive.local.dir'")
	}
	return nil
}
