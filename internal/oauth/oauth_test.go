package oauth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/davtir78/salesforce-s3-archiver/internal/config"
	"github.com/golang-jwt/jwt/v5"
)

// tokenServer records the last token request and answers with status/body.
func tokenServer(t *testing.T, status int, body any) (*httptest.Server, *url.Values) {
	t.Helper()
	var got url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != loginEndpoint || r.Method != http.MethodPost {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/x-www-form-urlencoded" {
			http.Error(w, "bad content type "+ct, http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		got = r.PostForm
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(srv.Close)
	return srv, &got
}

var okResponse = map[string]string{"access_token": "tok", "instance_url": "https://org.my.salesforce.com", "token_type": "Bearer"}

func TestClientCredentials(t *testing.T) {
	srv, got := tokenServer(t, http.StatusOK, okResponse)
	resp, err := Login(config.AuthConfig{TokenUrl: srv.URL, ClientCred: &config.ClientCredAuth{ClientId: "id", ClientSecret: "s&cret="}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.AccessToken != "tok" || resp.InstanceURL != "https://org.my.salesforce.com" {
		t.Errorf("unexpected response %+v", resp)
	}
	if got.Get("grant_type") != "client_credentials" || got.Get("client_id") != "id" || got.Get("client_secret") != "s&cret=" {
		t.Errorf("unexpected form %v", *got)
	}
}

func TestUserPassword(t *testing.T) {
	srv, got := tokenServer(t, http.StatusOK, okResponse)
	_, err := Login(config.AuthConfig{TokenUrl: srv.URL, UserPass: &config.UserPassAuth{ClientId: "id", ClientSecret: "sec", Username: "u@example.com", Password: "p+w"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Get("grant_type") != "password" || got.Get("username") != "u@example.com" || got.Get("password") != "p+w" {
		t.Errorf("unexpected form %v", *got)
	}
}

func TestJwtBearer(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(t.TempDir(), "key.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err := os.WriteFile(keyPath, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	srv, got := tokenServer(t, http.StatusOK, okResponse)

	claims := func(audience string) jwt.MapClaims {
		t.Helper()
		_, err := Login(config.AuthConfig{TokenUrl: srv.URL, Jwt: &config.JwtAuth{ClientId: "id", Username: "u@example.com", PrivateKey: keyPath, Audience: audience}})
		if err != nil {
			t.Fatal(err)
		}
		if got.Get("grant_type") != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
			t.Fatalf("unexpected grant type %q", got.Get("grant_type"))
		}
		parsed, err := jwt.Parse(got.Get("assertion"), func(*jwt.Token) (any, error) { return &key.PublicKey, nil },
			jwt.WithValidMethods([]string{"RS256"}))
		if err != nil {
			t.Fatalf("assertion does not verify with the key: %v", err)
		}
		return parsed.Claims.(jwt.MapClaims)
	}

	c := claims("")
	if c["iss"] != "id" || c["sub"] != "u@example.com" || c["aud"] != srv.URL {
		t.Errorf("unexpected default claims %v", c)
	}
	if c := claims("https://login.salesforce.com"); c["aud"] != "https://login.salesforce.com" {
		t.Errorf("audience override not used: aud=%v", c["aud"])
	}
}

func TestLoginErrors(t *testing.T) {
	srv, _ := tokenServer(t, http.StatusBadRequest, map[string]string{"error": "invalid_client", "error_description": "invalid client credentials"})
	_, err := Login(config.AuthConfig{TokenUrl: srv.URL, ClientCred: &config.ClientCredAuth{ClientId: "id", ClientSecret: "bad"}})
	if err == nil || !strings.Contains(err.Error(), "400") || !strings.Contains(err.Error(), "invalid_client") {
		t.Errorf("expected the status and Salesforce error in the message, got %v", err)
	}
	if _, err := Login(config.AuthConfig{TokenUrl: srv.URL}); err == nil {
		t.Errorf("a config without an auth method must fail")
	}
	if _, err := Login(config.AuthConfig{TokenUrl: srv.URL, Jwt: &config.JwtAuth{ClientId: "id", PrivateKey: filepath.Join(t.TempDir(), "missing.pem")}}); err == nil {
		t.Errorf("a missing private key must fail")
	}
}
