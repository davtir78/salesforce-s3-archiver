package query

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/cache"
	"github.com/davtir78/salesforce-s3-archiver/internal/config"
)

func downloadServer(t *testing.T, logHandler http.HandlerFunc) (*config.EventLogConfig, func()) {
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/services/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"access_token": "tok", "instance_url": "http://x"})
	})
	mux.HandleFunc("/log", func(w http.ResponseWriter, r *http.Request) {
		logHandler(w, r)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	srv := httptest.NewServer(mux)
	conf := &config.EventLogConfig{
		Name:                        "t",
		RequestTimeout:              30,
		DownloadStallTimeoutSeconds: 1,
		Auth: config.AuthConfig{
			TokenUrl:   srv.URL,
			ClientCred: &config.ClientCredAuth{ClientId: "id", ClientSecret: "secret"},
		},
	}
	return conf, func() { close(release); srv.Close() }
}

func TestDownloadStallsAreCancelled(t *testing.T) {
	cases := map[string]http.HandlerFunc{
		"no response headers": func(w http.ResponseWriter, r *http.Request) {},
		"body stops midway": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "1000")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("EVENT_TYPE,REQUEST_ID\n"))
			w.(http.Flusher).Flush()
		},
	}
	for name, handler := range cases {
		t.Run(name, func(t *testing.T) {
			conf, stop := downloadServer(t, handler)
			defer stop()
			dir := t.TempDir()
			start := time.Now()
			_, err := DownloadCsvFile(context.Background(), conf, &cache.DummyCache{}, &EventLogfileRecord{Id: "0AT1", LogFile: "/log"}, dir)
			if err == nil {
				t.Fatal("expected a stall error")
			}
			if elapsed := time.Since(start); elapsed > 5*time.Second {
				t.Errorf("stalled download was not cancelled promptly: %s", elapsed)
			}
			if !strings.Contains(err.Error(), "no data received") {
				t.Errorf("error should explain the stall: %v", err)
			}
			if entries, _ := os.ReadDir(dir); len(entries) != 0 {
				t.Errorf("partial download left behind")
			}
		})
	}
}

func TestSlowButSteadyDownloadCompletes(t *testing.T) {
	const chunks = 12 // ~2.4s total, longer than the 1s stall timeout
	conf, stop := downloadServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(chunks*10))
		w.WriteHeader(http.StatusOK)
		for i := 0; i < chunks; i++ {
			w.Write([]byte("0123456789"))
			w.(http.Flusher).Flush()
			time.Sleep(200 * time.Millisecond)
		}
	})
	defer stop()
	path, err := DownloadCsvFile(context.Background(), conf, &cache.DummyCache{}, &EventLogfileRecord{Id: "0AT2", LogFile: "/log"}, t.TempDir())
	if err != nil {
		t.Fatalf("a download that keeps making progress must not be cancelled: %v", err)
	}
	if b, _ := os.ReadFile(path); len(b) != chunks*10 {
		t.Errorf("downloaded %d bytes", len(b))
	}
}
