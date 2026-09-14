// Package mocksf is a mock Salesforce org for local and CI testing.
//
// It implements enough of the OAuth, REST (SOQL, EventLogFile, limits) and
// Pub/Sub gRPC APIs for the collectors to run end to end, plus an admin API
// for fault injection and a ledger of everything generated so archives can be
// reconciled. Behaviour is modelled on public Salesforce documentation; it is
// not a substitute for validating against a real org.
package mocksf

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/integration/stream/pubsub/proto"
	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	"google.golang.org/grpc"
)

type Options struct {
	// Public base URL of the HTTP server as seen by clients (instance_url).
	BaseURL      string
	OrgId        string
	UserId       string
	ClientId     string
	ClientSecret string
	TokenTTL     time.Duration

	Topics []string
	// Events retained per topic for replay (0 = unlimited).
	RetentionEvents int
	// Interval between keepalive FetchResponses on idle subscriptions.
	KeepaliveInterval time.Duration
	// Maximum events per FetchResponse.
	MaxEventsPerResponse int

	// Rows returned per SOQL page before nextRecordsUrl is used.
	QueryPageSize int
	// Event types generated as EventLogFile entries.
	EventLogTypes []string
	// Rows per generated EventLogFile CSV.
	EventLogRows int
	// Objects that custom SOQL queries may target.
	CustomObjects []string
}

func (o *Options) defaults() {
	if o.OrgId == "" {
		o.OrgId = "00DMOCK000000001AAA"
	}
	if o.UserId == "" {
		o.UserId = "005MOCK000000001AAA"
	}
	if o.ClientId == "" {
		o.ClientId = "mock-client-id"
	}
	if o.ClientSecret == "" {
		o.ClientSecret = "mock-client-secret"
	}
	if o.TokenTTL == 0 {
		o.TokenTTL = 2 * time.Hour
	}
	if len(o.Topics) == 0 {
		o.Topics = []string{"/event/LoginEventStream", "/event/ApiEventStream"}
	}
	if o.KeepaliveInterval == 0 {
		o.KeepaliveInterval = 30 * time.Second
	}
	if o.MaxEventsPerResponse == 0 {
		o.MaxEventsPerResponse = 100
	}
	if o.QueryPageSize == 0 {
		o.QueryPageSize = 200
	}
	if len(o.EventLogTypes) == 0 {
		o.EventLogTypes = []string{"Login", "API", "URI"}
	}
	if o.EventLogRows == 0 {
		o.EventLogRows = 50
	}
	if len(o.CustomObjects) == 0 {
		o.CustomObjects = []string{"SetupAuditTrail"}
	}
}

// Faults are toggled at runtime through the admin API.
type Faults struct {
	// Reject the next N token requests with 500.
	FailLogins int `json:"failLogins"`
	// Close each subscription stream with UNAVAILABLE after delivering N events (0 = off).
	DropStreamAfterEvents int `json:"dropStreamAfterEvents"`
	// Return HTTP 500 for the next N REST requests (queries/downloads).
	FailRestRequests int `json:"failRestRequests"`
	// Truncate EventLogFile downloads after N bytes for the next download.
	TruncateNextDownload int `json:"truncateNextDownload"`
}

type Server struct {
	opts Options

	mu     sync.Mutex
	tokens map[string]time.Time
	faults Faults

	topics map[string]*topic

	elfs      []*eventLogFile
	elfLedger map[string][]string // file Id -> REQUEST_IDs

	custom       map[string][]map[string]any // object -> rows
	customLedger map[string][]string

	queryCursors map[string]queryCursor

	grpcServer *grpc.Server
	httpServer *http.Server

	stopGen chan struct{}
	genWG   sync.WaitGroup

	proto.UnimplementedPubSubServer
}

func New(opts Options) (*Server, error) {
	opts.defaults()
	s := &Server{
		opts:         opts,
		tokens:       map[string]time.Time{},
		topics:       map[string]*topic{},
		elfLedger:    map[string][]string{},
		custom:       map[string][]map[string]any{},
		customLedger: map[string][]string{},
		queryCursors: map[string]queryCursor{},
	}
	for _, name := range opts.Topics {
		t, err := newTopic(name, opts.RetentionEvents)
		if err != nil {
			return nil, err
		}
		s.topics[name] = t
	}
	return s, nil
}

func (s *Server) Options() Options { return s.opts }

// Handler returns the HTTP handler (OAuth, REST and admin API).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/services/oauth2/token", s.handleToken)
	mux.HandleFunc("/services/oauth2/userinfo", s.handleUserInfo)
	mux.HandleFunc("/services/data/", s.handleData)
	s.registerAdmin(mux)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	return logRequests(mux)
}

// ServeGRPC serves the Pub/Sub API on lis until the listener is closed.
func (s *Server) ServeGRPC(lis net.Listener) error {
	s.mu.Lock()
	s.grpcServer = grpc.NewServer()
	proto.RegisterPubSubServer(s.grpcServer, s)
	srv := s.grpcServer
	s.mu.Unlock()
	return srv.Serve(lis)
}

// ServeHTTP serves the HTTP handler on lis.
func (s *Server) ServeHTTP(lis net.Listener) error {
	s.mu.Lock()
	s.httpServer = &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second}
	srv := s.httpServer
	s.mu.Unlock()
	err := srv.Serve(lis)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// ReplayID returns the mock's replay ID encoding for an event sequence number.
func ReplayID(seq uint64) []byte { return replayBytes(seq) }

// Start listens on httpAddr and grpcAddr (use "127.0.0.1:0" for random ports)
// and serves until Stop. If opts.BaseURL is empty it is derived from httpAddr.
func Start(opts Options, httpAddr, grpcAddr string) (*Server, string, string, error) {
	httpLis, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return nil, "", "", err
	}
	grpcLis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		httpLis.Close()
		return nil, "", "", err
	}
	httpURL := "http://" + httpLis.Addr().String()
	if opts.BaseURL == "" {
		opts.BaseURL = httpURL
	}
	s, err := New(opts)
	if err != nil {
		httpLis.Close()
		grpcLis.Close()
		return nil, "", "", err
	}
	go func() {
		if err := s.ServeHTTP(httpLis); err != nil {
			log.Errorf("mock http server: %v", err)
		}
	}()
	go func() {
		if err := s.ServeGRPC(grpcLis); err != nil {
			log.Debugf("mock grpc server stopped: %v", err)
		}
	}()
	return s, httpURL, grpcLis.Addr().String(), nil
}

func (s *Server) Stop() {
	s.StopGenerator()
	s.mu.Lock()
	g, h := s.grpcServer, s.httpServer
	s.mu.Unlock()
	if g != nil {
		g.Stop()
	}
	if h != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		h.Shutdown(ctx)
	}
}

func (s *Server) SetFaults(f Faults) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = f
}

func (s *Server) GetFaults() Faults {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.faults
}

// ExpireTokens invalidates every issued access token.
func (s *Server) ExpireTokens() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens = map[string]time.Time{}
}

func (s *Server) issueToken() string {
	tok := "00DMOCK!" + randomHex(24)
	s.mu.Lock()
	s.tokens[tok] = time.Now().Add(s.opts.TokenTTL)
	s.mu.Unlock()
	return tok
}

func (s *Server) validToken(tok string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	exp, ok := s.tokens[tok]
	return ok && time.Now().Before(exp)
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	return ""
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b)
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Debugf("mock http %s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}
