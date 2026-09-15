// mock-salesforce runs a mock Salesforce org (OAuth, REST, Pub/Sub gRPC) for
// local and CI testing. See internal/mocksf.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	"github.com/davtir78/salesforce-s3-archiver/internal/mocksf"
)

func main() {
	httpAddr := flag.String("http", envOr("MOCK_HTTP_ADDR", ":8080"), "HTTP listen address (OAuth, REST, admin)")
	grpcAddr := flag.String("grpc", envOr("MOCK_GRPC_ADDR", ":7011"), "Pub/Sub gRPC listen address")
	baseURL := flag.String("base-url", envOr("MOCK_BASE_URL", ""), "public base URL returned as instance_url (default http://<http addr>)")
	topics := flag.String("topics", envOr("MOCK_TOPICS", "/event/LoginEventStream,/event/ApiEventStream"), "comma-separated topics")
	clientId := flag.String("client-id", envOr("MOCK_CLIENT_ID", "mock-client-id"), "accepted OAuth client ID")
	clientSecret := flag.String("client-secret", envOr("MOCK_CLIENT_SECRET", "mock-client-secret"), "accepted OAuth client secret")
	retention := flag.Int("retention-events", 0, "events retained per topic for replay (0 = unlimited)")
	keepalive := flag.Duration("keepalive", 30*time.Second, "keepalive interval on idle subscriptions")
	pageSize := flag.Int("page-size", 200, "SOQL page size")
	eps := flag.Int("events-per-second", 0, "background events per second per topic")
	elfEvery := flag.Int("eventlogfile-every", 0, "seconds between generated EventLogFiles (0 = off)")
	customEvery := flag.Int("custom-every", 0, "seconds between generated custom query rows (0 = off)")
	flag.Parse()

	opts := mocksf.Options{
		BaseURL:           *baseURL,
		ClientId:          *clientId,
		ClientSecret:      *clientSecret,
		Topics:            strings.Split(*topics, ","),
		RetentionEvents:   *retention,
		KeepaliveInterval: *keepalive,
		QueryPageSize:     *pageSize,
	}
	srv, httpURL, grpcListen, err := mocksf.Start(opts, *httpAddr, *grpcAddr)
	if err != nil {
		log.Errorf("starting mock: %v", err)
		os.Exit(1)
	}
	log.Infof("Mock Salesforce listening: http=%s grpc=%s instance_url=%s", httpURL, grpcListen, srv.Options().BaseURL)

	if *eps > 0 || *elfEvery > 0 || *customEvery > 0 {
		srv.StartGenerator(mocksf.GeneratorConfig{EventsPerSecond: *eps, EventLogFileEverySeconds: *elfEvery, CustomRecordEverySeconds: *customEvery})
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	srv.Stop()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
