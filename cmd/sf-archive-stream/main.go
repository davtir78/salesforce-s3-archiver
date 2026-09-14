// sf-archive-stream archives Salesforce Pub/Sub API (Real-Time Event
// Monitoring) topics to S3. Replay checkpoints only advance after events are
// durably archived.
package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/archive"
	"github.com/davtir78/salesforce-s3-archiver/internal/cache"
	"github.com/davtir78/salesforce-s3-archiver/internal/checkpoint"
	"github.com/davtir78/salesforce-s3-archiver/internal/config"
	"github.com/davtir78/salesforce-s3-archiver/internal/integration/stream"
	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	"github.com/davtir78/salesforce-s3-archiver/internal/metrics"
)

func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("config", "config.yml", "path to the YAML config file")
	version := flag.Bool("version", false, "print version and exit")
	resetTopics := flag.String("reset-checkpoints", "", "operator recovery: delete stored replay checkpoints for these comma-separated topics (or \"all\") and exit. The collector must be stopped; on next start initialReplay decides where each topic resumes")
	confirm := flag.Bool("confirm", false, "required with -reset-checkpoints")
	flag.Parse()
	if *version {
		fmt.Println(archive.CollectorVersion)
		return 0
	}

	conf, err := config.ReadConfigFile(*configPath)
	if err != nil {
		log.Errorf("Error loading config: %v", err)
		return 2
	}
	if err := stream.IntegrityCheck(&conf); err != nil {
		log.Errorf("Invalid config: %v", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sink, err := archive.FromConfig(ctx, conf.Archive)
	if err != nil {
		log.Errorf("Error creating archive sink: %v", err)
		return 1
	}
	redisClient, err := cache.BuildRedisClient(conf.EventStream.Cache)
	if err != nil {
		log.Errorf("Error creating Redis client: %v", err)
		return 1
	}
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Errorf("Redis is unreachable; refusing to run without durable checkpoints: %v", err)
		return 1
	}

	store := checkpoint.NewRedisStore(redisClient, conf.EventStream.Cache.Redis.KeyPrefix)
	if *resetTopics != "" {
		return resetCheckpoints(ctx, &conf, store, *resetTopics, *confirm)
	}

	metrics.Serve(ctx, conf.MetricsAddr)
	log.Infof("Starting stream collector %s for %d topic(s), version %s", conf.EventStream.Name, len(conf.EventStream.Topics), archive.CollectorVersion)

	if err := stream.RunService(ctx, &conf, sink, store, stream.DefaultClientFactory(&conf)); err != nil {
		log.Errorf("Stream collector failed: %v", err)
		return 1
	}
	return 0
}

// resetCheckpoints deletes replay checkpoints so topics can be re-bootstrapped
// after a replay ID was rejected (e.g. retention expired or sandbox refresh).
// Any events between the old checkpoint and the new start point must be
// recovered another way (EventLogFiles or stored event objects).
func resetCheckpoints(ctx context.Context, conf *config.Config, store checkpoint.Store, topicsArg string, confirm bool) int {
	topics, err := resolveResetTopics(conf.EventStream.Topics, topicsArg)
	if err != nil {
		log.Errorf("%v", err)
		return 2
	}
	if !confirm {
		log.Errorf("Refusing to reset checkpoints for %v without -confirm. This can create a gap in the archive.", topics)
		return 2
	}
	ttl := time.Duration(conf.EventStream.LeaseTTLSeconds) * time.Second
	if ttl == 0 {
		ttl = 30 * time.Second
	}
	for _, topic := range topics {
		acquireCtx, cancel := context.WithTimeout(ctx, 2*ttl)
		lease, err := store.Acquire(acquireCtx, checkpoint.Key(conf.EventStream.Name, topic), ttl)
		cancel()
		if err != nil {
			log.Errorf("Could not acquire the lease for %s (is a collector still running?): %v", topic, err)
			return 1
		}
		old, ok, err := lease.Load(ctx)
		if err == nil {
			err = lease.Clear(ctx)
		}
		lease.Release(ctx)
		if err != nil {
			log.Errorf("Resetting checkpoint for %s failed: %v", topic, err)
			return 1
		}
		if ok {
			log.Warnf("CHECKPOINT RESET topic=%s previousReplayId=%s: events after this point that are no longer retained by Salesforce must be backfilled", topic, base64.StdEncoding.EncodeToString(old))
		} else {
			log.Infof("No checkpoint stored for %s", topic)
		}
	}
	log.Infof("Reset complete. Start the collector with eventStream.initialReplay set to EARLIEST or LATEST.")
	return 0
}

// resolveResetTopics validates -reset-checkpoints arguments against the
// configured topics, so a typo cannot silently "succeed" while the real
// checkpoint is left in place.
func resolveResetTopics(configured []string, arg string) ([]string, error) {
	if strings.TrimSpace(arg) == "all" {
		return configured, nil
	}
	known := map[string]bool{}
	for _, t := range configured {
		known[t] = true
	}
	var topics, unknown []string
	for _, t := range strings.Split(arg, ",") {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if !known[t] {
			unknown = append(unknown, t)
			continue
		}
		topics = append(topics, t)
	}
	if len(unknown) > 0 {
		return nil, fmt.Errorf("unknown topic(s) %v: -reset-checkpoints only accepts topics configured in eventStream.topics %v (or \"all\")", unknown, configured)
	}
	if len(topics) == 0 {
		return nil, fmt.Errorf("no topics given to -reset-checkpoints")
	}
	return topics, nil
}
