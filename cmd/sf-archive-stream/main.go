// sf-archive-stream archives Salesforce Pub/Sub API (Real-Time Event
// Monitoring) topics to S3. Replay checkpoints only advance after events are
// durably archived.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	metrics.Serve(ctx, conf.MetricsAddr)
	log.Infof("Starting stream collector %s for %d topic(s), version %s", conf.EventStream.Name, len(conf.EventStream.Topics), archive.CollectorVersion)

	store := checkpoint.NewRedisStore(redisClient, conf.EventStream.Cache.Redis.KeyPrefix)
	if err := stream.RunService(ctx, &conf, sink, store, stream.DefaultClientFactory(&conf)); err != nil {
		log.Errorf("Stream collector failed: %v", err)
		return 1
	}
	return 0
}
