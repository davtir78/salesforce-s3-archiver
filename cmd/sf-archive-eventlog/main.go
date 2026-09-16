// sf-archive-eventlog archives Salesforce EventLogFiles, custom SOQL query
// results and org limits to S3 on a polling interval.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/archive"
	"github.com/davtir78/salesforce-s3-archiver/internal/cache"
	"github.com/davtir78/salesforce-s3-archiver/internal/config"
	"github.com/davtir78/salesforce-s3-archiver/internal/integration/eventlog"
	"github.com/davtir78/salesforce-s3-archiver/internal/integration/stream/pubsub/grpcclient"
	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	"github.com/davtir78/salesforce-s3-archiver/internal/metrics"
	"github.com/davtir78/salesforce-s3-archiver/internal/oauth"
)

const defaultPollInterval = 300 * time.Second

func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("config", "config.yml", "path to the YAML config file")
	once := flag.Bool("once", false, "run a single poll and exit (for scheduled tasks / CronJobs)")
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
	instance := conf.EventLog
	if instance == nil {
		log.Errorf("Invalid config: 'eventLog' must be defined")
		return 2
	}
	queries, err := eventlog.ParseQueryFiles(instance.CustomQueryFiles, instance.ApiVer)
	if err != nil {
		log.Errorf("Error parsing external query files: %v", err)
		return 2
	}
	instance.CustomQueries = append(instance.CustomQueries, queries...)
	if instance.FieldMappingFile != "" {
		mapping, err := eventlog.ParseMappingFile(instance.FieldMappingFile)
		if err != nil {
			log.Errorf("Error parsing field mapping file: %v", err)
			return 2
		}
		instance.FieldMapping = mapping
	}
	if err := eventlog.IntegrityCheck(&conf); err != nil {
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
	db, err := cache.BuildCache(instance.Cache)
	if err != nil {
		log.Errorf("Error creating cache: %v", err)
		return 1
	}

	orgId := conf.OrgId
	if orgId == "" {
		orgId, err = discoverOrgId(ctx, &instance.Auth)
		if err != nil {
			log.Errorf("Could not determine the organisation ID (set 'orgId' to override): %v", err)
			return 1
		}
	}

	metrics.Serve(ctx, conf.MetricsAddr)
	collector := eventlog.NewCollector(instance, orgId, conf.Env, db, sink)

	interval := time.Duration(instance.PollIntervalSeconds) * time.Second
	if interval == 0 {
		interval = defaultPollInterval
	}
	log.Infof("Starting event log collector %s for org %s (poll interval %s, version %s)", instance.Name, orgId, interval, archive.CollectorVersion)
	metrics.SetReady(true)

	for {
		start := time.Now()
		err := collector.Poll(ctx)
		if ctx.Err() != nil {
			log.Infof("Event log collector stopped")
			return 0
		}
		if err != nil {
			log.Errorf("Poll completed with errors (watermarks not advanced past failures): %v", err)
		} else {
			log.Infof("Poll completed in %s", time.Since(start).Round(time.Millisecond))
		}
		if *once {
			if err != nil {
				return 1
			}
			return 0
		}
		select {
		case <-ctx.Done():
			log.Infof("Event log collector stopped")
			return 0
		case <-time.After(interval):
		}
	}
}

func discoverOrgId(ctx context.Context, auth *config.AuthConfig) (string, error) {
	login, err := oauth.Login(*auth)
	if err != nil {
		return "", err
	}
	info, err := grpcclient.UserInfo(ctx, auth.TokenUrl, login.AccessToken)
	if err != nil {
		return "", err
	}
	if info.OrganizationID == "" {
		return "", fmt.Errorf("userinfo response has no organization_id")
	}
	return info.OrganizationID, nil
}
