package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/archive"
	"github.com/davtir78/salesforce-s3-archiver/internal/checkpoint"
	"github.com/davtir78/salesforce-s3-archiver/internal/config"
	"github.com/davtir78/salesforce-s3-archiver/internal/integration/stream/pubsub/grpcclient"
	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	"github.com/davtir78/salesforce-s3-archiver/internal/metrics"
)

// OptionsFromConfig builds subscriber options for a topic.
func OptionsFromConfig(conf *config.Config, topic string) SubscriberOptions {
	es := conf.EventStream
	return SubscriberOptions{
		Topic:         topic,
		Instance:      es.Name,
		OrgId:         conf.OrgId,
		Appetite:      es.Appetite,
		MaxEvents:     es.Batch.MaxEvents,
		MaxAge:        time.Duration(es.Batch.MaxAgeSeconds) * time.Second,
		InitialReplay: es.InitialReplay,
		LeaseTTL:      time.Duration(es.LeaseTTLSeconds) * time.Second,
	}
}

// ClientFactory creates a Pub/Sub client per topic.
type ClientFactory func() (Client, func(), error)

func DefaultClientFactory(conf *config.Config) ClientFactory {
	return func() (Client, func(), error) {
		c, err := grpcclient.NewGRPCClient(grpcclient.Options{
			Endpoint: conf.EventStream.PubSubEndpoint,
			Insecure: conf.EventStream.PubSubInsecure,
			Auth:     conf.EventStream.Auth,
		})
		if err != nil {
			return nil, nil, err
		}
		return c, c.Close, nil
	}
}

// RunService runs one subscriber per configured topic. The first fatal error
// cancels every subscriber (each flushes what it can) and is returned.
func RunService(ctx context.Context, conf *config.Config, sink archive.Sink, store checkpoint.Store, newClient ClientFactory) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
		started  sync.WaitGroup
	)
	for _, topic := range conf.EventStream.Topics {
		client, closeFn, err := newClient()
		if err != nil {
			cancel()
			wg.Wait()
			return fmt.Errorf("creating Pub/Sub client for %s: %w", topic, err)
		}
		sub := NewSubscriber(OptionsFromConfig(conf, topic), client, sink, store)
		wg.Add(1)
		started.Add(1)
		go func(topic string) {
			defer wg.Done()
			defer closeFn()
			started.Done()
			if err := sub.Run(ctx); err != nil {
				log.Errorf("Topic %s stopped: %v", topic, err)
				errOnce.Do(func() {
					firstErr = fmt.Errorf("topic %s: %w", topic, err)
					cancel()
				})
			}
		}(topic)
	}
	started.Wait()
	metrics.SetReady(true)
	wg.Wait()
	metrics.SetReady(false)
	if firstErr != nil {
		return firstErr
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		log.Infof("Stream collector stopped")
	}
	return nil
}
