package stream

import (
	"errors"
	"fmt"
	"strings"

	"github.com/davtir78/salesforce-s3-archiver/internal/config"
)

// Config checks specific to the event stream integration
func IntegrityCheck(conf *config.Config) error {
	if conf.EventStream == nil {
		return errors.New("Config eventStream must be defined")
	}
	if conf.EventStream.Name == "" {
		return errors.New("Config eventStream instanceName must be defined")
	}
	if err := config.CheckAuth(&conf.EventStream.Auth); err != nil {
		return err
	}
	// Replay checkpoints must be durable: refuse to run without Redis.
	if conf.EventStream.Cache == nil || conf.EventStream.Cache.Redis == nil {
		return errors.New("'eventStream.cache.redis' is required: the stream collector refuses to run without durable replay checkpoints")
	}
	if err := config.CheckCache(conf.EventStream.Cache); err != nil {
		return err
	}
	if err := checkTopics(conf.EventStream.Topics); err != nil {
		return err
	}
	switch strings.ToUpper(conf.EventStream.InitialReplay) {
	case "", ReplayEarliest, ReplayLatest:
	default:
		return fmt.Errorf("'eventStream.initialReplay' must be EARLIEST, LATEST or empty, got '%s'", conf.EventStream.InitialReplay)
	}
	if conf.EventStream.LeaseTTLSeconds != 0 && conf.EventStream.LeaseTTLSeconds < 3 {
		return errors.New("'eventStream.leaseTtlSeconds' must be at least 3")
	}
	return nil
}

func checkTopics(topics []string) error {
	if topics == nil {
		return errors.New("'eventStream.topics' must be a list of strings")
	}
	if len(topics) == 0 {
		return errors.New("Empty 'eventStream.topics'")
	}
	seen := map[string]bool{}
	for _, t := range topics {
		if seen[t] {
			return fmt.Errorf("Duplicate topic '%s' in 'eventStream.topics'", t)
		}
		seen[t] = true
	}
	return nil
}
