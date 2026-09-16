package eventlog

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/davtir78/salesforce-s3-archiver/internal/config"
	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	"github.com/spf13/viper"
)

var validEventType = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// limitOrOffset matches LIMIT or OFFSET as a SOQL keyword, however it is spaced.
var limitOrOffset = regexp.MustCompile(`(?i)\b(LIMIT|OFFSET)\b`)

// selectsField reports whether a SOQL select list contains field.
func selectsField(selected []string, field string) bool {
	for _, s := range selected {
		if strings.EqualFold(strings.TrimSpace(s), field) {
			return true
		}
	}
	return false
}

const (
	defaultApiVer  = "55.0"
	defaultTimeout = 30
	apiNameRest    = "rest"
	apiNameTooling = "tooling"
	defaultApiName = apiNameRest
)

// Config checks specific to the event log integration
func IntegrityCheck(conf *config.Config) error {
	instance := conf.EventLog
	if instance == nil {
		return errors.New("Config eventLog must be defined")
	}
	if instance.Name == "" {
		// Cache keys are namespaced by instance name; sharing one Redis between
		// unnamed instances would mix watermarks and tokens between orgs.
		return errors.New("Config eventLog instanceName must be defined")
	}
	if instance.RequestTimeout == 0 {
		instance.RequestTimeout = defaultTimeout
	}
	if err := config.CheckAuth(&instance.Auth); err != nil {
		return err
	}
	if err := config.CheckCache(instance.Cache); err != nil {
		return err
	}
	if instance.ApiVer == "" {
		log.Warnf("Config 'apiVer' not defined, using default: '%s'", defaultApiVer)
		instance.ApiVer = defaultApiVer
	}
	for eventTypeIndex := range instance.EventTypes {
		eventType := &instance.EventTypes[eventTypeIndex]
		// Event types are interpolated into SOQL string literals.
		if !validEventType.MatchString(*eventType) {
			return fmt.Errorf("Instance '%s' contains an invalid event type: '%s' (letters, digits and underscore only)", instance.Name, *eventType)
		}
	}
	for customQueryIndex := range instance.CustomQueries {
		customQuery := &instance.CustomQueries[customQueryIndex]
		if customQuery.ApiVer == "" {
			customQuery.ApiVer = instance.ApiVer
		}
		if customQuery.Timestamp == "" {
			return fmt.Errorf("All custom queries must contain a 'timestamp' attribute")
		}
		switch customQuery.ApiName {
		case "":
			customQuery.ApiName = defaultApiName
		case apiNameRest, apiNameTooling:
			// do nothing
		default:
			return fmt.Errorf("The 'apiName' must be either '%s' or '%s'", apiNameRest, apiNameTooling)
		}
		if customQuery.Soql.From == "" {
			return fmt.Errorf("All custom queries must contain a SOQL 'from' definition")
		}
		if len(customQuery.Soql.Select) == 0 {
			return fmt.Errorf("All custom queries must contain at least one SOQL 'select' attribute")
		}
		// The timestamp value is part of each row's de-duplication key, so it
		// must actually be selected.
		if !selectsField(customQuery.Soql.Select, customQuery.Timestamp) {
			return fmt.Errorf("Custom query on '%s' must select its timestamp field '%s'", customQuery.Soql.From, customQuery.Timestamp)
		}
		if customQuery.EndTimestamp != "" && !selectsField(customQuery.Soql.Select, customQuery.EndTimestamp) {
			return fmt.Errorf("Custom query on '%s' must select its endTimestamp field '%s'", customQuery.Soql.From, customQuery.EndTimestamp)
		}
		if limitOrOffset.MatchString(customQuery.Soql.Tail) {
			return fmt.Errorf("Custom query on '%s' must not use LIMIT or OFFSET in 'tail': a truncated result set would move the watermark past rows that were never read", customQuery.Soql.From)
		}
	}
	if instance.Limits.ApiVer == "" {
		instance.Limits.ApiVer = instance.ApiVer
	}

	return nil
}

func ParseQueryFiles(files []string, instanceApiVer string) ([]config.QueryConfig, error) {
	queries := []config.QueryConfig{}
	for index := range files {
		query, err := readExternalQueryConf(files[index])
		if err != nil {
			return nil, fmt.Errorf("Could not read file '%s': %s", files[index], err)
		} else {
			queries = append(queries, query...)
		}
	}
	return queries, nil
}

func ParseMappingFile(file string) (config.FieldMappingConfig, error) {
	localViper := viper.New()
	localViper.SetConfigFile(file)
	err := localViper.ReadInConfig()
	if err != nil {
		return config.FieldMappingConfig{}, err
	}
	mappingConf := config.FieldMappingFileModel{}
	if err := localViper.Unmarshal(&mappingConf); err != nil {
		return config.FieldMappingConfig{}, err
	}
	return mappingConf.Mapping, nil
}

func readExternalQueryConf(file string) ([]config.QueryConfig, error) {
	localViper := viper.New()
	localViper.SetConfigFile(file)
	err := localViper.ReadInConfig()
	if err != nil {
		return nil, err
	}
	queryConf := config.ExternalQueryFileConfig{}
	if err := localViper.Unmarshal(&queryConf); err != nil {
		return nil, err
	}
	return queryConf.Queries, nil
}
