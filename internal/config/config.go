package config

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"regexp"

	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

type AuthConfig struct {
	TokenUrl   string          `mapstructure:"tokenUrl"`
	UserPass   *UserPassAuth   `mapstructure:"userPass"`
	Jwt        *JwtAuth        `mapstructure:"jwt"`
	ClientCred *ClientCredAuth `mapstructure:"clientCred"`
}

type UserPassAuth struct {
	ClientId     string `mapstructure:"clientId"`
	ClientSecret string `mapstructure:"clientSecret"`
	Username     string `mapstructure:"username"`
	Password     string `mapstructure:"password"`
}

type JwtAuth struct {
	ClientId   string `mapstructure:"clientId"`
	PrivateKey string `mapstructure:"privateKey"`
	Username   string `mapstructure:"username"`
}

type ClientCredAuth struct {
	ClientId     string `mapstructure:"clientId"`
	ClientSecret string `mapstructure:"clientSecret"`
}

type CacheConfig struct {
	Redis *RedisConfig `mapstructure:"redis"`
}

type RedisConfig struct {
	Host     string `mapstructure:"host"`
	Port     uint   `mapstructure:"port"`
	DbNumber uint   `mapstructure:"dbNumber"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	// Expiry for tokens and de-duplication markers only (default 7 days).
	// Watermarks and stream replay checkpoints never expire.
	ExpireDays uint `mapstructure:"expireDays"`
	// "standalone" (default, also ElastiCache cluster-mode-disabled) or "cluster".
	Mode                string             `mapstructure:"mode"`
	TLS                 RedisTLSConfig     `mapstructure:"tls"`
	IAMAuth             RedisIAMAuthConfig `mapstructure:"iamAuth"`
	DialTimeoutSeconds  uint               `mapstructure:"dialTimeoutSeconds"`
	ReadTimeoutSeconds  uint               `mapstructure:"readTimeoutSeconds"`
	WriteTimeoutSeconds uint               `mapstructure:"writeTimeoutSeconds"`
	MaxRetries          int                `mapstructure:"maxRetries"`
	KeyPrefix           string             `mapstructure:"keyPrefix"`
}

type RedisTLSConfig struct {
	Enabled            bool   `mapstructure:"enabled"`
	InsecureSkipVerify bool   `mapstructure:"insecureSkipVerify"`
	CAFile             string `mapstructure:"caFile"`
	ServerName         string `mapstructure:"serverName"`
}

type RedisIAMAuthConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	CacheName  string `mapstructure:"cacheName"`
	UserId     string `mapstructure:"userId"`
	Region     string `mapstructure:"region"`
	Serverless bool   `mapstructure:"serverless"`
}

type ArchiveConfig struct {
	S3    *S3Config           `mapstructure:"s3"`
	Local *LocalArchiveConfig `mapstructure:"local"`
}

type S3Config struct {
	Bucket               string `mapstructure:"bucket"`
	Prefix               string `mapstructure:"prefix"`
	Region               string `mapstructure:"region"`
	Endpoint             string `mapstructure:"endpoint"`
	ForcePathStyle       bool   `mapstructure:"forcePathStyle"`
	KmsKeyId             string `mapstructure:"kmsKeyId"`
	StorageClass         string `mapstructure:"storageClass"`
	MaxAttempts          int    `mapstructure:"maxAttempts"`
	UploadTimeoutSeconds int    `mapstructure:"uploadTimeoutSeconds"`
}

type LocalArchiveConfig struct {
	Dir string `mapstructure:"dir"`
}

type BatchConfig struct {
	MaxEvents     int `mapstructure:"maxEvents"`
	MaxAgeSeconds int `mapstructure:"maxAgeSeconds"`
}

type EventStreamConfig struct {
	Name     string       `mapstructure:"instanceName"`
	Auth     AuthConfig   `mapstructure:"auth"`
	Cache    *CacheConfig `mapstructure:"cache"`
	Appetite int32        `mapstructure:"appetite"`
	Topics   []string     `mapstructure:"topics"`
	// Pub/Sub API endpoint, default api.pubsub.salesforce.com:7443.
	PubSubEndpoint string `mapstructure:"pubsubEndpoint"`
	// Disable TLS for the Pub/Sub connection (mock servers only).
	PubSubInsecure bool `mapstructure:"pubsubInsecure"`
	// Where to start when no checkpoint exists: "EARLIEST" or "LATEST".
	// Empty (default) refuses to start without a checkpoint.
	InitialReplay string      `mapstructure:"initialReplay"`
	Batch         BatchConfig `mapstructure:"batch"`
	// Lease TTL guarding each topic's checkpoint against concurrent writers.
	LeaseTTLSeconds int `mapstructure:"leaseTtlSeconds"`
}

type FieldNames = []string
type FieldMappingConfig = map[string]FieldNames

type FieldMappingFileModel struct {
	Mapping FieldMappingConfig `mapstructure:"mapping"`
}

type LimitsConfig struct {
	ApiVer string   `mapstructure:"apiVer"`
	Names  []string `mapstructure:"names"`
}

type ExternalQueryFileConfig struct {
	Queries []QueryConfig `mapstructure:"queries"`
}

type QueryConfig struct {
	Soql         SoqlConfig `mapstructure:"soql"`
	ApiVer       string     `mapstructure:"apiVer"`
	Timestamp    string     `mapstructure:"timestamp"`
	EndTimestamp string     `mapstructure:"endTimestamp"`
	ApiName      string     `mapstructure:"apiName"`
	CustomId     []string   `mapstructure:"customId"`
}

type SoqlConfig struct {
	Select []string `mapstructure:"select"`
	From   string   `mapstructure:"from"`
	Where  string   `mapstructure:"where"`
	Tail   string   `mapstructure:"tail"`
}

type TimeIntervalConfig struct {
	Hours   uint `mapstructure:"hours"`
	Minutes uint `mapstructure:"minutes"`
}

type EventLogConfig struct {
	Name                string             `mapstructure:"instanceName"`
	ApiVer              string             `mapstructure:"apiVer"`
	RequestTimeout      uint               `mapstructure:"requestTimeout"`
	Auth                AuthConfig         `mapstructure:"auth"`
	Cache               *CacheConfig       `mapstructure:"cache"`
	EventTypes          []string           `mapstructure:"eventTypes"`
	FieldMappingFile    string             `mapstructure:"fieldMappingFile"`
	FieldMapping        FieldMappingConfig `mapstructure:"fieldMapping"`
	InitialTimeInterval TimeIntervalConfig `mapstructure:"initialTimeInterval"`
	SkipLogFiles        bool               `mapstructure:"skipLogFiles"`
	SkipLimits          bool               `mapstructure:"skipLimits"`
	NoInterval          bool               `mapstructure:"noInterval"`
	CustomQueryFiles    []string           `mapstructure:"customQueryFiles"`
	CustomQueries       []QueryConfig      `mapstructure:"customQueries"`
	Limits              LimitsConfig       `mapstructure:"limits"`
	// Seconds between polls (default 300).
	PollIntervalSeconds uint `mapstructure:"pollIntervalSeconds"`
	// Minutes subtracted from watermarks on each poll so late-arriving
	// records are not skipped (default 60). Duplicates are removed downstream.
	WatermarkOverlapMinutes uint `mapstructure:"watermarkOverlapMinutes"`
	// Cancel an EventLogFile download that receives no data for this many
	// seconds (default 120). Downloads have no overall timeout.
	DownloadStallTimeoutSeconds uint `mapstructure:"downloadStallTimeoutSeconds"`
	// Polls a file may fail CSV parsing before its raw lines are archived to a
	// quarantine object and processing moves past it (default 3).
	MalformedFileAttempts int `mapstructure:"malformedFileAttempts"`
	// Records per archived object for custom queries (default 10000).
	RecordsPerObject int `mapstructure:"recordsPerObject"`
}

type Config struct {
	Version     string             `mapstructure:"version"`
	IsTemplate  bool               `mapstructure:"isTemplate"`
	EventStream *EventStreamConfig `mapstructure:"eventStream"`
	EventLog    *EventLogConfig    `mapstructure:"eventLog"`
	Archive     ArchiveConfig      `mapstructure:"archive"`
	// Organisation ID override; normally discovered from the userinfo endpoint.
	OrgId string `mapstructure:"orgId"`
	// Address for the /metrics and /healthz HTTP server, e.g. ":9090". Empty disables it.
	MetricsAddr string `mapstructure:"metricsAddr"`
	LogLevel    string `mapstructure:"logLevel"`
}

func envVarDecoder() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data any,
	) (any, error) {
		if t == reflect.TypeOf(Config{}) {
			scanEnvVars(data.(map[string]any))
			return data, nil
		} else {
			return data, nil
		}
	}
}

// Check if the value of a field is an env var "$VAR_NAM", and read it.
func scanEnvVars(dict map[string]any) {
	for key, val := range dict {
		switch val := val.(type) {
		case map[string]any:
			scanEnvVars(val)
		case string:
			var re = regexp.MustCompile(`^\$[a-zA-Z_]+[a-zA-Z0-9_]*`)
			loc := re.FindStringIndex(val)
			// Regex is a full match
			isEnvVar := len(loc) == 2 && (loc[0] == 0 && loc[1] == len(val))
			if isEnvVar {
				varName := val[1:]
				envVal, exists := os.LookupEnv(varName)
				if exists {
					dict[key] = envVal
				} else {
					log.Fatalf(fmt.Errorf("Env var %s does not exist", varName))
				}
			}
		}
	}
}

// ReadConfigFile loads a YAML config file. Values of the form "$VAR" are
// replaced with the environment variable VAR.
func ReadConfigFile(path string) (Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return Config{}, fmt.Errorf("reading config file '%s': %w", path, err)
	}
	return unmarshalConfig(v)
}

// ReadConfig decodes the config already loaded into the global viper instance.
func ReadConfig() (Config, error) {
	return unmarshalConfig(viper.GetViper())
}

func unmarshalConfig(v *viper.Viper) (Config, error) {
	conf := Config{}
	decoderConf := viper.DecodeHook(
		mapstructure.ComposeDecodeHookFunc(
			envVarDecoder(),
		),
	)
	if err := v.Unmarshal(&conf, decoderConf); err != nil {
		return Config{}, err
	}

	if err := integrityCheck(&conf); err != nil {
		return Config{}, err
	}

	if conf.LogLevel != "" {
		log.SetLevel(conf.LogLevel)
	}

	return conf, nil
}

// Check config integrity, the parts that are common to both integrations
func integrityCheck(conf *Config) error {
	if conf.IsTemplate {
		return errors.New("Config file is a template")
	}
	verCompos := strings.Split(conf.Version, ".")
	if len(verCompos) != 2 {
		return errors.New("Conf file, version key must be 'X.Y'")
	}
	major, err := strconv.Atoi(verCompos[0])
	if err != nil {
		return errors.New("Conf file, wrong version key format, major must be a number")
	}
	minor, err := strconv.Atoi(verCompos[1])
	if err != nil {
		return errors.New("Conf file, wrong version key format, minor must be a number")
	}
	if major != 3 {
		return fmt.Errorf("Conf file major version is '%d', expected '3'", major)
	}
	if minor != 0 {
		log.Warnf("Conf file minor version is '%d', expected '0'", minor)
	}
	return CheckArchive(&conf.Archive)
}
