package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds application configuration.
type Config struct {
	AdminAK string        `mapstructure:"admin_key" yaml:"admin_key"`
	Server  ServerConfig  `mapstructure:"server" yaml:"server"`
	Broker  BrokerConfig  `mapstructure:"broker" yaml:"broker"`
	Storage StorageConfig `mapstructure:"storage" yaml:"storage"`
	Logging LoggingConfig `mapstructure:"logging" yaml:"logging"`
}

// ServerConfig holds HTTP server configuration.
type ServerConfig struct {
	Port            int           `mapstructure:"port" yaml:"port"`
	Host            string        `mapstructure:"host" yaml:"host"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout" yaml:"shutdown_timeout"`
}

// BrokerConfig holds broker configuration.
type BrokerConfig struct {
	QueueCount             int           `mapstructure:"queue_count" yaml:"queue_count"`
	MaxRetry               int           `mapstructure:"max_retry" yaml:"max_retry"`
	ProcessingTimeout      time.Duration `mapstructure:"processing_timeout" yaml:"processing_timeout"`
	TimeoutCheckInterval   time.Duration `mapstructure:"timeout_check_interval" yaml:"timeout_check_interval"`
	DataDir                string        `mapstructure:"data_dir" yaml:"data_dir"`
	RetryBackoffBase       time.Duration `mapstructure:"retry_backoff_base" yaml:"retry_backoff_base"`
	RetryBackoffMultiplier float64       `mapstructure:"retry_backoff_multiplier" yaml:"retry_backoff_multiplier"`
	RetryBackoffMax        time.Duration `mapstructure:"retry_backoff_max" yaml:"retry_backoff_max"`
	MessageRetention       time.Duration `mapstructure:"message_retention" yaml:"message_retention"`
	MessageExpiryFactor    int           `mapstructure:"message_expiry_factor" yaml:"message_expiry_factor"`
	NewGroupStartPosition  string        `mapstructure:"new_group_start_position" yaml:"new_group_start_position"`
	CancelledCacheLimit    int           `mapstructure:"cancelled_cache_limit" yaml:"cancelled_cache_limit"`
}

// StorageConfig holds storage configuration.
type StorageConfig struct {
	FlushInterval    time.Duration `mapstructure:"flush_interval" yaml:"flush_interval"`
	CompactInterval  time.Duration `mapstructure:"compact_interval" yaml:"compact_interval"`
	CompactThreshold int64         `mapstructure:"compact_threshold" yaml:"compact_threshold"`
	MaxRecordSize    int           `mapstructure:"max_record_size" yaml:"max_record_size"`
	BufferSize       int           `mapstructure:"buffer_size" yaml:"buffer_size"`
}

// LoggingConfig holds logging configuration.
type LoggingConfig struct {
	Level      string `mapstructure:"level" yaml:"level"`
	OutputPath string `mapstructure:"output_path" yaml:"output_path"`
	Format     string `mapstructure:"format" yaml:"format"`
}

// Load loads config from file/env.
func Load() (*Config, error) {
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("./config")
	v.AddConfigPath("./mq/config")
	v.SetEnvPrefix("MQ")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Set defaults
	v.SetDefault("admin_key", "")

	// Server defaults
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.host", "")
	v.SetDefault("server.shutdown_timeout", "10s")

	// Broker defaults
	v.SetDefault("broker.queue_count", 4)
	v.SetDefault("broker.max_retry", 3)
	v.SetDefault("broker.processing_timeout", "30s")
	v.SetDefault("broker.timeout_check_interval", "5s")
	v.SetDefault("broker.data_dir", "./data")
	v.SetDefault("broker.retry_backoff_base", "1s")
	v.SetDefault("broker.retry_backoff_multiplier", 2.0)
	v.SetDefault("broker.retry_backoff_max", "60s")
	v.SetDefault("broker.message_retention", "168h")
	v.SetDefault("broker.message_expiry_factor", 2)
	v.SetDefault("broker.new_group_start_position", "topic_progress")
	v.SetDefault("broker.cancelled_cache_limit", 10000)

	// Storage defaults
	v.SetDefault("storage.flush_interval", "100ms")
	v.SetDefault("storage.compact_interval", "5m")
	v.SetDefault("storage.compact_threshold", 10*1024*1024) // 10MB
	v.SetDefault("storage.max_record_size", 64*1024*1024)   // 64MB
	v.SetDefault("storage.buffer_size", 64*1024)            // 64KB

	// Logging defaults
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.output_path", "stdout")
	v.SetDefault("logging.format", "console")

	_ = v.ReadInConfig() // ignore missing config

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
