package config

import (
	"strings"

	"github.com/spf13/viper"
)

// Config holds application configuration.
type Config struct {
	AdminAK string `mapstructure:"admin_ak"`
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
	v.SetDefault("admin_ak", "")

	_ = v.ReadInConfig() // ignore missing config

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
