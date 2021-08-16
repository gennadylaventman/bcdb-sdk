package main

import (
	"github.com/IBM-Blockchain/bcdb-sdk/pkg/config"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Config struct {
	ConnectionConfig config.ConnectionConfig
	SessionConfig    config.SessionConfig
	AdminConfig      config.SessionConfig
}

func ReadConfig(configFilePath string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(configFilePath)

	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrap(err, "error reading the config file")
	}

	c := &Config{}
	if err := v.UnmarshalExact(c); err != nil {
		return nil, errors.Wrap(err, "error while unmarshaling config")
	}

	return c, nil
}
