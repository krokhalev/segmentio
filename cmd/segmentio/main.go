package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/krokhalev/segmentio/internal/kafka/forwarder"
	"github.com/krokhalev/segmentio/internal/kafka/getter"
	"github.com/krokhalev/segmentio/internal/kafka/helper"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

func readYamlConfig() Config {
	var confPath string
	flag.StringVar(&confPath, "config", ".", "PATH to config folder")
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(confPath)
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Fatal().Str("error", fmt.Sprintf("config file not found in path: %s", confPath)).Msg("viper read error")
		} else {
			log.Fatal().Str("error", fmt.Sprintf("error while reading config file: %s", err.Error())).Msg("viper read error")
		}
	}
	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		log.Fatal().Str("error", fmt.Sprintf("Unable to unmarshall the config: %s", err.Error())).Msg("viper unmarshal error")
	}
	return config
}

func main() {
	config := readYamlConfig()

	level, err := zerolog.ParseLevel(config.LogLevel)
	if err != nil {
		log.Fatal().Err(err).Str("error", fmt.Sprintf("unknown log level: %s", config.LogLevel)).Msg("parse level error")
	}
	ctx := context.Background()
	ctx = log.Logger.Level(level).WithContext(ctx)

	if config.Segmentio.AutoCreateTopic {
		helper.CreateTopicBeforePublication(
			ctx,
			config.Segmentio.Host+":"+config.Segmentio.Port,
			config.Segmentio.Topic,
			config.Segmentio.Secure,
			config.Segmentio.SkipVerify,
		)
	}

	if config.Segmentio.Forwarder.ProduceMessages {
		forwarder.ProduceMessages(
			ctx,
			config.Segmentio.Host+":"+config.Segmentio.Port,
			config.Segmentio.Topic,
			config.Segmentio.Secure,
			config.Segmentio.SkipVerify,
			config.Segmentio.Forwarder.MessagesCount,
		)
	}

	if config.Segmentio.Getter.ConsumeMessages {
		getter.ConsumeAndCommit(
			ctx,
			config.Segmentio.Host+":"+config.Segmentio.Port,
			config.Segmentio.Topic,
			config.Segmentio.Secure,
			config.Segmentio.SkipVerify,
			config.Segmentio.Getter.GroupID,
			config.Segmentio.Getter.CommitMessages,
		)
	}
}
