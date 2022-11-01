package main

type Config struct {
	Segmentio SegmentioConfig `mapstructure:"segmentio"`
	LogLevel  string          `mapstructure:"log_level"`
}

type SegmentioConfig struct {
	Host            string         `mapstructure:"host"`
	Port            string         `mapstructure:"port"`
	Secure          bool           `mapstructure:"secure"`
	SkipVerify      bool           `mapstructure:"skip_verify"`
	Topic           string         `mapstructure:"topic"`
	AutoCreateTopic bool           `mapstructure:"auto_create_topic"`
	Producer        ProducerConfig `mapstructure:"producer"`
	Consumer        ConsumerConfig `mapstructure:"consumer"`
}

type ProducerConfig struct {
	ProduceMessages bool `mapstructure:"produce_messages"`
	MessagesCount   int  `mapstructure:"messages_count"`
}

type ConsumerConfig struct {
	ConsumeMessages bool   `mapstructure:"consume_messages"`
	GroupID         string `mapstructure:"group_id"`
	CommitMessages  bool   `mapstructure:"commit_messages"`
}
