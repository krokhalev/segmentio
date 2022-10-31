package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/topics"
	"time"
)

func topicContains(topics []kafka.Topic, topic string) bool {
	for _, t := range topics {
		if t.Name == topic {
			return true
		}
	}
	return false
}

func ConsumeAndCommit() {
	transport := &kafka.Transport{}
	tlsConfig := tls.Config{InsecureSkipVerify: true}
	transport.TLS = &tlsConfig

	client := &kafka.Client{
		Addr:      kafka.TCP("172.30.254.97:9992"),
		Timeout:   10 * time.Second,
		Transport: transport,
	}

	ctx := context.Background()
	kafkaTopics, err := topics.List(ctx, client)
	if err != nil {
		fmt.Println(err)
	}
	if !topicContains(kafkaTopics, "testtopic") {
		fmt.Printf("no such topic available - %s", "testtopic")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"172.30.254.97:9992"},
		GroupID: "consumer-group-id",
		Topic:   "testtopic",
		Dialer:  &kafka.Dialer{TLS: &tlsConfig},
	})
	for {
		msg, err := reader.FetchMessage(ctx)
		if errors.Is(err, context.Canceled) {
			return
		}
		if err != nil {
			fmt.Println(err)
			continue
		} else {
			fmt.Printf("successful get message: %s", msg.Value)
		}

		err = reader.CommitMessages(ctx, msg)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Printf("successful commit message: %s", msg.Value)
		}
	}
}
