package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

func CreateTopicBeforePublication() {
	topic := "testtopic"           // change to current
	adress := "172.30.254.97:9992" // change to current
	tlsConfig := tls.Config{InsecureSkipVerify: true}

	w := &kafka.Writer{
		Addr:                   kafka.TCP(adress),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		Transport: &kafka.Transport{
			TLS: &tlsConfig,
		},
	}

	messages := []kafka.Message{
		{
			Value: []byte("create testtopic"),
		},
	}

	var err error
	const retries = 3
	for i := 0; i < retries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// attempt to create topic prior to publishing the message
		err = w.WriteMessages(ctx, messages...)
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}

		if err != nil {
			fmt.Printf("unexpected error %v", err)
		}
	}

	if err := w.Close(); err != nil {
		fmt.Printf("failed to close writer: %v", err)
	}
}
