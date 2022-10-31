package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/segmentio/kafka-go"
)

func ProduceMessages() {
	tlsConfig := tls.Config{InsecureSkipVerify: true}

	writer := &kafka.Writer{
		BatchSize: 1,
		Async:     false,
		Addr:      kafka.TCP("172.30.254.97:9992"),
		Topic:     "testtopic",
		Transport: &kafka.Transport{
			TLS: &tlsConfig,
		},
		RequiredAcks: kafka.RequireAll,
	}

	msg := kafka.Message{
		Value: []byte("message one"),
		//Topic: "testtopic",
	}

	//msgs := []kafka.Message{
	//	{
	//		Value: []byte("message one"),
	//	},
	//	{
	//		Value: []byte("message two"),
	//	},
	//	{
	//		Value: []byte("message three"),
	//	},
	//}

	ctx := context.Background()
	//writer.WriteMessages(ctx, msgs...) // if many messages
	err := writer.WriteMessages(ctx, msg) // if one message
	if err != nil {
		fmt.Println(err)
	}
}
