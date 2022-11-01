package helper

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"time"
)

func CreateTopicBeforePublication(ctx context.Context, address, topic string, secure, skipVerify bool) {
	transport := kafka.Transport{}
	if secure {
		if skipVerify {
			transport.TLS = &tls.Config{InsecureSkipVerify: skipVerify}
		} else {
			transport.TLS = &tls.Config{}
		}
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(address),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		Transport:              &transport,
	}

	message := kafka.Message{Value: []byte(fmt.Sprintf("create %s", topic))}

	var err error
	const retries = 3
	for i := 0; i < retries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// attempt to create topic prior to publishing the message
		err = writer.WriteMessages(ctx, message)
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}

		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("unexpected error")
		}
	}

	if err = writer.Close(); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to close writer")
	}
}
