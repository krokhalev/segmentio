package forwarder

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

func ProduceMessages(ctx context.Context, address, topic string, secure, skipVerify bool, messagesCount int) {
	transport := kafka.Transport{}
	if secure {
		if skipVerify {
			transport.TLS = &tls.Config{InsecureSkipVerify: skipVerify}
		} else {
			transport.TLS = &tls.Config{}
		}
	}

	writer := &kafka.Writer{
		BatchSize:    1,
		Async:        false,
		Addr:         kafka.TCP(address),
		Topic:        topic,
		Transport:    &transport,
		RequiredAcks: kafka.RequireAll,
	}

	for i := 0; i <= messagesCount; i++ {
		message := kafka.Message{
			Value: []byte(fmt.Sprintf("message %v", i)),
			//Topic: topic, // send message to specific topic
		}
		err := writer.WriteMessages(ctx, message)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("write message error")
		} else {
			log.Ctx(ctx).Info().Str("info", fmt.Sprintf("successful write message: %v", i)).Msg("successful write")
		}
	}

	// send batch
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
	//ctx := context.Background()
	//writer.WriteMessages(ctx, msgs...)
}
