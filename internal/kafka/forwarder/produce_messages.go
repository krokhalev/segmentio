package forwarder

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"os"
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

	file, err := os.Open("messages.txt")
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("open file with messages error")
	}
	defer file.Close()

	fscanner := bufio.NewScanner(file)
	var messages []string
	for fscanner.Scan() {
		messages = append(messages, fscanner.Text())
	}

	lenMsgs := len(messages)
	i := 0
	counter := 0
	for {
		if counter == messagesCount {
			break
		}
		if i == lenMsgs {
			i = 0
		}
		message := kafka.Message{
			Value: []byte(messages[i]),
			//Topic: topic, // send message to specific topic
		}
		err := writer.WriteMessages(ctx, message)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("write message error")
		} else {
			log.Ctx(ctx).Info().Str("info", fmt.Sprintf("successful write message: %v", i)).Msg("successful write")
		}
		i++
		counter++
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
