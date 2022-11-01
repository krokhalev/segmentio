package getter

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
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

func ConsumeAndCommit(ctx context.Context, address, topic string, secure, skipVerify bool, groupID string, commitMessages bool) {
	transport := kafka.Transport{}
	if secure {
		if skipVerify {
			transport.TLS = &tls.Config{InsecureSkipVerify: skipVerify}
		} else {
			transport.TLS = &tls.Config{}
		}
	}

	client := &kafka.Client{
		Addr:      kafka.TCP(address),
		Timeout:   10 * time.Second,
		Transport: &transport,
	}

	kafkaTopics, err := topics.List(ctx, client)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("get topics list error")
	}
	if !topicContains(kafkaTopics, topic) {
		err = errors.New("")
		log.Ctx(ctx).Error().Err(err).Str("error", fmt.Sprintf("no such topic available: %s", topic)).Msg("get topics list error")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{address},
		GroupID: groupID,
		Topic:   topic,
		Dialer:  &kafka.Dialer{TLS: transport.TLS},
	})
	for {
		msg, err := reader.FetchMessage(ctx)
		if errors.Is(err, context.Canceled) {
			log.Ctx(ctx).Error().Err(err).Msg("message fetching canceled with context")
			return
		}
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("fetch message error")
			continue
		} else {
			log.Ctx(ctx).Info().Str("info", fmt.Sprintf("successful fetch message: %s", msg.Value)).Msg("successful fetch")
		}

		if commitMessages {
			err = reader.CommitMessages(ctx, msg)
			if err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("commit message error")
			} else {
				log.Ctx(ctx).Info().Str("info", fmt.Sprintf("successful commit message: %s", msg.Value)).Msg("successful commit")
			}
		}
	}
}
