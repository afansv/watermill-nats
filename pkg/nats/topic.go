package nats

import (
	"time"

	"github.com/nats-io/nats.go"
)

type topicInterpreter struct {
	js                nats.JetStreamManager
	subjectCalculator SubjectCalculator
	ackWaitTimeout    time.Duration
	queueGroupPrefix  string
}

func newTopicInterpreter(
	js nats.JetStreamManager,
	formatter SubjectCalculator,
	ackWaitTimeout time.Duration,
	queueGroupPrefix string,
) *topicInterpreter {
	if formatter == nil {
		// this should always be setup to the default
		panic("no subject calculator")
	}

	return &topicInterpreter{
		js:                js,
		subjectCalculator: formatter,
		ackWaitTimeout:    ackWaitTimeout,
		queueGroupPrefix:  queueGroupPrefix,
	}
}

func (b *topicInterpreter) ensureConsumer(topic string) error {
	subjectDetail := b.subjectCalculator(b.queueGroupPrefix, topic)
	if subjectDetail.QueueGroup == "" {
		return nil
	}

	_, err := b.js.ConsumerInfo(topic, subjectDetail.QueueGroup)
	if err != nil {
		_, err = b.js.AddConsumer(topic, &nats.ConsumerConfig{
			Durable:     subjectDetail.QueueGroup,
			Name:        subjectDetail.QueueGroup,
			Description: "added by watermill-nats",
			AckWait:     b.ackWaitTimeout,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *topicInterpreter) ensureStream(topic string) error {
	_, err := b.js.StreamInfo(topic)

	if err != nil {
		// TODO: provision durable as well
		// or simply provide override capability
		_, err = b.js.AddStream(&nats.StreamConfig{
			Name:        topic,
			Description: "",
			Subjects:    b.subjectCalculator(b.queueGroupPrefix, topic).All(),
		})

		if err != nil {
			return err
		}
	}

	return err
}
