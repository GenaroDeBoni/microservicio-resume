package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// ReaderOptions configura el lector de Kafka.
type ReaderOptions struct {
	Brokers  []string
	Topic    string
	GroupID  string
	ClientID string
}

// WriterOptions configura el escritor de Kafka.
type WriterOptions struct {
	Brokers  []string
	ClientID string
}

// NewKafkaReader crea un lector de Kafka usando segmentio/kafka-go.
func NewKafkaReader(opts ReaderOptions) (Reader, error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        opts.Brokers,
		Topic:          opts.Topic,
		GroupID:        opts.GroupID,
		MinBytes:       1,
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})
	return &kafkaReader{reader: r}, nil
}

// NewKafkaWriter crea un escritor de Kafka usando segmentio/kafka-go.
func NewKafkaWriter(opts WriterOptions) (Writer, error) {
	w := &kafka.Writer{
		Addr:         kafka.TCP(opts.Brokers...),
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}
	return &kafkaWriter{writer: w}, nil
}

// kafkaReader adapta kafka.Reader a la interfaz Reader.
type kafkaReader struct {
	reader *kafka.Reader
}

func (k *kafkaReader) FetchMessage(ctx context.Context) (Message, error) {
	msg, err := k.reader.FetchMessage(ctx)
	if err != nil {
		return Message{}, err
	}
	return Message{
		Topic:     msg.Topic,
		Key:       msg.Key,
		Value:     msg.Value,
		Offset:    msg.Offset,
		Partition: int32(msg.Partition),
	}, nil
}

func (k *kafkaReader) CommitMessages(ctx context.Context, msgs ...Message) error {
	kafkaMsgs := make([]kafka.Message, len(msgs))
	for i, m := range msgs {
		kafkaMsgs[i] = kafka.Message{
			Topic:     m.Topic,
			Key:       m.Key,
			Value:     m.Value,
			Offset:    m.Offset,
			Partition: int(m.Partition),
		}
	}
	return k.reader.CommitMessages(ctx, kafkaMsgs...)
}

func (k *kafkaReader) Close() error {
	return k.reader.Close()
}

// kafkaWriter adapta kafka.Writer a la interfaz Writer.
type kafkaWriter struct {
	writer *kafka.Writer
}

func (k *kafkaWriter) WriteMessages(ctx context.Context, msgs ...Message) error {
	kafkaMsgs := make([]kafka.Message, len(msgs))
	for i, m := range msgs {
		kafkaMsgs[i] = kafka.Message{
			Topic: m.Topic,
			Key:   m.Key,
			Value: m.Value,
		}
	}
	return k.writer.WriteMessages(ctx, kafkaMsgs...)
}

func (k *kafkaWriter) Close() error {
	return k.writer.Close()
}