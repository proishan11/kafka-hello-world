package writer

import (
	"bytes"
	"context"
	"encoding/binary"
	kafkago "github.com/segmentio/kafka-go"
	"kafka-hello-world/serdes/input"
)

// Send sends the kafka message to a topic
func Send() error {
	writerConfig := kafkago.WriterConfig{
		Brokers:      []string{"kafka:29092"},
		Topic:        "hello.csv.in",
		RequiredAcks: 0,
		Async:        false,
		Dialer: &kafkago.Dialer{
			ClientID:  "kafka-hello-world",
			DualStack: true,
		},
	}
	writer := kafkago.NewWriter(writerConfig)

	msg := input.In{
		Firstname: &input.UnionNullString{
			String:    "First",
			UnionType: 1,
		},
	}

	var output bytes.Buffer
	err := msg.Serialize(&output)
	if err != nil {
		return err
	}

	tempSchemaID := 42
	dataBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(dataBytes, uint32(tempSchemaID))
	out := append(output.Bytes(), dataBytes...)

	err = writer.WriteMessages(context.Background(), kafkago.Message{
		Value: out,
	})
	if err != nil {
		return err
	}

	return nil

}
