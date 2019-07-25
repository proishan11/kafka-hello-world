package reader

import (
	"bytes"
	"context"
	"fmt"
	kafkago "github.com/segmentio/kafka-go"
	"kafka-hello-world/serdes/input"
)

// Read reads incoming messages from kafka
func Read() error {
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "hello.csv.in",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	r.SetOffset(42)

	for {
		m, err := r.ReadMessage(context.Background())
		decodedMsg, err := input.DeserializeIn(bytes.NewReader(m.Value[5:]))
		if err != nil {
			return err
		}
		fmt.Println(decodedMsg)
		// fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
}
