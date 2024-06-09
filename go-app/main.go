package main

import (
    "fmt"
    "log"
    "time"
    "context"

    "github.com/segmentio/kafka-go"
)

func main() {
    topic := "test-topic"
    partition := 0

    conn, err := kafka.DialLeader(context.Background(), "tcp", "kafka:9092", topic, partition)
    if err != nil {
        log.Fatal("failed to dial leader:", err)
    }

    conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
    _, err = conn.WriteMessages(
        kafka.Message{Value: []byte("Hello Kafka")},
    )
    if err != nil {
        log.Fatal("failed to write messages:", err)
    }

    fmt.Println("Message written to Kafka")
    conn.Close()
}