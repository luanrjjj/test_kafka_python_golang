package main

import (
    "fmt"
    "log"
    "time"
    "context"

    "github.com/segmentio/kafka-go"
)

func main() {
    topic := "topic_test"

    go consumer(topic, "consumer1")

    time.Sleep(300 * time.Second)
    fmt.Println("Done")
}

func consumer (topic string, consumer string) {
    fmt.Println("Consumer started")

    r:= kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{"kafka:29092"},
        // GroupID: consumer,
        Topic: topic,
    })

    for {
        m, err := r.ReadMessage(context.Background())
        if err != nil {
            log.Fatal(err)
        }

        fmt.Printf("Message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
    }
    
    r.Close()
}