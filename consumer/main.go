package main

import (
    "fmt"
    "log"
    "os"
    "time"
    "context"
    "encoding/json"

    "github.com/segmentio/kafka-go"
)

type StockData struct {
    Date string `json:"date"`
    Data struct {
        Open   string `json:"1. open"`
        High   string `json:"2. high"`
        Low    string `json:"3. low"`
        Close  string `json:"4. close"`
        Volume string `json:"5. volume"`
    } `json:"data"`
}

func main() {
    topic := os.Getenv("KAFKA_TOPIC_NAME")

    go consumer(topic)

    time.Sleep(300 * time.Second)
    fmt.Println("Done")
}

func consumer (topic string) {
    fmt.Println("Consumer started")

    r:= kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{os.Getenv("KAFKA_SERVER") + ":" + os.Getenv("KAFKA_PORT")},
        Topic: topic,
    })

    for {
        m, err := r.ReadMessage(context.Background())
        if err != nil {
            log.Fatal(err)
        }

        var stockData StockData
        err = json.Unmarshal(m.Value, &stockData)
        if err != nil {
            log.Printf("Error unmarshaling: %v", err)
            continue
        }

        jsonData, err := json.Marshal(stockData)
        if err != nil {
            log.Printf("Error marshaling: %v", err)
            continue
        }

        fmt.Println(string(jsonData))    
    }
    
    r.Close()
}