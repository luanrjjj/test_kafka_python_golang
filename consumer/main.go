package main

import (
    "fmt"
    "log"
    "time"
    "context"
    "github.com/gorilla/websocket"
    "net/http"
    "sync"


    "github.com/segmentio/kafka-go"
)

type Hub struct {
    clients map[*websocket.Conn]bool
    broadcast chan []byte
    register chan *websocket.Conn
    unregister chan *websocket.Conn
    mu sync.Mutex
}

var upgrader = websocket.Upgrader{
    ReadBufferSize:  1024,
    WriteBufferSize: 1024,
    CheckOrigin: func(r *http.Request) bool {
        return true
    },
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
        conn, err := upgrader.Upgrade(w, r, nil)

        if err != nil {
            log.Println("Upgrade:", err)
            return
        }

        defer conn.Close()

        for {
            _, _, err := conn.ReadMessage()
            if err != nil {
                break
            }
        }
    // return nil
}

func main() {
    topic := "topic_test"
    // hub := NewHub()

    // go hub.run()

    http.HandleFunc("/ws", wsHandler)

    http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        http.ServeFile(w, r, "index.html")
    })

    log.Println("Starting server on : 8080")

    log.Fatal(http.ListenAndServe(":8080", nil))

    go consumer(topic)

    time.Sleep(300 * time.Second)
    fmt.Println("Done")
}

func consumer (topic string) {
    fmt.Println("Consumer started")

    r:= kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{"kafka:29092"},
        Topic: topic,
    })

    for {
        m, err := r.ReadMessage(context.Background())

        if err != nil {
            log.Fatal(err)
        }

        message := fmt.Sprintf("Message at topic/partition/offset %v/%v/%v: %s = %s", 
        m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
        fmt.Println(message)

        // hub.broadcast <- []byte(message)

        fmt.Printf("Message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
    }
    
    r.Close()
}