package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"github.com/IBM/sarama"
)

func main() {
	topic := "messages"
	brokersUrl := []string{"localhost:9092"}
	worker, err := connectConsumer(brokersUrl)
	if err != nil {
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}	

	fmt.Println("Consumer started")

	// Channel to receive UNIX signals to interrupt or terminate program
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Channel waiting for a signal to exit the code
	exitCode := make(chan bool)

	go func() {
		for {
			select {
			case err := <- consumer.Errors():
				fmt.Println(err)
			case msg := <- consumer.Messages():
				msgValueInBytes := msg.Value
				var msgValue Message

				err := json.Unmarshal(msgValueInBytes, &msgValue)
				if err != nil {
					fmt.Printf("Failed to unmarshal message: %v", err)
					continue
				}
				fmt.Println(msgValue.User + ": " + msgValue.Body)
				
			case <- signals:
				fmt.Println("Interrupt is detected")
				exitCode <- true
			}
		}
	}()

	<- exitCode

	if err := worker.Close(); err != nil {
		panic(err)
	}
}

type Message struct {
	User 	string	`json:"user"`
	Body 	string  `json:"body"`
}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	conn,err := sarama.NewConsumer(brokersUrl, config)

	if err != nil {
		return nil, err
	}
	return conn, nil
}