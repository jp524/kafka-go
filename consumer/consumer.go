package main

import (
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

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	// Channel with signal for finish
	doneChannel := make(chan struct{})

	go func() {
		for {
			select {
			case err := <- consumer.Errors():
				fmt.Println(err)
			case msg := <- consumer.Messages():
				msgCount++
				fmt.Printf("Received message Count %d: | Topic(%s) | Message(%s) \n", msgCount, string(msg.Topic), string(msg.Value))
			case <- signalChannel:
				fmt.Println("Interrupt is detected")
				doneChannel <- struct{}{}
			}
		}
	}()

	<- doneChannel
	fmt.Println("Processed", msgCount, "messages")

	if err := worker.Close(); err != nil {
		panic(err)
	}
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