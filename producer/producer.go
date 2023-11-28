package main

import (
	"fmt"
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

func main() {
	app := fiber.New()
	app.Post("/messages", createMessage)
	app.Listen(":3000")
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, conf)

	if err != nil {
		return nil, err
	}
	return conn, nil
}

func PushCommentToQueue(topic string, message []byte) error {
	brokersUrl := []string{"localhost:9092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

type Message struct {
	User 	string	`json:"user"`
	Body 	string  `json:"body"`
}

func createMessage(c *fiber.Ctx) error {
	// Instantiate new Message struct
	msg:= new(Message)

	// Parse Message as body for HTTP request and return if error
	if err := c.BodyParser(msg); err != nil {
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"comment": err,
		})
		return err
	}

	// Convert Message struct into bytes and send it to Kafka
	msgInBytes, err := json.Marshal(msg)
	PushCommentToQueue("messages", msgInBytes)

	// Return Message in JSON format
	err = c.JSON(&fiber.Map{
		"success": true,
		"comment": "Message pushed successfully",
		"message": msg,
	})
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"comment": "Error creating product",
		})
		return err	
	}
	return err
}
