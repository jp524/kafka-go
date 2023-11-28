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

// connectProducer connects to Kafka as a synchronous producer.
func connectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
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

// pushMessageToQueue pushes messages to a Kafka topic's queue
func pushMessageToQueue(topic string, message []byte) error {
	brokersUrl := []string{"localhost:9092"}
	producer, err := connectProducer(brokersUrl)
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

// createMessage instantiates a Message struct and returns error if message cannot be sent to Kafka.
func createMessage(c *fiber.Ctx) error {
	msg:= new(Message)

	if err := verifyMessageFormat(c, msg); err != nil {
		return err
	}

	// Encode message into bytes and send it to Kafka
	msgInBytes, err := json.Marshal(msg)
	pushMessageToQueue("messages", msgInBytes)

	if err = displayHttpResponse(c, msg); err != nil {
		return err
	}

	return err
}

// verifyMessageFormat parses the body of the request and returns error if invalid format is used.
func verifyMessageFormat(c *fiber.Ctx, msg *Message) error {
	err := c.BodyParser(msg)

	if err != nil {
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"comment": err,
		})
	}
	return err
}

// Displays response to HTTP request as JSON
func displayHttpResponse(c *fiber.Ctx, msg *Message) error {
	err := c.JSON(&fiber.Map{
		"success": true,
		"comment": "Message pushed successfully",
		"message": msg,
	})
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"comment": "Error creating product",
		})
	}
	return err
}
