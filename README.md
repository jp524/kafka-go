# Kafka Go

Producer and Consumer to exchanges messages through the CLI by using Kafka.

1. Run the producer and consumer in separate processes: `go run producer/producer.go` and `go run consumer/consumer.go`
2. Send message in another process with:
```
curl --location 'http://127.0.0.1:3000/messages' \
--header 'Content-Type: application/json' \
--data '{
    "user": "Jane",
    "body":"Hello, world!"
  }'
```
3. View messages in the process where the consumer is running