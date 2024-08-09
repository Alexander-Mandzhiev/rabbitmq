package main

import (
	"context"
	"log"
	"rabbit_mickroservices/internal"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("guest", "guest", "localhost:5672")
	internal.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	internal.FailOnError(err, "Failed to open a channel")
	defer client.Close()

	if err := client.CreateQueue("values", true, false); err != nil {
		panic(err)
	}

	if err := client.CreateBinding("values", "*", "amq.topic"); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 10; i++ {
		if err := client.Send(ctx, "amq.topic", "*", amqp091.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp091.Persistent,
			Body:         []byte(`"id":"1","value":1`),
		}); err != nil {
			panic(err)
		}
	}

	log.Println(client)

}
