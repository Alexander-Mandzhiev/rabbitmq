package main

import (
	"context"
	"log"
	"rabbit_mickroservices/internal"
	"time"

	"golang.org/x/sync/errgroup"
)

func main() {

	conn, err := internal.ConnectRabbitMQ("guest", "guest", "localhost:5672")
	internal.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	internal.FailOnError(err, "Failed to open a channel")
	defer client.Close()

	messages, err := client.Consume("values", "field-values", false)
	internal.FailOnError(err, "Failed to consume")

	var blocking chan struct{}

	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	g.SetLimit(10)

	go func() {
		for message := range messages {
			msg := message
			g.Go(func() error {
				log.Printf("New Message: %v", msg)
				time.Sleep(10 * time.Second)

				if !message.Redelivered {
					message.Nack(false, true)
				}

				if err := message.Ack(false); err != nil {
					log.Println("Failed to acknowledge message")
					return err
				}

				log.Printf("Acknowledge message %s\n", message.Body)
				return nil
			})
		}
	}()

	log.Println("Consumer to close the programm press CTRL+C")
	<-blocking
}

/*
	if err := client.CreateQueue("todolist-queue", true, false); err != nil {
		log.Panicf("Failed to declare a queue: %s", err)
	}
	//failOnError(err, "Failed to declare a queue")

	if err := client.CreateBinding("todolist-queue", "*", "direct"); err != nil {
		log.Panicf("Failed to binding a queue: %s", err)
	}

	//failOnError(err, "Failed to register a consumer")
*/

/*

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ressponse := entity.FieldValues{}
	res, err := json.Marshal(ressponse)
	failOnError(err, "failed to marshal")

	if err := client.Send(ctx, "direct", "*", amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Transient,
		Body:         []byte(res),
	}); err != nil {
		log.Panicf("Failed to binding a queue: %s", err)
	}
		////////////

	msg := &entity.MessageRabbitMQ{}
	if err := json.Unmarshal(message.Body, msg); err != nil {
		message.Nack(false, false)
		log.Printf("Failed unmarshal message: %v", err)
		continue
	}

	arg := entity.FieldValues{
		Value:       msg.Value,
		TaskFieldID: msg.TaskFieldID,
		TaskID:      msg.TaskID,
	}

	switch msg.Cmd {
	case "create-field-value":
		//	res, err := handlers.Create(arg)
		//	failOnError(err, "Error creating field value")
		//	fmt.Println(res)
		fmt.Println("I'am create.")
	case "update-field-value":
		fmt.Println("I'am update.")
	default:
		fmt.Println("I'am delete.")
	}
*/
