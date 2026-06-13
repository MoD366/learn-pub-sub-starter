package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	const connStr = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		fmt.Println("Error connecting to MQ: ", err)
	}
	defer conn.Close()
	fmt.Println("Connection established.")
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Error creating channel: ", err)
	}
	_, queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, fmt.Sprintf("%v.*", routing.GameLogSlug), pubsub.SimpleQueueDurable)
	if err != nil {
		log.Fatalf("Couldn't bind queue: %v\n", err)
	} else {
		fmt.Printf("Successfully connected to %v\n", queue.Name)
	}
	gamelogic.PrintServerHelp()
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "pause":
			log.Println("Sending pause message...")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Printf("Could not publish pause message: %v\n", err)
			}
		case "resume":
			log.Println("Sending resume message...")
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
		case "quit":
			log.Println("Exiting...")
		default:
			log.Println("Invalid command...")
		}
		if input[0] == "quit" {
			break
		}
	}
}
