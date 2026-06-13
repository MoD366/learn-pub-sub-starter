package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	const connStr = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("Error connecting to MQ: %v\n", err)
	}
	defer conn.Close()
	fmt.Println("Connection established.")
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Error creating channel: ", err)
	}
	usrname, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Error setting username: %v\n", err)
	}

	gameState := gamelogic.NewGameState(usrname)

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, fmt.Sprintf("%v.%v", routing.PauseKey, usrname), routing.PauseKey, pubsub.SimpleQueueTransient, handlerPause(gameState))
	if err != nil {
		log.Fatalf("Unable to subscribe to channel: %v\n", err)
	}
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%v", usrname), "army_moves.*", pubsub.SimpleQueueTransient, handlerMove(gameState, ch))
	if err != nil {
		log.Fatalf("Unable to subscribe to channel: %v\n", err)
	}
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, "war", routing.WarRecognitionsPrefix+".*", pubsub.SimpleQueueDurable, handlerWar(gameState, ch))
	if err != nil {
		log.Fatalf("Unable to subscribe to channel: %v\n", err)
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "spawn":
			err := gameState.CommandSpawn(input)
			if err != nil {
				fmt.Printf("Failed spawning unit: %v\n", err)
				continue
			}
		case "move":
			armyMove, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Printf("Failed moving unit: %v\n", err)
				continue
			}
			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%v", usrname), armyMove)
			if err != nil {
				fmt.Printf("error publishing move: %v\n", err)
				continue
			}
			fmt.Printf("Successfully moved unit %s to %v", input[2], armyMove.ToLocation)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "quit":
			gamelogic.PrintQuit()
		default:
			log.Println("Invalid command...")
			continue
		}
		if input[0] == "quit" {
			break
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(move gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		moveOutcome := gs.HandleMove(move)
		switch moveOutcome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, fmt.Sprintf("%v.%v", routing.WarRecognitionsPrefix, move.Player), gamelogic.RecognitionOfWar{Attacker: move.Player, Defender: gs.GetPlayerSnap()})
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(row gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(row)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			err := publishLog(row.Attacker.Username, winner+" won war against "+loser, ch)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			err := publishLog(row.Attacker.Username, winner+" won war against "+loser, ch)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			err := publishLog(row.Attacker.Username, "A war between "+winner+" and "+loser+" resulted in a draw", ch)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Println("Error processing war outcome.")
			return pubsub.NackDiscard
		}
	}
}

func publishLog(attacker, logMessage string, ch *amqp.Channel) error {
	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+attacker, routing.GameLog{CurrentTime: time.Now(), Message: logMessage, Username: attacker})
	return err
}
