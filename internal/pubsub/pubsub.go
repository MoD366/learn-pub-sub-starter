package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int
type Acktype string

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

const (
	Ack         Acktype = "Ack"
	NackRequeue Acktype = "NackRequeue"
	NackDiscard Acktype = "NackDiscard"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {

	jsonData, err := json.Marshal(val)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/json", Body: jsonData})
	if err != nil {
		return err
	}
	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Could not create channel: %v\n", err)
	}
	isDurable := false
	if queueType == SimpleQueueDurable {
		isDurable = true
	}
	queue, err := ch.QueueDeclare(queueName, isDurable, !isDurable, !isDurable, false, amqp.Table{"x-dead-letter-exchange": "peril_dlx"})
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Could not declare queue: %v\n", err)
	}
	err = ch.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("Could not bind queue: %v\n", err)
	}
	return ch, queue, nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) Acktype) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	chann, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	unmarshaller := func(data []byte) (T, error) {
		var target T
		err = json.Unmarshal(data, &target)
		return target, err
	}

	go func() {
		defer ch.Close()
		for channel := range chann {
			target, err := unmarshaller(channel.Body)
			if err != nil {
				fmt.Printf("Could not unmarshal message: %v\n", err)
			}
			switch handler(target) {
			case Ack:
				channel.Ack(false)
				fmt.Println("Ack")
			case NackDiscard:
				channel.Nack(false, false)
				fmt.Println("NackDiscard")
			case NackRequeue:
				channel.Nack(false, true)
				fmt.Println("NackRequeue")
			}
		}
	}()
	return nil
}
