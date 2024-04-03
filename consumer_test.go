package rabbitmq

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"testing"
	"time"
)

func TestConsumer_Consume(t *testing.T) {
	conn, err := newConn()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	exchange := ExchangeParams{Name: "test-consumer-exchange", Kind: amqp.ExchangeTopic, Durable: true, AutoDelete: false}
	queue := QueueParams{Name: "", Exclusive: true}

	c, err := NewConsumer(conn, queue, WithExchangeDeclare(exchange, "test.*"))
	if err != nil {
		t.Error(err)
		return
	}

	deliveryChan := c.Consume("test-consumer", false, false, false, false, nil)

	// wait for consumer to initialize
	time.Sleep(2 * time.Second)
	wg := sync.WaitGroup{}

	for i := 0; i < 50; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			correlationId := fmt.Sprintf("%d", i)

			err = ch.PublishWithContext(context.Background(), exchange.Name, "test.one", false, false, amqp.Publishing{
				ContentType:   "text/plain",
				CorrelationId: correlationId,
				Body:          []byte("message"),
			})
			if err != nil {
				t.Error(err)
				return
			}

			for delivery := range deliveryChan {
				fmt.Printf("received in %d: %s\n", i, delivery.CorrelationId)

				if delivery.CorrelationId == correlationId {
					delivery.Ack(false)
					break
				}
			}
		}(i)
	}

	wg.Wait()
}
