package rabbitmq

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"testing"
	"time"
)

func TestPublisher_Publish(t *testing.T) {
	conn, err := newConn()
	if err != nil {
		t.Fatal(err)
	}

	// server
	router := NewRouter()
	routerGroup := router.Group(exchangeParams, queueParams, qos, consumer)

	messageReceived := false
	routerGroup.Route("test.foo", func(ctx *DeliveryContext) {
		ctx.Ack()
		messageReceived = true
	})

	server := NewServer(conn, router)

	go func() {
		err := server.ListenAndServe(context.Background())
		if err != nil {
			t.Error(err)
		}
	}()

	<-time.After(1 * time.Second)

	// publisher
	publisher, err := NewPublisher(conn, exchangeParams, WithQueueDeclaration(queueParams, "test.foo"), WithRetries())
	if err != nil {
		t.Fatal(err)
	}

	err = publisher.Publish(context.Background(), "test.foo", false, false, amqp.Publishing{
		ContentType:  "text/plain",
		Body:         []byte("body"),
		DeliveryMode: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	if !messageReceived {
		t.Error("message was not received on server side")
	}
}

func TestPublisher_Reconnect(t *testing.T) {
	conn, err := newConn()
	if err != nil {
		t.Fatal(err)
	}

	// server
	router := NewRouter()
	routerGroup := router.Group(exchangeParams, queueParams, qos, consumer)

	messageReceived := false
	routerGroup.Route("test.foo", func(ctx *DeliveryContext) {
		ctx.Ack()
		messageReceived = true
	})

	server := NewServer(conn, router)

	go func() {
		err := server.ListenAndServe(context.Background())
		if err != nil {
			t.Error(err)
		}
	}()

	<-time.After(1 * time.Second)

	// publisher
	publisher, err := NewPublisher(conn, exchangeParams, WithQueueDeclaration(queueParams, "test.foo"), WithRetries())
	if err != nil {
		t.Fatal(err)
	}

	// restart rabbitmq server here manually
	log.Println("Restart rabbitmq server manually within 5 seconds")

	time.Sleep(5 * time.Second)

	err = publisher.Publish(context.Background(), "test.foo", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("body"),
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	if !messageReceived {
		t.Error("message was not received on server side")
	}
}
