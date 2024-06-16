package rabbitmq

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"log/slog"
	"os"
	"testing"
	"time"
)

func newConn() (*Connection, error) {
	connUrl := fmt.Sprintf(
		"amqp://%s:%s@%s:%s/",
		os.Getenv("USER"),
		os.Getenv("PASSWORD"),
		os.Getenv("HOST"),
		os.Getenv("PORT"),
	)

	connection := NewConnection(
		func() (*amqp.Connection, error) {
			return amqp.Dial(connUrl)
		},
		slog.Default(),
	)

	return connection, nil
}

var exchangeParams = ExchangeParams{
	Name:       "test",
	Kind:       "direct",
	AutoDelete: true,
}

var queueParams = QueueParams{
	Name:       "test",
	AutoDelete: true,
}

var qos = QualityOfService{
	PrefetchCount: 5,
}

var consumer = ConsumerParams{
	ConsumerName: "test",
	AutoAck:      false,
	ConsumerArgs: nil,
}

func TestServerRouting(t *testing.T) {
	conn, err := newConn()
	if err != nil {
		t.Fatal(err)
	}

	router := NewRouter()
	routerGroup := router.Group(exchangeParams, queueParams, qos, consumer)

	fooMessage := []byte("foo")
	fooMessageReceived := false
	fooController := func(ctx *DeliveryContext) {
		if string(ctx.Delivery.Body) != string(fooMessage) {
			t.Errorf("unexpected message received in foo %s", ctx.Delivery.Body)
		}

		err = ctx.Delivery.Ack(false)
		if err != nil {
			t.Error(err)
		}

		fooMessageReceived = true
	}

	barMessage := []byte("bar")
	barMessageReceived := false
	barController := func(ctx *DeliveryContext) {
		if string(ctx.Delivery.Body) != string(barMessage) {
			t.Errorf("unexpected message received in bar %s", ctx.Delivery.Body)
		}

		err = ctx.Delivery.Ack(false)
		if err != nil {
			t.Error(err)
		}

		barMessageReceived = true
	}

	routerGroup.Route("test.foo", fooController)
	routerGroup.Route("test.bar", barController)

	server := NewServer(conn, router)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		err = server.ListenAndServe(context.Background())
		if err != nil {
			t.Error(err)
		}
	}()

	<-time.After(1 * time.Second)

	publisherCh, err := conn.conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		err = publisherCh.Publish(exchangeParams.Name, "test.foo", false, false, amqp.Publishing{
			ContentType: "text/plain",
			Body:        fooMessage,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 10; i++ {
		err = publisherCh.Publish(exchangeParams.Name, "test.bar", false, false, amqp.Publishing{
			ContentType:  "text/plain",
			Body:         barMessage,
			DeliveryMode: 2,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	<-time.After(3 * time.Second)

	if !fooMessageReceived {
		t.Error("foo message was not received")
	}
	if !barMessageReceived {
		t.Error("bar message was not received")
	}
}

func TestServer_Shutdown(t *testing.T) {
	conn, err := newConn()
	if err != nil {
		t.Fatal(err)
	}

	router := NewRouter()
	routerGroup := router.Group(exchangeParams, queueParams, qos, consumer)

	gracefulStopCompleted := false
	routerGroup.Route("test.foo", func(ctx *DeliveryContext) {
		time.Sleep(5 * time.Second)
		gracefulStopCompleted = true
	})

	server := NewServer(conn, router)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		err = server.ListenAndServe(context.Background())
		if err != nil {
			t.Error(err)
		}
	}()

	<-time.After(1 * time.Second)

	publisherCh, err := conn.conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	err = publisherCh.Publish(exchangeParams.Name, "test.foo", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("message"),
	})
	if err != nil {
		t.Fatal(err)
	}

	<-time.After(1 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = server.Shutdown(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !gracefulStopCompleted {
		t.Fail()
	}
}

func TestServer_Reconnect(t *testing.T) {
	conn, err := newConn()
	if err != nil {
		t.Fatal(err)
	}

	router := NewRouter()
	routerGroup := router.Group(
		ExchangeParams{
			Name:       "test-durable",
			Kind:       "direct",
			Durable:    true,
			AutoDelete: false,
		}, QueueParams{
			Name:       "test-durable",
			Durable:    true,
			AutoDelete: false,
		}, qos, consumer,
	)

	messageReceived := false
	messageReceivedTwice := false

	routerGroup.Route("test.foo", func(ctx *DeliveryContext) {
		time.Sleep(1 * time.Second)
		messageReceived = true

		// restart rabbitmq server here manually
		log.Println("Restart rabbitmq server manually within 5 seconds")

		time.Sleep(5 * time.Second)

		if !ctx.Ack() {
			return
		}

		messageReceivedTwice = true
	})

	server := NewServer(conn, router)
	if err != nil {
		t.Fatal(err)
	}

	errChan := make(chan error)
	go func() {
		err = server.ListenAndServe(context.Background())
		if err != nil {
			errChan <- err
		}
	}()

	time.Sleep(1 * time.Second)

	// check for ListenAndServe error
	select {
	case err = <-errChan:
		t.Fatal(err)
	default:
	}

	publisher, err := conn.conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	err = publisher.Publish("test-durable", "test.foo", false, false, amqp.Publishing{
		DeliveryMode: 2,
		ContentType:  "text/plain",
		Body:         []byte("message"),
	})
	if err != nil {
		t.Fatal(err)
	}

	<-time.After(30 * time.Second)

	server.Shutdown(context.Background())

	if !messageReceived || !messageReceivedTwice {
		t.Error("message was not received")
	}
}
