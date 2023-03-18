package publisher

import (
	"context"
	"fmt"
	"github.com/Maksumys/go-hare"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"log"
	"os"
)

func main() {
	connUrl := fmt.Sprintf(
		"amqp://%s:%s@%s:%s/",
		os.Getenv("USER"),
		os.Getenv("PASSWORD"),
		os.Getenv("HOST"),
		os.Getenv("PORT"),
	)

	conn, err := amqp.Dial(connUrl)

	if err != nil {
		panic(err)
	}

	connection := rabbitmq.NewConnection(conn, func() (*amqp.Connection, error) {
		conn, err := amqp.Dial(connUrl)
		if err != nil {
			return nil, errors.WithMessage(err, "di provideRabbit NewConnection Dial failed")
		}

		return conn, nil
	})

	router := provideRouter()
	server := rabbitmq.NewServer(connection, router)

	err = server.ListenAndServe(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

// provideRouter func creates a new router and binds controller functions to different routing keys.
// Routing key is analogues to binding key. Routing key is used to bind group queue to group exchange.
func provideRouter() *rabbitmq.Router {
	router := rabbitmq.NewRouter()

	router.Group(
		rabbitmq.ExchangeParams{Name: "userEvents", Kind: "Direct"},
		rabbitmq.QueueParams{Name: "users.events"},
		rabbitmq.QualityOfService{},
		rabbitmq.ConsumerParams{},
		rabbitmq.WithRouterEngine(rabbitmq.NewDirectRouterEngine()), // use direct to speed up routing
	).
		Route("users.events.delete", func(ctx *rabbitmq.DeliveryContext) {
			// process delivery
			ctx.Ack()
		}).
		Route("users.events.update", func(ctx *rabbitmq.DeliveryContext) {
			// process delivery
			ctx.Ack()
		})

	// server will bind queue "users.events" to exchange "userEvents" on "users.events.delete" and "users.events.update"
	// binding keys

	router.Group(
		rabbitmq.ExchangeParams{Name: "loggingEvents", Kind: "topic"},
		rabbitmq.QueueParams{Name: "logs"},
		rabbitmq.QualityOfService{},
		rabbitmq.ConsumerParams{},
		rabbitmq.WithRouterEngine(rabbitmq.NewTopicRouterEngine()), // no need to declare, topic is used by default
		rabbitmq.WithNumWorkers(50),
	).
		Route("logs.system.*", func(ctx *rabbitmq.DeliveryContext) {
			// process delivery
			ctx.Ack()
		})

	// server will bind queue "logs" to exchange "loggingEvents" on "logs.system.*" binding keys

	return router
}
