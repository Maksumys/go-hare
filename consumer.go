package rabbitmq

import (
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

type Consumer struct {
	Conn    *Connection
	Channel *amqp.Channel

	dstDeliveryChan chan amqp.Delivery

	declareExchange bool
	exchangeParams  ExchangeParams

	queueParams   QueueParams
	bindingKey    string
	declaredQueue amqp.Queue

	qos QualityOfService
}

type ConsumerOption func(p *Consumer)

// WithExchangeDeclare будет создавать exchange при создании канала подключения
// если queueBindingKey = "", биндинг будет осуществляться по названию очереди
func WithExchangeDeclare(exchangeParams ExchangeParams, queueBindingKey string) ConsumerOption {
	return func(c *Consumer) {
		c.declareExchange = true
		c.exchangeParams = exchangeParams
		c.bindingKey = queueBindingKey
	}
}

func WithConsumerQos(qos QualityOfService) ConsumerOption {
	return func(c *Consumer) {
		c.qos = qos
	}
}

func NewConsumer(conn *Connection, queueParams QueueParams, opts ...ConsumerOption) (*Consumer, error) {
	c := &Consumer{
		Conn:            conn,
		dstDeliveryChan: make(chan amqp.Delivery),
		queueParams:     queueParams,
	}

	for _, opt := range opts {
		opt(c)
	}

	err := c.prepareChannelAndTransport()
	if err != nil {
		return nil, errors.WithMessage(err, "RabbitMQConsumer NewConsumer prepareChannelAndTransport failed")
	}

	return c, nil
}

func (c *Consumer) Consume(consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) <-chan amqp.Delivery {
	go func() {
		err := c.consume(consumer, autoAck, exclusive, noLocal, noWait, args)
		if err != nil {
			logrus.Errorf("RabbitMQConsumer Consume consume failed")
		}
	}()

	return c.dstDeliveryChan
}

func (c *Consumer) DeclaredQueue() amqp.Queue {
	return c.declaredQueue
}

func (c *Consumer) consume(consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) error {
	defer close(c.dstDeliveryChan)

	for {
		srcDeliveryChan, err := c.Channel.Consume(c.queueParams.Name, consumer, autoAck, exclusive, noLocal, noWait, args)
		if err != nil {
			return errors.Wrap(err, "RabbitMQConsumer consume Consume failed")
		}

		go func() {
			for delivery := range srcDeliveryChan {
				c.dstDeliveryChan <- delivery
			}
		}()

		select {
		case err = <-c.Conn.NotifyClose(make(chan error)):
			if err != nil {
				return errors.WithMessage(err, "RabbitMQConsumer consume NotifyClose")
			}

			return nil
		case err = <-c.Conn.NotifyReconnect(make(chan error)):
			if err != nil {
				return errors.WithMessage(err, "RabbitMQConsumer consume NotifyReconnect")
			}

			err = c.prepareChannelAndTransport()
			if err != nil {
				return errors.WithMessage(err, "RabbitMQConsumer consume prepareChannelAndTransport failed")
			}

			continue
		}
	}
}

func (c *Consumer) prepareChannelAndTransport() error {
	ch, err := c.Conn.Channel()
	if err != nil {
		return errors.Wrap(err, "RabbitMQConsumer prepareChannelAndTransport Channel failed")
	}

	c.Channel = ch

	err = c.declareAndBind()
	if err != nil {
		return errors.WithMessage(err, "RabbitMQConsumer prepareChannelAndTransport declareAndBind failed")
	}

	return nil
}

func (c *Consumer) declareAndBind() error {
	if c.declareExchange {
		err := c.Channel.ExchangeDeclare(
			c.exchangeParams.Name,
			c.exchangeParams.Kind,
			c.exchangeParams.Durable,
			c.exchangeParams.AutoDelete,
			c.exchangeParams.Internal,
			c.exchangeParams.NoWait,
			c.exchangeParams.Args,
		)
		if err != nil {
			return errors.Wrap(err, "RabbitMQConsumer declareAndBind ExchangeDeclare failed")
		}
	}

	queue, err := c.Channel.QueueDeclare(
		c.queueParams.Name,
		c.queueParams.Durable,
		c.queueParams.AutoDelete,
		c.queueParams.Exclusive,
		c.queueParams.NoWait,
		c.queueParams.Args,
	)
	if err != nil {
		return errors.Wrap(err, "RabbitMQConsumer declareAndBind QueueDeclare failed")
	}

	c.declaredQueue = queue

	if c.declareExchange {
		if c.bindingKey == "" {
			c.bindingKey = c.declaredQueue.Name
		}

		err = c.Channel.QueueBind(c.declaredQueue.Name, c.bindingKey, c.exchangeParams.Name, false, nil)
		if err != nil {
			return errors.Wrap(err, "RabbitMQConsumer declareAndBind QueueBind failed")
		}
	}

	err = c.Channel.Qos(c.qos.PrefetchCount, c.qos.PrefetchSize, false)
	if err != nil {
		return errors.Wrap(err, "RabbitMQConsumer declareAndBind Qos failed")
	}

	return nil
}
