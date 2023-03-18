package rabbitmq

import (
	"context"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type DeliveryContext struct {
	context.Context
	Delivery amqp.Delivery
	Channel  *amqp.Channel

	handlers []ControllerFunc
	index    int8

	Acked  bool
	Nacked bool
}

func NewDeliveryContext(
	baseCtx context.Context,
	delivery amqp.Delivery,
	ch *amqp.Channel,
	handlers []ControllerFunc,
) *DeliveryContext {
	return &DeliveryContext{
		Context:  baseCtx,
		Delivery: delivery,
		Channel:  ch,
		handlers: handlers,
		index:    -1,
	}
}

func (c *DeliveryContext) Next() {
	c.index++
	for c.index < int8(len(c.handlers)) {
		c.handlers[c.index](c)
		if c.Nacked {
			return
		}

		c.index++
	}
}

func (c *DeliveryContext) BindJSON(ptr interface{}) error {
	return json.Unmarshal(c.Delivery.Body, ptr)
}

func (c *DeliveryContext) Nack(requeue bool, err error) bool {
	if c.Acked {
		logrus.Warningf("message with key %s acked", c.Delivery.RoutingKey)
		return false
	}

	if c.Nacked {
		logrus.Warningf("message with key %s already nacked", c.Delivery.RoutingKey)
		return false
	}

	logrus.Errorf("AMQP Consumer %v not acknowledged, error: %v", c.Delivery.RoutingKey, err)

	err = c.Delivery.Nack(false, requeue)
	if err != nil {
		return false
	}

	return true
}

func (c *DeliveryContext) Ack() bool {
	if c.Nacked {
		logrus.Warningf("message with key %s nacked", c.Delivery.RoutingKey)
		return false
	}

	if c.Acked {
		return true
	}

	err := c.Delivery.Ack(false)
	if err != nil {
		logrus.Warningf("RabbitMQ DeliveryContext Ack failed: %s", err)
		return false
	}

	logrus.Tracef("AMQP Consumer %v acknowledged", c.Delivery.RoutingKey)
	c.Acked = true
	return true
}
