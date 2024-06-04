package rabbitmq

import (
	"context"
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
)

type DeliveryContext struct {
	context.Context
	Delivery amqp.Delivery
	Channel  *amqp.Channel

	handlers []ControllerFunc
	index    int8

	Acked  bool
	Nacked bool

	userValues map[string]any
}

func NewDeliveryContext(
	baseCtx context.Context,
	delivery amqp.Delivery,
	ch *amqp.Channel,
	handlers []ControllerFunc,
) *DeliveryContext {
	return &DeliveryContext{
		Context:    baseCtx,
		Delivery:   delivery,
		Channel:    ch,
		handlers:   handlers,
		index:      -1,
		userValues: make(map[string]any),
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
		return false
	}

	if c.Nacked {
		return false
	}

	err = c.Delivery.Nack(false, requeue)

	return err == nil
}

func (c *DeliveryContext) Ack() bool {
	if c.Nacked {
		return false
	}

	if c.Acked {
		return true
	}

	err := c.Delivery.Ack(false)
	if err != nil {
		return false
	}

	c.Acked = true
	return true
}

func (c *DeliveryContext) Set(key string, value any) {
	c.userValues[key] = value
}

func (c *DeliveryContext) Get(key string) (any, bool) {
	value, ok := c.userValues[key]
	return value, ok
}
