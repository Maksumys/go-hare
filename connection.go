package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"github.com/Maksumys/go-hare/pkg/backoff"
	amqp "github.com/rabbitmq/amqp091-go"
	"log/slog"
	"sync"
	"sync/atomic"
)

type ConnectionFactory func() (*amqp.Connection, error)

func NewConnection(conn *amqp.Connection, factory ConnectionFactory, logger *slog.Logger) *Connection {
	c := &Connection{
		Connection:        conn,
		connectionFactory: factory,
		notifyClose:       make([]chan error, 0),
		notifyReconnect:   make([]chan error, 0),
		mu:                sync.RWMutex{},
		logger:            logger,
	}

	go c.watchReconnect(context.Background())

	return c
}

type Connection struct {
	*amqp.Connection
	connectionFactory func() (*amqp.Connection, error)
	reconnecting      atomic.Bool
	notifyClose       []chan error
	notifyReconnect   []chan error
	mu                sync.RWMutex
	logger            *slog.Logger
}

func (c *Connection) Close() {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Warn(fmt.Sprintf("RabbitMQServer closeConn recovered: %v", r))
		}
	}()

	currentlyReconnecting := !c.reconnecting.CompareAndSwap(false, true)
	if currentlyReconnecting {
		return
	}

	err := c.Connection.Close()
	if err != nil {
		c.logger.Debug(fmt.Sprintf("RabbitMQServer closeConn Close failed: %v", err))
	}
}

func (c *Connection) NotifyClose(ch chan error) chan error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.notifyClose = append(c.notifyClose, ch)
	return ch
}

func (c *Connection) NotifyReconnect(ch chan error) chan error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.notifyReconnect = append(c.notifyReconnect, ch)
	return ch
}

func (c *Connection) IsAlive() bool {
	return !c.reconnecting.Load()
}

func (c *Connection) watchReconnect(ctx context.Context) {
	for {
		errClose := <-c.Connection.NotifyClose(make(chan *amqp.Error, 1))
		if errClose != nil {
			go c.logger.Warn("RabbitMQServer ListenAndServe NotifyClose")

			err := c.attemptReconnect(ctx)
			if err != nil {
				go c.logger.Warn(fmt.Sprintf("RabbitMQServer ListenAndServe attemptReconnecting failed: %v", err))

				c.broadcastReconnect(err)
				return
			}

			go c.logger.Info("RabbitMQServer ListenAndServe reconnected successfully")
			c.broadcastReconnect(nil)
			continue
		}

		c.broadcastClose(nil)
		return
	}
}

func (c *Connection) attemptReconnect(ctx context.Context) error {
	backoff := backoff.NewDefaultSigmoidBackoff()

	currentlyReconnecting := !c.reconnecting.CompareAndSwap(false, true)
	if currentlyReconnecting {
		return nil
	}

	for {
		err := backoff.Retry(ctx)
		if err != nil {
			return errors.Join(err, errors.New("RabbitMQServer attemptReconnecting failed"))
		}

		c.Connection, err = c.connectionFactory()
		if err != nil {
			go c.logger.Info(fmt.Sprintf("RabbitMQServer attemptReconnecting failed: %s", err))
			continue
		}

		c.reconnecting.Swap(false)
		return nil
	}
}

func (c *Connection) broadcastClose(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	channels := c.notifyClose
	c.notifyClose = make([]chan error, 0)
	c.notifyReconnect = make([]chan error, 0)

	for _, ch := range channels {
		ch <- err
		close(ch)
	}
}

func (c *Connection) broadcastReconnect(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	channels := c.notifyReconnect
	c.notifyReconnect = make([]chan error, 0)
	c.notifyClose = make([]chan error, 0)

	for _, ch := range channels {
		ch <- err
		close(ch)
	}
}
