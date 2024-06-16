package rabbitmq

import (
	"context"
	"errors"
	"github.com/Maksumys/go-hare/pkg/backoff"
	amqp "github.com/rabbitmq/amqp091-go"
	"log/slog"
	"sync"
)

var (
	ErrConnect           = errors.New("connect to rabbitmq failed")
	ErrConnectorNotFound = errors.New("connector not found")
	ErrClose             = errors.New("close connection failed")
)

type Connector func() (*amqp.Connection, error)

func NewConnection(connector Connector, logger *slog.Logger) *Connection {
	c := &Connection{
		connector: connector,
		mu:        sync.RWMutex{},
		logger:    logger,
		isClosed:  false,
	}

	return c
}

type Connection struct {
	conn      *amqp.Connection
	connector func() (*amqp.Connection, error)
	isClosed  bool
	mu        sync.RWMutex
	logger    *slog.Logger
}

func (c *Connection) Connect(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connector != nil {
		if c.conn, err = c.connector(); err != nil {
			err = errors.Join(err, ErrConnect)
			return
		}
	} else {
		err = ErrConnectorNotFound
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				reason, ok := <-c.conn.NotifyClose(make(chan *amqp.Error))
				if !ok {
					if c.isClosed {
						return
					}

					if reason != nil {
						go c.logger.ErrorContext(ctx, "rabbitmq connection closed", "error", reason.Error())
					} else {
						go c.logger.ErrorContext(ctx, "rabbitmq connection closed")
					}

					backoffPolicy := backoff.NewDefaultSigmoidBackoff()

					c.mu.Lock()

					for {
						go c.logger.DebugContext(ctx, "trying to reconnect to rabbitmq")

						c.conn, err = c.connector()

						if err != nil {
							go c.logger.ErrorContext(ctx, "failed to reconnect to rabbitmq", "error", err.Error())
							if err = backoffPolicy.Retry(ctx); err != nil {
								break
							}
						} else {
							go c.logger.DebugContext(ctx, "successfully reconnected to rabbitmq")

							break
						}
					}

					c.mu.Unlock()
				}
			}
		}
	}()

	return
}

func (c *Connection) GetConnection() *amqp.Connection {
	return c.conn
}

func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.isClosed = true

	if err := c.conn.Close(); err != nil {
		return errors.Join(err, ErrClose)
	}

	return nil
}
