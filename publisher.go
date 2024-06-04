package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"github.com/Maksumys/go-hare/pkg/backoff"
	amqp "github.com/rabbitmq/amqp091-go"
	"log/slog"
	"sync/atomic"
	"time"
)

const (
	DefaultCircuitBreakerConsecutiveFailuresAllowed = 10
	defaultBackoffMaxTimeout                        = 15 * time.Second
)

func NewPublisher(connection *Connection, exchangeParams ExchangeParams, opts ...PublisherOption) (*Publisher, error) {
	p := &Publisher{
		Conn:           connection,
		exchangeParams: exchangeParams,
		maxTimeout:     defaultBackoffMaxTimeout,
		close:          make(chan struct{}, 1),
	}

	for _, opt := range opts {
		opt(p)
	}

	err := p.prepareChannelAndTransport()
	if err != nil {
		return nil, errors.Join(err, errors.New("RabbitMQPublisher NewPublisher prepareChannelAndTransport failed"))
	}

	go p.watchReconnect()

	return p, nil
}

type PublisherOption func(p *Publisher)

func WithRetries() PublisherOption {
	return func(p *Publisher) {
		p.retryPublish = true
	}
}

func WithQueueDeclaration(queueParams QueueParams, bindingKey string) PublisherOption {
	return func(p *Publisher) {
		p.declareQueue = true
		p.queueParams = queueParams
		p.bindingKey = bindingKey
	}
}

func WithLogger(logger *slog.Logger) PublisherOption {
	return func(p *Publisher) {
		p.logger = logger
	}
}

type Publisher struct {
	Conn *Connection
	ch   *amqp.Channel

	consecutiveErrors atomic.Uint32
	connClosed        bool

	exchangeParams ExchangeParams
	declareQueue   bool
	queueParams    QueueParams
	bindingKey     string

	retryPublish bool
	maxTimeout   time.Duration

	logger *slog.Logger

	close chan struct{}
}

func (p *Publisher) Publish(ctx context.Context, key string, mandatory, immediate bool, msg amqp.Publishing) (err error) {
	backoffRetry := backoff.NewSigmoidBackoff(p.maxTimeout, 0.5, 15, 0)
	for {
		err = p.ch.PublishWithContext(ctx, p.exchangeParams.Name, key, mandatory, immediate, msg)
		if err != nil {
			slog.WarnContext(ctx, fmt.Sprintf("RabbitMQPublisher Publish failed: %v", err))
			p.consecutiveErrors.Add(1)

			if !p.retryPublish || p.connClosed {
				return err
			}

			err = backoffRetry.Retry(ctx)
			if err != nil {
				return errors.Join(err, errors.New("RabbitMQPublisher Publish BackoffRetry failed"))
			}

			continue
		}

		p.consecutiveErrors.Store(0)

		return
	}
}

// Broken возвращает true в случае, если последовательное количество ошибок Publish > DefaultCircuitBreakerConsecutiveFailuresAllowed
func (p *Publisher) Broken() bool {
	return p.consecutiveErrors.Load() > DefaultCircuitBreakerConsecutiveFailuresAllowed
}

func (p *Publisher) Close() error {
	p.close <- struct{}{}

	err := p.ch.Close()
	if err != nil {
		return errors.Join(err, errors.New("rabbitmq Publisher Close Close failed"))
	}

	return nil
}

func (p *Publisher) watchReconnect() {
	defer func() {
		p.connClosed = true
	}()

	for {
		select {
		case err := <-p.Conn.NotifyClose(make(chan error)):
			if err != nil {
				slog.Error(fmt.Sprintf("RabbitMQPublisher watchReconnect NotifyClose: %v", err))
			}

			return
		case err := <-p.Conn.NotifyReconnect(make(chan error)):
			if err != nil {
				slog.Error(fmt.Sprintf("RabbitMQPublisher watchReconnect NotifyReconnect: %v", err))
				return
			}

			err = p.prepareChannelAndTransport()
			if err != nil {
				slog.Error(fmt.Sprintf("RabbitMQPublisher watchReconnect prepareChannelAndTransport failed: %v", err))
				return
			}

			slog.Info("RabbitMQPublisher watchReconnect reconnected successfully")
			continue
		case <-p.close:
			return
		}
	}
}

func (p *Publisher) prepareChannelAndTransport() error {
	err := p.newChannel()
	if err != nil {
		return errors.Join(err, errors.New("RabbitMQPublisher watchReconnect newChannel failed"))
	}

	err = p.declareAndBind(p.bindingKey)
	if err != nil {
		return errors.Join(err, errors.New("RabbitMQPublisher watchReconnect declareAndBind failed"))
	}

	return nil
}

func (p *Publisher) newChannel() error {
	var err error
	p.ch, err = p.Conn.Channel()
	return err
}

func (p *Publisher) declareAndBind(bindingKey string) error {
	err := p.ch.ExchangeDeclare(
		p.exchangeParams.Name,
		p.exchangeParams.Kind,
		p.exchangeParams.Durable,
		p.exchangeParams.AutoDelete,
		p.exchangeParams.Internal,
		p.exchangeParams.NoWait,
		p.exchangeParams.Args,
	)
	if err != nil {
		return errors.Join(err, errors.New("RabbitMQPublisher declareAndBind ExchangeDeclare failed"))
	}

	if p.declareQueue {
		queue, err := p.ch.QueueDeclare(
			p.queueParams.Name,
			p.queueParams.Durable,
			p.queueParams.AutoDelete,
			p.queueParams.Exclusive,
			p.queueParams.NoWait,
			p.queueParams.Args,
		)
		if err != nil {
			return errors.Join(err, errors.New("RabbitMQPublisher declareAndBind QueueDeclare failed"))
		}

		err = p.ch.QueueBind(queue.Name, bindingKey, p.exchangeParams.Name, false, nil)
		if err != nil {
			return errors.Join(err, errors.New("RabbitMQPublisher declareAndBind QueueBind failed"))
		}
	}

	return nil
}
