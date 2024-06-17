package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"github.com/Maksumys/go-hare/pkg/backoff"
	amqp "github.com/rabbitmq/amqp091-go"
	"log/slog"
	"time"
)

const (
	DefaultCircuitBreakerConsecutiveFailuresAllowed = 10
	defaultBackoffMaxTimeout                        = 15 * time.Second
)

var (
	ErrPrepareChannel  = errors.New("prepare channel error")
	ErrCreateChannel   = errors.New("failed to create channel in rabbitmq publisher")
	ErrDeclareExchange = errors.New("failed to declare exchange in rabbitmq publisher")
	ErrDeclareQueue    = errors.New("failed to declare queue in rabbitmq publisher")
	ErrQueueBind       = errors.New("failed to bind queue in rabbitmq publisher")
	ErrPublishMessage  = errors.New("failed to publish message in rabbitmq publisher")
)

func NewPublisher(connection *Connection, exchangeParams ExchangeParams, opts ...PublisherOption) (*Publisher, error) {
	p := &Publisher{
		Conn:           connection,
		exchangeParams: exchangeParams,
		maxTimeout:     defaultBackoffMaxTimeout,
		close:          make(chan struct{}, 1),
		isClosed:       false,
	}

	for _, opt := range opts {
		opt(p)
	}

	if p.logger == nil {
		p.logger = slog.Default()
	}

	if err := p.prepareChannelAndTransport(); err != nil {
		return nil, errors.Join(err, ErrPrepareChannel)
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

func WithPublisherLogger(logger *slog.Logger) PublisherOption {
	return func(p *Publisher) {
		p.logger = logger
	}
}

type Publisher struct {
	Conn *Connection
	ch   *amqp.Channel

	exchangeParams ExchangeParams
	declareQueue   bool
	queueParams    QueueParams
	bindingKey     string

	retryPublish bool
	maxTimeout   time.Duration

	logger *slog.Logger

	close    chan struct{}
	isClosed bool
}

func (p *Publisher) Publish(ctx context.Context, key string, mandatory, immediate bool, msg amqp.Publishing) (err error) {
	backoffRetry := backoff.NewSigmoidBackoff(p.maxTimeout, 0.5, 15, 0)
	for {
		if err = p.ch.PublishWithContext(ctx, p.exchangeParams.Name, key, mandatory, immediate, msg); err != nil {
			slog.WarnContext(ctx, fmt.Sprintf("RabbitMQPublisher Publish failed: %v", err))

			if !p.retryPublish || p.Conn.isClosed || p.ch.IsClosed() {
				return err
			}

			if err = backoffRetry.Retry(ctx); err != nil {
				return errors.Join(err, ErrPublishMessage)
			}

			continue
		}

		return
	}
}

func (p *Publisher) Close() error {
	p.isClosed = true

	p.close <- struct{}{}

	if err := p.ch.Close(); err != nil {
		return errors.Join(err, errors.New("rabbitmq Publisher Close Close failed"))
	}

	return nil
}

func (p *Publisher) watchReconnect() {
	for {
		select {
		case <-p.close:
			return
		default:
			reason, ok := <-p.ch.NotifyClose(make(chan *amqp.Error))
			if !ok {
				if p.isClosed {
					return
				}

				go p.logger.Error("rabbitmq channel stopped")

				if err := p.prepareChannelAndTransport(); err != nil {
					go p.logger.Error(fmt.Sprintf("watchReconnect Close failed:  %v", reason))
				}
			}
		}
	}
}

func (p *Publisher) prepareChannelAndTransport() error {
	if err := p.newChannel(); err != nil {
		return err
	}

	if err := p.declareAndBind(p.bindingKey); err != nil {
		return err
	}

	return nil
}

func (p *Publisher) newChannel() error {
	backoffRetry := backoff.NewSigmoidBackoff(p.maxTimeout, 0.5, 15, 100)

	for {
		conn := p.Conn.GetConnection()

		if conn != nil && !conn.IsClosed() {
			var err error
			p.ch, err = conn.Channel()

			if err != nil {
				p.logger.Error("failed to create new channel", "error", err)
				continue
			}

			return nil
		}

		err := backoffRetry.Retry(context.Background())

		if err != nil {
			return err
		}
	}
}

func (p *Publisher) declareAndBind(bindingKey string) error {
	if err := p.ch.ExchangeDeclare(
		p.exchangeParams.Name,
		p.exchangeParams.Kind,
		p.exchangeParams.Durable,
		p.exchangeParams.AutoDelete,
		p.exchangeParams.Internal,
		p.exchangeParams.NoWait,
		p.exchangeParams.Args,
	); err != nil {
		return errors.Join(err, ErrDeclareExchange)
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
			return errors.Join(err, ErrDeclareQueue)
		}

		if err = p.ch.QueueBind(queue.Name, bindingKey, p.exchangeParams.Name, false, nil); err != nil {
			return errors.Join(err, ErrQueueBind)
		}
	}

	return nil
}
