package rabbitmq

import (
	"context"
	"fmt"
	otel2 "github.com/Maksumys/go-hare/middlewares"
	"github.com/Maksumys/go-hare/pkg/backoff"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"go.opentelemetry.io/otel"
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
	}

	for _, opt := range opts {
		opt(p)
	}

	err := p.prepareChannelAndTransport()
	if err != nil {
		return nil, errors.WithMessage(err, "RabbitMQPublisher NewPublisher prepareChannelAndTransport failed")
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

func WithTraceInjection() PublisherOption {
	return func(p *Publisher) {
		p.withTraceInjection = true
	}
}

func WithLogger(cfg LoggerConfig) PublisherOption {
	return func(p *Publisher) {
		p.loggerCfg = cfg
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

	loggerCfg LoggerConfig

	withTraceInjection bool
}

func (p *Publisher) Publish(ctx context.Context, key string, mandatory, immediate bool, msg amqp.Publishing) (err error) {
	if p.withTraceInjection {
		if msg.Headers == nil {
			msg.Headers = make(amqp.Table, 3)
		}

		injectTrace(ctx, msg.Headers)
	}

	backoff := backoff.NewSigmoidBackoff(p.maxTimeout, 0.5, 15, 0)
	for {
		err = p.ch.Publish(p.exchangeParams.Name, key, mandatory, immediate, msg)
		if err != nil {
			logrus.WithContext(ctx).Warningf("RabbitMQPublisher Publish failed: %v", err)
			p.consecutiveErrors.Add(1)

			if !p.retryPublish || p.connClosed {
				return err
			}

			err = backoff.Retry(ctx)
			if err != nil {
				return errors.WithMessage(err, "RabbitMQPublisher Publish BackoffRetry failed")
			}

			continue
		}

		p.consecutiveErrors.Store(0)

		p.log(ctx, LogParams{
			ExchangeName: p.exchangeParams.Name,
			Key:          key,
		})

		return
	}
}

// Broken возвращает true в случае, если последовательное количество ошибок Publish > DefaultCircuitBreakerConsecutiveFailuresAllowed
func (p *Publisher) Broken() bool {
	return p.consecutiveErrors.Load() > DefaultCircuitBreakerConsecutiveFailuresAllowed
}

func (p *Publisher) watchReconnect() {
	defer func() {
		p.connClosed = true
	}()

	for {
		select {
		case err := <-p.Conn.NotifyClose(make(chan error)):
			if err != nil {
				logrus.Errorf("RabbitMQPublisher watchReconnect NotifyClose: %v", err)
			}

			return
		case err := <-p.Conn.NotifyReconnect(make(chan error)):
			if err != nil {
				logrus.Errorf("RabbitMQPublisher watchReconnect NotifyReconnect: %v", err)
				return
			}

			err = p.prepareChannelAndTransport()
			if err != nil {
				logrus.Errorf("RabbitMQPublisher watchReconnect prepareChannelAndTransport failed: %v", err)
				return
			}

			logrus.Info("RabbitMQPublisher watchReconnect reconnected successfully")
			continue
		}
	}
}

func (p *Publisher) prepareChannelAndTransport() error {
	err := p.newChannel()
	if err != nil {
		return errors.WithMessage(err, "RabbitMQPublisher watchReconnect newChannel failed")
	}

	err = p.declareAndBind(p.bindingKey)
	if err != nil {
		return errors.WithMessage(err, "RabbitMQPublisher watchReconnect declareAndBind failed")
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
		return errors.Wrap(err, "RabbitMQPublisher declareAndBind ExchangeDeclare failed")
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
			return errors.Wrap(err, "RabbitMQPublisher declareAndBind QueueDeclare failed")
		}

		err = p.ch.QueueBind(queue.Name, bindingKey, p.exchangeParams.Name, false, nil)
		if err != nil {
			return errors.Wrap(err, "RabbitMQPublisher declareAndBind QueueBind failed")
		}
	}

	return nil
}

func (p *Publisher) log(ctx context.Context, params LogParams) {
	if p.loggerCfg.Logger == nil {
		return
	}

	var msg string
	if p.loggerCfg.Formatter != nil {
		msg = p.loggerCfg.Formatter(&params)
	} else {
		msg = fmt.Sprint(params)
	}

	p.loggerCfg.Logger.TraceStr(ctx, msg)
}

func injectTrace(ctx context.Context, headers map[string]interface{}) {
	otel.GetTextMapPropagator().Inject(ctx, otel2.HeadersCarrier(headers))
}
