package logger

import (
	"context"
	"fmt"
	"github.com/Maksumys/go-hare"
)

type Logger interface {
	InfoStr(ctx context.Context, message string)
	ErrorStr(ctx context.Context, message string)
}

type LogParams struct {
	ExchangeName string
	RoutingKey   string
	Acked        bool
}

type Config struct {
	Logger    Logger
	Formatter func(params *LogParams) string
}

func Middleware(cfg Config) rabbitmq.ControllerFunc {
	if cfg.Logger == nil {
		cfg.Logger = defaultLogger{}
	}

	if cfg.Formatter == nil {
		cfg.Formatter = func(params *LogParams) string {
			return fmt.Sprint(*params)
		}
	}

	return func(c *rabbitmq.DeliveryContext) {
		c.Next()

		cfg.Logger.InfoStr(c.Context, cfg.Formatter(&LogParams{
			ExchangeName: c.Delivery.Exchange,
			RoutingKey:   c.Delivery.RoutingKey,
			Acked:        c.Acked,
		}))
	}
}

type defaultLogger struct{}

func (d defaultLogger) InfoStr(ctx context.Context, message string) {
	fmt.Println(message)
}

func (d defaultLogger) ErrorStr(ctx context.Context, message string) {
	fmt.Println(message)
}
