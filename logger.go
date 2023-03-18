package rabbitmq

import "context"

type Logger interface {
	InfoStr(ctx context.Context, message string)
}

type LogParams struct {
	ExchangeName string
	Key          string
}

type LoggerConfig struct {
	Logger    Logger
	Formatter func(params *LogParams) string
}
