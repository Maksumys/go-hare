package otel

import (
	"github.com/Maksumys/go-hare"
	"github.com/Maksumys/go-hare/middlewares"
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const tracerName = "otelamqp"

func Middleware(serviceName string, opts ...Option) rabbitmq.ControllerFunc {
	cfg := config{}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if cfg.TracerProvider == nil {
		cfg.TracerProvider = otel.GetTracerProvider()
	}
	tracer := cfg.TracerProvider.Tracer(
		tracerName,
	)
	if cfg.Propagators == nil {
		cfg.Propagators = otel.GetTextMapPropagator()
	}

	return func(c *rabbitmq.DeliveryContext) {
		savedCtx := c.Context
		defer func() {
			c.Context = savedCtx
		}()

		ctx := cfg.Propagators.Extract(c.Context, middlewares.HeadersCarrier(c.Delivery.Headers))
		opts := []oteltrace.SpanStartOption{
			oteltrace.WithSpanKind(oteltrace.SpanKindConsumer),
			oteltrace.WithAttributes(semconv.ServiceNameKey.String(serviceName)),
			oteltrace.WithAttributes(semconv.MessagingRabbitmqRoutingKeyKey.String(c.Delivery.RoutingKey)),
		}

		ctx, span := tracer.Start(ctx, c.Delivery.RoutingKey, opts...)
		defer span.End()

		c.Context = ctx

		c.Next()
	}
}
