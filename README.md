<!-- Description -->
## About The Project

Wrapper over golang amqp library that simplifies consumer and producer usage with additinal features. Inspired by [gin](https://github.com/gin-gonic/gin)

Key features:
* Autoreconnect for amqp.Client
* Message router for single queue deliveries with both direct and topic routings
* Server abstraction over amqp consumer
* Consumers graceful shutdown
* Producer retries with backoff and circuit breaker


## Installation

```sh
go get github.com/Maksumys/go-hare
```

<!-- USAGE EXAMPLES -->
## Examples

See [examples](https://github.com/Maksumys/go-hare/rabbitmq/tree/main/examples)


<!-- LICENSE -->
## License

Licensed under the [MIT License](https://github.com/Maksumys/go-hare/rabbitmq/blob/main/LICENSE)
