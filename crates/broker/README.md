# weida-broker

The message broker layer of [weida](https://crates.io/crates/weida): queues at endpoint paths,
in the spirit of RabbitMQ, built from weida's own patterns rather than a protocol of its own.

- **Producers** send to a queue's path as an exchange and get a confirm (`Accepted`) from the
  broker. A producer that will not wait sends a one-way transfer and gets the transport
  receipt.
- **Consumers** are ordinary `weida::Subscriber`s. They receive nothing until they grant
  credit, an absolute and monotone delivery limit, so duplicated or reordered grants change
  nothing.
- **Settlement:** a consumer reports `Processed` on a cursor. A delivery that ends without it
  goes back to the queue, and a consumer that stops reporting stops receiving.

Queues live in memory. The broker therefore certifies `Accepted` and nothing above it: no
`Stored` until there is a durable store. A confirm covers one hop; whether a consumer processed
a message is the application's reply to give.

## Learn more

- [Website](https://weida.doodleshnookie.net/), with the
  [broker decision](https://weida.doodleshnookie.net/decisions/0018-minimal-broker/) and the
  [guarantees](https://weida.doodleshnookie.net/reference/guarantees/)
- [API documentation](https://docs.rs/weida-broker)
- [Repository](https://github.com/tuco86/weida)

Status: alpha. The redelivery count on the wire and retiring a queue are still to come.

## License

MIT OR Apache-2.0, at your option.
