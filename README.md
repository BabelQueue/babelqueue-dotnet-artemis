# BabelQueue — Apache ActiveMQ Artemis (.NET)

`BabelQueue.Artemis` — an Apache ActiveMQ Artemis transport for
[BabelQueue](https://babelqueue.com), built on **AMQP 1.0** (AMQPNetLite) and the
framework-agnostic [`BabelQueue.Core`](https://www.nuget.org/packages/BabelQueue.Core).

A canonical-envelope **publisher** and a URN-routed AMQP 1.0 **consumer**, so an Artemis-based
.NET service speaks the same wire contract (envelope shape, URN identity, trace propagation) as
the Java, Python, Go and Node SDKs. Implements
[§7 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-activemq-artemis).

Artemis speaks AMQP 1.0 (not RabbitMQ's 0-9-1), and gives the binding native primitives —
per-message settlement, scheduled delivery, a delivery counter and a dead-letter address — so
this transport maps onto them (the envelope stays `schema_version: 1`):

- the envelope JSON is the message **body**; the contract fields are mirrored onto the AMQP a JMS
  peer reads — `correlation-id` = `trace_id`, `creation-time` = `meta.created_at`, the
  `x-opt-jms-type` message annotation = URN (so a Java/JMS or AMQP consumer routes on `JMSType`
  without decoding the body) — plus the `bq-` application properties;
- consume settles per message: **`Accept` after success**; a throwing handler settles the
  message **`Modified{delivery-failed: true}`** so the broker redelivers it and increments the AMQP
  `delivery-count`;
- **`attempts = max(body, delivery-count)`** — the AMQP delivery-count header is 0-based (0 on
  first delivery), so it maps directly with no −1 (the Java JMS binding reads the 1-based
  `JMSXDeliveryCount` and subtracts 1, arriving at the same 0-based `attempts`);
- delay uses Artemis's **native** AMQP scheduled delivery (`x-opt-delivery-time`); terminal
  failures go to an opt-in `<queue>.dlq` carrying the canonical envelope plus the additive
  `dead_letter` block, cross-language alongside Artemis's own dead-letter address.

## Install

```bash
dotnet add package BabelQueue.Artemis
```

It pulls `BabelQueue.Core` and `AMQPNetLite.Core` transitively.

## Produce

```csharp
using Amqp;
using BabelQueue.Artemis;

var connection = new Connection(new Address("amqp://user:pass@localhost:5672"));
var session = new Session(connection);
var sender = new AmqpSenderLink(new SenderLink(session, "orders-sender", "orders"), "orders");

var id = await new ArtemisPublisher(sender)
    .PublishAsync("urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 1042 });
```

`PublishAsync(urn, data)` returns the message `meta.id`; overloads add a `traceId` and a relative
`TimeSpan delay` (native scheduled delivery).

## Consume

```csharp
using Amqp;
using BabelQueue;
using BabelQueue.Artemis;

var receiver = new AmqpReceiverLink(new ReceiverLink(session, "orders-worker", "orders"));
var dlq = new AmqpSenderLink(new SenderLink(session, "dlq-sender", "orders.dlq"), "orders.dlq");

var handlers = new Dictionary<string, BabelHandler>
{
    ["urn:babel:orders:created"] = (env, message, ct) =>
    {
        // env.Data, env.TraceId, env.Attempts ...
        return Task.CompletedTask;
    },
};

var options = new ArtemisConsumerOptions
{
    DeadLetterSender = dlq,   // enables the cross-language <queue>.dlq
    MaxTries = 3,
    OnError = (err, env, msg) => Console.Error.WriteLine(err),
};

var worker = new ArtemisConsumer(receiver, handlers, options);
await worker.RunAsync(cancellationToken); // receive → process → Accept, until cancelled
```

A successful handler `Accept`s the message. A throwing handler settles it
`Modified{delivery-failed: true}` (the broker redelivers and bumps `delivery-count`; a plain `Release` would
*not* bump it, so `MaxTries` would never be reached); once `MaxTries` is reached the envelope goes to
`<queue>.dlq` with a `dead_letter` block. The consumer routes on the `x-opt-jms-type` annotation
(falling back to the body URN), so it never decodes a message it cannot handle. Unknown-URN
strategy is one of `fail` / `delete` / `release` / `dead_letter`.

**Keep `max-delivery-attempts` ≥ `MaxTries`.** Because a failing handler now advances `delivery-count`, Artemis's native `max-delivery-attempts` (broker default 10) applies to this path too. If it is lower than `MaxTries` (e.g. `MaxTries = 15` with the default 10), the broker moves the message to its native dead-letter address first — verbatim, without the `dead_letter` block — and `<queue>.dlq` never fills. Set it on the address/queue to at least `MaxTries`.

**Custom receivers.** `IAmqpReceiver.ModifyAsync` is a default interface method that falls back to `ReleaseAsync`; a custom `IAmqpReceiver` (not `AmqpReceiverLink`) must override it with a *modified / delivery-failed* settle, otherwise a throwing handler redelivers without bumping the counter.

**Schema-version gate.** Before decoding, the consumer reads the `bq_schema_version` application property (underscores — ADR-0017). Absent or blank decodes as usual; any other value must be exactly `"1"`, otherwise the raw message is forwarded to `<queue>.dlq` and accepted without being decoded (with no `DeadLetterSender` configured the forward is a no-op, so the message is just accepted — it degrades to delete). If the DLQ write itself fails, the message is **not** accepted: it is `Release`d for redelivery (a plain AMQP *released* outcome, not *modified / delivery-failed*), reported via `OnError` (after the settle; a throwing `OnError` cannot leave it unsettled), the consumer backs off for one `ReceiveTimeout` and `RunAsync` keeps running.

> `AmqpSenderLink` / `AmqpReceiverLink` wrap the AMQPNetLite links; the publisher and consumer
> depend on the `IAmqpSender` / `IAmqpReceiver` seams, so they unit-test against mocks with no
> broker. A receiver link is single-threaded — run one `ArtemisConsumer` per link.

## Contract mapping (§7)

| Envelope | Apache ActiveMQ Artemis (AMQP 1.0) |
| :--- | :--- |
| body | message body (byte-identical across SDKs) |
| `job` (URN) | `x-opt-jms-type` annotation → JMSType (consumer routes on this) |
| `trace_id` | `correlation-id` → JMSCorrelationID |
| `meta.created_at` | `creation-time` → JMSTimestamp (Unix ms) |
| `meta.schema_version` | application property `bq-schema-version` (`"1"`) |
| `meta.lang` | application property `bq-source-lang` |
| `attempts` | `max(body, delivery-count)` (AMQP counter is 0-based) |
| reserve / ack | `Receive` → process → **`Accept`** |
| retry / delay | `Modified{delivery-failed}` redelivery (handler failure) · plain `Release` (gate DLQ-write failure, `release` unknown-URN) · native `x-opt-delivery-time` |
| dead-letter | `<queue>.dlq` + `dead_letter` block (alongside the native DLA) |

The `bq-` application-property values are strings (integers as decimal, e.g. `"1"`); `bq-app-id`
is `"babelqueue"`. The envelope is unchanged (`schema_version` stays `1`); Artemis is purely
additive.

## Build & test

```bash
dotnet test
```

The AMQP send/receive seams are mocked with Moq and `Amqp.Message` is constructed directly — no
Artemis, no network. xUnit, ≥90% line coverage (Coverlet).

## License

MIT
