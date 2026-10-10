# Changelog

All notable changes to `BabelQueue.Artemis` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

## [1.0.1] - 2026-10-10

### Fixed
- `ArtemisConsumer` now checks the `bq_schema_version` AMQP application property (underscores, ADR-0017) **before** decoding the body (§7.7). A value other than exactly `"1"` (ordinal, untrimmed) is forwarded raw to `<queue>.dlq` and then accepted, without decoding (a no-op forward when no `DeadLetterSender` is configured, so the message is just accepted). Absent or blank (empty / ASCII whitespace) falls through to the normal decode. No public API change.
- The schema-version gate no longer loses a message when the DLQ write fails: `ArtemisConsumer` now `Release`s it for redelivery instead of letting the exception escape, reports via `OnError`, backs off for one `ReceiveTimeout` and `RunAsync` keeps running; the message is never accepted and never reaches a handler. `OnError` is invoked **after** the Accept / Release and isolated, so a throwing callback cannot leave the message unsettled or stop the loop. Other DLQ paths are unchanged. No public API change.

## [1.0.0] - 2026-06-13

### Added
- Initial release. An Apache ActiveMQ Artemis transport on `BabelQueue.Core` over **AMQP 1.0**
  (AMQPNetLite), implementing §7 of the broker-bindings contract. Artemis offers native
  settlement/scheduled-delivery/delivery-counter/dead-letter-address, so the binding maps onto
  them rather than re-implementing: `ArtemisPublisher` (body = canonical envelope,
  `correlation-id` = `trace_id`, `creation-time` = `meta.created_at`, the `x-opt-jms-type`
  annotation = URN — so a Java/JMS or AMQP consumer routes on `JMSType`; a delay uses native
  AMQP scheduled delivery via `x-opt-delivery-time`) and `ArtemisConsumer` (per-message
  settlement — `Accept` after success; a throwing handler `Release`s the message for broker
  redelivery; **`attempts = max(body, delivery-count)`** with the AMQP counter 0-based, so no
  −1; terminal failures go to `<queue>.dlq` with the additive `dead_letter` block;
  `fail`/`delete`/`release`/`dead_letter` unknown-URN strategies; poison bodies forwarded raw to
  the DLQ). The publisher and consumer depend on the `IAmqpSender`/`IAmqpReceiver` seams (with
  `AmqpSenderLink`/`AmqpReceiverLink` adapters over AMQPNetLite), so the suite runs against Moq
  mocks and constructible `Amqp.Message` objects — no Artemis, no network. xUnit, ≥90% line
  coverage. The envelope is unchanged (`schema_version: 1`); Apache ActiveMQ Artemis is purely
  additive.
