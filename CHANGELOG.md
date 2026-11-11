# Changelog

All notable changes to `BabelQueue.Artemis` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Added

- **`ArtemisPublisher.PublishEnvelopeAsync(envelope, headers, ...)`.** Sends an already-built `Envelope` exactly as given, with `delay` and the §7 projection and the optional out-of-band `headers`, and returns `meta.id`. Use it as the `send` callback of `Telemetry.PublishAsync(..., headers, env => publisher.PublishEnvelopeAsync(env, headers), queue)`: the producer span's `messaging.message.id` was the `meta.id` of the envelope `Telemetry` built, while `PublishWithHeadersAsync(urn, data, ...)` rebuilt a different one, so the span id never matched the wire. Now it does. `PublishWithHeadersAsync` and `PublishAsync` delegate to it; their behaviour is unchanged. Envelope unchanged (`schema_version` stays `1`); no new `BabelQueue.Core` requirement.

## [1.3.0] - 2026-11-03

### Added

- **Dead-letter observer (ADR-0039 section 5.1).** `ArtemisConsumerOptions.DeadLetterObserver` takes the core's optional `BabelQueue.IDeadLetterObserver` (needs a `BabelQueue.Core` that ships it). `ArtemisConsumer` calls it once per dead-letter the SDK itself carried out: a message written through `DeadLetterSender`, right after the write is confirmed: an exhausted `failed` message, an unmapped URN under `dead_letter`, a poison body, and a raw message rejected by the schema-version gate (empty `Reason`). `Queue` is `meta.queue` when the envelope carries one, else the configured/derived queue name; `DeadLetterQueue` is the sender address. It is not called when the write fails (the message is released), for a redelivery, when the broker dead-letters on its own, or when no sender is set. The call goes through `DeadLetterObservers.NotifySafely`, so an observer exception is caught and logged and never changes the settle outcome. Without an observer nothing changes. Envelope unchanged (`schema_version` stays 1).
- **W3C `traceparent` propagation (ADR-0028).** `ArtemisPublisher.PublishWithHeadersAsync(urn, data, headers, traceId, delay)` overlays an out-of-band header carrier (e.g. the one `Telemetry.PublishAsync(..., headers, ...)` fills from the producer span) as string AMQP application properties (`traceparent` / `tracestate` are JMS-legal names), **beside** the frozen envelope and the `bq-` / `bq_` contract projection, never inside the body. The transport-owned `bq_*` properties (including `bq_delay`) are always dropped from the carrier (parity with the Go reference), keys already present are never overwritten and empty keys/values are skipped; a `null`/empty carrier is identical to `PublishAsync`.
- `ArtemisTraceHeaders.Extract(...)`: surfaces a received message's application properties as a flat map to hand to `Telemetry.Wrap(handler, headers)`, so the consumer span becomes a true child of the producer span. The transport-owned contract keys are left out of the map, so it can be handed back to `PublishWithHeadersAsync` safely. With no `traceparent` the core falls back to the v0.1 `trace_id` mapping. The `BabelHandler` signature is unchanged; call `ArtemisTraceHeaders.Extract(message)` inside the handler.
- Envelope unchanged (`schema_version` stays 1); additive, no wire change.

### Changed

- `BabelQueue.Core` dependency raised from `1.13.0` to `1.14.0` (`IDeadLetterObserver`).

## [1.2.0] - 2026-10-26

Implements ADR-0037 decisions 1 and 2. The envelope stays at `schema_version: 1`; every change is a consumer-side settle
decision. Ships as a **MINOR**.

### Added

- `ArtemisConsumerOptions.Queue`: the consumer's queue name for `dead_letter.original_queue` of a poison record (default: the
  `DeadLetterSender` address without its `.dlq` suffix).

### Changed

- **Behaviour change: a poison body is written to `<queue>.dlq` as the ADR-0035 record.** Before, a non-conformant body was
  forwarded raw (no `dead_letter` block, decoded with a lenient UTF-8 decode) and a failed write escaped the poll. Now a strict
  JSON object is spliced byte for byte with a `dead_letter` block (`reason: "poison"`), anything else is wrapped as `raw`
  (base64 plus `raw_encoding` for invalid UTF-8), via `BabelQueue.Core`'s `PoisonRecord`; the message is accepted only after
  the write, a failed write `Release`s it (reported, one `ReceiveTimeout` backoff). Under `UnknownUrn = delete`, or with no
  `DeadLetterSender`, it is accepted and dropped (a `Trace` warning). It never reaches a handler and is never retried.
- **Behaviour change: the `bq_schema_version` gate follows ADR-0037 decision 1.** Under `UnknownUrn = delete` a gated message is
  now accepted without a DLQ record (before, it was written even then); the raw body is copied as received (a binary body stays
  binary instead of being re-encoded as text); with no `DeadLetterSender` it is accepted, dropped and logged. Only the
  configured `UnknownUrn` strategy is read (`release` behaves as `fail`).
- `BabelQueue.Core` dependency raised from `1.0.0` to `1.13.0` (`PoisonRecord`).
- Vendored conformance suite updated to v1.8.0 (tests only; no wire change, `schema_version` stays 1); the runners cover the `poison_dlq` and `version_gate_artemis` sections.
- CI pins the `fetch.sh` it downloads by sha256 (`BQ_FETCH_SH_SHA256`) before running it.

## [1.1.0] - 2026-10-10

### Added
- `IAmqpReceiver.ModifyAsync(message, deliveryFailed, undeliverableHere = false, cancellationToken)` — settles a message with an AMQP *modified* outcome (`deliveryFailed: true` makes the broker increment `delivery-count`). It is a **default interface method** that falls back to `ReleaseAsync`, so existing `IAmqpReceiver` implementations keep compiling (source and binary compatible). **Custom receivers must override `ModifyAsync`** (as `AmqpReceiverLink` does, via `ReceiverLink.Modify`), otherwise a throwing handler still redelivers without bumping the counter and `MaxTries` is never reached on that receiver.

### Fixed
- `ArtemisConsumer` now checks the `bq_schema_version` AMQP application property (underscores, ADR-0017) **before** decoding the body (§7.7). A value other than exactly `"1"` (ordinal, untrimmed) is forwarded raw to `<queue>.dlq` and then accepted, without decoding (a no-op forward when no `DeadLetterSender` is configured, so the message is just accepted). Absent or blank (empty / ASCII whitespace) falls through to the normal decode. No public API change.
- The schema-version gate no longer loses a message when the DLQ write fails: `ArtemisConsumer` now `Release`s it for redelivery instead of letting the exception escape, reports via `OnError`, backs off for one `ReceiveTimeout` and `RunAsync` keeps running; the message is never accepted and never reaches a handler. `OnError` is invoked **after** the Accept / Release and isolated, so a throwing callback cannot leave the message unsettled or stop the loop. Other DLQ paths are unchanged. No public API change.
- `ArtemisConsumer` no longer redelivers a throwing handler's message forever. The handler-failure path settled with a plain AMQP *released* outcome, which Artemis cancels **without** incrementing `delivery-count`, so `attempts = max(body, delivery-count)` never advanced, `MaxTries` was never reached and the message never made it to `<queue>.dlq`. It now settles `Modified{delivery-failed: true}` (`undeliverable-here: false`), which bumps the counter, so `MaxTries` is reached and the existing terminal DLQ path runs. The schema-version-gate DLQ-write-failure path and the `release` unknown-URN strategy intentionally stay a plain `Release` (counter not bumped). Note: because the counter now advances on handler failures, Artemis's native `max-delivery-attempts` (default 10) becomes reachable on this path — set it to at least `MaxTries` on the address/queue, or the broker moves the message to its native DLA (verbatim, no `dead_letter` block) before `MaxTries` is reached. Wire envelope and §7 property names unchanged.

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
