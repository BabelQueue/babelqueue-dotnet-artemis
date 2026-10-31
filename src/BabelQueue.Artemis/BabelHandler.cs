using Amqp;

namespace BabelQueue.Artemis;

/// <summary>
/// Processes one decoded, validated envelope and the raw AMQP message it arrived on. Completing
/// normally acknowledges it (the consumer <c>Accept</c>s it); throwing leaves it for the broker
/// to redeliver (the consumer settles it <c>Modified{delivery-failed}</c>, incrementing the AMQP <c>delivery-count</c>),
/// up to max-tries before it is dead-lettered.
/// </summary>
/// <remarks>
/// To continue a W3C <c>traceparent</c> that rode beside the envelope (ADR-0028), hand
/// <c>ArtemisTraceHeaders.Extract(message)</c> to <c>Telemetry.Wrap(handler, headers)</c>; the consumer span then becomes a true child of the
/// producer span. With no <c>traceparent</c> in the application properties the core falls back to the v0.1 <c>trace_id</c> mapping.
/// </remarks>
public delegate Task BabelHandler(Envelope envelope, Message message, CancellationToken cancellationToken);
