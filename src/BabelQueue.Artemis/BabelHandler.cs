using Amqp;

namespace BabelQueue.Artemis;

/// <summary>
/// Processes one decoded, validated envelope and the raw AMQP message it arrived on. Completing
/// normally acknowledges it (the consumer <c>Accept</c>s it); throwing leaves it for the broker
/// to redeliver (the consumer <c>Release</c>s it, incrementing the AMQP <c>delivery-count</c>),
/// up to max-tries before it is dead-lettered.
/// </summary>
public delegate Task BabelHandler(Envelope envelope, Message message, CancellationToken cancellationToken);
