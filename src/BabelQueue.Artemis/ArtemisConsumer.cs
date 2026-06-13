using Amqp;

namespace BabelQueue.Artemis;

/// <summary>
/// Receives from an Artemis address over AMQP 1.0, decodes and validates each message, routes it
/// to the handler registered for its URN (read from the <c>x-opt-jms-type</c> annotation), and
/// <c>Accept</c>s it on success. A throwing handler <c>Release</c>s the message so the broker
/// redelivers it (incrementing the AMQP <c>delivery-count</c>); once max-tries is reached the
/// envelope goes to <c>&lt;queue&gt;.dlq</c> with a <c>dead_letter</c> block. <c>attempts</c> is
/// reconciled to <c>max(body, delivery-count)</c> — the AMQP counter is 0-based (0 on first
/// delivery), so it maps directly with no −1 (the Java JMS binding reads the 1-based
/// <c>JMSXDeliveryCount</c> and subtracts 1, arriving at the same 0-based <c>attempts</c>). The
/// loop never stops on a bad message — observe via <see cref="ArtemisConsumerOptions.OnError"/>.
/// </summary>
public sealed class ArtemisConsumer
{
    private readonly IAmqpReceiver _receiver;
    private readonly IReadOnlyDictionary<string, BabelHandler> _handlers;
    private readonly ArtemisConsumerOptions _options;

    public ArtemisConsumer(
        IAmqpReceiver receiver,
        IReadOnlyDictionary<string, BabelHandler> handlers,
        ArtemisConsumerOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(receiver);
        ArgumentNullException.ThrowIfNull(handlers);
        _receiver = receiver;
        _handlers = handlers;
        _options = options ?? new ArtemisConsumerOptions();
    }

    /// <summary>Receive one message (up to the receive timeout), route + settle it. Returns 1, or 0 on timeout.</summary>
    public async Task<int> PollAsync(CancellationToken cancellationToken = default)
    {
        var message = await _receiver.ReceiveAsync(_options.ReceiveTimeout, cancellationToken).ConfigureAwait(false);
        if (message is null)
        {
            return 0;
        }
        await HandleAsync(message, cancellationToken).ConfigureAwait(false);
        return 1;
    }

    /// <summary>Poll until <paramref name="cancellationToken"/> is cancelled.</summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await PollAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task HandleAsync(Message message, CancellationToken cancellationToken)
    {
        var envelope = Reconcile(EnvelopeCodec.Decode(AmqpProperties.Text(message)), message);

        if (!EnvelopeCodec.Accepts(envelope))
        {
            _options.OnError?.Invoke(
                new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Artemis."), envelope, message);
            await DeadLetterRawAsync(message, cancellationToken).ConfigureAwait(false);
            await _receiver.AcceptAsync(message, cancellationToken).ConfigureAwait(false);
            return;
        }

        var urn = AmqpProperties.JmsTypeOf(message) ?? EnvelopeCodec.Urn(envelope);
        if (!_handlers.TryGetValue(urn, out var handler))
        {
            await OnUnknownUrnAsync(message, envelope, urn, cancellationToken).ConfigureAwait(false);
            return;
        }

        try
        {
            await handler(envelope, message, cancellationToken).ConfigureAwait(false);
            await _receiver.AcceptAsync(message, cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // The consume loop must survive any handler exception.
        catch (Exception error)
#pragma warning restore CA1031
        {
            _options.OnError?.Invoke(error, envelope, message);
            await RetryOrDeadLetterAsync(message, envelope, error, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Sets <c>attempts</c> to <c>max(current, delivery-count)</c>. The AMQP delivery-count
    /// header is 0-based (0 on first delivery), so it maps directly with no −1; the max never
    /// lowers a higher body count carried by a message republished from another SDK.
    /// </summary>
    private static Envelope Reconcile(Envelope envelope, Message message)
    {
        var deliveryCount = AmqpProperties.DeliveryCount(message);
        return deliveryCount > envelope.Attempts ? envelope with { Attempts = deliveryCount } : envelope;
    }

    private async Task OnUnknownUrnAsync(Message message, Envelope envelope, string urn, CancellationToken cancellationToken)
    {
        switch (_options.UnknownUrn)
        {
            case UnknownUrnStrategy.Delete:
                await _receiver.AcceptAsync(message, cancellationToken).ConfigureAwait(false);
                return;
            case UnknownUrnStrategy.DeadLetter:
                await DeadLetterAsync(envelope, "unknown_urn", null, cancellationToken).ConfigureAwait(false);
                await _receiver.AcceptAsync(message, cancellationToken).ConfigureAwait(false);
                return;
            case UnknownUrnStrategy.Release:
                await _receiver.ReleaseAsync(message, cancellationToken).ConfigureAwait(false);
                return;
            default:
                // Fail: surface and do NOT settle — the broker redelivers, then dead-letters.
                _options.OnError?.Invoke(new UnknownUrnException(urn), envelope, message);
                throw new UnknownUrnException(urn);
        }
    }

    private async Task RetryOrDeadLetterAsync(Message message, Envelope envelope, Exception error, CancellationToken cancellationToken)
    {
        if (envelope.Attempts + 1 < _options.MaxTries)
        {
            // Release leaves it unsettled for redelivery — the broker increments delivery-count.
            await _receiver.ReleaseAsync(message, cancellationToken).ConfigureAwait(false);
        }
        else if (_options.DeadLetterSender is not null)
        {
            await DeadLetterAsync(envelope, "failed", error, cancellationToken).ConfigureAwait(false);
            await _receiver.AcceptAsync(message, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await _receiver.AcceptAsync(message, cancellationToken).ConfigureAwait(false); // terminal, no DLQ → drop
        }
    }

    private async Task DeadLetterAsync(Envelope envelope, string reason, Exception? error, CancellationToken cancellationToken)
    {
        var sender = _options.DeadLetterSender;
        if (sender is null)
        {
            return;
        }
        var annotated = DeadLetters.Annotate(
            envelope, reason, OriginalQueue(envelope), envelope.Attempts, error?.Message, error?.GetType().FullName);
        await sender.SendAsync(AmqpProperties.ToMessage(annotated), cancellationToken).ConfigureAwait(false);
    }

    private async Task DeadLetterRawAsync(Message message, CancellationToken cancellationToken)
    {
        var sender = _options.DeadLetterSender;
        if (sender is null)
        {
            return;
        }
        await sender.SendAsync(AmqpProperties.Raw(AmqpProperties.Text(message)), cancellationToken).ConfigureAwait(false);
    }

    private static string OriginalQueue(Envelope envelope) => envelope.Meta?.Queue ?? string.Empty;
}
