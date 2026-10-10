using System.Globalization;
using Amqp;

namespace BabelQueue.Artemis;

/// <summary>
/// Receives from an Artemis address over AMQP 1.0, decodes and validates each message, routes it
/// to the handler registered for its URN (read from the <c>x-opt-jms-type</c> annotation), and
/// <c>Accept</c>s it on success. A throwing handler settles the message with an AMQP
/// <c>Modified{delivery-failed: true}</c> outcome so the broker redelivers it <i>and</i> increments the AMQP
/// <c>delivery-count</c> (a plain <c>Release</c> would not bump it, so max-tries would never be reached); once max-tries is reached the
/// envelope goes to <c>&lt;queue&gt;.dlq</c> with a <c>dead_letter</c> block. <c>attempts</c> is
/// reconciled to <c>max(body, delivery-count)</c> — the AMQP counter is 0-based (0 on first
/// delivery), so it maps directly with no −1 (the Java JMS binding reads the 1-based
/// <c>JMSXDeliveryCount</c> and subtracts 1, arriving at the same 0-based <c>attempts</c>).
/// Because the counter now advances on handler failures, the broker's native <c>max-delivery-attempts</c> must be
/// set to at least <see cref="ArtemisConsumerOptions.MaxTries"/>, or the broker dead-letters first (verbatim, no
/// <c>dead_letter</c> block).
/// The consume loop never stops on a bad message — observe via <see cref="ArtemisConsumerOptions.OnError"/>.
/// Before decoding, the <c>bq_schema_version</c> application property is checked (§7.7): absent or blank
/// decodes normally, any value other than <c>"1"</c> sends the raw message to the DLQ without decoding
/// it, then <c>Accept</c>s it. If that DLQ write fails the message is <b>not</b> accepted: it is
/// <c>Release</c>d for redelivery, the failure is reported via <see cref="ArtemisConsumerOptions.OnError"/> and the
/// loop keeps running.
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
        // §7.7: version-gate on the bq_schema_version application property before decoding the body. The
        // value is compared exactly as sent (no trimming), like the Pulsar/Go/Python/Node consumers (GR-5).
        var declaredVersion = SchemaVersionValue(message);
        if (declaredVersion is not null && !IsBlankSchemaVersion(declaredVersion)
            && !string.Equals(
                declaredVersion,
                EnvelopeCodec.SchemaVersion.ToString(CultureInfo.InvariantCulture),
                StringComparison.Ordinal))
        {
            // The body is deliberately not decoded, so OnError gets an empty envelope. The raw message goes
            // to the DLQ (same path as a poison body) and is then accepted. Settle FIRST, report afterwards:
            // a throwing OnError must never leave the message unsettled or stop the loop (it is isolated).
            var rejected = new BabelQueueException(
                $"Rejected an Artemis message: unsupported {AmqpProperties.SchemaVersion} property "
                + $"'{declaredVersion}' (supported: {EnvelopeCodec.SchemaVersion}); body not decoded.");
            try
            {
                await DeadLetterRawAsync(message, cancellationToken).ConfigureAwait(false);
            }
#pragma warning disable CA1031 // A failed DLQ write must release the message, whatever the cause.
            catch (Exception dlqError) when (dlqError is not OperationCanceledException)
#pragma warning restore CA1031
            {
                // The gated message could not be parked: never Accept it. Release it for redelivery (it never
                // reaches a handler: the gate rejects it again), report, and back off for one receive interval
                // so a persistent DLQ failure doesn't spin. The loop keeps running.
                await _receiver.ReleaseAsync(message, cancellationToken).ConfigureAwait(false);
                SafeReport(rejected, message);
                SafeReport(
                    new BabelQueueException(
                        "Failed to dead-letter a schema-version-gated Artemis message; released for redelivery, not accepted.",
                        dlqError),
                    message);
                try
                {
                    await Task.Delay(_options.ReceiveTimeout, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Shutting down: the message is already released.
                }

                return;
            }

            await _receiver.AcceptAsync(message, cancellationToken).ConfigureAwait(false);
            SafeReport(rejected, message);
            return;
        }

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
    /// Invokes <see cref="ArtemisConsumerOptions.OnError"/> on the schema-version-gate path, where it must not be
    /// able to disturb settlement: a throwing callback is swallowed so the already-performed Accept / Release
    /// stands and <c>RunAsync</c> keeps running.
    /// </summary>
    private void SafeReport(Exception error, Message message)
    {
        try
        {
            _options.OnError?.Invoke(error, new Envelope(null, null, null, null, 0, null), message);
        }
#pragma warning disable CA1031 // A misbehaving observer must not leave the gated message unsettled.
        catch (Exception)
#pragma warning restore CA1031
        {
        }
    }

    /// <summary>The <c>bq_schema_version</c> application property as text, or null when absent.</summary>
    private static string? SchemaVersionValue(Message message)
    {
        var properties = message.ApplicationProperties;
        if (properties is not null
            && properties.Map.TryGetValue(AmqpProperties.SchemaVersion, out var value)
            && value is not null)
        {
            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        return null;
    }

    /// <summary>
    /// True when the declared schema version is empty or consists <b>only</b> of ASCII whitespace
    /// (space, <c>\t</c>, <c>\n</c>, U+000B, <c>\f</c>, <c>\r</c>). Anything else (NBSP, U+0085, U+FEFF, …)
    /// is <b>not</b> blank, unlike <see cref="string.IsNullOrWhiteSpace"/>.
    /// </summary>
    private static bool IsBlankSchemaVersion(string value)
    {
        foreach (var c in value)
        {
            if (c != ' ' && (c < '\t' || c > '\r'))
            {
                return false;
            }
        }

        return true;
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
            // Modified{delivery-failed} requeues it AND makes the broker increment delivery-count, so
            // attempts = max(body, delivery-count) advances and MaxTries is eventually reached. A plain
            // Release would not bump the counter (Artemis cancels it without counting) → endless redelivery.
            await _receiver.ModifyAsync(message, deliveryFailed: true, undeliverableHere: false, cancellationToken)
                .ConfigureAwait(false);
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
