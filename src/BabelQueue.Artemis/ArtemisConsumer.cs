using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
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
/// Before decoding, the <c>bq_schema_version</c> application property is checked (§7.7, ADR-0037 decision 1): absent or
/// blank decodes normally; any value other than <c>"1"</c> is settled on the <b>first delivery</b>, without
/// decoding, calling a handler or retrying. Under <c>UnknownUrn = delete</c> it is accepted (dropped); with a
/// <see cref="ArtemisConsumerOptions.DeadLetterSender"/> the body is written <b>exactly as received</b> (no
/// <c>dead_letter</c> block) to the DLQ and then accepted; without a sender it is accepted and dropped (logged). If the DLQ
/// write fails the message is <b>not</b> accepted: it is <c>Release</c>d for redelivery (the gate rejects it again, so the
/// write is retried), the failure is reported via <see cref="ArtemisConsumerOptions.OnError"/> and the loop keeps running.
/// Only the configured <see cref="ArtemisConsumerOptions.UnknownUrn"/> strategy is read; <c>release</c> behaves as <c>fail</c>.
/// <para>
/// A poison body — not valid UTF-8, not a JSON object, an object with no valid <c>job</c>/<c>urn</c> identity, or any other
/// non-conformant envelope — is never handed to a handler and never retried (ADR-0037 decision 2). Under
/// <c>UnknownUrn = delete</c> it is accepted (dropped); with a DLQ sender it is written as the ADR-0035 record
/// (<c>reason: "poison"</c>, original bytes preserved) and accepted only after the write succeeded (a failed write releases it
/// exactly like the version gate); without a sender it is accepted and dropped (logged).
/// </para>
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
            // ADR-0037 decision 1: settled on the first delivery, never decoded, never retried. OnError gets an empty
            // envelope. Settle FIRST, report afterwards: a throwing OnError must never leave the message unsettled
            // or stop the loop (it is isolated).
            await SettleToDeadLetterAsync(
                message,
                () => AmqpProperties.RawLike(message),
                new BabelQueueException(
                    $"Rejected an Artemis message: unsupported {AmqpProperties.SchemaVersion} property "
                    + $"'{declaredVersion}' (supported: {EnvelopeCodec.SchemaVersion}); body not decoded."),
                "schema-version-gated",
                cancellationToken).ConfigureAwait(false);
            return;
        }

        // Invalid UTF-8 is poison (a lenient decode would hand a corrupted payload to a handler).
        var bodyBytes = AmqpProperties.BodyBytes(message);
        var text = StrictUtf8(bodyBytes);
        var envelope = Reconcile(
            text is null ? new Envelope(null, null, null, null, 0, null) : EnvelopeCodec.Decode(text), message);

        if (text is null || !EnvelopeCodec.Accepts(envelope))
        {
            // ADR-0037 decision 2: the poison route -- the ADR-0035 record to the DLQ, never a retry.
            var cause = new BabelQueueException(text is null
                ? "Rejected an Artemis message: the body is not valid UTF-8."
                : "Rejected a non-conformant BabelQueue envelope from Artemis.");
            await SettleToDeadLetterAsync(
                message,
                () => AmqpProperties.Raw(Encoding.UTF8.GetString(PoisonBody(
                    bodyBytes,
                    cause.Message,
                    cause.GetType().FullName,
                    DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    QueueName(message),
                    envelope.Attempts))),
                cause,
                "poison",
                cancellationToken,
                envelope).ConfigureAwait(false);
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
    private void SafeReport(Exception error, Message message, Envelope? envelope = null)
    {
        try
        {
            _options.OnError?.Invoke(error, envelope ?? new Envelope(null, null, null, null, 0, null), message);
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

    /// <summary>
    /// ADR-0037 decisions 1 and 2: settle a delivery that is terminal on its first delivery (a version-gated message or a
    /// poison body). <c>UnknownUrn = delete</c> accepts it (dropped); without a DLQ sender it is accepted and dropped (logged);
    /// otherwise the record from <paramref name="build"/> is written to the DLQ and the message accepted only after the write
    /// succeeded. A failed write <c>Release</c>s it (never accepted, never retried by the SDK; the rejection repeats, so the
    /// write is tried again), reports it and backs off for one receive interval. Reporting is isolated from settling.
    /// </summary>
    private async Task SettleToDeadLetterAsync(
        Message message,
        Func<Message> build,
        BabelQueueException rejected,
        string kind,
        CancellationToken cancellationToken,
        Envelope? envelope = null)
    {
        var sender = _options.DeadLetterSender;
        if (_options.UnknownUrn == UnknownUrnStrategy.Delete)
        {
            await _receiver.AcceptAsync(message, cancellationToken).ConfigureAwait(false);
            SafeReport(rejected, message, envelope);
            return;
        }

        if (sender is null)
        {
            System.Diagnostics.Trace.TraceWarning(
                $"BabelQueue.Artemis: dropped a {kind} message on its first delivery: dead-lettering is off (no DeadLetterSender).");
            await _receiver.AcceptAsync(message, cancellationToken).ConfigureAwait(false);
            SafeReport(rejected, message, envelope);
            return;
        }

        try
        {
            await sender.SendAsync(build(), cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // A failed DLQ write must release the message, whatever the cause.
        catch (Exception dlqError) when (dlqError is not OperationCanceledException)
#pragma warning restore CA1031
        {
            // The message could not be parked: never Accept it. Release it for redelivery (it never reaches a handler),
            // report, and back off for one receive interval so a persistent DLQ failure doesn't spin.
            await _receiver.ReleaseAsync(message, cancellationToken).ConfigureAwait(false);
            SafeReport(rejected, message, envelope);
            SafeReport(
                new BabelQueueException(
                    $"Failed to dead-letter a {kind} Artemis message; released for redelivery, not accepted.",
                    dlqError),
                message,
                envelope);
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
        SafeReport(rejected, message, envelope);
    }

    private static readonly UTF8Encoding StrictUtf8Encoding = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private static string? StrictUtf8(byte[] body)
    {
        try
        {
            return StrictUtf8Encoding.GetString(body);
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }

    /// <summary>
    /// The queue a delivery came from, for the poison record's <c>dead_letter.original_queue</c>:
    /// <see cref="ArtemisConsumerOptions.Queue"/> when set, else the address of the DLQ sender without its
    /// <c>.dlq</c> suffix (the <c>&lt;queue&gt;.dlq</c> convention), else the message's <c>to</c> address.
    /// </summary>
    private string QueueName(Message message)
    {
        var configured = _options.Queue;
        if (!string.IsNullOrWhiteSpace(configured))
        {
            return configured;
        }

        var address = _options.DeadLetterSender?.Address;
        if (!string.IsNullOrWhiteSpace(address))
        {
            return address.EndsWith(".dlq", StringComparison.Ordinal) && address.Length > ".dlq".Length
                ? address[..^".dlq".Length]
                : address;
        }

        var to = message.Properties?.To;
        return string.IsNullOrWhiteSpace(to) ? "default" : to;
    }

    private static readonly JsonWriterOptions PoisonWriterOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        Indented = false,
    };

    /// <summary>
    /// The poison DLQ record (ADR-0035, <see cref="PoisonRecord"/>): a body that is a strict JSON object without a
    /// top-level <c>dead_letter</c> key is spliced byte for byte; anything else is carried as <c>raw</c> (plain text when
    /// valid UTF-8, otherwise base64 plus <c>raw_encoding: "base64"</c>). The <c>dead_letter</c> block has reason
    /// <c>poison</c>.
    /// </summary>
    internal static byte[] PoisonBody(byte[] body, string? error, string? exception, long failedAt, string originalQueue, int attempts)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, PoisonWriterOptions))
        {
            writer.WriteStartObject();
            writer.WriteString("reason", "poison");
            if (error is null) { writer.WriteNull("error"); } else { writer.WriteString("error", error); }
            if (exception is null) { writer.WriteNull("exception"); } else { writer.WriteString("exception", exception); }
            writer.WriteNumber("failed_at", failedAt);
            writer.WriteString("original_queue", originalQueue);
            writer.WriteNumber("attempts", attempts);
            writer.WriteString("lang", EnvelopeCodec.SourceLang);
            writer.WriteEndObject();
        }

        return PoisonRecord.Build(body, Encoding.UTF8.GetString(stream.ToArray()));
    }

    private static string OriginalQueue(Envelope envelope) => envelope.Meta?.Queue ?? string.Empty;
}
