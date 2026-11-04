namespace BabelQueue.Artemis;

/// <summary>
/// Sends canonical-envelope messages to one Artemis address with the §7 AMQP 1.0 projection: the
/// body is the envelope JSON, <c>correlation-id</c> = <c>trace_id</c>, <c>creation-time</c> =
/// <c>meta.created_at</c>, the <c>x-opt-jms-type</c> annotation = URN, plus the <c>bq-</c>
/// application properties — so a Java (JMS) or .NET/Node/Python peer routes and correlates
/// without decoding the body. The envelope is unchanged (<c>schema_version</c> stays 1); Artemis
/// is purely additive.
///
/// <para>A positive delay uses Artemis's native AMQP scheduled delivery (the
/// <c>x-opt-delivery-time</c> annotation).</para>
/// </summary>
public sealed class ArtemisPublisher
{
    private readonly IAmqpSender _sender;

    /// <param name="sender">A sender for the target address (mockable in tests).</param>
    public ArtemisPublisher(IAmqpSender sender)
    {
        ArgumentNullException.ThrowIfNull(sender);
        _sender = sender;
    }

    /// <summary>
    /// Builds the canonical envelope for <c>(urn, data)</c>, sends it with the §7 projection, and
    /// returns the message id (<c>meta.id</c>). A positive <paramref name="delay"/> schedules
    /// native delayed delivery.
    /// </summary>
    public Task<string> PublishAsync(
        string urn,
        IReadOnlyDictionary<string, object?>? data = null,
        string? traceId = null,
        TimeSpan? delay = null,
        CancellationToken cancellationToken = default)
        => PublishWithHeadersAsync(urn, data, headers: null, traceId, delay, cancellationToken);

    /// <summary>
    /// The header-aware (ADR-0028) counterpart of
    /// <see cref="PublishAsync(string, IReadOnlyDictionary{string, object?}?, string?, TimeSpan?, CancellationToken)"/>:
    /// in addition to the §7 projection it overlays the out-of-band <paramref name="headers"/> (e.g. a W3C
    /// <c>traceparent</c> from <c>Telemetry.PublishAsync(..., headers, ...)</c>) as string application properties.
    /// The headers ride <b>beside</b> the frozen envelope, never inside it, and never overwrite a <c>bq_</c> contract
    /// property. A <c>null</c>/empty header map is identical to <c>PublishAsync</c>.
    /// </summary>
    /// <param name="urn">The message URN to publish.</param>
    /// <param name="data">The message payload, or <c>null</c> for an empty body.</param>
    /// <param name="headers">The out-of-band transport headers to ride beside the envelope.</param>
    /// <param name="traceId">An existing trace id to continue, or <c>null</c> to mint one.</param>
    /// <param name="delay">A positive delay schedules native delayed delivery.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task<string> PublishWithHeadersAsync(
        string urn,
        IReadOnlyDictionary<string, object?>? data,
        IReadOnlyDictionary<string, string>? headers,
        string? traceId = null,
        TimeSpan? delay = null,
        CancellationToken cancellationToken = default)
    {
        var envelope = EnvelopeCodec.Make(urn, data, _sender.Address, traceId);
        var message = AmqpProperties.ToMessage(envelope, delay, headers);
        await _sender.SendAsync(message, cancellationToken).ConfigureAwait(false);
        return envelope.Meta?.Id ?? string.Empty;
    }
}
