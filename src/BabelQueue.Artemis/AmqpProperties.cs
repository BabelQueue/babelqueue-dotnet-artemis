using System.Globalization;
using System.Text;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace BabelQueue.Artemis;

/// <summary>
/// Projects the envelope's contract fields onto the AMQP 1.0 message a JMS peer reads, and reads
/// them back. Body = envelope JSON; <c>correlation-id</c> = <c>trace_id</c> (JMSCorrelationID);
/// <c>creation-time</c> = <c>meta.created_at</c> (JMSTimestamp); the <c>x-opt-jms-type</c>
/// message annotation = URN (JMSType, the AMQP-JMS mapping); plus the <c>bq_</c> application
/// properties (string-valued). The names use <strong>underscores</strong>, not hyphens: a JMS
/// property name must be a valid Java identifier, and every Artemis SDK uses the same JMS-legal
/// form for cross-protocol parity. A positive delay sets the <c>x-opt-delivery-time</c>
/// annotation Artemis honours for scheduled delivery. The body stays authoritative (Contract §7.2).
/// </summary>
internal static class AmqpProperties
{
    public const string SchemaVersion = "bq_schema_version";
    public const string SourceLang = "bq_source_lang";
    public const string Attempts = "bq_attempts";
    public const string AppId = "bq_app_id";
    public const string Delay = "bq_delay";
    public const string AppIdValue = "babelqueue";

    /// <summary>
    /// The application properties the transport owns (the §7.2 <c>bq_</c> projection plus <c>bq_delay</c>). They are
    /// never accepted from, nor surfaced as, out-of-band transport headers (parity with the Go reference).
    /// </summary>
    private static readonly HashSet<string> Contract = new(StringComparer.Ordinal)
    {
        SchemaVersion, SourceLang, Attempts, AppId, Delay,
    };

    /// <summary>True when <paramref name="key"/> is a contract property the transport owns.</summary>
    public static bool IsContract(string key) => Contract.Contains(key);

    private static readonly Symbol JmsType = new("x-opt-jms-type");
    private static readonly Symbol ScheduledDelivery = new("x-opt-delivery-time");

    public static Message ToMessage(Envelope envelope, TimeSpan? delay = null, IReadOnlyDictionary<string, string>? headers = null)
    {
        var message = new Message(EnvelopeCodec.Encode(envelope))
        {
            Properties = new Properties { ContentType = "application/json" },
            ApplicationProperties = new ApplicationProperties(),
        };

        if (!string.IsNullOrEmpty(envelope.TraceId))
        {
            message.Properties.CorrelationId = envelope.TraceId;
        }

        var meta = envelope.Meta;
        if (meta is not null)
        {
            if (meta.CreatedAt > 0)
            {
                message.Properties.CreationTime = DateTimeOffset.FromUnixTimeMilliseconds(meta.CreatedAt).UtcDateTime;
            }

            message.ApplicationProperties.Map[SchemaVersion] = meta.SchemaVersion.ToString(CultureInfo.InvariantCulture);
            if (!string.IsNullOrEmpty(meta.Lang))
            {
                message.ApplicationProperties.Map[SourceLang] = meta.Lang;
            }
        }

        message.ApplicationProperties.Map[Attempts] = envelope.Attempts.ToString(CultureInfo.InvariantCulture);
        message.ApplicationProperties.Map[AppId] = AppIdValue;

        if (!string.IsNullOrEmpty(envelope.Job))
        {
            message.MessageAnnotations = new MessageAnnotations();
            message.MessageAnnotations.Map[JmsType] = envelope.Job;
        }

        if (delay is { } window && window > TimeSpan.Zero)
        {
            var ms = (long)window.TotalMilliseconds;
            message.ApplicationProperties.Map[Delay] = ms.ToString(CultureInfo.InvariantCulture);
            message.MessageAnnotations ??= new MessageAnnotations();
            message.MessageAnnotations.Map[ScheduledDelivery] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + ms;
        }

        Merge(message, headers);

        return message;
    }

    /// <summary>
    /// Overlays the out-of-band string <paramref name="headers"/> (e.g. a W3C <c>traceparent</c>, ADR-0028) onto the
    /// message's application properties, beside the <c>bq_</c> contract projection. An existing property is never
    /// overwritten, a contract property name (<see cref="IsContract"/>, including <c>bq_delay</c>) is always dropped,
    /// and empty keys/values are skipped. Keys are applied in
    /// ordinal order so the result is deterministic. A <c>null</c>/empty map is a no-op.
    /// </summary>
    public static void Merge(Message message, IReadOnlyDictionary<string, string>? headers)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (headers is null || headers.Count == 0)
        {
            return;
        }

        message.ApplicationProperties ??= new ApplicationProperties();
        foreach (var key in headers.Keys.OrderBy(static k => k, StringComparer.Ordinal))
        {
            var value = headers[key];
            if (string.IsNullOrEmpty(key) || string.IsNullOrEmpty(value) || IsContract(key) || message.ApplicationProperties.Map.ContainsKey(key))
            {
                continue;
            }

            message.ApplicationProperties.Map[key] = value;
        }
    }

    /// <summary>A DLQ message whose body is the given text (no envelope to annotate).</summary>
    public static Message Raw(string body) => new(body);

    /// <summary>
    /// A copy of the incoming body for the raw version-gate record (ADR-0037): the bytes exactly as received. A binary
    /// (<c>data</c> section) body stays binary, a text (<c>amqp-value</c> string) body stays text.
    /// </summary>
    public static Message RawLike(Message incoming) =>
        incoming.Body is byte[] bytes
            ? new Message { BodySection = new Data { Binary = bytes } }
            : new Message(Text(incoming));

    /// <summary>The message body as bytes (AMQP value string UTF-8-encoded, or the binary body as is).</summary>
    public static byte[] BodyBytes(Message message) =>
        message.Body switch
        {
            null => Array.Empty<byte>(),
            byte[] bytes => bytes,
            _ => Encoding.UTF8.GetBytes(message.Body.ToString() ?? string.Empty),
        };

    /// <summary>The message body as text (AMQP value string, or UTF-8-decoded binary).</summary>
    public static string Text(Message message)
    {
        var body = message.Body;
        return body switch
        {
            null => string.Empty,
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            _ => body.ToString() ?? string.Empty,
        };
    }

    /// <summary>The URN carried by the <c>x-opt-jms-type</c> annotation, or null if absent.</summary>
    public static string? JmsTypeOf(Message message)
    {
        var annotations = message.MessageAnnotations;
        if (annotations is not null && annotations.Map.TryGetValue(JmsType, out var value) && value is not null)
        {
            return value.ToString();
        }
        return null;
    }

    /// <summary>The broker's AMQP <c>delivery-count</c> (0-based: 0 on first delivery).</summary>
    public static int DeliveryCount(Message message)
    {
        var header = message.Header;
        return header is null ? 0 : (int)header.DeliveryCount;
    }
}

/// <summary>
/// Out-of-band transport-header carrier for Artemis (ADR-0028). A W3C <c>traceparent</c> (and <c>tracestate</c>)
/// rides as a string AMQP application property <b>beside</b> the frozen envelope — never inside it — next to the
/// §7 <c>bq_</c> projection (the names are JMS-legal identifiers, so a Java/JMS peer reads them too). This is the
/// consume-side half: it surfaces a received message's application properties as a flat map a handler hands to
/// <c>Telemetry.Wrap(handler, headers)</c>, so the consumer span becomes a true child of the producer span. With no
/// <c>traceparent</c> the core falls back to the v0.1 <c>trace_id</c> mapping.
/// </summary>
public static class ArtemisTraceHeaders
{
    /// <summary>
    /// The string-valued application properties of a received message as a flat map (empty when it has none).
    /// Only out-of-band headers (e.g. <c>traceparent</c>) surface: the <c>bq_</c> contract properties the transport
    /// owns are left out, so handing the map back to <c>PublishWithHeadersAsync</c> cannot re-inject them. Non-string
    /// and empty values are skipped.
    /// </summary>
    public static Dictionary<string, string> Extract(Message message)
    {
        ArgumentNullException.ThrowIfNull(message);

        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        var properties = message.ApplicationProperties;
        if (properties is null)
        {
            return headers;
        }

        foreach (var entry in properties.Map)
        {
            if (entry.Key is string key && entry.Value is string value && value.Length > 0 && !AmqpProperties.IsContract(key))
            {
                headers[key] = value;
            }
        }

        return headers;
    }
}
