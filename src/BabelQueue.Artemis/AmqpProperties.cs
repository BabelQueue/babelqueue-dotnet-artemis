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
/// message annotation = URN (JMSType, the AMQP-JMS mapping); plus the <c>bq-</c> application
/// properties (string-valued, matching the Java JMS <c>setStringProperty</c> projection). A
/// positive delay sets the <c>x-opt-delivery-time</c> annotation Artemis honours for scheduled
/// delivery. The body stays authoritative (Contract §7.2).
/// </summary>
internal static class AmqpProperties
{
    public const string SchemaVersion = "bq-schema-version";
    public const string SourceLang = "bq-source-lang";
    public const string Attempts = "bq-attempts";
    public const string AppId = "bq-app-id";
    public const string Delay = "bq-delay";
    public const string AppIdValue = "babelqueue";

    private static readonly Symbol JmsType = new("x-opt-jms-type");
    private static readonly Symbol ScheduledDelivery = new("x-opt-delivery-time");

    public static Message ToMessage(Envelope envelope, TimeSpan? delay = null)
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

        return message;
    }

    /// <summary>Forward an undecodable message's raw body to the DLQ (no envelope to annotate).</summary>
    public static Message Raw(string body) => new(body);

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
