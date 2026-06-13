using System.Text;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using BabelQueue;
using BabelQueue.Artemis;
using Xunit;

namespace BabelQueue.Artemis.Tests;

/// <summary>The §7 AMQP 1.0 projection and the native-metadata reads (no broker).</summary>
public sealed class AmqpPropertiesTests
{
    private const string Urn = "urn:babel:orders:created";

    private static Envelope Envelope(int attempts = 0) =>
        EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, "orders", "trace-1")
            with { Attempts = attempts };

    [Fact]
    public void ToMessageProjectsBodyCorrelationCreationAndProperties()
    {
        var envelope = Envelope(attempts: 2);
        var message = AmqpProperties.ToMessage(envelope);

        Assert.Equal(Urn, EnvelopeCodec.Decode(AmqpProperties.Text(message)).Job);
        Assert.Equal("trace-1", message.Properties.CorrelationId);
        Assert.Equal(
            DateTimeOffset.FromUnixTimeMilliseconds(envelope.Meta!.CreatedAt).UtcDateTime,
            message.Properties.CreationTime);

        Assert.Equal("1", message.ApplicationProperties.Map[AmqpProperties.SchemaVersion]);
        Assert.Equal("dotnet", message.ApplicationProperties.Map[AmqpProperties.SourceLang]);
        Assert.Equal("2", message.ApplicationProperties.Map[AmqpProperties.Attempts]);
        Assert.Equal("babelqueue", message.ApplicationProperties.Map[AmqpProperties.AppId]);
        Assert.Equal(Urn, AmqpProperties.JmsTypeOf(message));
    }

    [Fact]
    public void ToMessageWithDelaySetsScheduledDeliveryAnnotationAndBqDelay()
    {
        var message = AmqpProperties.ToMessage(Envelope(), TimeSpan.FromSeconds(30));

        Assert.Equal("30000", message.ApplicationProperties.Map[AmqpProperties.Delay]);
        Assert.True(message.MessageAnnotations.Map.ContainsKey(new Symbol("x-opt-delivery-time")));
    }

    [Fact]
    public void TextDecodesBinaryBodyAndToleratesNull()
    {
        var binary = new Message(Encoding.UTF8.GetBytes("hello"));
        Assert.Equal("hello", AmqpProperties.Text(binary));
        Assert.Equal(string.Empty, AmqpProperties.Text(new Message()));
    }

    [Fact]
    public void JmsTypeOfIsNullWhenAnnotationAbsent()
    {
        Assert.Null(AmqpProperties.JmsTypeOf(new Message("body")));
    }

    [Fact]
    public void DeliveryCountIsZeroWhenHeaderAbsentAndReadsTheHeaderOtherwise()
    {
        Assert.Equal(0, AmqpProperties.DeliveryCount(new Message("body")));
        var withHeader = new Message("body") { Header = new Header { DeliveryCount = 4 } };
        Assert.Equal(4, AmqpProperties.DeliveryCount(withHeader));
    }

    [Fact]
    public void RawWrapsTheBodyVerbatim()
    {
        Assert.Equal("not-json", AmqpProperties.Text(AmqpProperties.Raw("not-json")));
    }
}
