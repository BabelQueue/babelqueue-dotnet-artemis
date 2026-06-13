using Amqp;
using Amqp.Types;
using BabelQueue;
using BabelQueue.Artemis;
using Moq;
using Xunit;

namespace BabelQueue.Artemis.Tests;

/// <summary>Publisher behaviour against a mocked sender (no broker).</summary>
public sealed class ArtemisPublisherTests
{
    private const string Urn = "urn:babel:orders:created";

    private static (Mock<IAmqpSender> Sender, Func<Message?> Sent) SenderOn(string address)
    {
        var sender = new Mock<IAmqpSender>();
        sender.SetupGet(s => s.Address).Returns(address);
        Message? captured = null;
        sender
            .Setup(s => s.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
            .Callback<Message, CancellationToken>((m, _) => captured = m)
            .Returns(Task.CompletedTask);
        return (sender, () => captured);
    }

    [Fact]
    public async Task PublishProjectsTheEnvelopeAndReturnsTheMessageId()
    {
        var (sender, sent) = SenderOn("orders");

        var id = await new ArtemisPublisher(sender.Object)
            .PublishAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, "trace-1");

        var message = sent();
        Assert.NotNull(message);
        Assert.Equal("trace-1", message!.Properties.CorrelationId);
        Assert.Equal("babelqueue", message.ApplicationProperties.Map[AmqpProperties.AppId]);
        Assert.Equal(Urn, message.MessageAnnotations.Map[new Symbol("x-opt-jms-type")]);

        var decoded = EnvelopeCodec.Decode(AmqpProperties.Text(message));
        Assert.Equal(Urn, decoded.Job);
        Assert.Equal("orders", decoded.Meta!.Queue);
        Assert.Equal(id, decoded.Meta.Id);
    }

    [Fact]
    public async Task PublishWithDelaySchedulesNativeDelivery()
    {
        var (sender, sent) = SenderOn("orders");

        await new ArtemisPublisher(sender.Object).PublishAsync(Urn, null, null, TimeSpan.FromSeconds(15));

        Assert.Equal("15000", sent()!.ApplicationProperties.Map[AmqpProperties.Delay]);
    }

    [Fact]
    public async Task PublishWithoutTraceMintsAFreshTrace()
    {
        var (sender, sent) = SenderOn("orders");

        await new ArtemisPublisher(sender.Object).PublishAsync(Urn);

        Assert.False(string.IsNullOrEmpty(EnvelopeCodec.Decode(AmqpProperties.Text(sent()!)).TraceId));
    }

    [Fact]
    public void ConstructorRejectsNullSender()
    {
        Assert.Throws<ArgumentNullException>(() => new ArtemisPublisher(null!));
    }
}
