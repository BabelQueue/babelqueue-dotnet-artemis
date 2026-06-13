using Amqp;
using Amqp.Framing;
using Amqp.Types;
using BabelQueue;
using BabelQueue.Artemis;
using Moq;
using Xunit;

namespace BabelQueue.Artemis.Tests;

/// <summary>
/// Consumer behaviour against a mocked receiver (no broker): attempts = max(body, delivery-count)
/// with no −1, Accept on success, Release on retryable failure, the cross-language DLQ, and the
/// unknown-URN strategies.
/// </summary>
public sealed class ArtemisConsumerTests
{
    private const string Urn = "urn:babel:orders:created";

    private static string Body(int attempts) =>
        EnvelopeCodec.Encode(
            EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["order_id"] = 1 }, "orders", "trace-1")
                with { Attempts = attempts });

    private static Message Incoming(string body, string? jmsType, uint deliveryCount)
    {
        var message = new Message(body);
        if (deliveryCount > 0)
        {
            message.Header = new Header { DeliveryCount = deliveryCount };
        }
        if (jmsType is not null)
        {
            message.MessageAnnotations = new MessageAnnotations();
            message.MessageAnnotations.Map[new Symbol("x-opt-jms-type")] = jmsType;
        }
        return message;
    }

    private static Mock<IAmqpReceiver> ReceiverWith(Message? message)
    {
        var receiver = new Mock<IAmqpReceiver>();
        receiver.Setup(r => r.ReceiveAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>())).ReturnsAsync(message);
        receiver.Setup(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        receiver.Setup(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        return receiver;
    }

    private static (Mock<IAmqpSender> Sender, Func<Message?> Sent) DlqSender()
    {
        var sender = new Mock<IAmqpSender>();
        sender.SetupGet(s => s.Address).Returns("orders.dlq");
        Message? captured = null;
        sender
            .Setup(s => s.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
            .Callback<Message, CancellationToken>((m, _) => captured = m)
            .Returns(Task.CompletedTask);
        return (sender, () => captured);
    }

    private static Dictionary<string, BabelHandler> Handler(Action<Envelope> onHandle, bool @throw = false) => new()
    {
        [Urn] = (env, _, _) =>
        {
            onHandle(env);
            return @throw ? throw new InvalidOperationException("boom") : Task.CompletedTask;
        },
    };

    [Fact]
    public async Task SuccessAcknowledges()
    {
        var receiver = ReceiverWith(Incoming(Body(0), Urn, 0)); // first delivery: AMQP delivery-count 0
        var seen = -1;

        var count = await new ArtemisConsumer(receiver.Object, Handler(e => seen = e.Attempts)).PollAsync();

        Assert.Equal(1, count);
        Assert.Equal(0, seen);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task RoutesByBodyUrnWhenAnnotationAbsent()
    {
        var receiver = ReceiverWith(Incoming(Body(0), jmsType: null, deliveryCount: 0));
        var handled = false;

        await new ArtemisConsumer(receiver.Object, Handler(_ => handled = true)).PollAsync();

        Assert.True(handled);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Theory]
    [InlineData(0, 3, 3)] // delivery-count 3 (0-based) wins over body 0 → attempts 3, no −1
    [InlineData(5, 2, 5)] // body 5 wins over delivery-count 2 → never lowered
    [InlineData(0, 0, 0)] // first delivery keeps the body count
    public async Task AttemptsIsMaxOfBodyAndDeliveryCount(int bodyAttempts, uint deliveryCount, int expected)
    {
        var receiver = ReceiverWith(Incoming(Body(bodyAttempts), Urn, deliveryCount));
        var seen = -1;

        // High max-tries so the handler runs even at elevated attempts.
        var options = new ArtemisConsumerOptions { MaxTries = 99 };
        await new ArtemisConsumer(receiver.Object, Handler(e => seen = e.Attempts), options).PollAsync();

        Assert.Equal(expected, seen);
    }

    [Fact]
    public async Task ThrowingHandlerReleasesForRedelivery()
    {
        var receiver = ReceiverWith(Incoming(Body(0), Urn, 0)); // first delivery fails → release
        Exception? reported = null;
        var options = new ArtemisConsumerOptions { MaxTries = 3, OnError = (e, _, _) => reported = e };

        await new ArtemisConsumer(receiver.Object, Handler(_ => { }, @throw: true), options).PollAsync();

        Assert.IsType<InvalidOperationException>(reported);
        receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task TerminalFailureGoesToDlqWithDeadLetterBlockAndAccepts()
    {
        var receiver = ReceiverWith(Incoming(Body(2), Urn, 0)); // attempts 2, next 3 == maxTries
        var (dlq, sent) = DlqSender();
        var options = new ArtemisConsumerOptions { MaxTries = 3, DeadLetterSender = dlq.Object };

        await new ArtemisConsumer(receiver.Object, Handler(_ => { }, @throw: true), options).PollAsync();

        dlq.Verify(s => s.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        Assert.Equal("failed", EnvelopeCodec.Decode(AmqpProperties.Text(sent()!)).DeadLetter!.Reason);
    }

    [Fact]
    public async Task TerminalFailureWithoutDlqDropsAndAccepts()
    {
        var receiver = ReceiverWith(Incoming(Body(2), Urn, 0));
        var options = new ArtemisConsumerOptions { MaxTries = 3 };

        await new ArtemisConsumer(receiver.Object, Handler(_ => { }, @throw: true), options).PollAsync();

        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task NonConformantForwardedRawToDlqAndAccepts()
    {
        var receiver = ReceiverWith(Incoming("not-json", jmsType: null, deliveryCount: 0));
        var (dlq, _) = DlqSender();
        Exception? reported = null;
        var options = new ArtemisConsumerOptions { DeadLetterSender = dlq.Object, OnError = (e, _, _) => reported = e };

        await new ArtemisConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        dlq.Verify(s => s.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        Assert.NotNull(reported);
    }

    [Fact]
    public async Task UnknownUrnFailThrowsAndDoesNotSettle()
    {
        var receiver = ReceiverWith(Incoming(Body(0), Urn, 1));
        var consumer = new ArtemisConsumer(receiver.Object, new Dictionary<string, BabelHandler>());

        await Assert.ThrowsAsync<UnknownUrnException>(() => consumer.PollAsync());
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
        receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task UnknownUrnDeleteAccepts()
    {
        var receiver = ReceiverWith(Incoming(Body(0), Urn, 1));
        var options = new ArtemisConsumerOptions { UnknownUrn = UnknownUrnStrategy.Delete };

        await new ArtemisConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UnknownUrnReleaseReleases()
    {
        var receiver = ReceiverWith(Incoming(Body(0), Urn, 1));
        var options = new ArtemisConsumerOptions { UnknownUrn = UnknownUrnStrategy.Release };

        await new ArtemisConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UnknownUrnDeadLetterGoesToDlq()
    {
        var receiver = ReceiverWith(Incoming(Body(0), Urn, 1));
        var (dlq, sent) = DlqSender();
        var options = new ArtemisConsumerOptions
        {
            UnknownUrn = UnknownUrnStrategy.DeadLetter,
            DeadLetterSender = dlq.Object,
        };

        await new ArtemisConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        dlq.Verify(s => s.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        Assert.Equal("unknown_urn", EnvelopeCodec.Decode(AmqpProperties.Text(sent()!)).DeadLetter!.Reason);
    }

    [Fact]
    public async Task PollReturnsZeroOnTimeout()
    {
        var receiver = ReceiverWith(null);
        Assert.Equal(0, await new ArtemisConsumer(receiver.Object, new Dictionary<string, BabelHandler>()).PollAsync());
    }

    [Fact]
    public async Task RunStopsWhenCancelled()
    {
        var receiver = ReceiverWith(Incoming(Body(0), Urn, 1));
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await new ArtemisConsumer(receiver.Object, new Dictionary<string, BabelHandler>()).RunAsync(cts.Token);

        receiver.Verify(r => r.ReceiveAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public void ConstructorRejectsNullArguments()
    {
        var receiver = ReceiverWith(null);
        Assert.Throws<ArgumentNullException>(() => new ArtemisConsumer(null!, new Dictionary<string, BabelHandler>()));
        Assert.Throws<ArgumentNullException>(() => new ArtemisConsumer(receiver.Object, null!));
    }
}
