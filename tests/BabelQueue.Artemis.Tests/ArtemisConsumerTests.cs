using System.Text;
using System.Text.Json;
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
        receiver
            .Setup(r => r.ModifyAsync(It.IsAny<Message>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
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

    private static Message WithSchemaVersion(string body, string? version)
    {
        var message = Incoming(body, Urn, 0);
        if (version is not null)
        {
            message.ApplicationProperties = new ApplicationProperties();
            message.ApplicationProperties.Map["bq_schema_version"] = version;
        }

        return message;
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" \t")]
    [InlineData("1")]
    public async Task AbsentBlankOrCurrentSchemaVersionPropertyDecodesAndRunsHandler(string? version)
    {
        var receiver = ReceiverWith(WithSchemaVersion(Body(0), version));
        var calls = 0;

        await new ArtemisConsumer(receiver.Object, Handler(_ => calls++)).PollAsync();

        Assert.Equal(1, calls);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Theory]
    [InlineData("2")]
    [InlineData("x")]
    [InlineData(" 1")]
    public async Task UnsupportedSchemaVersionPropertyIsRejectedWithoutDecoding(string version)
    {
        // The body is not valid JSON, but the decoder does not throw on it (it yields an empty envelope that
        // the Accepts() branch would also send to the DLQ with the same settle). The two paths differ only in
        // the OnError message, which is what proves the property gate fired before the decoder ran.
        const string body = "{ this is not json";
        var receiver = ReceiverWith(WithSchemaVersion(body, version));
        var (sender, sent) = DlqSender();
        Exception? reported = null;
        var calls = 0;
        var options = new ArtemisConsumerOptions { DeadLetterSender = sender.Object, OnError = (e, _, _) => reported = e };

        await new ArtemisConsumer(receiver.Object, Handler(_ => calls++), options).PollAsync();

        Assert.Equal(0, calls);
        Assert.NotNull(reported);
        Assert.Contains("bq_schema_version", reported!.Message);
        Assert.Contains($"'{version}'", reported.Message);
        Assert.DoesNotContain("non-conformant", reported.Message);
        Assert.Equal(body, AmqpProperties.Text(sent()!));
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Theory]
    [InlineData("2")]
    [InlineData("x")]
    public async Task SchemaVersionGateDlqFailureReleasesDoesNotAcceptAndLoopSurvives(string version)
    {
        var receiver = ReceiverWith(WithSchemaVersion(Body(0), version));
        var sender = new Mock<IAmqpSender>();
        sender.SetupGet(s => s.Address).Returns("orders.dlq");
        sender
            .Setup(s => s.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("dlq down"));
        var reported = new List<Exception>();
        var calls = 0;
        var options = new ArtemisConsumerOptions
        {
            DeadLetterSender = sender.Object,
            ReceiveTimeout = TimeSpan.FromMilliseconds(1),
            OnError = (e, _, _) => reported.Add(e),
        };

        var count = await new ArtemisConsumer(receiver.Object, Handler(_ => calls++), options).PollAsync();

        Assert.Equal(1, count);
        Assert.Equal(0, calls);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
        receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.ModifyAsync(It.IsAny<Message>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
        Assert.Equal(2, reported.Count);
        Assert.IsType<InvalidOperationException>(reported[1].InnerException);
    }

    [Fact]
    public async Task SchemaVersionGateThrowingOnErrorStillAcceptsAfterSuccessfulDeadLetter()
    {
        var receiver = ReceiverWith(WithSchemaVersion(Body(0), "2"));
        var sender = new Mock<IAmqpSender>();
        sender.SetupGet(s => s.Address).Returns("orders.dlq");
        var calls = 0;
        var options = new ArtemisConsumerOptions
        {
            DeadLetterSender = sender.Object,
            OnError = (_, _, _) => throw new InvalidOperationException("observer exploded"),
        };

        var count = await new ArtemisConsumer(receiver.Object, Handler(_ => calls++), options).PollAsync();

        Assert.Equal(1, count);
        Assert.Equal(0, calls);
        sender.Verify(s => s.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task SchemaVersionGateThrowingOnErrorStillReleasesAfterDeadLetterFailure()
    {
        var receiver = ReceiverWith(WithSchemaVersion(Body(0), "2"));
        var sender = new Mock<IAmqpSender>();
        sender.SetupGet(s => s.Address).Returns("orders.dlq");
        sender
            .Setup(s => s.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("dlq down"));
        var calls = 0;
        var options = new ArtemisConsumerOptions
        {
            DeadLetterSender = sender.Object,
            ReceiveTimeout = TimeSpan.FromMilliseconds(1),
            OnError = (_, _, _) => throw new InvalidOperationException("observer exploded"),
        };

        var count = await new ArtemisConsumer(receiver.Object, Handler(_ => calls++), options).PollAsync();

        Assert.Equal(1, count);
        Assert.Equal(0, calls);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
        receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.ModifyAsync(It.IsAny<Message>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

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
    public async Task ThrowingHandlerSettlesModifiedDeliveryFailedForRedelivery()
    {
        var receiver = ReceiverWith(Incoming(Body(0), Urn, 0)); // first delivery fails → modified{delivery-failed}
        Exception? reported = null;
        var options = new ArtemisConsumerOptions { MaxTries = 3, OnError = (e, _, _) => reported = e };

        await new ArtemisConsumer(receiver.Object, Handler(_ => { }, @throw: true), options).PollAsync();

        Assert.IsType<InvalidOperationException>(reported);
        receiver.Verify(
            r => r.ModifyAsync(It.IsAny<Message>(), true, false, It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    /// <summary>
    /// A tiny in-memory broker honouring Artemis's counting rule: a <i>modified</i> outcome with
    /// <c>delivery-failed</c> bumps <c>delivery-count</c> on redelivery, a plain <i>released</i> one does not.
    /// </summary>
    private sealed class RedeliveringReceiver : IAmqpReceiver
    {
        private readonly string _body;
        private uint _deliveryCount;
        private bool _pending = true;

        public RedeliveringReceiver(string body) => _body = body;

        public int Accepts { get; private set; }

        public int Releases { get; private set; }

        public int Modifies { get; private set; }

        public Task<Message?> ReceiveAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            if (!_pending)
            {
                return Task.FromResult<Message?>(null);
            }

            _pending = false;
            var message = Incoming(_body, Urn, _deliveryCount);
            return Task.FromResult<Message?>(message);
        }

        public Task AcceptAsync(Message message, CancellationToken cancellationToken = default)
        {
            Accepts++;
            return Task.CompletedTask;
        }

        public Task ReleaseAsync(Message message, CancellationToken cancellationToken = default)
        {
            Releases++;
            _pending = true; // requeued, counter untouched
            return Task.CompletedTask;
        }

        public Task ModifyAsync(
            Message message, bool deliveryFailed, bool undeliverableHere = false, CancellationToken cancellationToken = default)
        {
            Modifies++;
            _pending = true;
            if (deliveryFailed)
            {
                _deliveryCount++;
            }

            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task ThrowingHandlerReachesMaxTriesAndDeadLettersAsDeliveryCountRises()
    {
        var receiver = new RedeliveringReceiver(Body(0));
        var (dlq, sent) = DlqSender();
        var calls = new List<int>();
        var options = new ArtemisConsumerOptions { MaxTries = 3, DeadLetterSender = dlq.Object };
        var consumer = new ArtemisConsumer(receiver, Handler(e => calls.Add(e.Attempts), @throw: true), options);

        var polls = 0;
        while (await consumer.PollAsync() == 1 && ++polls < 20)
        {
        }

        Assert.Equal(new[] { 0, 1, 2 }, calls); // attempts follow the broker's delivery-count: 0, 1, 2
        Assert.Equal(2, receiver.Modifies);
        Assert.Equal(0, receiver.Releases);
        Assert.Equal(1, receiver.Accepts);
        dlq.Verify(s => s.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        var dead = EnvelopeCodec.Decode(AmqpProperties.Text(sent()!));
        Assert.Equal("failed", dead.DeadLetter!.Reason);
        Assert.Equal(2, dead.Attempts);
    }

    /// <summary>An implementer written before <c>ModifyAsync</c> existed: only the original three members.</summary>
    private sealed class LegacyReceiver : IAmqpReceiver
    {
        public int Releases { get; private set; }

        public Task<Message?> ReceiveAsync(TimeSpan timeout, CancellationToken cancellationToken = default) =>
            Task.FromResult<Message?>(null);

        public Task AcceptAsync(Message message, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task ReleaseAsync(Message message, CancellationToken cancellationToken = default)
        {
            Releases++;
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task ModifyAsyncDefaultImplementationFallsBackToReleaseForLegacyReceivers()
    {
        var legacy = new LegacyReceiver();
        IAmqpReceiver receiver = legacy;

        await receiver.ModifyAsync(new Message("x"), deliveryFailed: true);

        Assert.Equal(1, legacy.Releases);
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
    public async Task NonConformantBodyIsWrittenAsAnAdr0035PoisonRecordAndAccepted()
    {
        var receiver = ReceiverWith(Incoming("not-json", jmsType: null, deliveryCount: 0));
        var (dlq, sent) = DlqSender();
        Exception? reported = null;
        var calls = 0;
        var options = new ArtemisConsumerOptions { DeadLetterSender = dlq.Object, OnError = (e, _, _) => reported = e };

        await new ArtemisConsumer(receiver.Object, Handler(_ => calls++), options).PollAsync();

        Assert.Equal(0, calls);
        dlq.Verify(s => s.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
        receiver.Verify(r => r.ModifyAsync(It.IsAny<Message>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
        Assert.NotNull(reported);
        using var record = JsonDocument.Parse(AmqpProperties.Text(sent()!));
        Assert.Equal("not-json", record.RootElement.GetProperty("raw").GetString());
        var block = record.RootElement.GetProperty("dead_letter");
        Assert.Equal("poison", block.GetProperty("reason").GetString());
        Assert.Equal("orders", block.GetProperty("original_queue").GetString());
    }

    [Fact]
    public async Task PoisonObjectIsSplicedByteForByteWithADeadLetterBlock()
    {
        const string body = "{\"job\":\"" + Urn + "\",  \"data\":{\"big\":1e400}}";
        var receiver = ReceiverWith(Incoming(body, jmsType: Urn, deliveryCount: 3));
        var (dlq, sent) = DlqSender();
        var options = new ArtemisConsumerOptions { DeadLetterSender = dlq.Object, Queue = "orders-q" };

        await new ArtemisConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        var text = AmqpProperties.Text(sent()!);
        Assert.StartsWith(body[..^1] + ",\"dead_letter\":", text, StringComparison.Ordinal);
        using var record = JsonDocument.Parse(text);
        var block = record.RootElement.GetProperty("dead_letter");
        Assert.Equal("poison", block.GetProperty("reason").GetString());
        Assert.Equal("orders-q", block.GetProperty("original_queue").GetString());
        Assert.Equal(3, block.GetProperty("attempts").GetInt32());
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task InvalidUtf8BodyIsWrappedAsBase64PoisonWithoutReplacementCharacters()
    {
        var bytes = new byte[] { 0x7B, 0xFF, 0xFE, 0x7D };
        var message = new Message { BodySection = new Data { Binary = bytes } };
        var receiver = ReceiverWith(message);
        var (dlq, sent) = DlqSender();
        var calls = 0;

        await new ArtemisConsumer(receiver.Object, Handler(_ => calls++), new ArtemisConsumerOptions { DeadLetterSender = dlq.Object }).PollAsync();

        Assert.Equal(0, calls);
        using var record = JsonDocument.Parse(AmqpProperties.Text(sent()!));
        Assert.Equal(Convert.ToBase64String(bytes), record.RootElement.GetProperty("raw").GetString());
        Assert.Equal("base64", record.RootElement.GetProperty("raw_encoding").GetString());
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task IdentitylessObjectIsPoisonNotAnUnknownUrn()
    {
        var receiver = ReceiverWith(Incoming("{}", jmsType: null, deliveryCount: 0));
        var (dlq, sent) = DlqSender();

        await new ArtemisConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), new ArtemisConsumerOptions { DeadLetterSender = dlq.Object }).PollAsync();

        using var record = JsonDocument.Parse(AmqpProperties.Text(sent()!));
        Assert.Equal("poison", record.RootElement.GetProperty("dead_letter").GetProperty("reason").GetString());
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task PoisonWithDeadLetteringOffIsAcceptedAndDropped()
    {
        var receiver = ReceiverWith(Incoming("not-json", jmsType: null, deliveryCount: 0));

        await new ArtemisConsumer(receiver.Object, new Dictionary<string, BabelHandler>()).PollAsync();

        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
        receiver.Verify(r => r.ModifyAsync(It.IsAny<Message>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task PoisonUnderDeleteStrategyIsAcceptedWithoutARecord()
    {
        var receiver = ReceiverWith(Incoming("not-json", jmsType: null, deliveryCount: 0));
        var (dlq, _) = DlqSender();
        var options = new ArtemisConsumerOptions { DeadLetterSender = dlq.Object, UnknownUrn = UnknownUrnStrategy.Delete };

        await new ArtemisConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        dlq.Verify(s => s.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task PoisonWhoseDeadLetterWriteFailsIsReleasedNotAcceptedAndReported()
    {
        var receiver = ReceiverWith(Incoming("not-json", jmsType: null, deliveryCount: 0));
        var sender = new Mock<IAmqpSender>();
        sender.SetupGet(s => s.Address).Returns("orders.dlq");
        sender
            .Setup(s => s.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("dlq down"));
        var reported = new List<Exception>();
        var options = new ArtemisConsumerOptions
        {
            DeadLetterSender = sender.Object,
            ReceiveTimeout = TimeSpan.FromMilliseconds(1),
            OnError = (e, _, _) => reported.Add(e),
        };

        var count = await new ArtemisConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.Equal(1, count);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
        receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.ModifyAsync(It.IsAny<Message>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
        Assert.Equal(2, reported.Count);
        Assert.IsType<InvalidOperationException>(reported[1].InnerException);
    }

    [Fact]
    public async Task PoisonWithAThrowingOnErrorIsStillAcceptedAfterTheWrite()
    {
        var receiver = ReceiverWith(Incoming("not-json", jmsType: null, deliveryCount: 0));
        var (dlq, _) = DlqSender();
        var options = new ArtemisConsumerOptions
        {
            DeadLetterSender = dlq.Object,
            OnError = (_, _, _) => throw new InvalidOperationException("observer exploded"),
        };

        await new ArtemisConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Theory]
    [InlineData(UnknownUrnStrategy.Fail)]
    [InlineData(UnknownUrnStrategy.Release)]
    [InlineData(UnknownUrnStrategy.DeadLetter)]
    public async Task VersionGatedMessageIsWrittenRawOnTheFirstDeliveryForEveryNonDeleteStrategy(string strategy)
    {
        // delivery-count 7 and MaxTries 2: the gate applies on the first delivery regardless of the counters.
        var body = Body(5);
        var message = WithSchemaVersion(body, "2");
        message.Header = new Header { DeliveryCount = 7 };
        var receiver = ReceiverWith(message);
        var (dlq, sent) = DlqSender();
        var options = new ArtemisConsumerOptions { DeadLetterSender = dlq.Object, UnknownUrn = strategy, MaxTries = 2 };

        await new ArtemisConsumer(receiver.Object, Handler(_ => { }), options).PollAsync();

        Assert.Equal(body, AmqpProperties.Text(sent()!));
        Assert.DoesNotContain("dead_letter", AmqpProperties.Text(sent()!), StringComparison.Ordinal);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.ModifyAsync(It.IsAny<Message>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task VersionGatedBinaryBodyIsWrittenAsTheSameBytes()
    {
        var bytes = Encoding.UTF8.GetBytes("{\n \"meta\": {\"schema_version\": 2} }\n");
        var message = new Message { BodySection = new Data { Binary = bytes } };
        message.ApplicationProperties = new ApplicationProperties();
        message.ApplicationProperties.Map["bq_schema_version"] = "2";
        var receiver = ReceiverWith(message);
        var (dlq, sent) = DlqSender();

        await new ArtemisConsumer(receiver.Object, Handler(_ => { }), new ArtemisConsumerOptions { DeadLetterSender = dlq.Object }).PollAsync();

        Assert.Equal(bytes, AmqpProperties.BodyBytes(sent()!));
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task VersionGatedMessageUnderDeleteStrategyIsAcceptedWithoutARecord()
    {
        var receiver = ReceiverWith(WithSchemaVersion(Body(0), "2"));
        var (dlq, _) = DlqSender();
        var options = new ArtemisConsumerOptions { DeadLetterSender = dlq.Object, UnknownUrn = UnknownUrnStrategy.Delete };

        await new ArtemisConsumer(receiver.Object, Handler(_ => { }), options).PollAsync();

        dlq.Verify(s => s.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task VersionGatedMessageWithDeadLetteringOffIsAcceptedAndDropped()
    {
        var receiver = ReceiverWith(WithSchemaVersion(Body(0), "2"));
        var calls = 0;

        await new ArtemisConsumer(receiver.Object, Handler(_ => calls++)).PollAsync();

        Assert.Equal(0, calls);
        receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
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
        receiver.Verify(r => r.ModifyAsync(It.IsAny<Message>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
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
