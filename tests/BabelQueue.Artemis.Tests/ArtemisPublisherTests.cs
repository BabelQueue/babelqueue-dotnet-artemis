using System.Diagnostics;
using System.Text.Json;
using Amqp;
using Amqp.Types;
using BabelQueue;
using BabelQueue.Artemis;
using BabelQueue.Tracing;
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

    // ADR-0028: a W3C traceparent rides as an application property beside the envelope, never inside it.
    private const string Traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    private const string Tracestate = "vendor=opaque,other=1";

    private static Dictionary<string, string> TraceHeaders() => new()
    {
        ["traceparent"] = Traceparent,
        ["tracestate"] = Tracestate,
    };

    private static HashSet<string> TopLevelKeys(string body)
    {
        using var doc = JsonDocument.Parse(body);
        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in doc.RootElement.EnumerateObject())
        {
            keys.Add(property.Name);
            if (property is { Name: "meta", Value.ValueKind: JsonValueKind.Object })
            {
                foreach (var inner in property.Value.EnumerateObject())
                {
                    keys.Add("meta." + inner.Name);
                }
            }
        }
        return keys;
    }

    private static async Task<Activity?> ConsumeUnderListener(Envelope envelope, IReadOnlyDictionary<string, string> headers, ActivityTraceId expectedTrace)
    {
        Activity? consumerSpan = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == Telemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity =>
            {
                if (activity.Kind == ActivityKind.Consumer && activity.TraceId == expectedTrace)
                {
                    consumerSpan = activity;
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        await Telemetry.Wrap(_ => Task.CompletedTask, headers)(envelope);
        return consumerSpan;
    }

    [Fact]
    public async Task PublishWithHeadersCarriesTraceparentAndLeavesTheBodyAnEnvelope()
    {
        var (sender, sent) = SenderOn("orders");
        var publisher = new ArtemisPublisher(sender.Object);

        await publisher.PublishAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, "trace-1");
        var plain = sent()!;
        await publisher.PublishWithHeadersAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, TraceHeaders(), "trace-1");
        var traced = sent()!;

        // The received message carries the header verbatim.
        Assert.Equal(Traceparent, traced.ApplicationProperties.Map["traceparent"]);
        Assert.Equal(Tracestate, traced.ApplicationProperties.Map["tracestate"]);
        Assert.Equal(Traceparent, ArtemisTraceHeaders.Extract(traced)["traceparent"]);

        // The body is the same envelope shape and never mentions the trace header.
        var body = AmqpProperties.Text(traced);
        Assert.DoesNotContain("traceparent", body, StringComparison.Ordinal);
        Assert.DoesNotContain(Traceparent, body, StringComparison.Ordinal);
        Assert.Equal(TopLevelKeys(AmqpProperties.Text(plain)), TopLevelKeys(body));
        Assert.Equal(Urn, EnvelopeCodec.Decode(body).Job);

        // The contract projection is untouched.
        Assert.Equal("trace-1", traced.Properties.CorrelationId);
        Assert.Equal("babelqueue", traced.ApplicationProperties.Map[AmqpProperties.AppId]);
        Assert.Equal(Urn, traced.MessageAnnotations.Map[new Symbol("x-opt-jms-type")]);
    }

    [Fact]
    public async Task PublishWithoutHeadersAddsNoTraceProperties()
    {
        var (sender, sent) = SenderOn("orders");
        var publisher = new ArtemisPublisher(sender.Object);

        await publisher.PublishAsync(Urn);
        var first = sent()!;
        await publisher.PublishWithHeadersAsync(Urn, null, new Dictionary<string, string>());
        var second = sent()!;
        await publisher.PublishWithHeadersAsync(Urn, null, null);
        var third = sent()!;

        foreach (var message in new[] { first, second, third })
        {
            Assert.False(message.ApplicationProperties.Map.ContainsKey("traceparent"));
            Assert.False(message.ApplicationProperties.Map.ContainsKey("tracestate"));
            Assert.Equal(first.ApplicationProperties.Map.Count, message.ApplicationProperties.Map.Count);
        }
    }

    [Fact]
    public async Task PublishWithHeadersNeverClobbersAContractProperty()
    {
        var (sender, sent) = SenderOn("orders");

        await new ArtemisPublisher(sender.Object).PublishWithHeadersAsync(
            Urn,
            null,
            new Dictionary<string, string> { [AmqpProperties.AppId] = "evil", [AmqpProperties.Attempts] = "9", [AmqpProperties.Delay] = "1", [""] = "x" },
            delay: TimeSpan.FromSeconds(15));

        var properties = sent()!.ApplicationProperties.Map;
        Assert.Equal("babelqueue", properties[AmqpProperties.AppId]);
        Assert.Equal("0", properties[AmqpProperties.Attempts]);
        Assert.Equal("15000", properties[AmqpProperties.Delay]);
    }

    [Fact]
    public async Task ConsumerSpanIsATrueChildOfTheProducerSpanViaTheReceivedProperties()
    {
        var (sender, sent) = SenderOn("orders");
        await new ArtemisPublisher(sender.Object).PublishWithHeadersAsync(Urn, null, TraceHeaders(), "trace-1");
        var received = sent()!;
        var envelope = EnvelopeCodec.Decode(AmqpProperties.Text(received));

        var span = await ConsumeUnderListener(
            envelope, ArtemisTraceHeaders.Extract(received), ActivityTraceId.CreateFromString(Traceparent.AsSpan(3, 32)));

        Assert.NotNull(span);
        Assert.Equal("b7ad6b7169203331", span!.ParentSpanId.ToHexString());
    }

    [Fact]
    public void ExtractSkipsNonStringAndEmptyValues()
    {
        var message = new Message("{}")
        {
            ApplicationProperties = new Amqp.Framing.ApplicationProperties(),
        };
        message.ApplicationProperties.Map["bq_attempts"] = 3;
        message.ApplicationProperties.Map["empty"] = string.Empty;
        message.ApplicationProperties.Map["traceparent"] = Traceparent;

        var headers = ArtemisTraceHeaders.Extract(message);

        Assert.Equal(Traceparent, Assert.Single(headers).Value);
        Assert.Empty(ArtemisTraceHeaders.Extract(new Message("{}")));
    }

    [Fact]
    public async Task PublishWithHeadersDropsEveryContractPropertyIncludingBqDelay()
    {
        // Parity with the Go reference: contract properties are owned by the transport and never accepted from
        // the carrier, even bq_delay on a publish that has no delay.
        var (sender, sent) = SenderOn("orders");
        var carrier = new Dictionary<string, string> { ["traceparent"] = Traceparent };
        foreach (var key in new[]
        {
            AmqpProperties.SchemaVersion, AmqpProperties.SourceLang, AmqpProperties.Attempts, AmqpProperties.AppId, AmqpProperties.Delay,
        })
        {
            carrier[key] = "evil";
        }

        await new ArtemisPublisher(sender.Object).PublishWithHeadersAsync(Urn, null, carrier, "trace-1");

        var properties = sent()!.ApplicationProperties.Map;
        Assert.False(properties.ContainsKey(AmqpProperties.Delay));
        Assert.Equal("babelqueue", properties[AmqpProperties.AppId]);
        Assert.Equal("0", properties[AmqpProperties.Attempts]);
        Assert.NotEqual("evil", properties[AmqpProperties.SchemaVersion]);
        Assert.False(properties.ContainsKey(AmqpProperties.SourceLang) && "evil".Equals(properties[AmqpProperties.SourceLang]));
        Assert.Equal(Traceparent, properties["traceparent"]);
    }

    [Fact]
    public void ExtractLeavesOutTheContractProperties()
    {
        var message = new Message("{}") { ApplicationProperties = new Amqp.Framing.ApplicationProperties() };
        message.ApplicationProperties.Map[AmqpProperties.Attempts] = "0";
        message.ApplicationProperties.Map[AmqpProperties.Delay] = "30000";
        message.ApplicationProperties.Map[AmqpProperties.AppId] = "babelqueue";
        message.ApplicationProperties.Map["traceparent"] = Traceparent;

        var map = ArtemisTraceHeaders.Extract(message);

        Assert.Equal(Traceparent, Assert.Single(map).Value);
    }
}
