using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using BabelQueue;
using BabelQueue.Artemis;
using Moq;
using Xunit;

namespace BabelQueue.Artemis.Tests;

/// <summary>
/// Apache ActiveMQ Artemis binding conformance against the vendored canonical suite's
/// <c>artemis</c> block: the §7 AMQP projection (the <c>x-opt-jms-type</c> annotation /
/// <c>correlation-id</c> + the <c>bq-</c> application properties) and the
/// <c>attempts = max(body, delivery-count)</c> reconciliation (the AMQP counter is 0-based, no
/// −1). No Artemis, no network.
/// </summary>
public sealed class ArtemisConformanceTests
{
    private static readonly string Dir = Path.Combine(AppContext.BaseDirectory, "conformance");

    private static JsonElement Artemis()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(Dir, "manifest.json")));
        return doc.RootElement.GetProperty("artemis").Clone();
    }

    [Fact]
    public void PropertyProjectionMatchesGolden()
    {
        var projection = Artemis().GetProperty("property_projection");
        var body = File.ReadAllText(Path.Combine(Dir, projection.GetProperty("envelope_file").GetString()!));
        var message = AmqpProperties.ToMessage(EnvelopeCodec.Decode(body));

        Assert.Equal(
            projection.GetProperty("jms_type").GetString(),
            message.MessageAnnotations.Map[new Symbol("x-opt-jms-type")] as string);
        Assert.Equal(
            projection.GetProperty("correlation_id").GetString(),
            message.Properties.CorrelationId as string);

        foreach (var golden in projection.GetProperty("properties").EnumerateObject())
        {
            Assert.Equal(golden.Value.GetString(), message.ApplicationProperties.Map[golden.Name] as string);
        }
    }

    [Fact]
    public async Task AttemptsReconciliationMatchesGolden()
    {
        foreach (var testCase in Artemis().GetProperty("attempts_reconciliation").GetProperty("cases").EnumerateArray())
        {
            var bodyAttempts = testCase.GetProperty("body_attempts").GetInt32();
            var deliveryCount = (uint)testCase.GetProperty("delivery_count").GetInt32();
            var expected = testCase.GetProperty("expected_attempts").GetInt32();
            var env = EnvelopeCodec.Make("urn:babel:orders:created", new Dictionary<string, object?> { ["x"] = 1 }, "orders")
                with { Attempts = bodyAttempts };

            var message = new Message(EnvelopeCodec.Encode(env))
            {
                MessageAnnotations = new MessageAnnotations(),
            };
            message.MessageAnnotations.Map[new Symbol("x-opt-jms-type")] = "urn:babel:orders:created";
            if (deliveryCount > 0)
            {
                message.Header = new Header { DeliveryCount = deliveryCount };
            }

            var receiver = new Mock<IAmqpReceiver>();
            receiver.Setup(r => r.ReceiveAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>())).ReturnsAsync(message);
            receiver.Setup(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            receiver.Setup(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

            var seen = -1;
            var handlers = new Dictionary<string, BabelHandler>
            {
                ["urn:babel:orders:created"] = (e, _, _) => { seen = e.Attempts; return Task.CompletedTask; },
            };
            await new ArtemisConsumer(receiver.Object, handlers, new ArtemisConsumerOptions { MaxTries = 99 }).PollAsync();

            Assert.Equal(expected, seen);
        }
    }

    [Fact]
    public async Task SchemaVersionGateMatchesGolden()
    {
        var gate = Artemis().GetProperty("schema_version_gate");
        var property = gate.GetProperty("property").GetString()!;
        var body = File.ReadAllText(Path.Combine(Dir, gate.GetProperty("fixture").GetString()!));
        var job = EnvelopeCodec.Decode(body).Job!;

        var cases = gate.GetProperty("cases").EnumerateArray().ToList();
        Assert.NotEmpty(cases);
        foreach (var testCase in cases)
        {
            var absent = testCase.TryGetProperty("absent", out var a) && a.GetBoolean();
            var value = absent ? null : testCase.GetProperty("value").GetString();
            var expect = testCase.GetProperty("expect").GetString();

            var message = new Message(body);
            if (!absent)
            {
                message.ApplicationProperties = new ApplicationProperties();
                message.ApplicationProperties.Map[property] = value;
            }

            var receiver = new Mock<IAmqpReceiver>();
            receiver.Setup(r => r.ReceiveAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>())).ReturnsAsync(message);
            receiver.Setup(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            receiver.Setup(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

            var dlq = new Mock<IAmqpSender>();
            var dlqSends = 0;
            dlq.Setup(s => s.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
                .Callback<Message, CancellationToken>((_, _) => dlqSends++)
                .Returns(Task.CompletedTask);

            var seen = 0;
            var handlers = new Dictionary<string, BabelHandler>
            {
                [job] = (_, _, _) => { seen++; return Task.CompletedTask; },
            };
            await new ArtemisConsumer(receiver.Object, handlers, new ArtemisConsumerOptions { DeadLetterSender = dlq.Object }).PollAsync();

            var label = absent ? "<absent>" : $"'{value}'";
            Assert.True(
                (expect == "decode" ? 1 : 0) == seen,
                $"{label}: expected {expect} but handler ran {seen} time(s)");
            Assert.Equal(expect == "decode" ? 0 : 1, dlqSends);
            receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
        }
    }
}
