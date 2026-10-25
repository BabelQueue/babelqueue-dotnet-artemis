using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
            receiver.Setup(r => r.ModifyAsync(It.IsAny<Message>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

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
            receiver.Setup(r => r.ModifyAsync(It.IsAny<Message>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

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

    // -- poison_dlq (ADR-0035) --------------------------------------------------------------------------------------

    private static JsonElement Section(string name)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(Dir, "manifest.json")));
        return doc.RootElement.GetProperty(name).Clone();
    }

    private static readonly JsonDocumentOptions DeepJson = new() { MaxDepth = 10_100 };

    [Fact]
    public void PoisonDlqRecordShapeMatchesGolden()
    {
        var section = Section("poison_dlq");
        var originalQueue = section.GetProperty("original_queue").GetString()!;
        var cases = section.GetProperty("cases").EnumerateArray().ToList();
        Assert.Equal(43, cases.Count);
        foreach (var testCase in cases)
        {
            var name = testCase.GetProperty("name").GetString();
            var bytes = testCase.TryGetProperty("body_base64", out var b64)
                ? Convert.FromBase64String(b64.GetString()!)
                : Encoding.UTF8.GetBytes(testCase.GetProperty("body").GetString()!);
            var expect = testCase.GetProperty("expect");
            var shape = expect.GetProperty("shape").GetString()!;

            var record = ArtemisConsumer.PoisonBody(bytes, "poison", "System.Exception", 1700000000000L, originalQueue, 0);
            var text = Encoding.UTF8.GetString(record);
            Assert.False(text.Contains('\uFFFD', StringComparison.Ordinal), $"{name}: the record holds U+FFFD");

            using var doc = JsonDocument.Parse(record, DeepJson);
            Assert.Equal(JsonValueKind.Object, doc.RootElement.ValueKind);
            var deadLetter = doc.RootElement.EnumerateObject().Where(p => p.Name == "dead_letter").ToList();
            Assert.True(deadLetter.Count == 1, $"{name}: exactly one top-level dead_letter");
            Assert.Equal("poison", deadLetter[0].Value.GetProperty("reason").GetString());
            Assert.Equal(originalQueue, deadLetter[0].Value.GetProperty("original_queue").GetString());
            var rest = doc.RootElement.EnumerateObject().Where(p => p.Name != "dead_letter").ToList();

            var want = expect.GetProperty("record");
            if (shape == "splice_or_wrapper" && !SameMembers(rest, want))
            {
                foreach (var literal in expect.GetProperty("splice_literals").EnumerateArray())
                {
                    Assert.True(text.Contains(literal.GetString()!, StringComparison.Ordinal), $"{name}: missing literal {literal.GetString()}");
                }

                Assert.DoesNotContain(rest, p => p.Name is "raw" or "raw_encoding");
                var keys = rest.Select(p => p.Name).Distinct(StringComparer.Ordinal).OrderBy(n => n, StringComparer.Ordinal).ToList();
                var wantKeys = expect.GetProperty("splice_keys").EnumerateArray().Select(k => k.GetString()!).OrderBy(n => n, StringComparer.Ordinal).ToList();
                Assert.Equal(wantKeys, keys);
                continue;
            }

            Assert.True(SameMembers(rest, want), $"{name}: the record without dead_letter differs: {text}");
        }
    }

    private static bool SameMembers(List<JsonProperty> members, JsonElement want)
    {
        var wantMembers = want.EnumerateObject().ToList();
        return members.Count == wantMembers.Count
            && members.All(m => wantMembers.Count(w => w.Name == m.Name && SameJson(m.Value, w.Value)) == 1);
    }

    private static bool SameJson(JsonElement a, JsonElement b)
    {
        if (a.ValueKind != b.ValueKind)
        {
            return false;
        }

        return a.ValueKind switch
        {
            JsonValueKind.Object => SameMembers(a.EnumerateObject().ToList(), b),
            JsonValueKind.Array => a.GetArrayLength() == b.GetArrayLength()
                && a.EnumerateArray().Zip(b.EnumerateArray(), SameJson).All(x => x),
            JsonValueKind.String => a.GetString() == b.GetString(),
            JsonValueKind.Number => a.GetRawText() == b.GetRawText(),
            _ => true,
        };
    }

    // -- version_gate_artemis (ADR-0037 decision 1) -----------------------------------------------------------------

    [Fact]
    public async Task VersionGateSettleMatchesGolden()
    {
        var section = Section("version_gate_artemis");
        var cases = section.GetProperty("cases").EnumerateArray().ToList();
        Assert.Equal(17, cases.Count);
        var bodies = section.GetProperty("incoming_bodies").EnumerateArray()
            .Select(b => (Name: b.GetProperty("name").GetString()!, Text: b.GetProperty("body").GetString()!))
            .ToList();
        Assert.Equal(3, bodies.Count);

        foreach (var testCase in cases)
        {
            var caseName = testCase.GetProperty("name").GetString();
            var deadLetterOn = testCase.GetProperty("dead_letter").GetString() == "on";
            var writeFails = testCase.TryGetProperty("dlq_write", out var w) && w.GetString() == "fails";
            var expect = testCase.GetProperty("expect");
            // `off` means no DLQ sender in this consumer, so outcome_no_writer (where present) is the expectation there.
            var outcome = !deadLetterOn && expect.TryGetProperty("outcome_no_writer", out var nw)
                ? nw.GetString()!
                : expect.GetProperty("outcome").GetString()!;

            foreach (var (bodyName, bodyText) in bodies)
            {
                foreach (var binary in new[] { false, true })
                {
                    var label = $"{caseName} / {bodyName} / {(binary ? "binary" : "text")}";
                    var expected = Encoding.UTF8.GetBytes(bodyText);
                    var message = binary
                        ? new Message { BodySection = new Data { Binary = expected } }
                        : new Message(bodyText);
                    message.ApplicationProperties = new ApplicationProperties();
                    message.ApplicationProperties.Map["bq_schema_version"] = "2";
                    if (testCase.TryGetProperty("bq_attempts", out var attempts))
                    {
                        message.ApplicationProperties.Map["bq_attempts"] = attempts.GetString();
                    }

                    var receiver = new Mock<IAmqpReceiver>();
                    receiver.Setup(r => r.ReceiveAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>())).ReturnsAsync(message);
                    receiver.Setup(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
                    receiver.Setup(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
                    receiver.Setup(r => r.ModifyAsync(It.IsAny<Message>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

                    var sent = new List<Message>();
                    var dlq = new Mock<IAmqpSender>();
                    dlq.SetupGet(s => s.Address).Returns("orders.dlq");
                    var send = dlq.Setup(s => s.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()));
                    if (writeFails)
                    {
                        send.Throws(new InvalidOperationException("dlq down"));
                    }
                    else
                    {
                        send.Callback<Message, CancellationToken>((m, _) => sent.Add(m)).Returns(Task.CompletedTask);
                    }

                    var handled = 0;
                    var handlers = new Dictionary<string, BabelHandler>
                    {
                        ["urn:babel:orders:created"] = (_, _, _) => { handled++; return Task.CompletedTask; },
                    };
                    var options = new ArtemisConsumerOptions
                    {
                        DeadLetterSender = deadLetterOn ? dlq.Object : null,
                        ReceiveTimeout = TimeSpan.FromMilliseconds(1),
                        MaxTries = 5,
                    };
                    if (testCase.TryGetProperty("on_unknown_urn", out var strategy))
                    {
                        options.UnknownUrn = strategy.GetString()!;
                    }

                    await new ArtemisConsumer(receiver.Object, handlers, options).PollAsync();

                    Assert.True(handled == 0, $"{label}: a version-gated delivery reached a handler");
                    receiver.Verify(r => r.ModifyAsync(It.IsAny<Message>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never());
                    switch (outcome)
                    {
                        case "ack":
                            receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once());
                            receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never());
                            Assert.True(sent.Count == 0, $"{label}: ack must not write a record");
                            break;
                        case "dlq_raw":
                            receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once());
                            receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never());
                            var record = Assert.Single(sent);
                            Assert.True(AmqpProperties.BodyBytes(record).SequenceEqual(expected), $"{label}: the DLQ record is not the incoming body byte for byte");
                            break;
                        case "unsettled":
                            receiver.Verify(r => r.AcceptAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never());
                            receiver.Verify(r => r.ReleaseAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once());
                            break;
                        default:
                            Assert.Fail($"{label}: unknown outcome {outcome}");
                            break;
                    }
                }
            }
        }
    }
}
