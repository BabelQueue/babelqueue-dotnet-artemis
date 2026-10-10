using Amqp;
using Amqp.Framing;
using BabelQueue.Artemis;
using Xunit;

namespace BabelQueue.Artemis.Tests;

/// <summary>
/// A <see cref="FactAttribute"/> that is skipped unless <c>BQ_INTEGRATION=1</c>, so the live-broker tests stay out of
/// the normal suite (xUnit 2 has no dynamic skip).
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
internal sealed class ArtemisIntegrationFactAttribute : FactAttribute
{
    public ArtemisIntegrationFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("BQ_INTEGRATION") != "1")
        {
            Skip = "Live-broker test: set BQ_INTEGRATION=1 (and BQ_ARTEMIS_URL) to run.";
        }
    }
}

/// <summary>
/// Live-broker integration tests for the Artemis transport (§7, AMQP 1.0) — opt-in. Enable with
/// <c>BQ_INTEGRATION=1</c> and (optionally) <c>BQ_ARTEMIS_URL</c> (default
/// <c>amqp://artemis:artemis@localhost:5672</c>). The CI <c>integration</c> job runs an
/// <c>apache/activemq-artemis</c> service container.
///
/// <para>Artemis auto-creates an address as MULTICAST for AMQP, so a message published before a receiver is
/// attached can be dropped. Every test therefore attaches its receivers (the work queue <i>and</i> the DLQ)
/// <b>before</b> publishing anything.</para>
/// </summary>
public sealed class ArtemisIntegrationTests
{
    private const string Urn = "urn:babel:orders:created";
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(60);

    private static string Url
    {
        get
        {
            var value = Environment.GetEnvironmentVariable("BQ_ARTEMIS_URL");
            return string.IsNullOrWhiteSpace(value) ? "amqp://artemis:artemis@localhost:5672" : value;
        }
    }

    [ArtemisIntegrationFact]
    public async Task HandlerErrorRaisesDeliveryCountThenDeadLettersAfterMaxTries()
    {
        var queue = "bq-it-orders-" + Guid.NewGuid().ToString("N")[..8];
        var dlqName = queue + ".dlq";
        var deliveryCounts = new List<uint>();

        var connection = await Connection.Factory.CreateAsync(new Address(Url));
        try
        {
            var session = new Session(connection);
            // Receivers first (multicast-drop rule).
            var work = new ReceiverLink(session, "bq-it-work", queue);
            var dlqReceiver = new ReceiverLink(session, "bq-it-dlq", dlqName);
            var workSender = new SenderLink(session, "bq-it-work-tx", queue);
            var dlqSender = new SenderLink(session, "bq-it-dlq-tx", dlqName);

            var handlers = new Dictionary<string, BabelHandler>
            {
                [Urn] = (_, message, _) =>
                {
                    deliveryCounts.Add(message.Header?.DeliveryCount ?? 0);
                    throw new InvalidOperationException("boom");
                },
            };
            var consumer = new ArtemisConsumer(new AmqpReceiverLink(work), handlers, new ArtemisConsumerOptions
            {
                DeadLetterSender = new AmqpSenderLink(dlqSender, dlqName),
                MaxTries = 3,
                ReceiveTimeout = TimeSpan.FromSeconds(1),
            });

            await new ArtemisPublisher(new AmqpSenderLink(workSender, queue))
                .PublishAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, "trace-it-1");

            var stop = DateTime.UtcNow + Deadline;
            while (deliveryCounts.Count < 3 && DateTime.UtcNow < stop)
            {
                await consumer.PollAsync();
            }

            Assert.Equal(3, deliveryCounts.Count);
            // Scenario 3: a handler error (Modified{delivery-failed}) makes the broker bump delivery-count.
            Assert.True(deliveryCounts[0] < deliveryCounts[1] && deliveryCounts[1] < deliveryCounts[2],
                "delivery-count must strictly increase across handler failures: " + string.Join(",", deliveryCounts));

            var dead = await dlqReceiver.ReceiveAsync(Deadline);
            Assert.NotNull(dead);
            dlqReceiver.Accept(dead);
            var envelope = EnvelopeCodec.Decode(AmqpProperties.Text(dead));
            Assert.NotNull(envelope.DeadLetter);
            Assert.Equal("failed", envelope.DeadLetter!.Reason);
            Assert.Equal(queue, envelope.DeadLetter.OriginalQueue);
            Assert.Equal("trace-it-1", envelope.TraceId);
        }
        finally
        {
            await connection.CloseAsync();
        }
    }

    [ArtemisIntegrationFact]
    public async Task SchemaVersionGateRejectionForwardsRawBodyToDlq()
    {
        var queue = "bq-it-gate-" + Guid.NewGuid().ToString("N")[..8];
        var dlqName = queue + ".dlq";
        // Deliberately not a valid v1 envelope: the gate must reject on the property alone (§7.7).
        const string Body = "{\"job\":\"urn:babel:orders:created\",\"data\":{},\"future\":true}";
        var handled = 0;
        var rejected = 0;

        var connection = await Connection.Factory.CreateAsync(new Address(Url));
        try
        {
            var session = new Session(connection);
            var work = new ReceiverLink(session, "bq-it-work", queue);
            var dlqReceiver = new ReceiverLink(session, "bq-it-dlq", dlqName);
            var workSender = new SenderLink(session, "bq-it-work-tx", queue);
            var dlqSender = new SenderLink(session, "bq-it-dlq-tx", dlqName);

            var handlers = new Dictionary<string, BabelHandler>
            {
                [Urn] = (_, _, _) =>
                {
                    handled++;
                    return Task.CompletedTask;
                },
            };
            var consumer = new ArtemisConsumer(new AmqpReceiverLink(work), handlers, new ArtemisConsumerOptions
            {
                DeadLetterSender = new AmqpSenderLink(dlqSender, dlqName),
                ReceiveTimeout = TimeSpan.FromSeconds(1),
                OnError = (_, _, _) => rejected++,
            });

            var message = new Message(Body)
            {
                ApplicationProperties = new ApplicationProperties(),
            };
            message.ApplicationProperties[AmqpProperties.SchemaVersion] = "2";
            await workSender.SendAsync(message);

            var stop = DateTime.UtcNow + Deadline;
            while (rejected == 0 && DateTime.UtcNow < stop)
            {
                await consumer.PollAsync();
            }

            Assert.True(rejected >= 1, "the gate must report the rejection");
            var dead = await dlqReceiver.ReceiveAsync(Deadline);
            Assert.NotNull(dead);
            dlqReceiver.Accept(dead);
            Assert.Equal(Body, AmqpProperties.Text(dead));
            Assert.Equal(0, handled);
        }
        finally
        {
            await connection.CloseAsync();
        }
    }
}
