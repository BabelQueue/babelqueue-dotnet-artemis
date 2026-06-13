using System.Diagnostics.CodeAnalysis;
using Amqp;

namespace BabelQueue.Artemis;

/// <summary>
/// A send seam over one Artemis address — the publisher and the consumer's dead-letter path
/// depend on this, not on the concrete AMQPNetLite <see cref="SenderLink"/>, so they unit-test
/// against a mock with no broker. <see cref="Address"/> is the queue the link targets (used to
/// stamp <c>meta.queue</c>). Wrap a real link with <see cref="AmqpSenderLink"/>.
/// </summary>
public interface IAmqpSender
{
    /// <summary>The address (queue name) this sender targets.</summary>
    string Address { get; }

    /// <summary>Send one AMQP message.</summary>
    Task SendAsync(Message message, CancellationToken cancellationToken = default);
}

/// <summary>
/// A receive seam over one Artemis address — the consumer depends on this, not on the concrete
/// AMQPNetLite <see cref="ReceiverLink"/>. <see cref="AcceptAsync"/> settles a message
/// (acknowledge); <see cref="ReleaseAsync"/> returns it for redelivery (the broker increments
/// the AMQP <c>delivery-count</c>). Wrap a real link with <see cref="AmqpReceiverLink"/>.
/// </summary>
public interface IAmqpReceiver
{
    /// <summary>Receive one message within <paramref name="timeout"/>, or <c>null</c> on timeout.</summary>
    Task<Message?> ReceiveAsync(TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>Settle (acknowledge) the message — it is removed from the queue.</summary>
    Task AcceptAsync(Message message, CancellationToken cancellationToken = default);

    /// <summary>Release the message for redelivery — the broker increments <c>delivery-count</c>.</summary>
    Task ReleaseAsync(Message message, CancellationToken cancellationToken = default);
}

/// <summary>Adapts an AMQPNetLite <see cref="SenderLink"/> to <see cref="IAmqpSender"/>.</summary>
[ExcludeFromCodeCoverage] // Thin pass-through over a live AMQP link; exercised by integration, not unit tests.
public sealed class AmqpSenderLink : IAmqpSender
{
    private readonly SenderLink _link;

    /// <param name="link">A live sender link to the target address.</param>
    /// <param name="address">The address the link targets (stamped onto <c>meta.queue</c>).</param>
    public AmqpSenderLink(SenderLink link, string address)
    {
        ArgumentNullException.ThrowIfNull(link);
        _link = link;
        Address = address ?? string.Empty;
    }

    public string Address { get; }

    public Task SendAsync(Message message, CancellationToken cancellationToken = default) =>
        _link.SendAsync(message);
}

/// <summary>Adapts an AMQPNetLite <see cref="ReceiverLink"/> to <see cref="IAmqpReceiver"/>.</summary>
[ExcludeFromCodeCoverage] // Thin pass-through over a live AMQP link; exercised by integration, not unit tests.
public sealed class AmqpReceiverLink : IAmqpReceiver
{
    private readonly ReceiverLink _link;

    /// <param name="link">A live receiver link to the source address.</param>
    public AmqpReceiverLink(ReceiverLink link)
    {
        ArgumentNullException.ThrowIfNull(link);
        _link = link;
    }

    public async Task<Message?> ReceiveAsync(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        await _link.ReceiveAsync(timeout).ConfigureAwait(false);

    // Settlement is a local disposition (non-blocking); wrap as a completed task for the seam.
    public Task AcceptAsync(Message message, CancellationToken cancellationToken = default)
    {
        _link.Accept(message);
        return Task.CompletedTask;
    }

    public Task ReleaseAsync(Message message, CancellationToken cancellationToken = default)
    {
        _link.Release(message);
        return Task.CompletedTask;
    }
}
