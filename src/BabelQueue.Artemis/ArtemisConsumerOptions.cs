using Amqp;

namespace BabelQueue.Artemis;

/// <summary>Tuning and hooks for <see cref="ArtemisConsumer"/>.</summary>
public sealed class ArtemisConsumerOptions
{
    /// <summary>
    /// A sender to <c>&lt;queue&gt;.dlq</c>; enables cross-language dead-lettering. Without it,
    /// a terminal failure degrades to accept-and-drop (the broker's own dead-letter address, if
    /// configured, still applies).
    /// </summary>
    public IAmqpSender? DeadLetterSender { get; set; }

    /// <summary>Attempts before terminal dead-lettering (default 3).</summary>
    public int MaxTries { get; set; } = 3;

    /// <summary>Strategy for a URN with no handler: <see cref="UnknownUrnStrategy"/> values (default <c>fail</c>).</summary>
    public string UnknownUrn { get; set; } = UnknownUrnStrategy.Fail;

    /// <summary>Called for a non-conformant message, an unmapped URN, or a throwing handler. The loop never stops.</summary>
    public Action<Exception, Envelope?, Message>? OnError { get; set; }

    /// <summary>Per-poll receive timeout (default 1s).</summary>
    public TimeSpan ReceiveTimeout { get; set; } = TimeSpan.FromSeconds(1);
}
