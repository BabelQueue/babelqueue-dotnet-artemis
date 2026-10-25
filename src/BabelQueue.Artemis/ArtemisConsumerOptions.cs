using Amqp;

namespace BabelQueue.Artemis;

/// <summary>Tuning and hooks for <see cref="ArtemisConsumer"/>.</summary>
public sealed class ArtemisConsumerOptions
{
    /// <summary>
    /// A sender to <c>&lt;queue&gt;.dlq</c>; enables cross-language dead-lettering. Without it,
    /// a terminal failure degrades to accept-and-drop (the broker's own dead-letter address, if
    /// configured, still applies). A poison body is written to it as the ADR-0035 record and a
    /// schema-version-gated message (ADR-0037) as the raw incoming body, in both cases on the first delivery.
    /// </summary>
    public IAmqpSender? DeadLetterSender { get; set; }

    /// <summary>Attempts before terminal dead-lettering (default 3).</summary>
    public int MaxTries { get; set; } = 3;

    /// <summary>
    /// The consumer's queue name, written as <c>dead_letter.original_queue</c> of a poison record. Default: the
    /// <see cref="DeadLetterSender"/> address without its <c>.dlq</c> suffix.
    /// </summary>
    public string? Queue { get; set; }

    /// <summary>
    /// Strategy for a URN with no handler: <see cref="UnknownUrnStrategy"/> values (default <c>fail</c>). The explicitly
    /// configured value also decides a poison body and a schema-version-gated message (ADR-0037): <c>delete</c> drops them;
    /// every other value (<c>release</c> behaves as <c>fail</c>) dead-letters them when a sender is set.
    /// </summary>
    public string UnknownUrn { get; set; } = UnknownUrnStrategy.Fail;

    /// <summary>Called for a non-conformant message, an unmapped URN, or a throwing handler. The loop never stops.</summary>
    public Action<Exception, Envelope?, Message>? OnError { get; set; }

    /// <summary>Per-poll receive timeout (default 1s).</summary>
    public TimeSpan ReceiveTimeout { get; set; } = TimeSpan.FromSeconds(1);
}
