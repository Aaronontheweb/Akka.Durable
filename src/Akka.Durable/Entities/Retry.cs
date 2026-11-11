namespace Akka.Durable;

/// <summary>
/// How often the library runs an effect's handler again until one attempt is acknowledged. Build one with
/// <see cref="Retry"/> and give it to <c>AddEffectHandler</c>.
/// </summary>
/// <remarks>
/// <para>Registering a handler with a policy says that the handler is safe to repeat: an attempt can do its work and
/// lose its answer, so the handler can run more than once for one effect.</para>
/// <para>The policy is journaled with each effect when it is issued, so a pending effect keeps the promise made then,
/// also after a deployment that changes the registration.</para>
/// </remarks>
public sealed record RetryPolicy
{
    internal RetryPolicy(TimeSpan first, TimeSpan max, int? attempts)
    {
        First = first;
        Max = max;
        Attempts = attempts;
    }

    /// <summary>The wait before attempt 2.</summary>
    public TimeSpan First { get; }

    /// <summary>The longest wait between two attempts.</summary>
    public TimeSpan Max { get; }

    /// <summary>How many attempts in all, the first included. Null: until one is acknowledged.</summary>
    public int? Attempts { get; }

    /// <summary>True when attempt <paramref name="next"/> may run.</summary>
    internal bool Allows(int next) => Attempts is not { } limit || next <= limit;

    /// <summary>The wait after attempt <paramref name="attempt"/> failed: <c>min(First × 2^(attempt−1), Max)</c>.</summary>
    internal TimeSpan WaitAfter(int attempt)
    {
        var wait = First;
        for (var n = 1; n < attempt && wait < Max; n++)
            wait = wait > Max / 2 ? Max : wait * 2;
        return wait < Max ? wait : Max;
    }
}

/// <summary>Builds the retry policies for <c>AddEffectHandler</c>.</summary>
/// <remarks>
/// There are no defaults without numbers, no jitter and no filter on exceptions. A value that is not valid throws
/// <see cref="ArgumentOutOfRangeException"/> where the handler is registered, which is at startup.
/// </remarks>
public static class Retry
{
    /// <summary>Waits <paramref name="every"/> before each new attempt, for <paramref name="attempts"/> attempts in all.</summary>
    /// <param name="every">The wait between two attempts. Positive.</param>
    /// <param name="attempts">How many attempts in all, the first included. At least 2.</param>
    public static RetryPolicy Every(TimeSpan every, int attempts)
    {
        Positive(every, nameof(every));
        AtLeastTwo(attempts);
        return new RetryPolicy(every, every, attempts);
    }

    /// <summary>
    /// Waits <c>min(first × 2^(n−1), max)</c> before attempt n+1, for <paramref name="attempts"/> attempts in all.
    /// </summary>
    /// <param name="first">The wait before attempt 2. Positive, and not greater than <paramref name="max"/>.</param>
    /// <param name="max">The longest wait. Positive.</param>
    /// <param name="attempts">How many attempts in all, the first included. At least 2.</param>
    public static RetryPolicy Backoff(TimeSpan first, TimeSpan max, int attempts)
    {
        FirstAndMax(first, max);
        AtLeastTwo(attempts);
        return new RetryPolicy(first, max, attempts);
    }

    /// <summary>
    /// Waits like <see cref="Backoff"/>, and never stops: the effect is attempted until one attempt is acknowledged.
    /// This is the outbox.
    /// </summary>
    /// <param name="first">The wait before attempt 2. Positive, and not greater than <paramref name="max"/>.</param>
    /// <param name="max">The longest wait. Positive.</param>
    public static RetryPolicy UntilAcknowledged(TimeSpan first, TimeSpan max)
    {
        FirstAndMax(first, max);
        return new RetryPolicy(first, max, attempts: null);
    }

    private static void Positive(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(name, value, "A wait between attempts must be positive.");
    }

    private static void FirstAndMax(TimeSpan first, TimeSpan max)
    {
        Positive(first, nameof(first));
        Positive(max, nameof(max));
        if (first > max)
            throw new ArgumentOutOfRangeException(nameof(first), first, $"The first wait must not be greater than the longest wait ({max}).");
    }

    private static void AtLeastTwo(int attempts)
    {
        if (attempts < 2)
            throw new ArgumentOutOfRangeException(nameof(attempts), attempts,
                "Attempts counts every attempt, the first included, so a policy that retries has at least 2.");
    }
}
