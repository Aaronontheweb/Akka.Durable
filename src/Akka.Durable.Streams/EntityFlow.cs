using Akka.Streams.Dsl;

namespace Akka.Durable.Streams;

/// <summary>
/// Flows and sinks that send commands to durable entities.
/// </summary>
public static class EntityFlow
{
    /// <summary>
    /// Sends each element as a command to the entity named by <paramref name="plan"/>, and emits the reply.
    /// At most <paramref name="parallelism"/> commands are in flight. Output keeps the input's order.
    /// </summary>
    public static Flow<TIn, TReply, NotUsed> Ask<TIn, TCommand, TReply>(
        IEntityRefs<TCommand> entities,
        Func<TIn, (EntityId Id, IReplies<TCommand, TReply> Command)> plan,
        int parallelism)
        where TReply : notnull
    {
        ArgumentNullException.ThrowIfNull(entities);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentOutOfRangeException.ThrowIfLessThan(parallelism, 1);

        return Flow.Create<TIn>()
            .SelectAsync(parallelism, async input =>
            {
                var (id, command) = plan(input);
                return await entities.Get(id).Ask(command);
            });
    }

    /// <summary>
    /// Sends each element as a command to the entity named by <paramref name="plan"/>, and emits nothing.
    /// </summary>
    /// <remarks>
    /// A command sent this way is not awaited, so a failure to deliver it is not seen by the stream. Use
    /// <see cref="Ask{TIn,TCommand,TReply}"/> when the reply matters.
    /// </remarks>
    public static Sink<TIn, Task<Done>> Tell<TIn, TCommand>(
        IEntityRefs<TCommand> entities,
        Func<TIn, (EntityId Id, TCommand Command)> plan)
    {
        ArgumentNullException.ThrowIfNull(entities);
        ArgumentNullException.ThrowIfNull(plan);

        return Sink.ForEach<TIn>(input =>
        {
            var (id, command) = plan(input);
            entities.Get(id).Tell(command);
        });
    }

    /// <summary>
    /// For each element, waits until the entity's state satisfies <paramref name="until"/>, and emits that
    /// snapshot. Output keeps the input's order.
    /// </summary>
    /// <remarks>
    /// The wait is a subscription, not a poll. An element whose entity does not reach the condition within
    /// <paramref name="timeout"/> fails the stream with an <see cref="OperationCanceledException"/>.
    /// </remarks>
    public static Flow<TIn, StateSnapshot<TState>, NotUsed> WaitForState<TIn, TCommand, TState, TEvent>(
        IDurableEntityRefs<TCommand, TState, TEvent> entities,
        Func<TIn, EntityId> idOf,
        Func<TIn, TState, bool> until,
        int parallelism,
        TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(entities);
        ArgumentNullException.ThrowIfNull(idOf);
        ArgumentNullException.ThrowIfNull(until);
        ArgumentOutOfRangeException.ThrowIfLessThan(parallelism, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);

        return Flow.Create<TIn>()
            .SelectAsync(parallelism, async input =>
            {
                using var limit = new CancellationTokenSource(timeout);
                return await entities.Get(idOf(input)).WaitForStateAsync(
                    state => until(input, state), limit.Token);
            });
    }
}
