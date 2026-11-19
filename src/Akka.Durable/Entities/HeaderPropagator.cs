using System.Collections.Immutable;
using Akka.Actor;
using Akka.Event;
using Akka.Serialization.V2;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Akka.Durable;

/// <summary>
/// Carries a few strings beside every command, without the library knowing what they mean. They are called headers.
/// Use them to say which request a command belongs to: a request id, a trace id.
/// </summary>
/// <remarks>
/// <para>Register it with <see cref="DurableHeaderExtensions.AddDurableHeaderPropagator{TPropagator}"/>. It is a
/// singleton. Call that again to add more: each one is called, in the order they were registered, and their headers
/// are merged. When two add the same key, the one registered later wins.</para>
/// <para>The library carries the headers beside a command, and stores them with each recorded effect (<c>Execute</c>)
/// the command's decision issues. <c>Decide</c>, <c>Evolve</c> and effect handlers never see them. A timer does not
/// carry the headers of the command that set it, and a reply and a state read carry none: a timer is a starting point of
/// its own, and <see cref="InjectForTimer"/> lets you mark it. See the guide page "Headers beside a command".</para>
/// <para>Headers are stored in the journal. Keep them small and stable: ids. Do not put secrets, tokens, large values
/// or personal data in them.</para>
/// <para>A propagator that throws is logged at Warning level and skipped for that call. It never fails a send or an
/// effect. One instance serves many threads at the same time, so it must be safe to call from many threads.</para>
/// </remarks>
public interface IHeaderPropagator
{
    /// <summary>
    /// A command is about to be sent to an entity or a workflow. Add the headers that should travel beside it. Add
    /// nothing to send none.
    /// </summary>
    /// <remarks>Called on the caller's thread, for every send. Keep it quick and do not block.</remarks>
    /// <param name="headers">The headers to send. Entries with a null value are ignored.</param>
    void Inject(IDictionary<string, string> headers);

    /// <summary>
    /// The library is about to run your code on behalf of a command that carried headers, which today is an effect
    /// handler. Put the headers where your code reads them: an <see cref="AsyncLocal{T}"/> is typical. The headers
    /// then reach everything the handler calls, and a command it sends picks them up again through
    /// <see cref="Inject"/>.
    /// </summary>
    /// <param name="headers">The headers the command carried.</param>
    /// <returns>An object whose <see cref="IDisposable.Dispose"/> undoes what this call did. It is called when the work has finished.</returns>
    IDisposable Restore(IReadOnlyDictionary<string, string> headers);

    /// <summary>
    /// A timer of an entity fired. Add the headers that the decision it starts should carry. Add nothing for none.
    /// </summary>
    /// <remarks>
    /// Called on the entity's thread, just before that decision, and never for the library's own reminders, whose keys
    /// start with <c>durable-</c>. The default does nothing.
    /// </remarks>
    /// <param name="entityType">The type of the entity.</param>
    /// <param name="entityId">The id of the entity.</param>
    /// <param name="key">The key of the timer that fired.</param>
    /// <param name="headers">The headers to add. Entries with a null value are ignored.</param>
    void InjectForTimer(EntityTypeName entityType, EntityId entityId, string key, IDictionary<string, string> headers) { }
}

/// <summary>Registers header propagators.</summary>
public static class DurableHeaderExtensions
{
    /// <summary>Registers <typeparamref name="TPropagator"/> as a header propagator. It is a singleton.</summary>
    /// <remarks>
    /// Registering a propagator is what makes this node send headers. Before you do, every node of the cluster must
    /// run a version of Akka.Durable that understands them. See the guide page "Headers beside a command".
    /// </remarks>
    /// <param name="services">The service collection.</param>
    public static IServiceCollection AddDurableHeaderPropagator<TPropagator>(this IServiceCollection services)
        where TPropagator : class, IHeaderPropagator
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddSingleton<IHeaderPropagator, TPropagator>();
        HeaderPropagation.Register(services);
        return services;
    }
}

/// <summary>
/// A command and the headers that travel beside it. Sent inside the <c>ShardingEnvelope</c> in place of the bare
/// command, only when there are headers. The entity unwraps it and decides on the command.
/// </summary>
/// <remarks>Everything here is sent between nodes. Change it extend-only: add fields with new indexes.</remarks>
/// <param name="Command">The command, as it would have been sent without headers.</param>
/// <param name="Headers">The headers. Never empty.</param>
[AkkaSerializable(Manifest = "durable-carried-v1")]
internal sealed record Carried(
    [property: AkkaField(0)] object Command,
    [property: AkkaField(1)] ImmutableDictionary<string, string> Headers) : IDurableEntityProtocol;

/// <summary>
/// The header propagators of one service provider, called in registration order. When none is registered, every call
/// costs a check of an array length and nothing else.
/// </summary>
internal sealed class HeaderPropagation
{
    /// <summary>No propagator: nothing is injected or restored.</summary>
    public static readonly HeaderPropagation None = new([], null);

    private readonly IHeaderPropagator[] _propagators;
    private readonly IServiceProvider? _services;

    // Found when the first propagator fails: a ref has no actor, so it logs through the actor system.
    private ILoggingAdapter? _systemLog;

    private HeaderPropagation(IHeaderPropagator[] propagators, IServiceProvider? services)
    {
        _propagators = propagators;
        _services = services;
    }

    /// <summary>
    /// Registers the one instance that refs and entity actors share. Called by <c>AddDurableEntities</c> and by
    /// <c>AddDurableHeaderPropagator</c>, so it exists whichever is called.
    /// </summary>
    public static void Register(IServiceCollection services) =>
        services.TryAddSingleton(provider => From(provider));

    private static HeaderPropagation From(IServiceProvider services)
    {
        var found = services.GetServices<IHeaderPropagator>().ToArray();
        return found.Length == 0 ? None : new HeaderPropagation(found, services);
    }

    /// <summary>
    /// The message to send for <paramref name="command"/>: the command itself when no propagator adds a header,
    /// otherwise a <see cref="Carried"/>. Called by a ref, on the caller's thread.
    /// </summary>
    public object Wrap(object command)
    {
        if (_propagators.Length == 0) return command;

        var headers = Collect(static (propagator, own) => propagator.Inject(own), nameof(IHeaderPropagator.Inject),
            "The command is sent without its headers.", log: null);
        return headers is null ? command : new Carried(command, headers);
    }

    /// <summary>
    /// Asks every propagator for the headers of the decision that a fired timer starts. Null when there are none.
    /// Called on the entity's thread, so it logs with the entity's logger.
    /// </summary>
    public ImmutableDictionary<string, string>? InjectForTimer(
        EntityTypeName entityType, EntityId entityId, string key, ILoggingAdapter log) =>
        _propagators.Length == 0
            ? null
            : Collect((propagator, own) => propagator.InjectForTimer(entityType, entityId, key, own),
                nameof(IHeaderPropagator.InjectForTimer), "The timer's decision has none of its headers.", log);

    /// <summary>
    /// Calls <see cref="IHeaderPropagator.Restore"/> of each propagator. Dispose the result when the work has finished.
    /// Null when there is nothing to undo.
    /// </summary>
    public IDisposable? Restore(ImmutableDictionary<string, string>? headers, ILoggingAdapter log)
    {
        if (headers is null || _propagators.Length == 0) return null;

        List<IDisposable>? undo = null;
        foreach (var propagator in _propagators)
        {
            try
            {
                if (propagator.Restore(headers) is { } restored) (undo ??= []).Add(restored);
            }
            catch (Exception error)
            {
                log.Warning(error, "Header propagator {0} threw in Restore. The effect runs without its headers.",
                    propagator.GetType().Name);
            }
        }

        return undo is null ? null : new Undo(undo, log);
    }

    /// <summary>
    /// Calls each propagator with its own dictionary, so one that throws half way adds nothing, and merges what they
    /// add in order: a later one wins on the same key. Null values and empty keys are dropped.
    /// </summary>
    /// <param name="call">The call to make on a propagator.</param>
    /// <param name="what">The name of the member, for the log.</param>
    /// <param name="consequence">What a failure costs, for the log.</param>
    /// <param name="log">The entity's logger, or null for the actor system's.</param>
    private ImmutableDictionary<string, string>? Collect(
        Action<IHeaderPropagator, IDictionary<string, string>> call, string what, string consequence, ILoggingAdapter? log)
    {
        Dictionary<string, string>? merged = null;
        foreach (var propagator in _propagators)
        {
            var own = new Dictionary<string, string>();
            try
            {
                call(propagator, own);
            }
            catch (Exception error)
            {
                (log ?? SystemLog).Warning(error, "Header propagator {0} threw in {1}. {2}",
                    propagator.GetType().Name, what, consequence);
                continue;
            }

            foreach (var (name, value) in own)
            {
                if (string.IsNullOrEmpty(name) || value is null) continue;
                (merged ??= [])[name] = value;
            }
        }

        return merged?.ToImmutableDictionary();
    }

    private ILoggingAdapter SystemLog => _systemLog ??= _services!.GetRequiredService<ActorSystem>().Log;

    /// <summary>Undoes the restores, last first. A failure is logged and the rest are still undone.</summary>
    private sealed class Undo(List<IDisposable> restored, ILoggingAdapter log) : IDisposable
    {
        public void Dispose()
        {
            for (var index = restored.Count - 1; index >= 0; index--)
            {
                try
                {
                    restored[index].Dispose();
                }
                catch (Exception error)
                {
                    log.Warning(error, "Disposing what header propagator {0} restored threw.", restored[index].GetType().Name);
                }
            }
        }
    }
}
