using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;
using Microsoft.Extensions.DependencyInjection;

namespace Akka.Durable.Tests.Fixtures;

using CounterContext = DecisionContext<CounterState, CounterCommand, CounterEvent>;

// A counter whose Decide is scripted by the text of a NoHandler command. It lets one spec issue several effects,
// chain them, hold a handler open and see every command that Decide received, in order. The counter's own
// definition does the rest.

public interface IProbeProtocol;

/// <summary>An effect whose handler answers with a command.</summary>
[AkkaSerializable(Manifest = "probe-effect-v1")]
public sealed record Probe([property: AkkaField(0)] string Name) : IProbeProtocol;

/// <summary>An effect whose handler has no answer.</summary>
[AkkaSerializable(Manifest = "probe-note-v1")]
public sealed record Note([property: AkkaField(0)] string Name) : IProbeProtocol;

/// <summary>An effect type that nobody registered a handler for.</summary>
[AkkaSerializable(Manifest = "probe-unrouted-v1", AllowEmpty = true)]
public sealed record Unrouted : IProbeProtocol;

[AkkaSerializer<IProbeProtocol>("probe-test", 22609)]
public sealed partial class ProbeSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

/// <summary>What the handlers saw and how they behave. One for each test host.</summary>
public sealed class ProbeWorld
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _holds = new();

    /// <summary>Every command Decide received, as text, in order.</summary>
    public ConcurrentQueue<string> Decided { get; } = new();

    public ConcurrentQueue<(Probe Effect, EffectDelivery Delivery)> ProbeCalls { get; } = new();

    public ConcurrentQueue<(Note Effect, EffectDelivery Delivery)> NoteCalls { get; } = new();

    /// <summary>What the handler of <see cref="Probe"/> does after it was let go. Default: answers <c>answer:Name</c>.</summary>
    public Func<Probe, CancellationToken, Task<CounterCommand>>? OnProbe { get; set; }

    public Func<Note, CancellationToken, Task>? OnNote { get; set; }

    /// <summary>The decision on a give-up whose text starts with the key. Default: persists it.</summary>
    public Func<string, CounterContext, Decision<CounterCommand, CounterEvent>?>? OnGaveUp { get; set; }

    public void Hold(params string[] names)
    {
        foreach (var name in names) _holds[name] = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public void Release(string name) => _holds[name].TrySetResult();

    internal Task WaitForRelease(string name, CancellationToken ct) =>
        _holds.TryGetValue(name, out var hold) ? hold.Task.WaitAsync(ct) : Task.CompletedTask;

    public string[] DecidedNow => [.. Decided];
}

public sealed class ProbeHandler(ProbeWorld world) : IEffectHandler<Probe, CounterCommand>
{
    public async Task<CounterCommand> HandleAsync(Probe effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        world.ProbeCalls.Enqueue((effect, delivery));
        await world.WaitForRelease(effect.Name, cancellationToken);
        return world.OnProbe is { } script
            ? await script(effect, cancellationToken)
            : new NoHandler($"answer:{effect.Name}");
    }
}

public sealed class NoteHandler(ProbeWorld world) : IEffectHandler<Note>
{
    public async Task HandleAsync(Note effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        world.NoteCalls.Enqueue((effect, delivery));
        await world.WaitForRelease(effect.Name, cancellationToken);
        if (world.OnNote is { } script) await script(effect, cancellationToken);
    }
}

public static class ProbeEntity
{
    public static IServiceCollection AddProbe(this IServiceCollection services, ProbeWorld world) => services
        .AddSingleton(world)
        .AddEffectHandler<Probe, CounterCommand, ProbeHandler>()
        .AddEffectHandler<Note, NoteHandler>();

    public static EntityDefinition<CounterState, CounterCommand, CounterEvent> Definition(
        ProbeWorld world, EntityOptions? options = null) =>
        (Counter.Definition with
        {
            Decide = (state, command, ctx) => Decide(world, state, command, ctx),
            Options = options ?? EntityOptions.Default,
        }).GaveUp(GaveUp);

    /// <summary>
    /// The give-up of a probe effect says who gave up, why, and after how many tries. The last part says that the
    /// notice carries the effect as it was issued.
    /// </summary>
    public static CounterCommand GaveUp(WorkGaveUp gaveUp) => gaveUp.Work switch
    {
        Probe probe => new EffectGaveUpNoted($"{probe.Name}:{gaveUp.Reason}:{gaveUp.Attempts}:{gaveUp.What == nameof(Probe)}"),
        Note note => new EffectGaveUpNoted($"{note.Name}:{gaveUp.Reason}:{gaveUp.Attempts}:{gaveUp.What == nameof(Note)}"),
        Unrouted => new Noop(),
        _ => Counter.GaveUp(gaveUp),
    };

    private static string Describe(CounterCommand command) => command.Value switch
    {
        NoHandler n => n.Note,
        EffectGaveUpNoted g => $"gaveup:{g.What}",
        Increment i => $"increment:{i.By}",
        var other => other?.GetType().Name ?? "empty",
    };

    private static Decision<CounterCommand, CounterEvent> Decide(
        ProbeWorld world, CounterState state, CounterCommand command, CounterContext ctx)
    {
        world.Decided.Enqueue(Describe(command));
        switch (command)
        {
            // "issue:a": one event and one effect. "issue2:a,b": two effects and no event.
            case NoHandler { Note: ['i', 's', 's', 'u', 'e', ':', .. var name] }:
                return ctx.Persist(new Incremented(100)).Execute(new Probe(name));
            case NoHandler { Note: ['i', 's', 's', 'u', 'e', '2', ':', .. var names] }:
                var pair = names.Split(',');
                return ctx.None().Execute(new Probe(pair[0])).Execute(new Probe(pair[1]));
            case NoHandler { Note: ['n', 'o', 't', 'e', ':', .. var name] }:
                return ctx.None().Execute(new Note(name));
            case NoHandler { Note: ['m', 'a', 'n', 'y', ':', .. var count] }:
                var many = ctx.None();
                for (var i = 0; i < int.Parse(count); i++) many = many.Execute(new Probe($"e{i}"));
                return many;
            case NoHandler { Note: ['l', 'a', 't', 'e', 'r', ':', .. var name] }:
                return ctx.None().Schedule("later", TimeSpan.FromMilliseconds(100), new NoHandler($"issue:{name}"));
            case NoHandler { Note: "unrouted" }:
                return ctx.Persist(new Incremented(100)).Execute(new Unrouted());

            // An answer: one event of its own. A locked counter stashes it, like any increment.
            case NoHandler { Note: ['a', 'n', 's', 'w', 'e', 'r', ':', ..] } when state.Locked:
                return ctx.Stash();
            case NoHandler { Note: ['a', 'n', 's', 'w', 'e', 'r', ':', ..] }:
                return ctx.Persist(new Incremented(1));
            case NoHandler { Note: "explode" }:
                throw new InvalidOperationException("Decide cannot decide this.");
            case NoHandler { Note: "ping" }:
                return ctx.Ignore();

            case EffectGaveUpNoted g when world.OnGaveUp?.Invoke(g.What, ctx) is { } scripted:
                return scripted;
        }

        return Counter.Decide(state, command, ctx);
    }
}
