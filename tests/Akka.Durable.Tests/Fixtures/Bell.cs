using System.Collections.Concurrent;
using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;

// Its own namespace: names such as Ring and Heard are too general to share with other fixtures.
namespace Akka.Durable.Tests.Fixtures.Bells;

using BellContext = DecisionContext<BellState, BellCommand, BellEvent>;

// An entity with three effect types and ONE give-up function for all of them: every give-up becomes the same command,
// which carries the effect as it was issued. Decide tells the effects apart. The effects are plain records that
// implement nothing. It is used on the entity actor directly, so its types need no serializer.

public sealed record BellState(ImmutableArray<string> Log);

/// <summary>Issues one effect: <c>ring</c>, <c>flash</c>, or anything else for a <see cref="Whisper"/>.</summary>
public sealed record Sound(string Kind, string Name);

/// <summary>The answer of <see cref="Ring"/> and of <see cref="Flash"/>.</summary>
public sealed record Sounded(string Name);

/// <summary>The one give-up command of the entity: the effect that got no answer, why, and after how many attempts.</summary>
public sealed record EffectFailed(object Work, GaveUpReason Reason, int Attempts);

public readonly union BellCommand(Sound, Sounded, EffectFailed);

public sealed record Heard(string Line);

public readonly union BellEvent(Heard);

/// <summary>Its handler answers with a command, and is attempted once.</summary>
public sealed record Ring(string Name, int Volume);

/// <summary>Its handler answers with a command, and is registered with a limited retry policy.</summary>
public sealed record Flash(string Name);

/// <summary>Its handler has no answer.</summary>
public sealed record Whisper(string Name);

/// <summary>What the definition's give-up function was given and returned, what Decide received, and what the handlers do.</summary>
public sealed class BellWorld
{
    /// <summary>Every notice the definition's <c>GaveUp</c> function was given, in order.</summary>
    public ConcurrentQueue<WorkGaveUp> Notices { get; } = new();

    /// <summary>The command the function returned for each notice, in the same order.</summary>
    public ConcurrentQueue<BellCommand> Mapped { get; } = new();

    /// <summary>Every command Decide received, in order.</summary>
    public ConcurrentQueue<BellCommand> Decided { get; } = new();

    /// <summary>Every handler call: the effect and its delivery.</summary>
    public ConcurrentQueue<(object Effect, EffectDelivery Delivery)> Calls { get; } = new();

    /// <summary>Runs in each handler call after it was recorded. Throw to fail the attempt. Default: returns at once.</summary>
    public Func<object, EffectDelivery, CancellationToken, Task> OnCall { get; set; } = (_, _, _) => Task.CompletedTask;

    /// <summary>True: a handler with an answer answers with the empty union, which is no answer.</summary>
    public bool AnswerWithNothing { get; set; }

    internal BellCommand Saw(WorkGaveUp notice, BellCommand command)
    {
        Notices.Enqueue(notice);
        Mapped.Enqueue(command);
        return command;
    }

    internal async Task<BellCommand> Answer(object effect, string name, EffectDelivery delivery, CancellationToken ct)
    {
        Calls.Enqueue((effect, delivery));
        await OnCall(effect, delivery, ct);
        if (AnswerWithNothing) return default;
        return new Sounded(name);
    }
}

public sealed class RingHandler(BellWorld world) : IEffectHandler<Ring, BellCommand>
{
    public Task<BellCommand> HandleAsync(Ring effect, EffectDelivery delivery, CancellationToken cancellationToken) =>
        world.Answer(effect, effect.Name, delivery, cancellationToken);
}

public sealed class FlashHandler(BellWorld world) : IEffectHandler<Flash, BellCommand>
{
    public Task<BellCommand> HandleAsync(Flash effect, EffectDelivery delivery, CancellationToken cancellationToken) =>
        world.Answer(effect, effect.Name, delivery, cancellationToken);
}

public sealed class WhisperHandler(BellWorld world) : IEffectHandler<Whisper>
{
    public async Task HandleAsync(Whisper effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        world.Calls.Enqueue((effect, delivery));
        await world.OnCall(effect, delivery, cancellationToken);
    }
}

public static class Bell
{
    public static readonly EntityTypeName TypeName = new("bell");

    /// <summary>The policy of <see cref="Flash"/>: three attempts, 100 ms apart.</summary>
    public static readonly RetryPolicy FlashPolicy = Retry.Every(TimeSpan.FromMilliseconds(100), attempts: 3);

    public static IServiceCollection AddBell(this IServiceCollection services, BellWorld world) => services
        .AddSingleton(world)
        .AddEffectHandler<Ring, BellCommand, RingHandler>()
        .AddEffectHandler<Flash, BellCommand, FlashHandler>(FlashPolicy)
        .AddEffectHandler<Whisper, WhisperHandler>();

    /// <summary>
    /// The definition. Its <c>GaveUp</c> is one line for all three effects; <paramref name="world"/> only records what
    /// the line was given and what it returned.
    /// </summary>
    public static EntityDefinition<BellState, BellCommand, BellEvent> Definition(
        BellWorld world, EntityOptions? options = null) => new(
        TypeName,
        _ => new BellState([]),
        (_, command, ctx) => Decide(world, command, ctx),
        (state, e) => e switch { Heard heard => new BellState(state.Log.Add(heard.Line)) },
        options,
        GaveUp: g => world.Saw(g, new EffectFailed(g.Work, g.Reason, g.Attempts)));

    private static Decision<BellCommand, BellEvent> Decide(BellWorld world, BellCommand command, BellContext ctx)
    {
        world.Decided.Enqueue(command);
        return command switch
        {
            // Each issues one effect, with an event in the same decision.
            Sound { Kind: "ring" } c => ctx.Persist(new Heard($"asked:{c.Name}")).Execute(new Ring(c.Name, Volume: 3)),
            Sound { Kind: "flash" } c => ctx.Persist(new Heard($"asked:{c.Name}")).Execute(new Flash(c.Name)),
            Sound c => ctx.Persist(new Heard($"asked:{c.Name}")).Execute(new Whisper(c.Name)),

            Sounded s => ctx.Persist(new Heard($"sounded:{s.Name}")),

            // The one effect this entity cares about, found by the effect the give-up carries.
            EffectFailed { Work: Ring ring } f =>
                ctx.Persist(new Heard($"ring-failed:{ring.Name}:{ring.Volume}:{f.Reason}:{f.Attempts}")),

            // Every other effect.
            EffectFailed f => ctx.Persist(new Heard($"effect-failed:{f.Work.GetType().Name}:{f.Reason}:{f.Attempts}")),
        };
    }
}
