using System.Collections.Immutable;
using Xunit;

namespace Akka.Durable.TestKit;

/// <summary>
/// Given / When / Then harness for pure Decide, Evolve and Resume tests. No ActorSystem involved.
/// </summary>
/// <example>
/// DeciderScenario.For(Counter.Definition)
///     .Given(new Incremented(2))
///     .When(new Increment(3))
///     .ThenPersisted(new Incremented(3))
///     .ThenState(s => Assert.Equal(5, s.Count));
/// </example>
public static class DeciderScenario
{
    public static DeciderScenario<TState, TCommand, TEvent> For<TState, TCommand, TEvent>(
        EntityDefinition<TState, TCommand, TEvent> definition) => new(definition);
}

public sealed class DeciderScenario<TState, TCommand, TEvent>(
    EntityDefinition<TState, TCommand, TEvent> definition)
{
    private ImmutableArray<TEvent> _history = [];
    private EntityId _entityId = new("test-entity");
    private DateTimeOffset _now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Prior events, folded through Evolve starting from InitialState.</summary>
    public DeciderScenario<TState, TCommand, TEvent> Given(params TEvent[] history)
    {
        _history = [.. history];
        return this;
    }

    public DeciderScenario<TState, TCommand, TEvent> ForEntity(EntityId entityId)
    {
        _entityId = entityId;
        return this;
    }

    public DeciderScenario<TState, TCommand, TEvent> At(DateTimeOffset utcNow)
    {
        _now = utcNow;
        return this;
    }

    public DeciderOutcome<TState, TCommand, TEvent> When(TCommand command)
    {
        var (state, context) = Recover();
        return new DeciderOutcome<TState, TCommand, TEvent>(definition, state, definition.Decide(state, command, context));
    }

    /// <summary>
    /// Recovers the entity from the given history and runs its <c>Resume</c>: the effects still pending for
    /// that state. A definition without <c>Resume</c> has none.
    /// </summary>
    public DeciderOutcome<TState, TCommand, TEvent> Resumed()
    {
        var (state, context) = Recover();
        Decision<TCommand, TEvent> pending = definition.Resume is { } resume
            ? resume(state, context)
            : Decisions.None<TCommand, TEvent>();
        return new DeciderOutcome<TState, TCommand, TEvent>(definition, state, pending);
    }

    private (TState State, DecisionContext<TState, TCommand, TEvent> Context) Recover()
    {
        var state = _history.Aggregate(definition.InitialState(_entityId), definition.Evolve);
        return (state, new DecisionContext<TState, TCommand, TEvent>(_entityId, definition.TypeName, _now));
    }
}

public sealed class DeciderOutcome<TState, TCommand, TEvent>(
    EntityDefinition<TState, TCommand, TEvent> definition,
    TState stateBefore,
    Decision<TCommand, TEvent> decision)
{
    public Decision<TCommand, TEvent> Decision => decision;

    public Handled<TCommand, TEvent> Handled => decision is Handled<TCommand, TEvent> handled
        ? handled
        : throw new Xunit.Sdk.XunitException($"Expected a Handled decision but got {decision.Value}");

    public ImmutableArray<EntityEffect> Effects => Handled.Effects.IsDefault ? [] : Handled.Effects;

    public DeciderOutcome<TState, TCommand, TEvent> ThenPersisted(params TEvent[] expected)
    {
        Assert.Equal(expected, Handled.Events.IsDefault ? [] : Handled.Events);
        return this;
    }

    public DeciderOutcome<TState, TCommand, TEvent> ThenNothingPersisted() => ThenPersisted();

    /// <summary>Asserts on the state after the decision's events are evolved.</summary>
    public DeciderOutcome<TState, TCommand, TEvent> ThenState(Action<TState> assert)
    {
        assert((Handled.Events.IsDefault ? [] : Handled.Events).Aggregate(stateBefore, definition.Evolve));
        return this;
    }

    public DeciderOutcome<TState, TCommand, TEvent> ThenReplied<TReply>(IReplies<TCommand, TReply> command, TReply expected)
        where TReply : notnull
    {
        ArgumentNullException.ThrowIfNull(command);
        Assert.Contains(Effects, e => e.Value is ReplyEffect r && Equals(r.Response, expected));
        return this;
    }

    /// <summary>Asserts that the decision issues <paramref name="expected"/> to its effect handler. No handler runs.</summary>
    public DeciderOutcome<TState, TCommand, TEvent> ThenExecuted(object expected)
    {
        ArgumentNullException.ThrowIfNull(expected);
        Assert.Contains(Effects, e => e.Value is ExecuteEffect x && Equals(x.Effect, expected));
        return this;
    }

    /// <summary>Asserts that the decision sends <paramref name="command"/> to the entity of protocol <typeparamref name="TOther"/> with id <paramref name="target"/>.</summary>
    public DeciderOutcome<TState, TCommand, TEvent> ThenSent<TOther>(EntityId target, TOther command)
    {
        ArgumentNullException.ThrowIfNull(command);
        Assert.Contains(Effects, e => e.Value is SendEffect s
            && s.CommandType == typeof(TOther) && s.Target.Equals(target) && Equals(s.Command, command));
        return this;
    }

    /// <summary>
    /// Asserts the ordered sequence of effect kinds. Effects that hold delegates (Run) can't be
    /// compared by value, so ordering is checked by type; inspect <see cref="Effects"/> for detail.
    /// </summary>
    public DeciderOutcome<TState, TCommand, TEvent> ThenEffects(params Type[] expectedEffectTypes)
    {
        Assert.Equal(expectedEffectTypes, Effects.Select(e => e.Value!.GetType()));
        return this;
    }

    public void ThenIgnored() => Assert.True(decision is Ignored, $"Expected Ignored but got {decision.Value}");
    public void ThenUnhandled() => Assert.True(decision is NotHandled, $"Expected NotHandled but got {decision.Value}");
    public void ThenStashed() => Assert.True(decision is Stashed, $"Expected Stashed but got {decision.Value}");
}
