using System.Collections.Immutable;
using Xunit;

namespace Akka.Durable.TestKit;

/// <summary>
/// Given / When / Then harness for pure Decide and Evolve tests. No ActorSystem involved.
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

    /// <summary>
    /// Decides <paramref name="command"/> against the recovered state. Fails the test when the decision calls a
    /// workflow type that the definition does not name with <c>Uses</c>.
    /// </summary>
    public DeciderOutcome<TState, TCommand, TEvent> When(TCommand command)
    {
        var (state, context) = Recover();
        return new DeciderOutcome<TState, TCommand, TEvent>(definition, state,
            EnsureDeclared(definition.Decide(state, command, context)));
    }

    /// <summary>
    /// Decides the give-up of <paramref name="effect"/>: the command its <see cref="IEffect{TCommand}.GaveUp"/> returns
    /// for <paramref name="reason"/>, against the recovered state. This is what the entity decides when the effect's
    /// handler fails, runs out of time, or is interrupted by a restart.
    /// </summary>
    /// <param name="effect">The effect that gave up, as the decision issued it.</param>
    /// <param name="reason">Why it gave up.</param>
    /// <param name="attempts">How many attempts were made. At least 1.</param>
    public DeciderOutcome<TState, TCommand, TEvent> WhenGaveUp<TEffect>(
        TEffect effect, GaveUpReason reason, int attempts = 1)
        where TEffect : notnull, IEffect<TCommand>
    {
        ArgumentNullException.ThrowIfNull(effect);
        ArgumentOutOfRangeException.ThrowIfLessThan(attempts, 1);
        return When(effect.GaveUp(new WorkGaveUp(effect, reason, Error: null, attempts)));
    }

    /// <summary>A call to a workflow the entity cannot make fails at run time, so the scenario fails here.</summary>
    private Decision<TCommand, TEvent> EnsureDeclared(Decision<TCommand, TEvent> decision)
    {
        if (decision is not Handled<TCommand, TEvent> { Effects: { IsDefault: false } effects }) return decision;

        foreach (var effect in effects)
        {
            if (effect.Value is not ExecuteEffect { Effect: IWorkflowCall call }) continue;

            if (!definition.Declares(call))
                throw new Xunit.Sdk.XunitException(
                    $"The decision calls workflow type [{call.Workflow.Value}] (workflow [{call.Id.Value}]), but " +
                    $"entity type [{definition.TypeName.Value}] does not declare it. Add .Uses(...) with the " +
                    $"definition of [{call.Workflow.Value}] to the entity definition.");

            if (call is not StartWorkflow && !definition.Used[call.Workflow].Answers)
                throw new Xunit.Sdk.XunitException(
                    $"The decision notifies or cancels workflow [{call.Workflow.Value}/{call.Id.Value}], but entity type " +
                    $"[{definition.TypeName.Value}] uses workflow type [{call.Workflow.Value}] without told and gaveUp. " +
                    "Declare it with .Uses(definition, told: ..., gaveUp: ...), so the answer and the give-up reach Decide.");
        }

        return decision;
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
    /// Asserts that the decision starts the workflow <paramref name="id"/> of the type <paramref name="workflow"/> with
    /// <paramref name="input"/>. Nothing is started.
    /// </summary>
    public DeciderOutcome<TState, TCommand, TEvent> ThenStarted<TInput, TWorkflowState, TFact, TOutcome>(
        WorkflowDefinition<TInput, TWorkflowState, TFact, TOutcome> workflow, WorkflowId id, TInput input)
        where TInput : notnull
        where TWorkflowState : notnull
        where TFact : notnull
        where TOutcome : notnull
    {
        ArgumentNullException.ThrowIfNull(workflow);
        return ThenCalled(new StartWorkflow(workflow.TypeName, id, input));
    }

    /// <summary>Asserts that the decision sends <paramref name="fact"/> to the workflow <paramref name="id"/>.</summary>
    public DeciderOutcome<TState, TCommand, TEvent> ThenNotified<TInput, TWorkflowState, TFact, TOutcome>(
        WorkflowDefinition<TInput, TWorkflowState, TFact, TOutcome> workflow, WorkflowId id, TFact fact)
        where TInput : notnull
        where TWorkflowState : notnull
        where TFact : notnull
        where TOutcome : notnull
    {
        ArgumentNullException.ThrowIfNull(workflow);
        return ThenCalled(new NotifyWorkflow(workflow.TypeName, id, fact));
    }

    /// <summary>Asserts that the decision asks the workflow <paramref name="id"/> to stop, with this reason and caller.</summary>
    public DeciderOutcome<TState, TCommand, TEvent> ThenCancelled<TInput, TWorkflowState, TFact, TOutcome>(
        WorkflowDefinition<TInput, TWorkflowState, TFact, TOutcome> workflow, WorkflowId id, string reason, string by)
        where TInput : notnull
        where TWorkflowState : notnull
        where TFact : notnull
        where TOutcome : notnull
    {
        ArgumentNullException.ThrowIfNull(workflow);
        return ThenCalled(new CancelWorkflow(workflow.TypeName, id, reason, by));
    }

    private DeciderOutcome<TState, TCommand, TEvent> ThenCalled(object expected)
    {
        var calls = Effects.Select(e => e.Value).OfType<ExecuteEffect>().Select(x => x.Effect).OfType<IWorkflowCall>().ToList();
        Assert.True(calls.Any(call => Equals(call, expected)),
            $"Expected the decision to issue {expected}, but it issued " +
            (calls.Count == 0 ? "no workflow calls." : string.Join(", ", calls) + "."));
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
