using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// An entity and a workflow say what a give-up becomes in the same place and with the same words: the argument
/// <c>GaveUp</c> of the definition, a function of the <see cref="WorkGaveUp"/> notice. The entity's returns one of its
/// commands, the workflow's one of its facts.
/// </summary>
public static class SideBySide
{
    /// <summary>The counter, with a give-up that names the effect and the reason.</summary>
    public static readonly EntityDefinition<CounterState, CounterCommand, CounterEvent> Entity = new(
        Counter.TypeName,
        _ => new CounterState(0, false),
        Counter.Decide,
        Counter.Evolve,
        GaveUp: g => new EffectGaveUpNoted($"{g.What}:{g.Reason}"));

    /// <summary>The checkout, with a give-up that names the effect and the reason.</summary>
    public static readonly WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Workflow = new(
        new WorkflowTypeName("checkouts"),
        Start: _ => new Reserving(),
        Checkout.Work,
        Checkout.Decide,
        Cancel: request => new CancelRequested(request.Reason, request.By),
        GaveUp: g => new WorkStuck(g.What, g.Reason));
}

/// <summary>The two functions, given the same notice. No ActorSystem.</summary>
public sealed class EntityAndWorkflowGiveUpSpecs
{
    public static TheoryData<GaveUpReason> Reasons =>
        [GaveUpReason.Failed, GaveUpReason.TimedOut, GaveUpReason.Interrupted, GaveUpReason.RetriesExhausted];

    [Theory]
    [MemberData(nameof(Reasons))]
    public void The_same_notice_becomes_the_entitys_command_and_the_workflows_fact_with_the_same_content(GaveUpReason reason)
    {
        var notice = new WorkGaveUp(new Compute(new IncrementViaService(3, Wait: false)), reason, Error: "boom", Attempts: 2);

        CounterCommand command = SideBySide.Entity.GaveUp!(notice);
        CheckoutFact fact = SideBySide.Workflow.GaveUp(notice);

        Assert.Equal(new EffectGaveUpNoted($"Compute:{reason}"), command.Value);
        Assert.Equal(new WorkStuck("Compute", reason), fact.Value);
    }

    [Theory]
    [MemberData(nameof(Reasons))]
    public void Each_Decide_gets_its_own_give_up(GaveUpReason reason)
    {
        var effect = new Compute(new IncrementViaService(3, Wait: false));

        // The entity: the scenario asks the definition's GaveUp for the command, and Decide decides it.
        DeciderScenario.For(SideBySide.Entity)
            .WhenGaveUp(effect, reason)
            .ThenPersisted(new GaveUpNoted($"Compute:{reason}", EndsServiceIncrement: false));

        // The workflow: the definition's GaveUp gives the fact, and Decide decides it.
        var at = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var transition = SideBySide.Workflow.Decide(
            new Reserving(),
            SideBySide.Workflow.GaveUp(new WorkGaveUp(effect, reason)),
            new DecideContext<PlaceOrder, CheckoutState, CheckoutOutcome>(new WorkflowId("w"), new PlaceOrder("o", 1), at, at));

        var next = Assert.IsType<NextTransition<CheckoutState>>(transition.Value);
        var attention = Assert.IsType<NeedsAttention>(next.State.Value);
        Assert.Equal($"Compute {reason}", attention.Why);
    }
}

/// <summary>Both run in one host: an entity's effect fails, and a workflow's work fails.</summary>
public sealed class HostedEntityAndWorkflowGiveUpSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    protected override WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Definition =>
        SideBySide.Workflow;

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddCheckoutHandlers()
            .AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>()
            .AddCounterHandlers();

    protected override void ConfigureMore(AkkaConfigurationBuilder builder) =>
        builder.WithCustomSerializer(CounterSerializer.CreateSerializerDetails).WithDurableEntity(SideBySide.Entity);

    [Fact]
    public async Task An_effect_that_fails_in_the_entity_and_work_that_fails_in_the_workflow_each_reach_their_Decide()
    {
        // The entity: the handler of Explode throws.
        var counter = DurableEntityRef<CounterCommand, CounterState, CounterEvent>("e1");
        await counter.Ask(new ExplodeLater(), AskCancellation());

        await AwaitAssertAsync(
            async () => Assert.Equal("Explode:Failed", (await counter.GetStateAsync(AskCancellation())).State.LastGaveUp),
            cancellationToken: TestCancellation);
        Assert.Empty(await PendingEffects<CounterCommand>("e1"));

        // The workflow: the handler of ReserveStock throws.
        Services.Reserve = (_, _) => throw new InvalidOperationException("boom");
        await CheckoutRef("w1").Start(new PlaceOrder("o1", 100), AskCancellation());

        var running = await SettledIn<NeedsAttention>("w1");
        var attention = Assert.IsType<NeedsAttention>(((CheckoutState)running.State).Value);
        Assert.IsType<Reserving>(attention.Was.Value);
        Assert.Equal("ReserveStock Failed", attention.Why);
    }
}
