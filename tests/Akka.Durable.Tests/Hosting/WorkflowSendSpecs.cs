using Akka.Actor;
using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Bumps;
using Akka.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Hosts the bump workflow, and the counter it sends commands to unless a spec leaves it out.
/// </summary>
public abstract class BumpWorkflowKit(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    protected static readonly TimeSpan Poll = TimeSpan.FromMilliseconds(20);

    protected virtual WorkflowDefinition<BumpCounter, BumpState, BumpFact, BumpOutcome> Definition => Bump.Definition;

    /// <summary>The counter the workflow sends to. Null when the counter's entity type is not hosted.</summary>
    protected virtual EntityDefinition<CounterState, CounterCommand, CounterEvent>? Target => Counter.Definition;

    protected sealed override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder
            .WithCustomSerializer(BumpSerializer.CreateSerializerDetails)
            .WithCustomSerializer(CounterSerializer.CreateSerializerDetails)
            .WithDurableWorkflow(Definition);

        if (Target is { } target)
            builder.WithDurableEntity(target);
    }

    protected IWorkflowRef<BumpCounter, BumpFact, BumpOutcome> BumpRef(string id) =>
        WorkflowRef<BumpCounter, BumpFact, BumpOutcome>(id);

    protected IDurableEntityRef<CounterCommand, CounterState, CounterEvent> CounterRef(string id) =>
        DurableEntityRef<CounterCommand, CounterState, CounterEvent>(id);

    /// <summary>Starts a bump and waits for its outcome.</summary>
    protected async Task<object> Run(string id, BumpCounter input)
    {
        var bump = BumpRef(id);
        await bump.Start(input, AskCancellation());
        return (await bump.WaitForOutcome(Poll, AskCancellation())).Value!;
    }
}

/// <summary>A workflow sends commands to a durable entity from Work, and each reply arrives as a fact.</summary>
public sealed class WorkflowSendSpecs(ITestOutputHelper output) : BumpWorkflowKit(output)
{
    [Fact]
    public async Task The_entity_journals_its_event_and_its_reply_moves_the_workflow_on()
    {
        Assert.Equal(new Bumped(5), await Run("b1", new BumpCounter("c1", 5)));

        // The counter journaled one event: the increment, sent once.
        var counter = await CounterRef("c1").GetStateAsync(AskCancellation());
        Assert.Equal(5, counter.State.Count);
        Assert.Equal(1, counter.SequenceNr);

        // Each reply was journaled as one of the workflow's facts, in order.
        var facts = (await JournalOf("b1")).OfType<WfFactRecorded>().Select(recorded => recorded.Fact);
        Assert.Equal<object>([(BumpFact)new CounterBumped(), (BumpFact)new CountRead(5)], facts);
    }

    /// <summary>Everything the journal holds for a workflow, in order.</summary>
    private async Task<IReadOnlyList<object>> JournalOf(string id)
    {
        var probe = CreateTestProbe();
        var persistenceId = EntityOptions.DefaultPersistenceIdFactory(
            new EntityId(id), new EntityTypeName(Definition.TypeName.Value));
        Sys.ActorOf(Props.Create(() => new JournalReader(persistenceId, probe.Ref)));
        var events = await probe.ExpectMsgAsync<object[]>(cancellationToken: TestCancellation);
        return [.. events.Select(stored => ((WfEvent)stored).Value!)];
    }
}

/// <summary>An entity that sends no reply: the work times out and the workflow receives the give-up fact.</summary>
public sealed class WorkflowSendTimeoutSpecs(ITestOutputHelper output) : BumpWorkflowKit(output)
{
    protected override WorkflowDefinition<BumpCounter, BumpState, BumpFact, BumpOutcome> Definition =>
        Bump.Definition with { Options = EntityOptions.Default with { EffectTimeout = TimeSpan.FromMilliseconds(300) } };

    // This counter's Decide ignores an increment, so it never replies to one.
    protected override EntityDefinition<CounterState, CounterCommand, CounterEvent> Target =>
        Counter.Definition with
        {
            Decide = (state, command, ctx) => command is Increment ? ctx.Ignore() : Counter.Decide(state, command, ctx),
        };

    [Fact]
    public async Task A_send_with_no_reply_gives_up_as_timed_out()
    {
        var outcome = Assert.IsType<BumpFailed>(await Run("b2", new BumpCounter("c2", 5)));

        Assert.Equal(nameof(Increment), outcome.What);
        Assert.Equal(GaveUpReason.TimedOut, outcome.Reason);
        Assert.Equal(0, (await CounterRef("c2").GetStateAsync(AskCancellation())).State.Count);
    }
}

/// <summary>A send to an entity type that is not hosted fails, and the error says which entity type is missing.</summary>
public sealed class WorkflowSendUnhostedSpecs(ITestOutputHelper output) : BumpWorkflowKit(output)
{
    protected override EntityDefinition<CounterState, CounterCommand, CounterEvent>? Target => null;

    [Fact]
    public async Task A_send_to_an_entity_type_that_is_not_hosted_gives_up_as_failed()
    {
        var outcome = Assert.IsType<BumpFailed>(await Run("b3", new BumpCounter("c3", 5)));

        Assert.Equal(nameof(Increment), outcome.What);
        Assert.Equal(GaveUpReason.Failed, outcome.Reason);
        Assert.Equal(
            $"No durable entity type with the command protocol {typeof(CounterCommand)} is hosted in this ActorSystem, " +
            "so the workflow cannot send Increment to [c3]. Host that entity type with WithDurableEntity.",
            outcome.Error);
    }
}
