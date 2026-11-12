using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Hosting;
using Akka.Persistence;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// A workflow and an entity, each with a handler that has a retry policy, in one actor system. The workflow's
/// charge is Backoff(1 min, 5 min, 6 attempts) and the entity's probe is Every(200 ms, 4 attempts).
/// </summary>
public sealed class WorkflowAndEntityRetrySpecs(ITestOutputHelper output) : WorkflowRetryKit(output)
{
    private readonly ProbeWorld _world = new();

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services)
    {
        base.ConfigureTestServices(context, services);
        services.AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>().AddSingleton(_world)
            .AddEffectHandler<Probe, CounterCommand, ProbeHandler>(Retry.Every(TimeSpan.FromMilliseconds(200), attempts: 4))
            .AddEffectHandler<Note, NoteHandler>();
    }

    protected override void ConfigureMore(AkkaConfigurationBuilder builder) => builder
        .WithCustomSerializer(CounterSerializer.CreateSerializerDetails)
        .WithCustomSerializer(ProbeSerializer.CreateSerializerDetails)
        .WithDurableEntity(ProbeEntity.Definition(_world, EntityOptions.Default with { EffectTimeout = TimeSpan.FromSeconds(30),  }));

    private IDurableEntityRef<CounterCommand, CounterState, CounterEvent> Counter(string id) =>
        DurableEntityRef<CounterCommand, CounterState, CounterEvent>(id);

    [Fact]
    public async Task A_workflow_waiting_for_its_retry_and_an_entity_retrying_its_effect_do_not_disturb_each_other()
    {
        ChargeFailsFirst(1);
        _world.OnProbe = (_, _) => _world.ProbeCalls.Count == 1
            ? throw new InvalidOperationException("not yet")
            : Task.FromResult<CounterCommand>(new NoHandler("answer:a"));
        var checkout = CheckoutRef("w1");
        await checkout.Start(new PlaceOrder("w1", 100), AskCancellation());
        await AwaitCharges(1);
        var due = await AwaitRetry("w1", 1, Minute);
        var stored = Assert.Single(await RemindersOf("w1"));

        // The entity's retries run on their own schedule, in real time, while the workflow waits.
        EntityRef<CounterCommand>("e1").Tell(new NoHandler("issue:a"));
        await AwaitAssertAsync(async () => Assert.Equal(101, (await Counter("e1").GetStateAsync(AskCancellation())).State.Count),
            TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);
        Assert.Equal([1, 2], _world.ProbeCalls.Select(c => c.Delivery.Attempt));
        Assert.StartsWith("counter/e1/", _world.ProbeCalls.First().Delivery.Id.Value, StringComparison.Ordinal);

        // The workflow was not touched by them: still on its first attempt, with its reminder where it was.
        Assert.Single(Charges);
        var still = Assert.Single(await RemindersOf("w1"));
        Assert.Equal((due, stored.DueTimeUtc), (still.StoredFor(), still.DueTimeUtc));

        // And the workflow's retry does not run the entity's effect again.
        Wake("w1", due);
        Assert.Equal(new OrderPlaced("charge-w1"), (await checkout.WaitForOutcome(AskCancellation())).Value);
        Assert.Equal([1, 2], Charges.Select(c => c.Attempt));
        Assert.StartsWith("checkouts/w1/", Charges[0].Id.Value, StringComparison.Ordinal);
        Assert.Equal(2, _world.ProbeCalls.Count);
        Assert.Equal(101, (await Counter("e1").GetStateAsync(AskCancellation())).State.Count);
    }

    [Fact]
    public async Task Everything_the_library_writes_to_a_journal_carries_the_marker_and_the_authors_events_do_not()
    {
        _world.OnProbe = (_, _) => Task.FromResult<CounterCommand>(new NoHandler("answer:a"));
        var checkout = CheckoutRef("k1");
        await checkout.Start(new PlaceOrder("k1", 100), AskCancellation());
        await checkout.WaitForOutcome(AskCancellation());

        // The entity writes an event of its own, the record of the effect, and the record of its answer.
        EntityRef<CounterCommand>("k2").Tell(new NoHandler("issue:a"));
        await AwaitAssertAsync(async () => Assert.Equal(101, (await Counter("k2").GetStateAsync(AskCancellation())).State.Count),
            TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);

        var workflowJournal = await StoredBy(EntityOptions.DefaultPersistenceIdFactory(new EntityId("k1"), new EntityTypeName("checkouts")));
        Assert.NotEmpty(workflowJournal);
        Assert.All(workflowJournal, record => Assert.IsAssignableFrom<IDurableJournalRecord>(record));

        var entityJournal = await StoredBy(EntityOptions.DefaultPersistenceIdFactory(new EntityId("k2"), Fixtures.Counter.TypeName));
        var own = entityJournal.OfType<CounterEvent>().ToArray();
        var library = entityJournal.Where(record => record is not CounterEvent).ToArray();
        Assert.NotEmpty(own);
        Assert.NotEmpty(library);
        Assert.All(own, e => Assert.False(((object)e) is IDurableJournalRecord, $"{e} carries the marker."));
        Assert.All(library, record => Assert.IsAssignableFrom<IDurableJournalRecord>(record));
    }

    [Theory]
    [InlineData(typeof(EffectIssued), true)]
    [InlineData(typeof(EffectRetried), true)]
    [InlineData(typeof(EffectAcknowledged), true)]
    [InlineData(typeof(EffectGaveUp), true)]
    [InlineData(typeof(EntitySnapshot), true)]
    [InlineData(typeof(WfEvent), true)]
    [InlineData(typeof(CounterEvent), false)]
    [InlineData(typeof(Incremented), false)]
    public void The_marker_is_on_the_libraries_records_and_not_on_the_authors_events(Type record, bool carries) =>
        Assert.Equal(carries, typeof(IDurableJournalRecord).IsAssignableFrom(record));

    private async Task<object[]> StoredBy(string persistenceId)
    {
        var probe = CreateTestProbe();
        Sys.ActorOf(Props.Create(() => new JournalReader(persistenceId, probe.Ref)));
        return await probe.ExpectMsgAsync<object[]>(cancellationToken: TestCancellation);
    }
}
