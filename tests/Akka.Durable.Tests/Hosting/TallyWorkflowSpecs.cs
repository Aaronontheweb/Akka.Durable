using Akka.Actor;
using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// One entity drives another through Send, receives its replies as commands through Accepts, and answers
/// the asker that started the work. Both entity types are hosted locally in the same process.
/// </summary>
public sealed class TallyWorkflowSpecs(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services)
    {
        services.AddSingleton<EffectRecorder>();
        services.AddSingleton<Gate>();
        services.AddScoped<ScopedRecorder>();
        services.AddCounterHandlers();
    }

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider) =>
        builder
            .WithCustomSerializer(CounterSerializer.CreateSerializerDetails)
            .WithCustomSerializer(TallySerializer.CreateSerializerDetails)
            .WithDurableEntity(Counter.Definition)
            .WithDurableEntity(Tally.Definition);

    private IDurableEntityRef<TallyCommand, TallyState, TallyEvent> TallyRef(string id) =>
        DurableEntityRef<TallyCommand, TallyState, TallyEvent>(id);

    private IEntityRef<CounterCommand> CounterRef(string id) => EntityRef<CounterCommand>(id);

    [Fact]
    public async Task Workflow_drives_the_counter_and_replies_to_its_own_asker()
    {
        var done = await TallyRef("t1").Ask(new StartTally("c1", 5), AskCancellation());

        Assert.Equal(new TallyDone(5), done);
        Assert.Equal(5, (await CounterRef("c1").Ask(new GetCount(), AskCancellation())).Count);
        Assert.Equal(new TallyState(null, 5), (await TallyRef("t1").GetStateAsync(AskCancellation())).State);
    }

    [Fact]
    public async Task A_repeated_start_gets_the_recorded_answer()
    {
        var tally = TallyRef("t2");
        await tally.Ask(new StartTally("c2", 3), AskCancellation());

        Assert.Equal(new TallyDone(3), await tally.Ask(new StartTally("c2", 3), AskCancellation()));
        Assert.Equal(3, (await CounterRef("c2").Ask(new GetCount(), AskCancellation())).Count);
    }

    [Fact]
    public async Task A_restarted_workflow_resumes_by_asking_the_counter_again()
    {
        // Lock the counter so the increment is stashed and the tally stays mid-flight.
        var counter = CounterRef("c3");
        await counter.Ask(new LockCounter(), AskCancellation());

        var tally = TallyRef("t3");
        tally.Tell(new StartTally("c3", 4));
        await AwaitAssertAsync(async () =>
            Assert.NotNull((await tally.GetStateAsync(AskCancellation())).State.Pending), cancellationToken: TestCancellation);

        var incarnation = await Sys.ActorSelection($"/user/durable-tally/t3").ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);
        Watch(incarnation);
        incarnation.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(incarnation, cancellationToken: TestCancellation);

        // Reactivation runs Resume: the tally asks the still-locked counter for its count (0) and finishes.
        await AwaitAssertAsync(async () =>
            Assert.Equal(new TallyState(null, 0), (await tally.GetStateAsync(AskCancellation())).State), cancellationToken: TestCancellation);

        // The stashed increment is still applied once the counter unlocks; its reply goes to the old incarnation's ask.
        await counter.Ask(new UnlockCounter(), AskCancellation());
        await AwaitAssertAsync(async () =>
            Assert.Equal(4, (await counter.Ask(new GetCount(), AskCancellation())).Count), cancellationToken: TestCancellation);
    }

    [Fact]
    public void Decisions_of_the_workflow_are_data()
    {
        var request = new StartTally("c9", 2);

        DeciderScenario.For(Tally.Definition)
            .When(request)
            .ThenPersisted(new TallyStarted(request))
            .ThenSent<CounterCommand>(new EntityId("c9"), new Increment(2));

        DeciderScenario.For(Tally.Definition)
            .Given(new TallyStarted(request))
            .When(new CounterCounted(7))
            .ThenPersisted(new TallyFinished(7))
            .ThenReplied(request, new TallyDone(7))
            .ThenEffects(typeof(ReplyEffect), typeof(StopEffect));

        DeciderScenario.For(Tally.Definition)
            .Given(new TallyStarted(request))
            .Resumed()
            .ThenSent<CounterCommand>(new EntityId("c9"), new GetCount());
    }
}
