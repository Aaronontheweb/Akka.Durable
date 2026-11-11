using Akka.Actor;
using Akka.Cluster.Sharding;
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

    private sealed record NotAccepted;

    private const string RefusalWarning = "which its definition does not accept";

    [Fact]
    public async Task A_message_the_definition_does_not_accept_is_refused_with_a_warning_naming_it()
    {
        var region = Host.Services.GetRequiredService<IRequiredActor<EntityRegion<TallyCommand>>>().ActorRef;

        await EventFilter.Warning(contains: $"[tally/t6] received a message of type {nameof(NotAccepted)}, {RefusalWarning}")
            .ExpectOneAsync(async () =>
            {
                region.Tell(new ShardingEnvelope("t6", new NotAccepted()), TestActor);
                await TallyRef("t6").GetStateAsync(AskCancellation());
            }, TestCancellation);
    }

    [Fact]
    public async Task A_message_the_definition_accepts_logs_no_refusal()
    {
        var region = Host.Services.GetRequiredService<IRequiredActor<EntityRegion<TallyCommand>>>().ActorRef;

        await EventFilter.Warning(contains: RefusalWarning).ExpectAsync(0, async () =>
        {
            region.Tell(new ShardingEnvelope("t7", new Accepted()), TestActor);
            await TallyRef("t7").GetStateAsync(AskCancellation());
        }, TestCancellation);
    }

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
    }
}
