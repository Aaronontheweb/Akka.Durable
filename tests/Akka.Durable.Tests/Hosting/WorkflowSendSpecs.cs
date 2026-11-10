using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Bumps;
using Akka.Hosting;
using Akka.Remote.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>Hosts the bump workflow "w1" and the railway it sends to. The counter's entity type is not hosted.</summary>
public abstract class BumpWorkflowKit(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    private readonly TaskCompletionSource _called = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

    protected virtual bool Sharded => false;
    protected virtual WorkflowDefinition<BumpInput, Sending, BumpFact, Ended> Definition => Bump.Definition;

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) => services
        .AddSingleton<Calculator>().AddSingleton<EffectRecorder>().AddScoped<ScopedCalculator>()
        .AddScoped<IEffectHandler<Calculate, RailwayCommand>, CalculateHandler>();

    protected sealed override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        if (Sharded)
            builder.WithRemoting("localhost", 0).WithClustering().AddStartup((system, _) =>
                Akka.Cluster.Cluster.Get(system).Join(Akka.Cluster.Cluster.Get(system).SelfAddress));

        var sharding = Sharded ? new ShardedHosting() : null;
        builder.WithCustomSerializer(BumpSerializer.CreateSerializerDetails).WithCustomSerializer(RailwaySerializer.CreateSerializerDetails)
            .WithDurableWorkflow(Definition, sharding).WithDurableEntity(Railway.Definition, sharding);
    }

    // The railway's service. The railway calls it once for each Adjust it receives.
    protected Calculator Calculator => Host.Services.GetRequiredService<Calculator>();
    protected int Calls => Host.Services.GetRequiredService<EffectRecorder>().Entries.Count(e => e.StartsWith("calculate:"));
    protected IWorkflowRef<BumpInput, Sending, BumpFact, Ended> W1 => WorkflowRef<BumpInput, Sending, BumpFact, Ended>("w1");

    /// <summary>Starts w1 with a railway service that answers only on <see cref="Release"/>, and waits until it is called.</summary>
    protected async Task StartGated(Plan plan)
    {
        Calculator.Calculate = async (value, ct) =>
        {
            _called.TrySetResult();
            await _release.Task.WaitAsync(ct);
            return value;
        };
        await W1.Start(new BumpInput(5, plan), AskCancellation());
        await _called.Task.WaitAsync(TestCancellation);
    }

    protected void Release() => _release.TrySetResult();
    protected async Task<object> Notify(BumpFact fact) => (await W1.Notify(fact, AskCancellation())).Value!;

    // A fact's value; a give-up without its error message, which specs check on their own.
    protected static object? Plain(BumpFact fact) => fact.Value is BumpGaveUp gaveUp ? gaveUp with { Error = "" } : fact.Value;

    /// <summary>Waits until w1's journal shows exactly these decided facts and the railway's count is this.</summary>
    protected Task AwaitDecided(BumpFact[] expected, int railwayCount) => AwaitAssertAsync(async () =>
    {
        var probe = CreateTestProbe();
        var persistenceId = EntityOptions.DefaultPersistenceIdFactory(new EntityId("w1"), new EntityTypeName(Definition.TypeName.Value));
        Sys.ActorOf(Props.Create(() => new JournalReader(persistenceId, probe.Ref)));
        var journal = await probe.ExpectMsgAsync<object[]>(cancellationToken: TestCancellation);
        var decided = journal.Select(e => ((WfEvent)e).Value).OfType<WfFactRecorded>().Select(r => Plain((BumpFact)r.Fact));
        Assert.Equal(expected.Select(Plain), decided);
        var railway = DurableEntityRef<RailwayCommand, RailwayState, RailwayEvent>("w1");
        Assert.Equal(railwayCount, (await railway.GetStateAsync(AskCancellation())).State.Count);
    }, TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);

    /// <summary>Kills w1's actor and waits until a new one, which has to recover, answers.</summary>
    protected async Task RestartWorkflow()
    {
        var type = new EntityTypeName(Definition.TypeName.Value);
        var actor = await Sys.ActorSelection(Sharded ? $"/system/sharding/{type.Value}/*/w1" : $"/user/{LocalEntityHost.ActorName(type)}/w1")
            .ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);
        Watch(actor);
        actor.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(actor, cancellationToken: TestCancellation);
        // A message sent before the host sees the stop is lost, so ask with a short limit until a new actor answers.
        await AwaitAssertAsync(() => W1.Status(AskCancellation(TimeSpan.FromMilliseconds(500))),
            TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);
    }
}

public enum During { Nothing, TwoFactsFromOutside, Restart, RestartAfterReply }

/// <summary>A workflow sends a command to a durable entity from Work. Runs once for each hosting mode.</summary>
public abstract class WorkflowSendSpecs(ITestOutputHelper output) : BumpWorkflowKit(output)
{
    // plan, input By, the railway's service fails with, the fact that ended w1, its error contains, railway count, Adjusts sent
    public static TheoryData<Plan, int, string?, BumpFact, string, int, int> Replies => new()
    {
        { Plan.Send, 5, null, new BumpAdjusted(5), "", 5, 1 },
        { Plan.Send, -3, null, new BumpRejected("negative result"), "", 0, 1 },
        { Plan.Send, 5, "calculator down", new BumpRejected("calculator down"), "", 0, 1 },
        { Plan.Twice, 5, null, new BumpAdjusted(10), "", 10, 2 },
        { Plan.ThenThrows, 5, null, new BumpGaveUp(nameof(Adjust), GaveUpReason.Failed, ""), "then failed", 5, 1 },
        { Plan.ThenNull, 5, null, new BumpGaveUp(nameof(Adjust), GaveUpReason.Failed, ""), "", 5, 1 },
        { Plan.Unhosted, 5, null, new BumpGaveUp(nameof(Increment), GaveUpReason.Failed, ""), nameof(CounterCommand), 0, 0 },
    };

    [Theory]
    [MemberData(nameof(Replies))]
    public async Task The_reply_or_the_give_up_is_the_fact_the_table_decides(
        Plan plan, int by, string? serviceFails, BumpFact expected, string errorNames, int railwayCount, int sends)
    {
        Calculator.Calculate = (value, _) =>
            serviceFails is null ? Task.FromResult(value) : Task.FromException<int>(new InvalidOperationException(serviceFails));

        await W1.Start(new BumpInput(by, plan), AskCancellation());
        var last = (await W1.WaitForOutcome(TimeSpan.FromMilliseconds(20), AskCancellation())).Last;

        Assert.Equal(Plain(expected), Plain(last));
        if (last.Value is BumpGaveUp gaveUp) Assert.Contains(errorNames, gaveUp.Error);
        await AwaitDecided(plan == Plan.Twice ? [new BumpAdjusted(5), expected] : [expected], railwayCount);
        Assert.Equal(sends, Calls);
    }

    // plan, what happens while the railway has not replied, the facts decided in order, Adjusts sent
    public static TheoryData<Plan, During, BumpFact[], int> InFlight => new()
    {
        { Plan.Stay, During.Nothing, [new BumpAdjusted(5)], 1 },
        { Plan.Stay, During.TwoFactsFromOutside, [new BumpAdjusted(5), new BumpNoted("a"), new BumpNoted("cancel:b")], 1 },
        { Plan.Stay, During.Restart, [new BumpGaveUp(nameof(Adjust), GaveUpReason.Interrupted, "")], 1 },
        { Plan.Stay, During.RestartAfterReply, [new BumpAdjusted(5), new BumpNoted("after")], 1 },
        { Plan.WithTimer, During.Nothing, [new BumpAdjusted(5), new BumpTimerDue()], 1 },
        { Plan.WithTimer, During.TwoFactsFromOutside, [new BumpAdjusted(5), new BumpNoted("a"), new BumpNoted("cancel:b"), new BumpTimerDue()], 1 },
        { Plan.WithTimer, During.Restart, [new BumpGaveUp(nameof(Adjust), GaveUpReason.Interrupted, ""), new BumpTimerDue()], 1 },
        { Plan.Both, During.Nothing, [new BumpAdjusted(5), new BumpAdjusted(5)], 2 },
        { Plan.Both, During.TwoFactsFromOutside, [new BumpAdjusted(5), new BumpAdjusted(5), new BumpNoted("a"), new BumpNoted("cancel:b")], 2 },
    };

    [Theory]
    [MemberData(nameof(InFlight))]
    public async Task Facts_are_decided_in_order_and_each_command_is_sent_once(Plan plan, During during, BumpFact[] expected, int sends)
    {
        await StartGated(plan);
        if (during == During.TwoFactsFromOutside)
        {
            Assert.IsType<Held>(await Notify(new BumpNoted("a")));
            Assert.IsType<Held>((await W1.Cancel("b", "spec", AskCancellation())).Value);
        }

        if (during == During.Restart) await RestartWorkflow();
        Release();
        if (during == During.RestartAfterReply)
        {
            await AwaitDecided(expected[..1], 5);
            await RestartWorkflow();
            Assert.IsType<Delivered>(await Notify(new BumpNoted("after")));
        }

        await AwaitDecided(expected, 5);
        Assert.Equal(sends, Calls);
    }
}

public sealed class WorkflowSendLocalSpecs(ITestOutputHelper output) : WorkflowSendSpecs(output);

public sealed class WorkflowSendShardedSpecs(ITestOutputHelper output) : WorkflowSendSpecs(output)
{
    protected override bool Sharded => true;
}

/// <summary>An entity that does not reply in time: the work times out, and its late reply is not a fact.</summary>
public sealed class WorkflowSendTimeoutSpecs(ITestOutputHelper output) : BumpWorkflowKit(output)
{
    protected override WorkflowDefinition<BumpInput, Sending, BumpFact, Ended> Definition =>
        Bump.Definition with { Options = EntityOptions.Default with { EffectTimeout = TimeSpan.FromMilliseconds(300) } };

    [Fact]
    public async Task A_send_with_no_reply_within_the_effect_timeout_gives_up_as_timed_out()
    {
        BumpFact timedOut = new BumpGaveUp(nameof(Adjust), GaveUpReason.TimedOut, "");
        await StartGated(Plan.Stay);
        await AwaitDecided([timedOut], 0);

        Release();
        await AwaitDecided([timedOut], 5);
        Assert.IsType<Delivered>(await Notify(new BumpNoted("after")));
        await AwaitDecided([timedOut, new BumpNoted("after")], 5);
        Assert.Equal(1, Calls);
    }
}
