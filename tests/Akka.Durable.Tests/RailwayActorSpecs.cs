using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Durable.Tests.Fixtures;
using Akka.Event;
using Akka.Hosting.TestKit;
using Akka.Persistence;
using Akka.Persistence.TestKit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>
/// The runtime side of Execute: handler scopes, timeouts, shutdown, and how outcomes rejoin the entity.
/// </summary>
public sealed class RailwayActorSpecs(ITestOutputHelper output) : PersistenceTestKit(output: output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly TimeSpan NoMessageWindow = TimeSpan.FromMilliseconds(150);
    private Calculator Calculator => Host.Services.GetRequiredService<Calculator>();
    private EffectRecorder Recorder => Host.Services.GetRequiredService<EffectRecorder>();

    protected override void ConfigureServices(HostBuilderContext context, IServiceCollection services)
    {
        services.AddSingleton<Calculator>();
        services.AddSingleton<EffectRecorder>();
        services.AddScoped<ScopedCalculator>();
        services.AddEffectHandler<Calculate, RailwayCommand, CalculateHandler>();
    }

    private IActorRef Spawn(string id, TimeSpan? timeout = null,
        EntityDefinition<RailwayState, RailwayCommand, RailwayEvent>? definition = null)
    {
        definition = (definition ?? Railway.Definition) with
        {
            Options = EntityOptions.Default with { EffectTimeout = timeout ?? TimeSpan.FromSeconds(10) }
        };
        return Sys.ActorOf(Props.Create(() => new EntityActor<RailwayState, RailwayCommand, RailwayEvent>(
            new EntityId(id), definition, Host.Services, Sys.Scheduler)));
    }

    private void Send(IActorRef actor, RailwayCommand command, IActorRef? sender = null) =>
        actor.Tell(command, sender ?? TestActor);

    private async Task<EntityStateReply> ReadState(IActorRef actor)
    {
        actor.Tell(GetEntityState.Instance, TestActor);
        return await ExpectMsgAsync<EntityStateReply>(cancellationToken: Ct);
    }

    [Fact]
    public async Task Failure_to_persist_the_outcome_never_replies()
    {
        var actor = Spawn("journal-failure");
        await ReadState(actor);
        Watch(actor);

        // Only the outcome's write fails: the write that issues the effect holds no event of the entity.
        await WithJournalWrite(write => write.SetInterceptorAsync(new FailEventWrites()), async () =>
        {
            Send(actor, new Adjust(3));
            await ExpectTerminatedAsync(actor, cancellationToken: Ct);
        });

        Assert.Equal(["calculate:3", "disposed"], Recorder.Entries);
        await ExpectNoMsgAsync(NoMessageWindow, Ct);
        var recovered = Spawn("journal-failure");
        Assert.Equal(new RailwayState(0), (await ReadState(recovered)).State);
    }

    [Fact]
    public async Task State_queries_stay_queued_while_the_outcome_is_persisting()
    {
        var actor = Spawn("journal-held");
        await ReadState(actor);
        var journal = new HeldWrite();
        await WithJournalWrite(write => write.SetInterceptorAsync(journal), async () =>
        {
            Send(actor, new Adjust(3));
            await journal.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
            try
            {
                Assert.Equal(["calculate:3", "disposed"], Recorder.Entries);
                actor.Tell(GetEntityState.Instance, TestActor);
                await ExpectNoMsgAsync(NoMessageWindow, Ct);
            }
            finally
            {
                journal.Release.TrySetResult();
            }

            var reply = await ExpectMsgAsync<AdjustmentReply>(cancellationToken: Ct);
            Assert.Equal(3, Assert.IsType<Adjusted>(reply.Value).Count);
            var state = await ExpectMsgAsync<EntityStateReply>(cancellationToken: Ct);
            Assert.Equal(new RailwayState(3), state.State);
        });
    }

    [Fact]
    public async Task State_queries_are_answered_while_a_handler_is_still_running()
    {
        var finish = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        Calculator.Calculate = (_, ct) => finish.Task.WaitAsync(ct);
        var actor = Spawn("in-flight");

        Send(actor, new Adjust(3));
        await AwaitAssertAsync(() => Assert.Equal(["calculate:3"], Recorder.Entries), cancellationToken: Ct);

        Assert.Equal(new RailwayState(0), (await ReadState(actor)).State);

        finish.SetResult(3);
        var reply = await ExpectMsgAsync<AdjustmentReply>(cancellationToken: Ct);
        Assert.Equal(new Adjusted(3), reply.Value);
    }

    [Fact]
    public async Task Timeout_is_logged_and_cancels_the_invocation_and_the_effect_gives_up_as_timed_out()
    {
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Calculator.Calculate = async (_, ct) =>
        {
            try
            {
                return await new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously).Task.WaitAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                cancelled.TrySetResult();
                throw;
            }
        };
        var actor = Spawn("timeout", TimeSpan.FromMilliseconds(100));

        await EventFilter.Custom<Warning>(warning => warning.Cause is TimeoutException).ExpectOneAsync(() =>
        {
            Send(actor, new Adjust(3));
            return Task.CompletedTask;
        }, Ct);

        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        var reply = await ExpectMsgAsync<AdjustmentReply>(cancellationToken: Ct);
        Assert.Equal(new AdjustmentRejected("gave up: TimedOut"), reply.Value);
        Assert.Equal(new RailwayState(0), (await ReadState(actor)).State);
    }

    [Fact]
    public async Task A_timed_out_handler_that_ignores_cancellation_keeps_its_scope_until_it_finishes()
    {
        var finish = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        Calculator.Calculate = (_, _) => finish.Task;
        var actor = Spawn("late-completion", TimeSpan.FromMilliseconds(100));

        await EventFilter.Custom<Warning>(warning => warning.Cause is TimeoutException).ExpectOneAsync(() =>
        {
            Send(actor, new Adjust(3));
            return Task.CompletedTask;
        }, Ct);

        Assert.Equal(["calculate:3"], Recorder.Entries);
        Assert.IsType<AdjustmentRejected>((await ExpectMsgAsync<AdjustmentReply>(cancellationToken: Ct)).Value);
        finish.SetResult(3);

        await AwaitAssertAsync(() => Assert.Equal(["calculate:3", "disposed"], Recorder.Entries),
            cancellationToken: Ct);
        // The give-up settled the effect. A late result cannot reply or persist.
        Assert.Equal(new RailwayState(0), (await ReadState(actor)).State);
        await ExpectNoMsgAsync(NoMessageWindow, Ct);
    }

    [Fact]
    public async Task Outcome_of_a_stopped_incarnation_is_dropped()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finish = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        Calculator.Calculate = (_, _) =>
        {
            entered.SetResult();
            return finish.Task;
        };
        var actor = Spawn("shutdown");
        Watch(actor);
        Send(actor, new Adjust(3));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Sys.Stop(actor);
        await ExpectTerminatedAsync(actor, cancellationToken: Ct);
        finish.SetResult(3);

        await AwaitAssertAsync(() => Assert.Equal(["calculate:3", "disposed"], Recorder.Entries),
            cancellationToken: Ct);
        await ExpectNoMsgAsync(NoMessageWindow, Ct);
    }

    [Fact]
    public async Task Each_outcome_replies_to_the_asker_of_the_request_that_issued_it()
    {
        Calculator.Calculate = (value, _) => Task.FromResult(value * 10);
        var actor = Spawn("origins");
        var first = CreateTestProbe();
        var second = CreateTestProbe();

        Send(actor, new Adjust(1), first.Ref);
        Send(actor, new Adjust(2), second.Ref);

        Assert.IsType<Adjusted>((await first.ExpectMsgAsync<AdjustmentReply>(cancellationToken: Ct)).Value);
        Assert.IsType<Adjusted>((await second.ExpectMsgAsync<AdjustmentReply>(cancellationToken: Ct)).Value);
        Assert.Equal(new RailwayState(30), (await ReadState(actor)).State);
    }

    [Fact]
    public async Task Stop_after_an_outcome_passivates_on_the_actor_scheduler()
    {
        var definition = Railway.Definition with
        {
            Decide = (state, command, ctx) => command switch
            {
                Calculated r => ctx.Persist(new Added(r.Result)).Reply(r.Request, new Adjusted(state.Count + r.Result)).Stop(),
                _ => Railway.Decide(state, command, ctx),
            }
        };
        var probe = CreateTestProbe();
        var props = Props.Create(() => new EntityActor<RailwayState, RailwayCommand, RailwayEvent>(
            new EntityId("async-stop"), definition, Host.Services, Sys.Scheduler));
        var parent = Sys.ActorOf(Props.Create(() => new TestEntityParent(props, probe.Ref, false)));

        Send(parent, new Adjust(3));

        var reply = await ExpectMsgAsync<AdjustmentReply>(cancellationToken: Ct);
        Assert.Equal(new Adjusted(3), reply.Value);
        await probe.ExpectMsgAsync<Passivate>(cancellationToken: Ct);
        var child = probe.LastSender;
        var stopped = await probe.ExpectMsgAsync<EntityStopped>(cancellationToken: Ct);
        Assert.Equal(child, stopped.Entity);
    }

    private sealed class HeldWrite : IJournalInterceptor
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // Holds the write of the entity's events: the outcome's. The write that issues the effect passes.
        public async Task InterceptAsync(IPersistentRepresentation message)
        {
            if (message.Payload is not RailwayEvent) return;
            Entered.TrySetResult();
            await Release.Task.WaitAsync(Ct);
        }
    }

    /// <summary>Fails a write that holds one of the entity's events.</summary>
    private sealed class FailEventWrites : IJournalInterceptor
    {
        public Task InterceptAsync(IPersistentRepresentation message) =>
            message.Payload is RailwayEvent ? throw new InvalidOperationException("the journal is down") : Task.CompletedTask;
    }
}
