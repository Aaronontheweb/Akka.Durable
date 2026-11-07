using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Hosting.TestKit;
using Akka.Event;
using Akka.Persistence;
using Akka.Persistence.TestKit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests;

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
    }

    private IActorRef Spawn(string id, TimeSpan? timeout = null)
    {
        var definition = Railway.Definition with
        {
            Options = EntityOptions.Default with { EffectTimeout = timeout ?? TimeSpan.FromSeconds(10) }
        };
        return Sys.ActorOf(Props.Create(() => new EntityActor<RailwayState, RailwayCommand, RailwayEvent>(
            new EntityId(id), definition, Host.Services, Sys.Scheduler)));
    }

    private void Send(IActorRef actor, RailwayCommand command) => actor.Tell(command, TestActor);

    private async Task<EntityStateReply> ReadState(IActorRef actor)
    {
        actor.Tell(GetEntityState.Instance, TestActor);
        return await ExpectMsgAsync<EntityStateReply>(cancellationToken: Ct);
    }

    [Fact]
    public async Task Failure_to_persist_the_success_branch_never_replies_success_or_selects_OnFailure()
    {
        var actor = Spawn("journal-failure");
        await ReadState(actor);
        Watch(actor);

        await WithJournalWrite(write => write.Fail(), async () =>
        {
            Send(actor, new Adjust(3));
            await ExpectTerminatedAsync(actor, cancellationToken: Ct);
        });

        Assert.Equal(["calculate:3", "disposed"], Recorder.Entries);
        await ExpectNoMsgAsync(NoMessageWindow, Ct);
        var recovered = Spawn("journal-failure");
        Assert.Equal(0, (await ReadState(recovered)).SequenceNr);
    }

    [Fact]
    public async Task State_queries_stay_queued_while_the_continuation_is_persisting()
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
            Assert.Equal(1, state.SequenceNr);
        });
    }

    [Fact]
    public async Task Exceptions_in_a_success_continuation_go_to_supervision_not_the_service_failure_branch()
    {
        var actor = Spawn("continuation-failure");
        await EventFilter.Custom<Error>(error => error.Cause.GetBaseException() is InvalidOperationException { Message: "continuation failed" }).ExpectOneAsync(() =>
        {
            Send(actor, new ThrowInContinuation());
            return Task.CompletedTask;
        }, Ct);

        await ExpectNoMsgAsync(NoMessageWindow, Ct);
        Assert.Equal(0, (await ReadState(actor)).SequenceNr);
    }

    [Fact]
    public async Task Timeout_selects_the_failure_branch_and_cancels_the_invocation()
    {
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Calculator.Wait = async ct =>
        {
            try
            {
                await new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously).Task.WaitAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                cancelled.TrySetResult();
                throw;
            }
        };
        var actor = Spawn("timeout", TimeSpan.FromMilliseconds(100));

        Send(actor, new TaskOnly());

        var reply = await ExpectMsgAsync<AdjustmentReply>(cancellationToken: Ct);
        Assert.Equal(nameof(TimeoutException), Assert.IsType<AdjustmentRejected>(reply.Value).Reason);
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Assert.Equal(0, (await ReadState(actor)).SequenceNr);
    }

    [Fact]
    public async Task A_timed_out_service_that_ignores_cancellation_keeps_its_scope_until_it_finishes()
    {
        var finish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Calculator.Wait = _ => finish.Task;
        var actor = Spawn("late-completion", TimeSpan.FromMilliseconds(100));
        Send(actor, new TaskOnly());

        var reply = await ExpectMsgAsync<AdjustmentReply>(cancellationToken: Ct);
        Assert.IsType<AdjustmentRejected>(reply.Value);
        Assert.Equal(["wait"], Recorder.Entries);
        finish.SetResult();

        await AwaitAssertAsync(() => Assert.Equal(["wait", "disposed"], Recorder.Entries),
            cancellationToken: Ct);
        // A late success cannot emit another reply or persist success events.
        Assert.Equal(0, (await ReadState(actor)).SequenceNr);
        await ExpectNoMsgAsync(NoMessageWindow, Ct);
    }

    [Fact]
    public async Task Shutdown_cancellation_does_not_execute_a_domain_failure_continuation()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Calculator.Wait = _ =>
        {
            entered.SetResult();
            return finish.Task;
        };
        var actor = Spawn("shutdown");
        Watch(actor);
        Send(actor, new TaskOnly());
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Sys.Stop(actor);
        await ExpectTerminatedAsync(actor, cancellationToken: Ct);
        finish.SetResult();

        await AwaitAssertAsync(() => Assert.Equal(["wait", "disposed"], Recorder.Entries),
            cancellationToken: Ct);
        await ExpectNoMsgAsync(NoMessageWindow, Ct);
    }

    private sealed class HeldWrite : IJournalInterceptor
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task InterceptAsync(IPersistentRepresentation message)
        {
            Entered.TrySetResult();
            await Release.Task.WaitAsync(Ct);
        }
    }
}
