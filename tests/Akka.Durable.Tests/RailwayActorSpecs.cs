using Akka.Actor;
using Akka.Cluster.Sharding;
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

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Captured_stages_are_rejected_before_repeating_events_or_messages(bool typed, bool fails)
    {
        if (fails)
            Calculator.Calculate = (_, _) => Task.FromException<int>(new InvalidOperationException("service failed"));
        var definition = Railway.Definition with
        {
            Decide = (_, command, ctx) =>
            {
                var c = (BeginAdjust)command.Value;
                if (typed)
                {
                    var captured = ctx.For(c).Persist(new Added(c.Before)).Tell(TestActor, "stage");
                    return captured.Run<ScopedCalculator, int>((service, ct) => service.CalculateAsync(c.By, ct),
                        onSuccess: (_, _) => captured.Reply(new Adjusted(999)),
                        onFailure: (_, _) => captured.Reply(new AdjustmentRejected("incorrect reply")));
                }
                var effects = ctx.Persist(new Added(c.Before)).Tell(TestActor, "stage");
                return effects.Run<ScopedCalculator, int>((service, ct) => service.CalculateAsync(c.By, ct),
                    onSuccess: (_, _) => effects,
                    onFailure: (_, _) => effects);
            }
        };
        var actor = Spawn($"captured-{typed}-{fails}", definition: definition);
        await EventFilter.Custom<Error>(error => error.Cause.GetBaseException() is InvalidOperationException ex
            && ex.Message.Contains("reused an earlier decision stage", StringComparison.Ordinal)).ExpectOneAsync(() =>
        {
            Send(actor, new BeginAdjust(10, 3));
            return Task.CompletedTask;
        }, Ct);

        await ExpectMsgAsync("stage", cancellationToken: Ct);
        var state = await ReadState(actor);
        Assert.Equal(new RailwayState(10), state.State);
        Assert.Equal(1, state.SequenceNr);
        Assert.Equal(["calculate:3", "disposed"], Recorder.Entries);
        await ExpectNoMsgAsync(NoMessageWindow, Ct);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Non_reply_continuations_receive_fresh_state_and_log_handled_failures(bool fails)
    {
        if (fails)
            Calculator.Calculate = (_, _) => Task.FromException<int>(new InvalidOperationException("service failed"));
        var definition = Railway.Definition with
        {
            Decide = (_, command, ctx) => ctx.Persist(new Added(10))
                .Run<ScopedCalculator, int>((service, ct) => service.CalculateAsync(3, ct),
                    onSuccess: (result, next) => next.Persist(new Added(result)).Tell(TestActor, next.State.Count),
                    onFailure: (error, next) => next.Tell(TestActor, next.State.Count))
        };
        var actor = Spawn($"non-reply-{fails}", definition: definition);
        if (fails)
        {
            await EventFilter.Custom<Warning>(warning => warning.Message.ToString()!.Contains("Run effect failed",
                StringComparison.Ordinal)).ExpectOneAsync(() =>
            {
                Send(actor, new BeginAdjust(10, 3));
                return Task.CompletedTask;
            }, Ct);
        }
        else
            Send(actor, new BeginAdjust(10, 3));

        await ExpectMsgAsync(10, cancellationToken: Ct);
        var state = await ReadState(actor);
        Assert.Equal(new RailwayState(fails ? 10 : 13), state.State);
        Assert.Equal(fails ? 1 : 2, state.SequenceNr);
        await ExpectNoMsgAsync(NoMessageWindow, Ct);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ReplyAndRun_replies_once_before_Task_or_result_work_finishes(bool hasResult, bool fails)
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finish = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        Calculator.Calculate = (_, ct) =>
        {
            entered.TrySetResult();
            return finish.Task.WaitAsync(ct);
        };
        Calculator.Wait = async ct =>
        {
            entered.TrySetResult();
            await finish.Task.WaitAsync(ct);
        };
        var definition = Railway.Definition with
        {
            Decide = (_, command, ctx) =>
            {
                var request = ctx.For((BeginAdjust)command.Value).Persist(new Added(10));
                return hasResult
                    ? request.ReplyAndRun<ScopedCalculator, int>(new Adjusted(10),
                        (service, ct) => service.CalculateAsync(3, ct),
                        onSuccess: (result, next) => next.Persist(new Added(result)),
                        onFailure: (error, next) => next)
                    : request.ReplyAndRun<ScopedCalculator>(new Adjusted(10),
                        (service, ct) => service.WaitAsync(ct),
                        onSuccess: next => next.Persist(new Added(1)),
                        onFailure: (error, next) => next);
            }
        };
        var actor = Spawn($"early-{hasResult}-{fails}", definition: definition);
        Send(actor, new BeginAdjust(10, 3));
        var reply = await ExpectMsgAsync<AdjustmentReply>(cancellationToken: Ct);
        Assert.Equal(new Adjusted(10), reply.Value);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        actor.Tell(GetEntityState.Instance, TestActor);
        try
        {
            await ExpectNoMsgAsync(NoMessageWindow, Ct);
        }
        finally
        {
            if (fails)
                finish.TrySetException(new InvalidOperationException("service failed"));
            else
                finish.TrySetResult(3);
        }

        var state = await ExpectMsgAsync<EntityStateReply>(cancellationToken: Ct);
        Assert.Equal(new RailwayState(fails ? 10 : hasResult ? 13 : 11), state.State);
        Assert.Equal(fails ? 1 : 2, state.SequenceNr);
        await ExpectNoMsgAsync(NoMessageWindow, Ct);
    }

    [Fact]
    public async Task ReplyAndStop_after_an_asynchronous_continuation_passivates_on_the_actor_scheduler()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finish = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        Calculator.Calculate = (_, ct) =>
        {
            entered.TrySetResult();
            return finish.Task.WaitAsync(ct);
        };
        var definition = Railway.Definition with
        {
            Decide = (_, command, ctx) => ctx.For((Adjust)command.Value).Run<ScopedCalculator, int>(
                (service, ct) => service.CalculateAsync(3, ct),
                onSuccess: (result, next) => next.Persist(new Added(result)).ReplyAndStop(new Adjusted(result)),
                onFailure: (error, next) => next.Reply(new AdjustmentRejected(error.Message)))
        };
        var probe = CreateTestProbe();
        var props = Props.Create(() => new EntityActor<RailwayState, RailwayCommand, RailwayEvent>(
            new EntityId("async-stop"), definition, Host.Services, Sys.Scheduler));
        var parent = Sys.ActorOf(Props.Create(() => new TestEntityParent(props, probe.Ref)));
        Send(parent, new Adjust(3));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        finish.SetResult(3);

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

        public async Task InterceptAsync(IPersistentRepresentation message)
        {
            Entered.TrySetResult();
            await Release.Task.WaitAsync(Ct);
        }
    }
}
