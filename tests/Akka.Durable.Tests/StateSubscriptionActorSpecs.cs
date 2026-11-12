using System.Collections.Immutable;
using Akka.Actor;
using Akka.Event;
using Akka.Cluster.Sharding;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Akka.TestKit;
using Akka.Reminders;
using Akka.Reminders.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>
/// The subscription at the boundaries a test can stand on: a probe in place of the region, and the entity actor
/// built directly under a probe as its parent. The test kit has no virtual clock, so a message that must not come is
/// shown with a short real window.
/// </summary>
public class StateSubscriptionActorSpecs(ITestOutputHelper output) : PersistenceTestKit(output: output)
{
    private static readonly TimeSpan Short = TimeSpan.FromMilliseconds(400);
    private static readonly TimeSpan MustHappen = TimeSpan.FromSeconds(20);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    protected override void ConfigureServices(HostBuilderContext context, IServiceCollection services)
    {
        services.AddSingleton<EffectRecorder>();
        services.AddSingleton<Gate>();
        services.AddScoped<ScopedRecorder>();
        services.AddCounterHandlers();
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        base.ConfigureAkka(builder, provider);
        builder.WithLocalReminders(reminders => reminders
            .WithStorage(_ => new InMemoryReminderStorage())
            .WithResolver(system => new DirectEntityReminderResolver(system)));
    }

    private Props EntityProps(string entityId)
    {
        var id = new EntityId(entityId);
        var services = Host.Services;
        var time = Sys.Scheduler;
        return Props.Create(() =>
            new EntityActor<CounterState, CounterCommand, CounterEvent>(id, Counter.Definition, services, time));
    }

    private void Send(IActorRef entity, CounterCommand command, IActorRef? sender = null) =>
        entity.Tell(command, sender ?? TestActor);

    private EntityRef<CounterCommand, CounterState, CounterEvent> CounterAt(IActorRef region, string id) =>
        new(new EntityId(id), region, Sys);

    private WorkflowRef<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> WorkflowAt(IActorRef region, string id) =>
        new(new WorkflowId(id), region, Sys);

    /// <summary>Expects one subscribe. Returns the watcher that sent it: the probe answers it.</summary>
    private async Task<IActorRef> ExpectSubscribe(TestProbe region, string id)
    {
        var envelope = await region.ExpectMsgAsync<ShardingEnvelope>(MustHappen, cancellationToken: Ct);
        Assert.Equal(id, envelope.EntityId);
        Assert.IsType<SubscribeToState>(envelope.Message);
        return region.LastSender;
    }

    private static WfState Running(CheckoutState state) =>
        new WfRunning(new PlaceOrder("w", 1), state, 1, DateTimeOffset.UtcNow, [], []);

    private static WfState Finished(CheckoutOutcome outcome) =>
        new WfFinished(new PlaceOrder("w", 1), outcome, DateTimeOffset.UtcNow);

    #region An entity with no subscribers

    [Fact]
    public async Task An_entity_with_no_subscribers_sends_nothing_but_its_reply_after_a_write()
    {
        var dead = CreateTestProbe();
        Sys.EventStream.Subscribe(dead.Ref, typeof(AllDeadLetters));
        var parentProbe = CreateTestProbe();
        var props = EntityProps("c1");
        var parent = Sys.ActorOf(Props.Create(() => new TestEntityParent(props, parentProbe.Ref, false)));

        Send(parent, new Increment(1));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Send(parent, new Increment(2));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        // Nothing else: not to the sender, not to the parent, not to dead letters.
        await ExpectNoMsgAsync(Short, Ct);
        await parentProbe.ExpectNoMsgAsync(Short, Ct);
        await dead.ExpectNoMsgAsync(Short, Ct);
    }

    #endregion

    #region The wire protocol of a subscriber

    [Fact]
    public async Task A_subscriber_gets_the_state_at_once_from_the_entity_actor_and_a_snapshot_after_each_write_with_events()
    {
        var entity = Sys.ActorOf(EntityProps("c2"));
        var subscriber = CreateTestProbe();
        Send(entity, new Increment(1));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        entity.Tell(SubscribeToState.Instance, subscriber.Ref);

        var initial = await subscriber.ExpectMsgAsync<EntityStateReply>(MustHappen, cancellationToken: Ct);
        Assert.Equal(entity, subscriber.LastSender);
        Assert.Equal(new CounterState(1, false), initial.State);
        Assert.Equal(1, initial.SequenceNr);

        Send(entity, new Increment(2));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        var pushed = await subscriber.ExpectMsgAsync<EntityStateReply>(MustHappen, cancellationToken: Ct);
        Assert.Equal(new CounterState(3, false), pushed.State);
        Assert.Equal(2, pushed.SequenceNr);
        await subscriber.ExpectNoMsgAsync(Short, Ct);
    }

    [Fact]
    public async Task A_subscriber_gets_no_snapshot_for_a_write_that_holds_only_a_library_record()
    {
        var entity = Sys.ActorOf(EntityProps("c3"));
        var subscriber = CreateTestProbe();
        entity.Tell(SubscribeToState.Instance, subscriber.Ref);
        await subscriber.ExpectMsgAsync<EntityStateReply>(MustHappen, cancellationToken: Ct);

        Send(entity, new Announce("hello"));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        await subscriber.ExpectNoMsgAsync(Short, Ct);
    }

    [Fact]
    public async Task A_subscriber_gets_no_snapshot_for_a_write_the_journal_fails()
    {
        var entity = Sys.ActorOf(EntityProps("c4"));
        var subscriber = CreateTestProbe();
        entity.Tell(SubscribeToState.Instance, subscriber.Ref);
        await subscriber.ExpectMsgAsync<EntityStateReply>(MustHappen, cancellationToken: Ct);
        Watch(entity);

        await WithJournalWrite(write => write.Fail(), async () =>
        {
            Send(entity, new Increment(1));
            await ExpectTerminatedAsync(entity, cancellationToken: Ct);
        });

        await subscriber.ExpectNoMsgAsync(Short, Ct);
    }

    [Fact]
    public async Task An_entity_sends_nothing_more_to_a_subscriber_that_has_stopped()
    {
        var dead = CreateTestProbe();
        Sys.EventStream.Subscribe(dead.Ref, typeof(AllDeadLetters));
        var entity = Sys.ActorOf(EntityProps("c5"));
        var subscriber = CreateTestProbe();
        entity.Tell(SubscribeToState.Instance, subscriber.Ref);
        await subscriber.ExpectMsgAsync<EntityStateReply>(MustHappen, cancellationToken: Ct);
        var watcher = CreateTestProbe();
        watcher.Watch(subscriber.Ref);

        Sys.Stop(subscriber.Ref);
        await watcher.ExpectTerminatedAsync(subscriber.Ref, MustHappen, cancellationToken: Ct);
        // A state read is answered after the entity has taken the stop of the subscriber from its mailbox.
        entity.Tell(GetEntityState.Instance, TestActor);
        await ExpectMsgAsync<EntityStateReply>(cancellationToken: Ct);

        Send(entity, new Increment(1));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Send(entity, new Increment(1));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        await dead.ExpectNoMsgAsync(Short, Ct);
    }

    [Fact]
    public async Task A_subscriber_gets_the_recovered_state_when_the_entity_restarts_and_each_later_snapshot()
    {
        var parentProbe = CreateTestProbe();
        var props = EntityProps("c7");
        var parent = Sys.ActorOf(Props.Create(() => new TestEntityParent(props, parentProbe.Ref, false)));
        var subscriber = CreateTestProbe();
        Send(parent, new Increment(1));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        parent.Tell(SubscribeToState.Instance, subscriber.Ref);
        await subscriber.ExpectMsgAsync<EntityStateReply>(MustHappen, cancellationToken: Ct);

        // A command with no case in Decide: the decision throws, and the parent restarts the entity.
        parent.Tell(default(CounterCommand), TestActor);

        var recovered = await subscriber.ExpectMsgAsync<EntityStateReply>(MustHappen, cancellationToken: Ct);
        Assert.Equal(new CounterState(1, false), recovered.State);
        Assert.Equal(1, recovered.SequenceNr);

        Send(parent, new Increment(1));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        var later = await subscriber.ExpectMsgAsync<EntityStateReply>(MustHappen, cancellationToken: Ct);
        Assert.Equal(new CounterState(2, false), later.State);
        Assert.Equal(2, later.SequenceNr);
    }

    [Fact]
    public async Task Two_subscribers_each_get_each_snapshot()
    {
        var entity = Sys.ActorOf(EntityProps("c6"));
        var one = CreateTestProbe();
        var two = CreateTestProbe();
        entity.Tell(SubscribeToState.Instance, one.Ref);
        entity.Tell(SubscribeToState.Instance, two.Ref);
        await one.ExpectMsgAsync<EntityStateReply>(MustHappen, cancellationToken: Ct);
        await two.ExpectMsgAsync<EntityStateReply>(MustHappen, cancellationToken: Ct);

        Send(entity, new Increment(4));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        Assert.Equal(new CounterState(4, false), (await one.ExpectMsgAsync<EntityStateReply>(MustHappen, cancellationToken: Ct)).State);
        Assert.Equal(new CounterState(4, false), (await two.ExpectMsgAsync<EntityStateReply>(MustHappen, cancellationToken: Ct)).State);
    }

    #endregion

    #region Entity refs against a probe in place of the region

    [Fact]
    public async Task An_entity_wait_returns_after_one_round_trip_when_the_state_already_satisfies_the_predicate()
    {
        var region = CreateTestProbe();
        var counter = CounterAt(region.Ref, "x");

        var wait = counter.WaitForStateAsync(s => s.Count == 2, Ct);
        var watcher = await ExpectSubscribe(region, "x");
        watcher.Tell(new EntityStateReply(new CounterState(2, false), 5), region.Ref);

        Assert.Equal(new StateSnapshot<CounterState>(new CounterState(2, false), 5), await wait.WaitAsync(MustHappen, Ct));
        await region.ExpectNoMsgAsync(Short, Ct);
    }

    [Fact]
    public async Task A_watcher_that_gets_no_answer_subscribes_again_until_an_entity_answers()
    {
        var region = CreateTestProbe();
        var counter = CounterAt(region.Ref, "x");

        var wait = counter.WaitForStateAsync(_ => true, Ct);
        var watcher = await ExpectSubscribe(region, "x");

        // No answer: it asks again, about once a second.
        watcher = await ExpectSubscribe(region, "x");
        watcher.Tell(new EntityStateReply(new CounterState(0, false), 0), region.Ref);

        Assert.Equal(0, (await wait.WaitAsync(MustHappen, Ct)).SequenceNr);
    }

    [Fact]
    public async Task An_entity_wait_returns_when_a_pushed_snapshot_comes_to_satisfy_it()
    {
        var region = CreateTestProbe();
        var counter = CounterAt(region.Ref, "x");

        var wait = counter.WaitForStateAsync(s => s.Count >= 2, Ct);
        var watcher = await ExpectSubscribe(region, "x");
        watcher.Tell(new EntityStateReply(new CounterState(0, false), 0), region.Ref);
        watcher.Tell(new EntityStateReply(new CounterState(1, false), 1), region.Ref);
        await Assert.ThrowsAsync<TimeoutException>(() => wait.WaitAsync(Short, Ct));
        watcher.Tell(new EntityStateReply(new CounterState(2, false), 2), region.Ref);

        Assert.Equal(2, (await wait.WaitAsync(MustHappen, Ct)).State.Count);
        await region.ExpectNoMsgAsync(Short, Ct);
    }

    [Fact]
    public async Task WaitForOutcome_sends_one_subscribe_and_never_reads_the_state()
    {
        var region = CreateTestProbe();
        var workflow = WorkflowAt(region.Ref, "w");

        var wait = workflow.WaitForOutcome(Ct);
        var watcher = await ExpectSubscribe(region, "w");
        watcher.Tell(new EntityStateReply(Running(new Reserving()), 1), region.Ref);
        // No polling: no read follows, and no second subscribe.
        await region.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(1500), Ct);
        await Assert.ThrowsAsync<TimeoutException>(() => wait.WaitAsync(TimeSpan.FromMilliseconds(100), Ct));

        watcher.Tell(new EntityStateReply(Running(new Charging("r")), 2), region.Ref);
        await region.ExpectNoMsgAsync(Short, Ct);
        watcher.Tell(new EntityStateReply(Finished(new OrderPlaced("charge")), 3), region.Ref);

        Assert.Equal(new OrderPlaced("charge"), (await wait.WaitAsync(MustHappen, Ct)).Value);
        await region.ExpectNoMsgAsync(Short, Ct);
    }

    [Fact]
    public async Task WaitForOutcome_throws_InvalidOperationException_when_the_workflow_has_not_been_started()
    {
        var region = CreateTestProbe();
        var workflow = WorkflowAt(region.Ref, "w");

        var wait = workflow.WaitForOutcome(Ct);
        var watcher = await ExpectSubscribe(region, "w");
        watcher.Tell(new EntityStateReply((WfState)new WfNotStarted(), 0), region.Ref);

        await Assert.ThrowsAsync<InvalidOperationException>(() => wait.WaitAsync(MustHappen, Ct));
        await region.ExpectNoMsgAsync(Short, Ct);
    }

    [Fact]
    public async Task A_workflow_wait_returns_at_once_when_the_status_already_satisfies_the_predicate()
    {
        var region = CreateTestProbe();
        var workflow = WorkflowAt(region.Ref, "w");

        var wait = workflow.WaitForStateAsync(s => s.Value is WorkflowRunning<CheckoutState>, Ct);
        var watcher = await ExpectSubscribe(region, "w");
        watcher.Tell(new EntityStateReply(Running(new Reserving()), 4), region.Ref);

        var found = await wait.WaitAsync(MustHappen, Ct);
        Assert.Equal(4, found.SequenceNr);
        Assert.IsType<WorkflowRunning<CheckoutState>>(found.State.Value);
        await region.ExpectNoMsgAsync(Short, Ct);
    }

    [Fact]
    public async Task A_workflow_wait_returns_when_the_status_comes_to_satisfy_the_predicate()
    {
        var region = CreateTestProbe();
        var workflow = WorkflowAt(region.Ref, "w");

        var wait = workflow.WaitForStateAsync(s => s.Value is WorkflowFinished<CheckoutOutcome>, Ct);
        var watcher = await ExpectSubscribe(region, "w");
        watcher.Tell(new EntityStateReply(Running(new Reserving()), 1), region.Ref);
        await Assert.ThrowsAsync<TimeoutException>(() => wait.WaitAsync(Short, Ct));
        watcher.Tell(new EntityStateReply(Finished(new OrderFailed("out of stock")), 2), region.Ref);

        var found = await wait.WaitAsync(MustHappen, Ct);
        Assert.Equal(2, found.SequenceNr);
        Assert.IsType<WorkflowFinished<CheckoutOutcome>>(found.State.Value);
        await region.ExpectNoMsgAsync(Short, Ct);
    }

    [Fact]
    public async Task A_workflow_wait_throws_when_its_token_fires()
    {
        var region = CreateTestProbe();
        var workflow = WorkflowAt(region.Ref, "w");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);

        var wait = workflow.WaitForStateAsync(_ => false, cts.Token);
        var watcher = await ExpectSubscribe(region, "w");
        watcher.Tell(new EntityStateReply(Running(new Reserving()), 1), region.Ref);
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait.WaitAsync(MustHappen, Ct));
    }

    [Fact]
    public async Task A_workflow_stream_ends_after_the_finished_snapshot_and_reads_nothing()
    {
        var region = CreateTestProbe();
        var workflow = WorkflowAt(region.Ref, "w");
        var read = new List<StateSnapshot<WorkflowStatus<CheckoutState, CheckoutOutcome>>>();

        var reading = Task.Run(async () =>
        {
            await foreach (var snapshot in workflow.WatchStateAsync(Ct)) read.Add(snapshot);
        }, Ct);
        var watcher = await ExpectSubscribe(region, "w");
        watcher.Tell(new EntityStateReply((WfState)new WfNotStarted(), 0), region.Ref);
        watcher.Tell(new EntityStateReply(Running(new Reserving()), 1), region.Ref);
        watcher.Tell(new EntityStateReply(Finished(new OrderPlaced("charge")), 2), region.Ref);

        // It ends without a token and without a stop of the reader.
        await reading.WaitAsync(MustHappen, Ct);

        // A reader that is slow may skip states, but it always reads the finished one.
        Assert.Equal(2, read[^1].SequenceNr);
        Assert.IsType<WorkflowFinished<CheckoutOutcome>>(read[^1].State.Value);
        Assert.Equal(read.Select(s => s.SequenceNr).Order(), read.Select(s => s.SequenceNr));
        await region.ExpectNoMsgAsync(Short, Ct);
    }

    #endregion
}
