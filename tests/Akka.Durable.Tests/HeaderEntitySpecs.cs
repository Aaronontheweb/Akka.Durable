using System.Collections.Concurrent;
using System.Collections.Immutable;
using Akka.Actor;
using Akka.Event;
using Akka.Cluster.Sharding;
using Akka.Durable.Tests.Fixtures;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>
/// Headers beside a command, against the entity actor itself, on the Akka.Persistence TestKit journal. A command that
/// comes with headers is a <see cref="Carried"/>, which the specs send to the actor the way a ref would send it to a
/// region. A "crash" is a PoisonPill; the next incarnation recovers from the journal or a snapshot. The counter's effects
/// have a short retry policy, and its effect time limit is short.
/// </summary>
public sealed class HeaderEntitySpecs(ITestOutputHelper output) : ObserverSpecsBase(output)
{
    // The retry of an attempt that was cut off by a restart is due after this time and the policy's wait.
    protected override EntityOptions Options => EntityOptions.Default with { EffectTimeout = TimeSpan.FromMilliseconds(300) };

    protected override void AddObservers(IServiceCollection services) => services
        .AddDurableObserver<RecordingObserver>()
        .AddDurableHeaderPropagator<RequestIdPropagator>();

    private readonly ConcurrentQueue<(string Effect, string? Request)> _sightings = new();

    private RequestIdPropagator Propagator =>
        Host.Services.GetServices<IHeaderPropagator>().OfType<RequestIdPropagator>().Single();

    private static ImmutableDictionary<string, string> Request(string id) =>
        ImmutableDictionary<string, string>.Empty.Add(RequestIdPropagator.Key, id);

    /// <summary>Sends the command beside the headers, as a ref does when a propagator adds some.</summary>
    private void SendCarried(IActorRef entity, CounterCommand command, string request) =>
        entity.Tell(new Carried(command, Request(request)), TestActor);

    /// <summary>What the handler of a probe effect sees as the request id of its flow of execution, and how it answers.</summary>
    private void SeeRequestInHandler(Func<Probe, string> answer) =>
        World.OnProbe = (probe, _) =>
        {
            _sightings.Enqueue((probe.Name, RequestIdPropagator.Current));
            return Task.FromResult<CounterCommand>(new NoHandler(answer(probe)));
        };

    [Fact]
    public async Task A_command_that_comes_with_headers_is_decided_as_usual_and_its_event_carries_them()
    {
        var entity = Spawn("carried");

        SendCarried(entity, new Increment(1), "req-1");
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct); // the reply goes to the sender of the carrier
        Send(entity, new Increment(2));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        var events = Seen.Events;
        HeaderAssert.Request("req-1", events[0].Headers);
        Assert.Empty(events[1].Headers); // a command that came alone has none
        Assert.Equal([new Increment(1), new Increment(2)], events.Select(e => e.Command)); // Decide got the commands
        Assert.Equal(3, (await StateOf(entity)).Count);
    }

    [Fact]
    public async Task The_message_sent_is_the_bare_command_unless_a_propagator_adds_headers()
    {
        var region = CreateTestProbe();
        var id = new EntityId("wire");
        var withPropagator = new EntityRef<CounterCommand>(id, region, Host.Services.GetRequiredService<HeaderPropagation>());
        var without = new EntityRef<CounterCommand>(id, region, HeaderPropagation.None);

        RequestIdPropagator.Current = "req-w";
        without.Tell(new Increment(1));
        var bare = await region.ExpectMsgAsync<ShardingEnvelope>(cancellationToken: Ct);
        withPropagator.Tell(new Increment(2));
        var tell = await region.ExpectMsgAsync<ShardingEnvelope>(cancellationToken: Ct);
        using var asked = new CancellationTokenSource();
        var ask = withPropagator.Ask(new Increment(3), asked.Token);
        var carriedAsk = await region.ExpectMsgAsync<ShardingEnvelope>(cancellationToken: Ct);
        await asked.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ask);

        // A propagator is registered, and nothing is injected: the message is the bare command again.
        RequestIdPropagator.Current = null;
        withPropagator.Tell(new Increment(4));
        var nothing = await region.ExpectMsgAsync<ShardingEnvelope>(cancellationToken: Ct);

        Assert.Equal(new Increment(1), Assert.IsType<CounterCommand>(bare.Message).Value);
        Assert.Equal(new Increment(4), Assert.IsType<CounterCommand>(nothing.Message).Value);
        foreach (var (envelope, by) in new[] { (tell, 2), (carriedAsk, 3) })
        {
            var carried = Assert.IsType<Carried>(envelope.Message);
            Assert.Equal(new Increment(by), Assert.IsType<CounterCommand>(carried.Command).Value);
            HeaderAssert.Request("req-w", carried.Headers);
        }

        Assert.All([bare, tell, carriedAsk, nothing], envelope => Assert.Equal("wire", envelope.EntityId));
    }

    [Fact]
    public async Task A_carrier_that_holds_no_command_is_refused_like_any_other_foreign_message()
    {
        Sys.EventStream.Subscribe(TestActor, typeof(UnhandledMessage));
        var entity = Spawn("foreign");

        entity.Tell(new Carried("not a command", Request("req-f")), TestActor);

        Assert.Equal("not a command", (await ExpectMsgAsync<UnhandledMessage>(cancellationToken: Ct)).Message);
        Send(entity, new Increment(1)); // the entity goes on
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Assert.Equal([new Increment(1)], Seen.Events.Select(e => e.Command));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task An_effect_that_is_attempted_again_after_a_restart_runs_with_the_headers_of_its_decision(
        bool fromSnapshot)
    {
        // With a snapshot after every write, the restart reads the pending effect, and its headers, from the snapshot.
        var options = fromSnapshot
            ? Options with { SnapshotEvery = 1, KeepSnapshots = 1, DeleteEventsOnSnapshot = true }
            : Options;
        SeeRequestInHandler(probe => $"answer:{probe.Name}");
        World.Hold("a");
        var entity = Spawn("durable", options);
        SendCarried(entity, new NoHandler("issue:a"), "req-d");
        await Await(() => World.ProbeCalls.Count == 1); // attempt 1 is in its handler, held
        await Kill(entity);
        World.Release("a");

        var recovered = Spawn("durable", options);

        await AwaitAssertAsync(async () => Assert.Equal(101, (await StateOf(recovered)).Count), cancellationToken: Ct);
        Assert.Equal([1, 2], World.ProbeCalls.Select(call => call.Delivery.Attempt));
        Assert.Equal([("a", "req-d")], _sightings); // only attempt 2 got past its hold, and it saw the request
        Assert.Equal(2, Propagator.Restored.Count); // both attempts were restored, each with the stored headers
        Assert.All(Propagator.Restored, headers => HeaderAssert.Request("req-d", headers));
        Assert.Equal(0, Propagator.Open); // and undone when the handler finished
    }

    [Fact]
    public async Task An_effect_issued_while_deciding_the_answer_of_another_effect_inherits_its_headers()
    {
        SeeRequestInHandler(probe => probe.Name == "a" ? "issue:b" : $"answer:{probe.Name}");
        var entity = Spawn("chain");

        SendCarried(entity, new NoHandler("issue:a"), "req-c");

        await Await(() => Seen.Effects.Count(e => e.Stage == EffectStage.Acknowledged) == 2);
        Assert.Equal([("a", "req-c"), ("b", "req-c")], _sightings); // the second hop ran with them too
        Assert.Equal(6, Seen.Effects.Length); // issued, started and acknowledged, for both
        Assert.All(Seen.Effects, e => HeaderAssert.Request("req-c", e.Headers));
        Assert.Equal(3, Seen.Events.Length); // the command, the answer of a, the answer of b
        Assert.All(Seen.Events, e => HeaderAssert.Request("req-c", e.Headers));
    }

    [Fact]
    public async Task A_command_a_timer_delivers_has_no_headers_even_when_the_command_that_set_the_timer_had_some()
    {
        var entity = Spawn("timer");

        SendCarried(entity, new NoHandler("later:t"), "req-t");

        // The timer fires, and its command issues an effect that is answered.
        await Await(() => Seen.Effects.Any(e => e.Stage == EffectStage.Acknowledged));
        HeaderAssert.Request("req-t", Assert.Single(Seen.Timers, t => t.Stage == TimerStage.Set).Headers);
        Assert.Empty(Assert.Single(Seen.Timers, t => t.Stage == TimerStage.Fired).Headers);
        Assert.Equal(new EventCause(CauseKind.Timer, "later"), Seen.Events[0].Cause);
        Assert.All(Seen.Events, e => Assert.Empty(e.Headers));
        Assert.All(Seen.Effects, e => Assert.Empty(e.Headers));
        Assert.Empty(Propagator.Restored); // an effect with no headers restores nothing
    }
}

/// <summary>
/// A timer is a starting point of its own. A propagator that overrides <c>InjectForTimer</c> gives the decision that a
/// fired timer starts its headers, and they go where a command's headers go. A propagator that throws there is skipped.
/// </summary>
public sealed class TimerHeaderSpecs(ITestOutputHelper output) : ObserverSpecsBase(output)
{
    protected override EntityOptions Options => EntityOptions.Default with { EffectTimeout = TimeSpan.FromMilliseconds(300) };

    protected override void AddObservers(IServiceCollection services) => services
        .AddDurableObserver<RecordingObserver>()
        .AddDurableHeaderPropagator<ThrowingPropagator>()
        .AddDurableHeaderPropagator<TimerOriginPropagator>();

    [Fact]
    public async Task A_propagator_that_marks_a_timer_gives_its_decision_headers_that_go_where_a_commands_headers_go()
    {
        const string mark = "timer:counter/timer-origin/later";
        var seen = new ConcurrentQueue<string?>();
        World.OnProbe = (probe, _) =>
        {
            seen.Enqueue(TimerOriginPropagator.Current);
            return Task.FromResult<CounterCommand>(new NoHandler($"answer:{probe.Name}"));
        };

        Send(Spawn("timer-origin"), new NoHandler("later:t")); // a bare command: nothing to carry to the timer

        await Await(() => Seen.Effects.Any(e => e.Stage == EffectStage.Acknowledged));
        Assert.Empty(Assert.Single(Seen.Timers, t => t.Stage == TimerStage.Set).Headers); // not the setter's headers
        HeaderAssert.Is(TimerOriginPropagator.Key, mark, Assert.Single(Seen.Timers, t => t.Stage == TimerStage.Fired).Headers);
        Assert.Equal(2, Seen.Events.Length); // the timer's command, and the answer of its effect
        Assert.All(Seen.Events, e => HeaderAssert.Is(TimerOriginPropagator.Key, mark, e.Headers));
        Assert.Equal(3, Seen.Effects.Length);
        Assert.All(Seen.Effects, e => HeaderAssert.Is(TimerOriginPropagator.Key, mark, e.Headers));
        Assert.Equal([mark], seen); // the handler ran with them restored
    }

    [Fact]
    public async Task A_timer_that_Decide_stashes_fires_once_when_it_is_decided_not_each_time_it_is_delivered()
    {
        var entity = Spawn("timer-stash");
        Send(entity, new LockCounter());
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Send(entity, new RemindLater(TimeSpan.FromMilliseconds(50))); // its Increment is stashed while the counter is locked
        await Await(() => World.DecidedNow.Contains("increment:1")); // Decide has seen the delivery, and stashed it
        Assert.DoesNotContain(Seen.Timers, t => t.Stage == TimerStage.Fired);

        Send(entity, new UnlockCounter()); // releases the stash: the same delivery is decided now
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        await Await(() => Seen.Events.Any(e => e.Cause.Kind == CauseKind.Timer));
        var fired = Assert.Single(Seen.Timers, t => t.Stage == TimerStage.Fired);
        HeaderAssert.Is(TimerOriginPropagator.Key, "timer:counter/timer-stash/tick", fired.Headers);
        var decided = Assert.Single(Seen.Events, e => e.Cause.Kind == CauseKind.Timer);
        HeaderAssert.Is(TimerOriginPropagator.Key, "timer:counter/timer-stash/tick", decided.Headers);
    }
}

/// <summary>The first propagator throws in <c>Inject</c>. The second one does its work.</summary>
public sealed class ThrowingHeaderPropagatorSpecs(ITestOutputHelper output) : ObserverSpecsBase(output)
{
    protected override void AddObservers(IServiceCollection services) => services
        .AddDurableObserver<RecordingObserver>()
        .AddDurableHeaderPropagator<ThrowingPropagator>()
        .AddDurableHeaderPropagator<RequestIdPropagator>();

    [Fact]
    public async Task A_propagator_that_throws_does_not_fail_the_send_and_adds_nothing_of_its_own()
    {
        // The other propagator's headers go, and what the one that threw had added does not.
        var region = CreateTestProbe();
        var counter = new EntityRef<CounterCommand>(new EntityId("throws"), region, Host.Services.GetRequiredService<HeaderPropagation>());
        RequestIdPropagator.Current = "req-x";
        counter.Tell(new Increment(1));
        var envelope = await region.ExpectMsgAsync<ShardingEnvelope>(cancellationToken: Ct);
        HeaderAssert.Is(RequestIdPropagator.Key, "req-x", Assert.IsType<Carried>(envelope.Message).Headers);
    }
}
