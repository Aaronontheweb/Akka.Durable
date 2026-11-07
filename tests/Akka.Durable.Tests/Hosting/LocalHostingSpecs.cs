using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Durable.Tests.Fixtures;
using Akka.Event;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Entities hosted in-process by the local child-per-entity host, addressed through IEntityRef.
/// </summary>
public class LocalHostingSpecs(ITestOutputHelper output) : CounterHostingSpec(output)
{
    protected override ShardOptions? Sharding => null;

    [Fact]
    public async Task Ask_through_an_entity_ref_works_from_outside_an_actor()
    {
        var counter = CounterRef("a");

        await counter.Ask(new Increment(2), AskCancellation());

        Assert.Equal(2, await CountOf(counter));
    }

    [Fact]
    public async Task One_entity_per_id_and_distinct_ids_are_independent()
    {
        var a = CounterRef("a");
        var b = CounterRef("b");

        await a.Ask(new Increment(1), AskCancellation());
        await a.Ask(new Increment(1), AskCancellation());
        await b.Ask(new Increment(5), AskCancellation());

        Assert.Equal(2, await CountOf(a));
        Assert.Equal(5, await CountOf(b));

        // Exactly one child actor per entity id under the host.
        Sys.ActorSelection("/user/durable-counter/*").Tell(new Identify("children"), TestActor);
        var children = await ReceiveNAsync(2, cancellationToken: TestCancellation).ToListAsync(TestCancellation);
        await ExpectNoMsgAsync(NoMsgWindow, TestCancellation);
        Assert.All(children, c => Assert.IsType<ActorIdentity>(c));
    }

    [Fact]
    public async Task Entity_ids_that_are_not_valid_actor_names_still_route()
    {
        var counter = CounterRef("tenant/42 #1");

        await counter.Ask(new Increment(3), AskCancellation());

        Assert.Equal(3, await CountOf(counter));
    }

    private Task<IActorRef> EntityActorOf(string id) =>
        Sys.ActorSelection($"/user/durable-counter/{Uri.EscapeDataString(id)}")
            .ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);

    [Fact]
    public async Task Host_stops_an_entity_that_passivates_itself()
    {
        // Stop sends Passivate to the host; the host must answer with the stop message so the
        // entity's actor actually terminates, rather than leaving the old incarnation running.
        var counter = CounterRef("a");
        await counter.Ask(new Increment(1), AskCancellation());
        var entity = await EntityActorOf("a");
        Watch(entity);

        await counter.Ask(new Shutdown(), AskCancellation());

        await ExpectTerminatedAsync(entity, cancellationToken: TestCancellation);
    }

    [Fact]
    public async Task Stopped_entity_is_reactivated_by_the_next_message_and_recovers_state()
    {
        var counter = CounterRef("a");
        await counter.Ask(new Increment(3), AskCancellation());
        var firstIncarnation = await EntityActorOf("a");
        Watch(firstIncarnation);

        await counter.Ask(new Shutdown(), AskCancellation());
        await ExpectTerminatedAsync(firstIncarnation, cancellationToken: TestCancellation);

        // A new incarnation answers, with its state recovered from the journal.
        Assert.Equal(3, await CountOf(counter));
        Assert.NotEqual(firstIncarnation, await EntityActorOf("a"));
    }

    [Fact]
    public Task Using_an_entity_immediately_after_it_stops_loses_no_messages() =>
        AssertStopThenUseLosesNothing(CounterRef("a"), rounds: 25);

    [Fact]
    public async Task Messages_sent_while_an_entity_passivates_are_delivered_to_its_next_incarnation()
    {
        var counter = CounterRef("a");
        await counter.Ask(new Increment(1), AskCancellation());

        counter.Tell(new Shutdown(), TestActor);
        for (var i = 0; i < 20; i++) counter.Tell(new Increment(1), TestActor);

        for (var i = 0; i < 21; i++) await ExpectMsgAsync<Accepted>(cancellationToken: TestCancellation);
        Assert.Equal(21, await CountOf(counter));
    }

    [Fact]
    public async Task Per_sender_ordering_is_preserved()
    {
        // Interleaving commands with queries makes any reordering visible in the replies.
        var counter = CounterRef("a");
        const int rounds = 200;
        for (var i = 0; i < rounds; i++)
        {
            counter.Tell(new Increment(1), TestActor);
            counter.Tell(new GetCount(), TestActor);
        }

        for (var i = 1; i <= rounds; i++)
        {
            await ExpectMsgAsync<Accepted>(cancellationToken: TestCancellation);
            await ExpectMsgAsync<CountIs>(c => c.Count == i, cancellationToken: TestCancellation);
        }
    }

    [Fact]
    public async Task Run_effects_resolve_services_from_the_host_container()
    {
        await CounterRef("a").Ask(new IncrementAndRecord(1), AskCancellation());

        Assert.Equal(["run:1", "disposed"], Recorder.Entries);
    }

    [Fact]
    public async Task Reminders_are_delivered_to_the_entity_through_its_host()
    {
        var counter = CounterRef("a");

        // Schedules a reminder whose payload is Increment(1).
        counter.Tell(new RemindLater(TimeSpan.FromMilliseconds(200)));

        await AwaitAssertAsync(
            async () => Assert.Equal(1, await CountOf(counter)),
            duration: TimeSpan.FromSeconds(15),
            interval: TimeSpan.FromMilliseconds(250),
            cancellationToken: TestCancellation);
    }

    [Fact]
    public async Task Typed_Tell_effect_converts_the_case_before_boxing()
    {
        var ctx = new DecisionContext<CounterState, CounterCommand, CounterEvent>(
            new EntityId("sender"), Counter.TypeName, DateTimeOffset.UnixEpoch, new CounterState(0, false));
        var effect = Assert.IsType<TellEffect>(
            ctx.None().Tell(CounterRef("a"), new Increment(1)).Effects.Single().Value);

        Assert.IsType<CounterCommand>(effect.Message);
        effect.Recipient.Tell(effect.Message, TestActor);
        await ExpectMsgAsync<Accepted>(cancellationToken: TestCancellation);
    }

    #region State reads

    [Fact]
    public Task GetState_returns_the_committed_state_and_sequence_number() =>
        AssertGetStateReturnsCommittedStateAndSequenceNr(DurableCounterRef("a"));

    [Fact]
    public async Task GetState_of_a_fresh_entity_returns_its_initial_state()
    {
        var snapshot = await DurableCounterRef("fresh").GetStateAsync(AskCancellation());

        Assert.Equal(new CounterState(0, false), snapshot.State);
        Assert.Equal(0, snapshot.SequenceNr);
    }

    [Fact]
    public Task GetState_waits_for_in_flight_effects() =>
        AssertGetStateWaitsForInFlightEffects(DurableCounterRef("a"));

    [Fact]
    public async Task GetState_is_not_treated_as_a_command()
    {
        var unhandled = CreateTestProbe();
        Sys.EventStream.Subscribe(unhandled.Ref, typeof(UnhandledMessage));
        var counter = DurableCounterRef("a");
        await counter.Ask(new Increment(1), AskCancellation());

        var snapshot = await counter.GetStateAsync(AskCancellation());

        Assert.Equal(1, snapshot.SequenceNr);
        await unhandled.ExpectNoMsgAsync(NoMsgWindow, TestCancellation);
        Assert.Equal(1, await CountOf(counter));
    }

    [Fact]
    public async Task A_plain_ref_can_be_upgraded_by_id_to_read_state()
    {
        var plain = CounterRef("a");
        await plain.Ask(new Increment(3), AskCancellation());

        var snapshot = await DurableEntityRefs<CounterCommand, CounterState, CounterEvent>().Get(plain.Id)
            .GetStateAsync(AskCancellation());

        Assert.Equal(3, snapshot.State.Count);
    }

    [Fact]
    public void A_state_type_that_does_not_match_the_definition_is_rejected() =>
        AssertWrongStateTypeIsRejected();

    #endregion
}
