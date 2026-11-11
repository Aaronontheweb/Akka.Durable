using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Reminders.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>The retry policies and the reserved timer keys, without an actor system.</summary>
public sealed class RetryPolicySpecs
{
    public static TheoryData<Func<RetryPolicy>> InvalidPolicies => new()
    {
        () => Retry.Every(TimeSpan.Zero, 3),
        () => Retry.Every(TimeSpan.FromSeconds(1), 1),
        () => Retry.Backoff(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1), 3),
        () => Retry.Backoff(TimeSpan.FromSeconds(-1), TimeSpan.FromSeconds(1), 3),
        () => Retry.Backoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), 1),
        () => Retry.UntilAcknowledged(TimeSpan.FromSeconds(1), TimeSpan.Zero),
    };

    [Theory]
    [MemberData(nameof(InvalidPolicies))]
    public void A_policy_with_a_value_that_is_not_valid_is_refused(Func<RetryPolicy> build) =>
        Assert.Throws<ArgumentOutOfRangeException>(build);

    [Fact]
    public void Backoff_doubles_the_wait_up_to_the_longest_wait()
    {
        var policy = Retry.Backoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), attempts: 6);

        Assert.Equal(
            [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5)],
            Enumerable.Range(1, 5).Select(policy.WaitAfter));
        Assert.True(policy.Allows(6));
        Assert.False(policy.Allows(7));
        Assert.True(Retry.UntilAcknowledged(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1)).Allows(1000));
    }

    [Theory]
    [InlineData("durable-effects-wake")]
    [InlineData("durable-mine")]
    public void A_timer_key_of_the_library_is_refused(string key)
    {
        var none = Decisions.None<CounterCommand, CounterEvent>();

        Assert.Throws<ArgumentException>(() => none.Schedule(key, TimeSpan.FromSeconds(1), new Increment(1)));
        Assert.Throws<ArgumentException>(() => none.CancelTimer(key));
    }
}

/// <summary>Records each attempt of <see cref="Compute"/> and fails the first ones.</summary>
public sealed class FlakyCompute
{
    public ConcurrentQueue<EffectDelivery> Deliveries { get; } = new();

    /// <summary>How many attempts fail before one succeeds.</summary>
    public int Failures { get; set; }
}

public sealed class FlakyComputeHandler(FlakyCompute flaky) : IEffectHandler<Compute, CounterCommand>
{
    public Task<CounterCommand> HandleAsync(Compute effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        flaky.Deliveries.Enqueue(delivery);
        return flaky.Deliveries.Count <= flaky.Failures
            ? throw new InvalidOperationException($"attempt {delivery.Attempt} failed")
            : Task.FromResult<CounterCommand>(new Computed(effect.Request, effect.Request.By * 2));
    }
}

/// <summary>One smoke spec for each behaviour of a handler registered with a retry policy.</summary>
public sealed class OutboxSmokeSpecs(ITestOutputHelper output) : CounterHostingSpec(output)
{
    protected override ShardedHosting? Sharding => null;

    private readonly SwitchableReminderStorage _reminders = new();

    protected override IReminderStorage CreateReminderStorage(ActorSystem system) => _reminders;

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>()
            .AddSingleton<FlakyCompute>()
            .AddEffectHandler<Compute, CounterCommand, FlakyComputeHandler>(Retry.Every(TimeSpan.FromMilliseconds(100), attempts: 3));

    private FlakyCompute Flaky => Host.Services.GetRequiredService<FlakyCompute>();

    private async Task<CounterState> SettledState(string id)
    {
        CounterState state = null!;
        await AwaitAssertAsync(async () =>
        {
            state = (await DurableCounterRef(id).GetStateAsync(AskCancellation())).State;
            Assert.Null(state.Pending);
        }, cancellationToken: TestCancellation);
        return state;
    }

    [Fact]
    public async Task A_handler_that_fails_twice_is_attempted_again_with_the_same_id_and_its_answer_is_decided_once()
    {
        Flaky.Failures = 2;

        await CounterRef("r1").Ask(new IncrementViaService(5, Wait: false), AskCancellation());

        Assert.Equal(10, (await SettledState("r1")).Count);
        var deliveries = Flaky.Deliveries.ToList();
        Assert.Equal([1, 2, 3], deliveries.Select(d => d.Attempt));
        Assert.Single(deliveries.Select(d => d.Id).Distinct());
        Assert.StartsWith("counter/r1/", deliveries[0].Id.Value);
        Assert.Empty(await PendingEffects<CounterCommand>("r1"));
    }

    [Fact]
    public async Task A_limited_policy_that_runs_out_gives_up_as_retries_exhausted_with_the_number_of_attempts()
    {
        Flaky.Failures = 10;

        await CounterRef("r2").Ask(new IncrementViaService(5, Wait: false), AskCancellation());

        var state = await SettledState("r2");
        Assert.Equal("compute:RetriesExhausted", state.LastGaveUp);
        Assert.Equal(0, state.Count);
        Assert.Equal(3, Flaky.Deliveries.Count);
    }

    [Fact]
    public async Task When_the_wake_cannot_be_stored_the_command_fails_and_nothing_is_journaled()
    {
        _reminders.Failing = true;

        await Assert.ThrowsAnyAsync<Exception>(() =>
            CounterRef("r3").Ask(new IncrementViaService(5, Wait: false), AskCancellation()));

        var snapshot = await DurableCounterRef("r3").GetStateAsync(AskCancellation());
        Assert.Equal(new CounterState(0, false), snapshot.State);
        Assert.Equal(0, snapshot.SequenceNr);
        Assert.Empty(Flaky.Deliveries);
    }
}
