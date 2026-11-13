using Akka.Durable.Tests.Fixtures;
using Akka.Event;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// What the host cannot see when it starts, through the public hosting API: the host starts, and the first
/// <c>Execute</c> is refused before the write.
/// </summary>
public abstract class ExecuteRefusedHostingSpec(ITestOutputHelper output) : CounterHostingSpec(output)
{
    protected override ShardedHosting? Sharding => null;

    /// <summary>Waits for the error the entity's supervisor logs for <paramref name="refusal"/>.</summary>
    protected Task AwaitRefusal(System.Collections.Concurrent.ConcurrentQueue<LogEvent> log, string refusal) =>
        AwaitAssertAsync(
            () => Assert.Contains(log, e => e is Error && e.Cause is InvalidOperationException && e.Cause.Message == refusal),
            cancellationToken: TestCancellation);
}

/// <summary>
/// A counter whose definition has no <c>GaveUp</c>, and one effect handler with no answer. The registration of such a
/// handler does not name the entity type, so the host starts.
/// </summary>
public sealed class HostedExecuteWithoutGaveUpSpecs(ITestOutputHelper output) : ExecuteRefusedHostingSpec(output)
{
    protected override EntityDefinition<CounterState, CounterCommand, CounterEvent> Definition =>
        Counter.Definition with { GaveUp = null };

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>()
            .AddEffectHandler<Announcement, AnnounceHandler>();

    [Fact]
    public async Task The_host_starts_and_the_first_Execute_is_refused_before_the_write_with_the_argument_to_add()
    {
        var counter = DurableCounterRef("n1");
        await counter.Ask(new Increment(1), AskCancellation()); // the host started, and the entity runs
        var log = LogCollector.Attach(Sys);

        CounterRef("n1").Tell(new Announce("hello")); // issues an Announcement; the reply comes after the write

        await AwaitRefusal(log,
            "Entity type [counter] issued the effect Announcement, and its definition does not say which command " +
            "Decide gets when an effect gets no answer. Add the argument \"GaveUp: gaveUp => new " +
            "YourCommand(gaveUp.Reason)\" to the EntityDefinition of [counter], where YourCommand is one of its commands " +
            "(CounterCommand). The effect was not journaled and not run.");

        var after = await counter.GetStateAsync(AskCancellation());
        Assert.Equal(new CounterState(1, false), after.State);
        Assert.Equal(1, after.SequenceNr); // the increment alone
        Assert.Empty(await PendingEffects<CounterCommand>("n1"));
        Assert.DoesNotContain(Recorder.Entries, entry => entry.StartsWith("announce:", StringComparison.Ordinal));
    }
}

/// <summary>
/// A counter with its <c>GaveUp</c>, and a handler for its effect <see cref="Compute"/> that answers with the railway's
/// command type. At start that handler looks like the railway's own, so the host starts.
/// </summary>
public sealed class HostedExecuteAnswerMismatchSpecs(ITestOutputHelper output) : ExecuteRefusedHostingSpec(output)
{
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>()
            .AddEffectHandler<Compute, RailwayCommand, EffectRouteStartupSpecs.ComputeForRailway>();

    [Fact]
    public async Task The_host_starts_and_the_first_Execute_is_refused_before_the_write()
    {
        var counter = DurableCounterRef("m1");
        await counter.Ask(new Increment(1), AskCancellation());
        var log = LogCollector.Attach(Sys);

        CounterRef("m1").Tell(new IncrementViaService(3, Wait: false)); // one event and the effect Compute

        await AwaitRefusal(log,
            "Entity type [counter] issued the effect Compute, and its handler ComputeForRailway answers with a " +
            "RailwayCommand. The answer goes to the entity's Decide, so it must be a CounterCommand. Register the " +
            "handler with services.AddEffectHandler<Compute, CounterCommand, YourHandler>(), or with " +
            "services.AddEffectHandler<Compute, YourHandler>() for a handler with no answer. The effect was not " +
            "journaled and not run.");

        // The event of the refused decision was not stored: no service increment is pending.
        var after = await counter.GetStateAsync(AskCancellation());
        Assert.Equal(new CounterState(1, false), after.State);
        Assert.Equal(1, after.SequenceNr);
        Assert.Empty(await PendingEffects<CounterCommand>("m1"));
    }
}
