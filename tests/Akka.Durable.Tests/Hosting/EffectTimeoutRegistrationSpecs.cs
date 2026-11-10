using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// <see cref="EntityOptions.EffectTimeout"/> must be positive, for entities and workflows, local and sharded,
/// and the refusal comes at registration, not when the first effect runs.
/// </summary>
public class EffectTimeoutRegistrationSpecs
{
    // Null means the definition carries no options at all, so the defaults apply.
    public static readonly TheoryData<bool, bool, long?, bool> Rows = Build();

    private static TheoryData<bool, bool, long?, bool> Build()
    {
        var data = new TheoryData<bool, bool, long?, bool>();
        long?[] ticks = [0, -1, TimeSpan.MinValue.Ticks, Timeout.InfiniteTimeSpan.Ticks, 1, TimeSpan.FromSeconds(30).Ticks];
        foreach (var workflow in new[] { false, true })
        foreach (var sharded in new[] { false, true })
            foreach (var t in ticks)
                data.Add(workflow, sharded, t, t > 0);
        return data;
    }

    private static void Register(bool workflow, bool sharded, EntityOptions? options)
    {
        var builder = new AkkaConfigurationBuilder(new ServiceCollection(), "test");
        var hosting = sharded ? new ShardedHosting() : null;
        if (workflow)
        {
            builder.WithDurableWorkflow(Checkout.Definition with { Options = options }, hosting);
            return;
        }

        builder.WithDurableEntity(Counter.Definition with { Options = options }, hosting);
    }

    [Theory]
    [MemberData(nameof(Rows))]
    public void Only_a_positive_effect_timeout_can_be_registered(bool workflow, bool sharded, long? ticks, bool accepted)
    {
        var options = ticks is { } t ? EntityOptions.Default with { EffectTimeout = TimeSpan.FromTicks(t) } : null;

        if (accepted)
        {
            Register(workflow, sharded, options);
            return;
        }

        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => Register(workflow, sharded, options));
        Assert.Contains(workflow ? Checkout.Definition.TypeName.Value : Counter.TypeName.Value, ex.Message);
        Assert.Contains(TimeSpan.FromTicks(ticks!.Value).ToString(), ex.Message);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void Default_options_and_no_idle_passivation_are_accepted(bool workflow, bool sharded)
    {
        Register(workflow, sharded, null);
        Register(workflow, sharded, EntityOptions.Default);
        Register(workflow, sharded, EntityOptions.Default with { PassivateIdleAfter = TimeSpan.Zero });
    }
}
