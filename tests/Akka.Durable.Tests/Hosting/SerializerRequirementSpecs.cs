using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// <c>WithDurableEntity</c> refuses to start an entity type whose state, command or event type would fall
/// back to the System.Object serializer.
/// </summary>
public class SerializerRequirementSpecs
{
    // Deliberately carry no [AkkaSerializable] and no serializer registration.
    public sealed record PlainState(int Value);
    public sealed record PlainCommand(int Value);
    public sealed record PlainEvent(int Value);

    private static readonly EntityDefinition<PlainState, PlainCommand, PlainEvent> PlainDefinition = new(
        new EntityTypeName("plain"),
        _ => new PlainState(0),
        (_, _, ctx) => ctx.Ignore(),
        (state, _) => state);

    [Fact]
    public async Task Entity_types_without_a_serializer_fail_host_startup()
    {
        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDurableEntities();
                services.AddAkka("serializer-requirement", builder => builder.WithDurableEntity(PlainDefinition));
            })
            .Build();

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => host.StartAsync(TestContext.Current.CancellationToken));

        var message = ex.ToString();
        Assert.Contains("plain", message);
        Assert.Contains(typeof(PlainState).FullName!, message);
        Assert.Contains(typeof(PlainCommand).FullName!, message);
        Assert.Contains(typeof(PlainEvent).FullName!, message);

        await host.StopAsync(TestContext.Current.CancellationToken);
    }
}
