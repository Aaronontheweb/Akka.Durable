using Akka.Durable.Abstractions.Entities;
using Xunit;

namespace Akka.Durable.Entities.Tests;

public class EntityOptionsSpecs
{
    [Fact]
    public void Default_persistence_id_is_type_name_dash_entity_id()
    {
        // The persistence id decides which journal history an entity reads, so this format is a
        // compatibility contract. Also guards against formatting the record structs themselves,
        // which would yield "EntityTypeName { Value = invoices }-EntityId { Value = invoice-123 }".
        var id = EntityOptions.DefaultPersistenceIdFactory(
            new EntityId("invoice-123"), new EntityTypeName("invoices"));

        Assert.Equal("invoices-invoice-123", id);
    }

    [Fact]
    public void Defaults_match_the_documented_policy()
    {
        var options = EntityOptions.Default;

        Assert.Same(EntityOptions.DefaultPersistenceIdFactory, options.PersistenceIdFactory);
        Assert.Equal(TimeSpan.FromSeconds(30), options.EffectTimeout);
        Assert.Equal(100, options.SnapshotEvery);
        Assert.Equal(2, options.KeepSnapshots);
        Assert.False(options.DeleteEventsOnSnapshot);
        Assert.Null(options.JournalPluginId);
        Assert.Null(options.SnapshotPluginId);
    }

    [Fact]
    public void WithPersistenceId_returns_a_copy_and_leaves_Default_untouched()
    {
        Func<EntityId, EntityTypeName, string> custom = (id, type) => $"custom/{type.Value}/{id.Value}";

        var options = EntityOptions.Default.WithPersistenceId(custom);

        Assert.NotSame(EntityOptions.Default, options);
        Assert.Same(custom, options.PersistenceIdFactory);
        Assert.Equal("custom/counter/42",
            options.PersistenceIdFactory(new EntityId("42"), new EntityTypeName("counter")));
        Assert.Same(EntityOptions.DefaultPersistenceIdFactory, EntityOptions.Default.PersistenceIdFactory);
    }

    [Fact]
    public void Definition_without_options_leaves_Options_null()
    {
        // The actor falls back to EntityOptions.Default in that case.
        var definition = new EntityDefinition<int, int, int>(
            new EntityTypeName("t"), _ => 0, (_, _, ctx) => ctx.Ignore(), (s, _) => s);

        Assert.Null(definition.Options);
    }
}
