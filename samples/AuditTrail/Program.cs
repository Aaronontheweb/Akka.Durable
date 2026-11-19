using Akka.Durable;
using Akka.Durable.Samples.AuditTrail.Audit;
using Akka.Durable.Samples.AuditTrail.Shop;
using Akka.Hosting;
using Akka.Persistence.Hosting;
using Akka.Reminders;
using Akka.Reminders.Storage;

var builder = WebApplication.CreateBuilder(args);
var expireAfter = TimeSpan.FromSeconds(builder.Configuration.GetValue("ExpireAfterSeconds", 5));

builder.Services.AddDurableEntities();
builder.Services.AddShop();
builder.Services.AddAuditTrail();

builder.Services.AddAkka("audit-trail", akka => akka
    .WithInMemoryJournal()
    .WithInMemorySnapshotStore()
    .WithLocalReminders(reminders => reminders
        .WithStorage(_ => new InMemoryReminderStorage())
        .WithResolver(system => new DurableEntityReminderResolver(system))
        .WithSettings(new ReminderSettings { MaxSlippage = TimeSpan.FromMilliseconds(500) }))
    .WithShop(expireAfter)
    .WithAuditCollector());

var app = builder.Build();
app.UseAuditTrail();

app.MapPost("/orders/{id}", async (string id, string? item, IEntityRefs<OrderCommand> orders, CancellationToken ct) =>
    (await orders.Get(new EntityId(id)).Ask(new PlaceOrder(item ?? "widget"), ct)).Status);

app.MapPost("/orders/{id}/pay", async (string id, IEntityRefs<OrderCommand> orders, CancellationToken ct) =>
    (await orders.Get(new EntityId(id)).Ask(new PayOrder(), ct)).Status);

app.Run();
