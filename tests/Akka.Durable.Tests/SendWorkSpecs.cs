using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Bumps;
using Xunit;
using Command = Akka.Durable.IReplies<Akka.Durable.Tests.Fixtures.RailwayCommand, Akka.Durable.Tests.Fixtures.AdjustmentReply>;

namespace Akka.Durable.Tests;

/// <summary>A send from Work is plain data: compared, asserted and named without an actor system.</summary>
public sealed class SendWorkSpecs
{
    private static readonly EntityId W1 = new("w1");
    private static readonly WorkContext<BumpInput, BumpFact> C = Ctx(Plan.Send);

    private static WorkContext<BumpInput, BumpFact> Ctx(Plan plan) =>
        new(new WorkflowId("w1"), new BumpInput(5, plan), new DateTimeOffset(2026, 11, 9, 12, 0, 0, TimeSpan.Zero));

    public static TheoryData<string, Command, string, Command, bool> Pairs => new()
    {
        { "r1", new Adjust(5), "r1", new Adjust(5), true },
        { "r1", new Adjust(5), "r2", new Adjust(5), false },
        { "r1", new Adjust(5), "r1", new Adjust(6), false },
        { "r1", new Adjust(5), "r1", new BeginAdjust(0, 5), false },
    };

    [Theory]
    [MemberData(nameof(Pairs))]
    public void Sends_are_equal_when_target_and_command_are_equal_whatever_then_does(
        string targetA, Command commandA, string targetB, Command commandB, bool equal)
    {
        var a = C.Send(new EntityId(targetA), commandA, Bump.FactOf);
        var b = C.Send(new EntityId(targetB), commandB, _ => new BumpTimerDue());

        Assert.Equal(new SendEffect(typeof(RailwayCommand), new EntityId(targetA), commandA), a.Effect);
        Assert.Equal(equal, a.Equals(b));
        Assert.Equal(equal, new WorkList<BumpFact>([a]).Equals(new WorkList<BumpFact>([b])));
        if (equal) Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    // Expected work is built here as a user would write it. Its then differs from the fixture's: it is not compared.
    public static TheoryData<Plan, WorkList<BumpFact>, string> Expected => new()
    {
        { Plan.Send, C.Send(W1, new Adjust(5), _ => new BumpTimerDue()), nameof(Adjust) },
        { Plan.Unhosted, C.Send(W1, new Increment(5), _ => new BumpTimerDue()), nameof(Increment) },
        { Plan.WithTimer, C.All(C.Send(W1, new Adjust(5), _ => new BumpTimerDue()), C.Timer(TimeSpan.FromMilliseconds(1), new BumpTimerDue())), nameof(Adjust) },
        { Plan.Both, C.All(C.Send(W1, new Adjust(5), _ => new BumpTimerDue()), C.Send(new EntityId("w1-b"), new Adjust(5), _ => new BumpTimerDue())), nameof(Adjust) },
    };

    [Theory]
    [MemberData(nameof(Expected))]
    public void Work_returns_the_expected_sends_and_a_give_up_names_the_command(Plan plan, WorkList<BumpFact> expected, string what)
    {
        var work = Bump.Work(new Sending(), Ctx(plan));

        Assert.Equal(expected, work);
        var send = Assert.IsType<ExecuteWork<BumpFact>>(work.Items[0]);
        Assert.All(Enum.GetValues<GaveUpReason>(), reason => Assert.Equal(what, new WorkGaveUp(send.Effect, reason).What));
    }
}
