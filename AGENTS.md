# Working on Akka.Durable

## Design rules

### 1. Keep ceremony to a minimum

Keep the amount of ceremony that developers must observe to a minimum.

**Developers** are the people who use this library in their application.

**Ceremony** is anything a user must write that carries no decision of theirs. Examples:

- an interface to implement on plain data;
- a method whose body is the same in every type;
- a registration that the library could work out;
- a type parameter that the compiler could infer;
- a second place to repeat a name.

**The test.** Apply it before you add or keep any public API:

- Can a user's record stay a plain record?
- Can this be one line on the definition in place of one member on every type?
- Can the library check it at host start in place of asking the user to declare it?

If the answer is yes, do that.

**The limit.** Less ceremony must never mean less safety. A mistake must still fail loudly and early: at compile time, or at host start, with a message that names the line to add. It must never fail silently at run time.

**An example.** An effect was a record that had to implement `IEffect<TCommand>` with a `GaveUp` method. Now an effect is a plain record, and the entity's definition has one line:

```csharp
// Before: an interface and a method on every effect.
public sealed record VerifyRecipient(string Recipient) : IEffect<InvoiceCommand>
{ public InvoiceCommand GaveUp(WorkGaveUp g) => new RecipientRejected($"directory: {g.Reason}"); }
// After: a plain record, and one argument of the entity's definition.
public sealed record VerifyRecipient(string Recipient);
GaveUp: g => new RecipientRejected($"directory: {g.Reason}"),
```

The safety stays. An entity that issues an effect and has no `GaveUp` fails at host start, or at the latest at its first `Execute`, before anything is written. The error names the argument to add.

**Samples.** The samples in `README.md` and in `docs/` follow the same rule. A reader must be able to read a sample and understand it before they adopt the library. Show a slice, not everything.

### 2. Never journal a timer or a reminder

A timer is a row in the reminder store (Akka.Reminders). That row is its only durable record. The library never writes a journal record that says a timer was set, will be set, or was cancelled. This is decided. Do not propose it again, do not design it, and do not build it. It has been proposed several times, each time to close the gap between the journal write and the reminder write.

**What the rule covers.** An entity's `Schedule` and `CancelTimer`, a workflow's wake, the wait of a retry, and any later feature that sets or removes a reminder.

**Why.**

- It is redundant. Storing a reminder is one quick write. A journal record that says "a reminder will be stored" is a second record of the same fact.
- It gives no safety. The journal and the reminder store are most likely in the same database. When the reminder cannot be stored, the journal record that would say so most likely cannot be written either.
- It costs. It adds stored record types, serializers, a settle step, recovery code and an ordering problem, on every decision that sets a timer.

- Storing a reminder is an awaited call. The caller gets a confirmation or an error back. Nothing has to be remembered in order to find out later.

**What to do when a reminder cannot be stored.** Fail the operation and send a nack to the sender. Do not hide the failure, do not retry it in secret, and do not record it for later. There is no fallback: no timer in memory, no second system, and no heartbeat that keeps an actor alive to set the timer again. When the durable timers do not work, do nothing more.

**Order.** What is already written when the failure comes depends on the order the developer gave. That is the developer's choice. The library does not add a record to make the two writes look like one.

**The gap that stays.** A process can stop between the journal write and the reminder write. That gap is known and accepted in the library. It is not a reason to journal timers. Do not build a journal record for it.

**Not covered by this rule.** `Execute` is different. A recorded effect is journaled with the decision, because its handler runs outside the entity and has no store of its own. A timer has one: the reminder store. An application may use what the library already has: an `Execute` effect with a retry policy, whose handler stores a reminder. That is the application's decision, and it needs nothing new in the library.

## How this repository is worked on

- The default branch is `dev`. Pull requests go to `dev`.
- The guide is in `docs/`, one topic for each page. It is written in simplified technical English: short sentences, one idea in each, present tense.
- Release notes are in `RELEASE_NOTES.md`, newest first. `build.ps1` takes the version from the first heading.
- `dotnet build` and `dotnet test` build and test the whole solution. The SDK is pinned in `global.json`.
