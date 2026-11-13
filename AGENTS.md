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

## How this repository is worked on

- The default branch is `dev`. Pull requests go to `dev`.
- The guide is in `docs/`, one topic for each page. It is written in simplified technical English: short sentences, one idea in each, present tense.
- Release notes are in `RELEASE_NOTES.md`, newest first. `build.ps1` takes the version from the first heading.
- `dotnet build` and `dotnet test` build and test the whole solution. The SDK is pinned in `global.json`.
