# Factos

Prototype context-first event-sourcing helpers for Gleam and KurrentDB.

This package deliberately does not model Event Sourcing as aggregates. A command
capability reads the facts relevant to one decision, folds a temporary decision
model, decides which new facts to record, and records them only when the relevant
context can be protected.

## Opinion

Event Sourcing is the persistence idea: accepted facts are the authoritative
state of the system. Aggregates, CQRS, projections, message brokers, and stream
versioning are implementation choices.

This prototype follows the shape described by Command Context Consistency and
Dynamic Consistency Boundaries:

1. A command defines the context it needs.
2. The context is expressed as a query over event types and tags.
3. The application folds only those facts into a decision model.
4. The application produces new facts.
5. The store must reject the append if matching facts appeared after the context
   was observed.

That last step requires store support. KurrentDB's normal append API supports
expected stream revision checks. That is useful, but it is not the same as a
DCB-style query-conditioned append.

## Domain Model

Keep commands, events, state, decisions, evolution, and errors in your app.

```gleam
import factos

pub type Command {
  RegisterUser(username: String)
}

pub type Event {
  UsernameReserved(username: String)
  UserRegistered(username: String)
}

pub type UsernameState {
  UsernameAvailable
  UsernameTaken
}

pub type DomainError {
  UsernameAlreadyTaken
}

pub fn evolve(state: UsernameState, event: Event) -> UsernameState {
  case state, event {
    UsernameAvailable, UsernameReserved(_) -> UsernameTaken
    UsernameAvailable, UserRegistered(_) -> UsernameTaken
    UsernameTaken, UsernameReserved(_) -> state
    UsernameTaken, UserRegistered(_) -> state
  }
}

pub fn decide(state: UsernameState, command: Command) {
  case state, command {
    UsernameAvailable, RegisterUser(username) -> Ok([UserRegistered(username)])
    UsernameTaken, RegisterUser(_) -> Error(UsernameAlreadyTaken)
  }
}

pub fn registration_decider() {
  factos.decider(
    initial: UsernameAvailable,
    decide: decide,
    evolve: evolve,
  )
}
```

`Decider` is inspired by FModel: it is a small pure domain component made from
three things only: initial state, a decision function, and an evolution function.
It does not know about KurrentDB, projections, subscriptions, HTTP, retries, or
repositories.

You can test a decider without any storage:

```gleam
factos.compute_events(
  decider: registration_decider(),
  events: [UsernameReserved("renata")],
  command: RegisterUser("renata"),
)
```

## Command Context

The command context is not a `User` aggregate. It is the facts relevant to the
decision: has this username been reserved or registered?

```gleam
import factos

pub fn username_context(username: String) -> factos.Query {
  factos.query([
    factos.query_item(
      types: [
        factos.event_type("UsernameReserved"),
        factos.event_type("UserRegistered"),
      ],
      tags: [factos.tag("username:" <> username)],
    ),
  ])
}
```

Query items are OR-combined. Within one item, event types are OR-combined and
tags are AND-combined.

## Tags

Tags are an explicit query contract. If a future command needs to select events
by username, account, invoice number, or product, that value must be exposed as a
tag when the event is written.

```gleam
factos.tag("username:renata")
factos.tag("account:abc123")
```

This is intentionally opinionated. Tags duplicate selected payload information,
but they make consistency and query needs visible at the event-store boundary.

## Codecs

The app owns encoding and decoding. The codec returns both the domain event and
the event-store metadata needed by the context API.

```gleam
pub fn codec() -> factos.EventCodec(Event, DecodeError) {
  factos.EventCodec(encode: encode, decode: decode_event)
}
```

`encode` returns a `factos.Proposed` with the domain event, event type, tags, and
the KurrentDB append message. `decode` returns a `factos.Decoded` with the domain
event, event type, and tags read from the stored event.

The library does not force one JSON shape for tags. In a real app, store tags in
custom metadata or payload fields consistently, and decode them at the boundary.

## Reading A Context

```gleam
factos.read_context(
  connection,
  query: username_context("renata"),
  decider: registration_decider(),
  codec: codec(),
  timeout: 5000,
)
```

`read_context` reads from `$all`, applies a server-side event-type filter when
possible, decodes events, filters them by the full query, folds state, and
returns a `factos.Context`.

The returned context includes:

1. The folded decision state.
2. The matching recorded events.
3. The highest observed sequence position.
4. A DCB-style append condition: `FailIfEventsMatch(query, after: position)`.

## Appending

The ideal append condition is:

```gleam
factos.FailIfEventsMatch(query, after: position)
```

That means: append these new facts only if no facts matching the command context
appeared after the position used for the decision.

This prototype models that condition, but the current KurrentDB-backed context
dispatch returns `UnsupportedAppendCondition` for it because regular KurrentDB
append checks stream revisions, not arbitrary event-type/tag queries.

```gleam
factos.dispatch_context(
  connection,
  stream: "facts",
  query: username_context("renata"),
  decider: registration_decider(),
  codec: codec(),
  command: RegisterUser("renata"),
  timeout: 5000,
)
```

Use this shape for stores that support DCB-style atomic append conditions. With
the current KurrentDB operation set, it is still useful as the honest API shape,
but it cannot complete successfully for `FailIfEventsMatch` yet.

## Stream Consistency

KurrentDB can safely protect a single stream with expected revision checks. This
is still useful when a stream is the right consistency boundary.

```gleam
factos.dispatch_stream(
  connection,
  stream: "user-renata",
  decider: registration_decider(),
  codec: codec(),
  command: RegisterUser("renata"),
  timeout: 5000,
)
```

This is aggregate-stream style consistency. It is not the definition of Event
Sourcing, and it may over-conflict when unrelated events share the same stream.

## Views

`View` is the projection-side equivalent of a decider's `evolve` function. It is
also pure and store-independent.

```gleam
let registrations =
  factos.view(initial: 0, evolve: fn(count, event) {
    case event {
      UserRegistered(_) -> count + 1
      UsernameReserved(_) -> count
    }
  })

factos.project(view: registrations, events: [
  UserRegistered("renata"),
  UserRegistered("lucy"),
])
```

Views can be merged when they consume the same event type:

```gleam
let dashboard = factos.merge_views(registrations, reservations)
```

Factos intentionally stops at pure projection computation. It does not provide a
materialized-view repository abstraction yet; persistence and delivery choices
belong outside the domain component.

## KurrentDB Tradeoffs

KurrentDB support available through the current dependency:

1. Read a single stream.
2. Append to a stream with `NoStream`, `Revision(n)`, `StreamExists`, or `Any`.
3. Read `$all` with event-type or stream-name filters.

Newer KurrentDB versions also support secondary and user-defined indexes that
can be consumed through `$all` stream-prefix filters such as `$idx-et-...` or
`$idx-user-...`. Those improve reads, but the docs describe secondary indexes as
eventually consistent. They should not be treated as a command-decision
consistency guarantee unless the write path can atomically enforce the same
condition.

## Inspired By FModel

Factos borrows FModel's useful core idea: model behavior as pure data structures
that hold functions (`Decider`, `View`) and keep infrastructure outside them.

Factos intentionally does not copy these FModel parts yet:

1. `Aggregate` wrappers, because the package is trying not to make aggregates the
   center of the model.
2. Generic repository traits, because Gleam code can pass concrete functions and
   records without committing to one application architecture.
3. Sagas/process managers, because they are event-driven messaging/workflow
   concerns and should remain separate from the event-sourcing core until a real
   use case needs them.

## Development

```sh
gleam test
```
