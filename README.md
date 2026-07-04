# Factos

Factos is a set of prototype Gleam libraries for context-first Event Sourcing.

The libraries are based on the interpretation described in Rico Fritzsche's
[Simply Event Sourcing](https://ricofritzsche.me/simply-event-sourcing/): Event
Sourcing is not defined by aggregates, aggregate roots, CQRS, message brokers,
microservices, or stream-per-object storage. Event Sourcing means accepted facts
are persisted as the authoritative history of the system, and that relevant
history is used when deciding whether new facts may be accepted.

Factos models that idea directly:

1. A command arrives with an intention.
2. A domain capability chooses the facts relevant to that decision.
3. Those facts are folded into a temporary decision state.
4. The decision either rejects the command with a domain error or produces new facts.
5. The store appends the new facts only if the relevant context has remained stable.

The consistency boundary follows the command decision. It is not forced to be a
predefined `User`, `Order`, or `Customer` aggregate stream.

## Libraries

This repository contains five Gleam libraries:

1. `factos`: store-independent domain primitives.
2. `factos_pog`: PostgreSQL backend implemented with the `pog` package.
3. `factos_sqlight`: SQLite backend implemented with the `sqlight` package.
4. `factos_kurrentdb_erlang`: KurrentDB backend for the Erlang target.
5. `factos_cf`: Cloudflare D1 backend for Workers.

The core library is intentionally small. It knows about facts, event types, tags,
queries, contexts, deciders, views, reactors, recorded events, loaded streams,
and append conditions. It does not know how bytes are encoded, where events are
stored, how effects are executed, whether projections are synchronous, or which
transport is used.

Backend libraries own storage details. They define storage codecs, persistence
errors, migrations, and dispatch functions for their storage technology.

## Concepts

### Events Are Facts

An event is a fact that has been accepted by the application. The event history is
the source of truth. Derived state can be rebuilt by folding events with an
evolution function.

Factos does not require a base `Event` interface. Your application defines its own
event type:

```gleam
pub type Event {
  UsernameReserved(username: String)
  UserRegistered(username: String)
  DisplayNameChanged(user_id: String, name: String)
}
```

### Deciders Are Pure Domain Capabilities

A `Decider` is a pure command-handling component made from:

1. an initial state,
2. a decision function, and
3. an evolution function.

The decision function receives the temporary state needed for one command and
returns either new events or a domain error. The evolution function folds accepted
events into that state.

```gleam
import factos

pub type Command {
  RegisterUser(username: String)
}

pub type State {
  UsernameAvailable
  UsernameTaken
}

pub type DomainError {
  UsernameAlreadyTaken
}

pub fn evolve(state: State, event: Event) -> State {
  case state, event {
    UsernameAvailable, UsernameReserved(_) -> UsernameTaken
    UsernameAvailable, UserRegistered(_) -> UsernameTaken
    UsernameAvailable, DisplayNameChanged(_, _) -> state
    UsernameTaken, UsernameReserved(_) -> state
    UsernameTaken, UserRegistered(_) -> state
    UsernameTaken, DisplayNameChanged(_, _) -> state
  }
}

pub fn decide(state: State, command: Command) -> Result(List(Event), DomainError) {
  case state, command {
    UsernameAvailable, RegisterUser(username) -> Ok([UserRegistered(username)])
    UsernameTaken, RegisterUser(_) -> Error(UsernameAlreadyTaken)
  }
}

pub fn registration_decider() -> factos.Decider(Command, State, Event, DomainError) {
  factos.decider(
    initial: UsernameAvailable,
    decide:,
    evolve:,
  )
}
```

Deciders are easy to test without any storage:

```gleam
factos.compute_events(
  decider: registration_decider(),
  events: [UsernameReserved("renata")],
  command: RegisterUser("renata"),
)
```

### Command Context Consistency

The command context is the set of facts required to make one decision.

For registering a username, the command does not need every event for a `User`
object. It only needs facts that can make that username unavailable, such as
`UsernameReserved` and `UserRegistered` for the same username.

```gleam
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

Factos query semantics are deliberately simple:

1. `factos.query([])` becomes `AllEvents`.
2. Query items are OR-combined.
3. Within one query item, event types are OR-combined.
4. Within one query item, tags are AND-combined.
5. Empty types in an item match any event type.
6. Empty tags in an item match any tags.

When a backend reads a context it returns a `factos.Context` containing:

1. the query that defined the context,
2. the folded decision state,
3. the matching recorded events,
4. the highest observed sequence position, and
5. an append condition: `FailIfEventsMatch(query, after: position)`.

That append condition captures Command Context Consistency: append the newly
decided facts only if no facts matching the command context appeared after the
position used for the decision.

### Dynamic Consistency Boundary Tags

Dynamic Consistency Boundary (DCB) applies the same context-first consistency
principle through a tag-based event-store contract. Event data is opaque to the
store, so anything that must be queryable for context reads or consistency checks
has to be exposed as an event type or tag when writing the event.

```gleam
factos.tag("username:renata")
factos.tag("account:abc123")
factos.tag("restaurant")
factos.tag("sku:burger")
```

Tags intentionally duplicate selected payload information. That duplication is
the contract: it makes future command-context queries visible at the event-store
boundary instead of hiding them inside opaque payloads.

### Stream Consistency Is Still Supported

Factos also supports stream-based workflows through `load_stream` and
`dispatch_stream` in the backends. This is useful when a single stream really is
the right boundary for a decision.

Stream revision checks are not the definition of Event Sourcing. They are one
possible consistency strategy. They can over-conflict when unrelated events share
the same stream, and they can under-model rules that require facts from multiple
streams.

## Core Library: `factos`

Import the core package when you want pure domain components and shared event
metadata types.

```gleam
import factos
```

The core library provides:

1. `EventType` and `Tag` wrappers for store-visible event metadata.
2. `Query` and `QueryItem` for command contexts.
3. `SequencePosition` for global event-log positions.
4. `AppendCondition` for context-stability requirements.
5. `Decider` for command-side decisions.
6. `View` for query-side projection folds.
7. `Reactor` for pure reactions from committed recorded events to application-owned effect values.
8. `Decoded`, `Recorded`, `Context`, and `LoadedStream` records used by backends.

### Pure Command Computation

Use `compute_events` when you already have relevant event history and want to test
or run a decider without storage:

```gleam
factos.compute_events(
  decider: registration_decider(),
  events: [UsernameReserved("renata")],
  command: RegisterUser("renata"),
)
```

Use `compute_state` when you want to apply the events produced by a decision to
an existing state:

```gleam
factos.compute_state(
  decider: registration_decider(),
  current: option.None,
  command: RegisterUser("renata"),
)
```

### Projection Computation

`View` is the projection-side equivalent of a decider's `evolve` function. It is
also pure and store-independent.

```gleam
let registrations =
  factos.view(initial: 0, evolve: fn(count, event) {
    case event {
      UserRegistered(_) -> count + 1
      UsernameReserved(_) -> count
      DisplayNameChanged(_, _) -> count
    }
  })

factos.project(view: registrations, events: [
  UserRegistered("renata"),
  UserRegistered("lucy"),
])
```

Views can be merged when they consume the same event type:

```gleam
let dashboard = factos.merge_views(registrations, display_name_changes)
```

Factos intentionally stops at pure computation. Materialized view storage,
catch-up subscriptions, effect delivery retries, and read-model rebuilds belong
to application or backend-specific code.

### Reactor Computation

`Reactor` is the side-effect-side equivalent of a view. It inspects committed
`Recorded(event)` values and returns application-owned effect values. It does not
execute IO.

```gleam
pub type Effect {
  SendWelcomeEmail(to: String, event_id: String)
}

let user_reactor =
  factos.reactor(fn(recorded) {
    case recorded.event {
      UserRegistered(username) -> [
        SendWelcomeEmail(to: username, event_id: recorded.id),
      ]
      UsernameReserved(_) -> []
      DisplayNameChanged(_, _) -> []
    }
  })
```

Backend dispatch functions return the committed `Recorded(event)` values for the
append, so applications can react only after the facts were accepted:

```gleam
let assert Ok(dispatch) =
  factos_sqlight.dispatch_stream(
    connection,
    stream: "user-renata",
    decider: registration_decider(),
    codec: codec(),
    command: RegisterUser("renata"),
  )

let effects = factos.react_all(user_reactor, dispatch.events)
```

Effect execution remains outside `factos`: applications can run effects
immediately, persist them durably, retry them, or ignore them during replay.

## SQLite Backend: `factos_sqlight`

The SQLite backend stores events in an append-only table named `factos_events` and
uses `BEGIN IMMEDIATE` while dispatching commands. That lets it enforce
`FailIfEventsMatch(query, after)` transactionally in the same database that stores
events.

```gleam
import factos/factos_sqlight
import sqlight

use connection <- sqlight.with_connection("events.sqlite3")
let assert Ok(Nil) = factos_sqlight.migrate(connection)
```

The schema contains:

1. `position`: monotonically increasing SQLite row position.
2. `id`: application-provided event id.
3. `stream`: stream name used for stream-based dispatch.
4. `revision`: per-stream revision.
5. `type`: event type name.
6. `tags`: newline-separated tag text.
7. `data`: opaque application-encoded bytes.

The table enforces `unique(stream, revision)` and indexes stream revisions and
positions.

### SQLite Codecs

Your application owns encoding and decoding. `factos_sqlight.EventCodec` keeps the
backend generic over event payloads and domain event types.

```gleam
pub fn codec() -> factos_sqlight.EventCodec(Event, DecodeError) {
  factos_sqlight.EventCodec(encode: encode, decode: decode)
}

fn encode(event: Event) -> factos_sqlight.Proposed(Event) {
  factos_sqlight.Proposed(
    id: "event-" <> event.username,
    event: event,
    type_: factos.event_type("UserRegistered"),
    tags: [factos.tag("username:" <> event.username)],
    data: bit_array.from_string(event.username),
  )
}
```

The encoder returns the domain event, event type, tags, and bytes to persist. The
decoder receives the stored row and must return `factos.Decoded(event)` with the
domain event, event type, and tags that should participate in query matching.

### SQLite Context Dispatch

Use `dispatch_context` when a command's consistency boundary is a query over event
types and tags rather than a single stream.

```gleam
factos_sqlight.dispatch_context(
  connection,
  stream: "facts",
  query: username_context("renata"),
  decider: registration_decider(),
  codec: codec(),
  command: RegisterUser("renata"),
)
```

`dispatch_context` performs the full read-decide-append flow inside a transaction:

1. begin an immediate SQLite transaction,
2. read matching events,
3. fold the decision state,
4. run the decider,
5. check whether matching events appeared after the observed position,
6. append produced events to the target stream, and
7. commit or roll back.

### SQLite Stream Dispatch

Use `dispatch_stream` when the stream is the intended consistency boundary.

```gleam
factos_sqlight.dispatch_stream(
  connection,
  stream: "user-renata",
  decider: registration_decider(),
  codec: codec(),
  command: RegisterUser("renata"),
)
```

The backend loads the stream, folds state, decides, and appends only if the
stream revision still matches the revision that was loaded.

## KurrentDB Erlang Backend: `factos_kurrentdb_erlang`

The KurrentDB backend integrates Factos with the Erlang-target KurrentDB client.
It supports stream reads, stream appends with expected revisions, and context
reads from `$all` using event-type filters.

```gleam
import factos/factos_kurrentdb_erlang
import kurrentdb
import kurrentdb_erlang

let assert Ok(client) =
  kurrentdb.from_connection_string(
    "kurrentdb://admin:changeit@localhost:2113?tls=true",
  )

let assert Ok(connection) =
  kurrentdb_erlang.new(client)
  |> kurrentdb_erlang.verify_ca_certificate_file("certs/ca.crt")
  |> kurrentdb_erlang.start(option.None)
```

### KurrentDB Codecs

`factos_kurrentdb_erlang.EventCodec` adapts between domain events and
`append_to_stream.Event` values from the KurrentDB client.

```gleam
pub fn codec() -> factos_kurrentdb_erlang.EventCodec(Event, DecodeError) {
  factos_kurrentdb_erlang.EventCodec(encode: encode, decode: decode)
}
```

The encoder returns `Proposed(event, type_, tags, message)`. The `message` is the
actual KurrentDB append event. The decoder receives a KurrentDB recorded event and
returns a `factos.Decoded(event)`.

### KurrentDB Stream Dispatch

KurrentDB's regular append API can protect a stream revision. Use
`dispatch_stream` for that flow.

```gleam
factos_kurrentdb_erlang.dispatch_stream(
  connection,
  stream: "user-renata",
  decider: registration_decider(),
  codec: codec(),
  command: RegisterUser("renata"),
  timeout: 10_000,
)
```

Empty streams map to `factos.NoEvents`; loaded streams map to
`factos.CurrentRevision(n)`. Appends use KurrentDB expected-revision checks.

### KurrentDB Context Reads

`read_context` can read from `$all`. It translates the event types in a
`factos.Query` into a KurrentDB `$all` event-type prefix filter, decodes events,
then applies full Factos query matching locally, including tags.

```gleam
factos_kurrentdb_erlang.read_context(
  connection,
  query: username_context("renata"),
  decider: registration_decider(),
  codec: codec(),
  timeout: 10_000,
)
```

The returned context still contains `FailIfEventsMatch(query, after: position)`.
However, KurrentDB's regular append operation cannot atomically enforce arbitrary
event-type/tag query conditions. For that reason `dispatch_context` returns
`UnsupportedAppendCondition` for `FailIfEventsMatch`.

This is intentional documentation of the tradeoff: KurrentDB stream revision
checks are useful, but they are not the same as a DCB-style query-conditioned
append. If your consistency rule is genuinely context-based across streams, you
need a write path that can atomically enforce that context condition.

## Example

The `examples/orders_sqlight/src/order_workflow.gleam` file contains a restaurant
order workflow using `factos_sqlight`. It demonstrates:

1. domain commands and events,
2. a custom state machine,
3. domain-specific errors,
4. stream dispatch for one order,
5. application-owned encoding and decoding, and
6. a projection view for kitchen summary data.

The `examples/tickets_pog/src/tickets_pog.gleam` file contains a concurrent
ticket-sale workflow using `factos_pog`. It demonstrates:

1. query-based command context consistency,
2. concurrent buyers racing against a shared capacity rule,
3. dispatch returning committed recorded events, and
4. a pure reactor that turns accepted ticket-sale facts into application effects.

Run each example from its package:

```sh
cd examples/orders_sqlight
gleam run

cd ../tickets_pog
gleam run
```

## Tradeoffs

Factos is a prototype. It deliberately leaves many production concerns outside the
core package:

1. event schema evolution,
2. snapshots,
3. subscriptions,
4. projection repositories,
5. retry policies,
6. side-effect orchestration,
7. idempotency policies beyond event ids,
8. serialization format choices, and
9. distributed deployment concerns.

Those are real engineering problems, but they are separate from the core Event
Sourcing definition. Factos keeps the starting point simple: persist accepted
facts, derive temporary decision state from relevant history, and record new facts
only if that relevant history is still valid.
