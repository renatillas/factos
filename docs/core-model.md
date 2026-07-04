# The Factos Core Model

The `factos` package is the store-independent part of the library. It gives
applications a standard shape for command decisions over an event log, and it
gives backends shared types for reads, append conditions, and committed records.

It does not persist anything by itself. It does not maintain materialized views.
It does not execute effects. Those behaviours are provided by backend packages
and application code.

The core model has one job: keep the decision, projection, and reaction logic
explicit and pure.

## Facts, not objects

Factos starts from accepted facts. An application defines its own event type:

```gleam
pub type Event {
  TicketSold(buyer: String)
}
```

A fact is authoritative once accepted. Current state is derived by folding facts,
not by mutating a stored object in place.

Factos does not require a base event interface. In Gleam the domain event type is
a custom type owned by the application.

## Deciders

A `Decider(command, state, event, domain_error)` is the command-side domain
component:

```gleam
factos.decider(
  initial: initial_state,
  decide: decide,
  evolve: evolve,
)
```

It has three parts:

1. `initial`: the state before any relevant facts are folded;
2. `evolve`: how an accepted fact changes decision state;
3. `decide`: how a command is accepted or rejected from that state.

The state is not necessarily a stored read model. It is the temporary state needed
for one decision.

```gleam
fn decide(state: State, command: Command) -> Result(List(Event), DomainError) {
  let TicketWindow(capacity, sold) = state
  case command {
    BuyTicket(buyer) ->
      case sold < capacity {
        True -> Ok([TicketSold(buyer)])
        False -> Error(SoldOut(capacity))
      }
  }
}
```

A decider can be tested without storage:

```gleam
factos.compute_events(
  decider: ticket_decider(),
  events: [TicketSold("renata")],
  command: BuyTicket("lucy"),
)
```

## Queries

A `Query` describes the facts relevant to a command. The backend uses it to read
history and protect the append.

```gleam
factos.query([
  factos.query_item(
    types: [factos.event_type("TicketSold")],
    tags: [factos.tag("event:gleamconf-2026")],
  ),
])
```

Query semantics are deliberately simple:

- query items are OR-combined;
- event types inside one item are OR-combined;
- tags inside one item are AND-combined;
- empty types match any event type;
- empty tags add no tag constraint.

`EventType` and `Tag` are opaque wrappers so applications are deliberate about
what is visible to stores.

## Contexts

A backend read returns a `Context(event, state)`:

```gleam
factos.Context(
  query: query,
  state: folded_state,
  events: recorded_events,
  position: observed_position,
  append_condition: append_condition,
)
```

The context contains the facts that were used to make the decision and the
condition needed to keep that decision valid until append time.

## Append conditions

The key condition is:

```gleam
factos.FailIfEventsMatch(query, after: position)
```

It means the backend must not append the newly decided facts if another matching
fact was accepted after the observed position.

This is the context-first consistency boundary. The boundary is the facts needed
by the rule, not a fixed aggregate object.

## Recorded events

Backends decode stored data into `Recorded(event)` values:

```gleam
factos.Recorded(
  id: id,
  stream: stream,
  revision: revision,
  position: position,
  type_: type_,
  version: version,
  tags: tags,
  metadata: metadata,
  event: event,
)
```

`revision` is per stream. `position` is a global log position. Context-first
checks use global positions because the facts relevant to a command may live in
many streams.

## Views

A `View(state, event)` is a pure projection fold:

```gleam
let sold_count =
  factos.view(initial: 0, evolve: fn(count, event) {
    case event {
      TicketSold(_) -> count + 1
    }
  })
```

The core package can run the computation:

```gleam
factos.project(view: sold_count, events: events)
```

It does not decide where the projected state is stored.

## Reactors

A `Reactor(event, effect)` is a pure reaction from committed recorded events to
application-owned effect values:

```gleam
fn ticket_reactor() -> factos.Reactor(Event, Effect) {
  factos.reactor(fn(recorded) {
    case recorded.event {
      TicketSold(buyer) -> [
        AnnounceTicketSale(buyer: buyer, position: recorded.position),
      ]
    }
  })
}
```

Reactors do not run IO. They make follow-up work explicit as data. Application or
infrastructure code decides whether that work is executed immediately, persisted
to an outbox, retried, or skipped during replay.

## What stays outside core

The core package intentionally does not solve:

- database selection;
- serialization;
- schema evolution;
- projection repositories;
- durable effect delivery;
- retries and dead letters;
- subscriptions and catch-up workers;
- deployment topology.

Backends and applications own those decisions.
