# Factos

Factos provides primitives and storage backends for building event-sourced systems
in Gleam.

The library helps with the repetitive part of event-sourced applications:

1. read previously stored events;
2. fold them into the state needed for a decision;
3. run your domain decision function;
4. persist the newly accepted events;
5. return the committed records so your application can update views or trigger
   effects.

The main backend today is `factos_pog`, which stores events in PostgreSQL with
[`pog`](https://hex.pm/packages/pog).

Factos is not a large framework. Your application still defines the commands,
events, state, errors, codecs, read models, and side effects. Factos gives those
pieces a standard shape and gives backends a standard way to run the
read-decide-append flow safely.

## What gets stored?

Backends store events. In `factos_pog`, those events are rows in PostgreSQL.

Materialized views are not stored by Factos itself. A `factos.View` is an
in-memory fold over events. If you want a durable read model, your application
stores the result wherever it wants: PostgreSQL tables, Redis, SQLite, files, or
something else.

Reactors are also not stored or executed by Factos. A `factos.Reactor` maps
committed event records to application-owned effect values. Your application
chooses whether to run those effects immediately, persist them in an outbox,
retry them, or ignore them during replay.

So the durable state provided by the backend is:

- the append-only event log;
- event metadata needed for reads and consistency checks.

Everything else is application state built from that log.

## What does `factos` provide?

The core package is store-independent. It provides the types and pure functions
used by backends and applications:

- `Decider`: your command decision logic as pure data.
- `Query`: the event types and tags needed for a decision.
- `Context`: previously stored events folded into decision state.
- `AppendCondition`: the condition a backend must protect before appending.
- `Recorded`: a stored event plus backend metadata.
- `View`: an in-memory projection fold.
- `Reactor`: a pure mapping from committed records to effect values.

A decider has this shape:

```gleam
factos.decider(
  initial: TicketWindow(capacity: 100, sold: 0),
  decide: decide,
  evolve: evolve,
)
```

`evolve` folds accepted events into state:

```gleam
fn evolve(state: State, event: Event) -> State {
  let TicketWindow(capacity, sold) = state
  case event {
    TicketSold(_) -> TicketWindow(capacity: capacity, sold: sold + 1)
  }
}
```

`decide` takes a command and the folded state, then either rejects the command or
returns new events:

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

You can test this without any database:

```gleam
factos.compute_events(
  decider: ticket_decider(),
  events: [TicketSold("renata")],
  command: BuyTicket("lucy"),
)
```

## What does `factos_pog` provide?

`factos_pog` is the PostgreSQL backend. It provides:

- database migrations for an append-only event log;
- an application codec boundary for event bytes;
- context reads by event type and tag;
- stream reads by stream name;
- dispatch functions that run the read-decide-append flow in PostgreSQL;
- committed `factos.Recorded(event)` values after successful appends.

The primary function is `dispatch_with_query`:

```gleam
let assert Ok(dispatch) =
  factos_pog.dispatch_with_query(
    connection,
    stream: buyer_stream(attempt),
    query: sale_query(),
    decider: ticket_decider(),
    codec: ticket_codec(),
    command: BuyTicket(buyer_name(attempt)),
  )
```

That call:

1. starts a PostgreSQL transaction;
2. locks the event table;
3. reads events matching `sale_query()`;
4. decodes those rows with `ticket_codec()`;
5. folds them with the decider's `evolve` function;
6. calls the decider's `decide` function with `BuyTicket(...)`;
7. checks that no matching event appeared since the context was read;
8. inserts the new events if the decision succeeded;
9. returns `Dispatch(event)`.

`Dispatch(event)` contains append metadata and the events committed by this
specific dispatch:

```gleam
pub type Dispatch(event) {
  Dispatch(append: Append, events: List(factos.Recorded(event)))
}
```

That is what your application can feed into reactors or projection updates.

## Events, commands, and command sourcing

Factos stores events: facts that were accepted by the application. A backend row
is an event record, not a command record.

The core package also provides command-handling helpers (`Decider`, `Context`, and
`dispatch` functions in the backends). Those helpers are an opinionated way to
build command processing on top of an event log:

```text
command + relevant previous events -> accepted new events or domain error
```

If you want lower-level event sourcing, you can use the same stored event log,
codecs, views, and reads without treating Factos as a complete command framework.
The command-dispatch path is a convenience for applications that want that
standard shape.

## Queries and tags

Backends do not understand your event payload bytes. If a command needs to find
facts by a payload value, write that value as a tag.

For a ticket-sale capacity rule:

```gleam
fn sale_query() -> factos.Query {
  factos.query([
    factos.query_item(
      types: [factos.event_type("TicketSold")],
      tags: [factos.tag("event:gleamconf-2026")],
    ),
  ])
}
```

This tells the backend: "read the accepted ticket-sale facts for this event and
protect that same context before appending more ticket sales".

Query semantics are small:

- query items are OR-combined;
- event types inside one item are OR-combined;
- tags inside one item are AND-combined;
- empty event types match any event type;
- empty tags add no tag constraint.

## How are views computed?

A view is an in-memory fold over events:

```gleam
let sold_count =
  factos.view(initial: 0, evolve: fn(count, event) {
    case event {
      TicketSold(_) -> count + 1
    }
  })
```

You can run it over events you already have:

```gleam
factos.project(view: sold_count, events: events)
```

Or your application can read events from a backend and store the projected value
itself. Factos does not maintain a projection table automatically.

Views can always be recomputed if the original events are still decodable. That
is why event codec compatibility matters.

## How are effects handled?

Reactors turn committed event records into effect values:

```gleam
pub type Effect {
  AnnounceTicketSale(buyer: String, position: factos.SequencePosition)
}

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

After dispatch:

```gleam
let effects = factos.react_all(ticket_reactor(), dispatch.events)
```

Factos does not send the email, publish the webhook, or mark the effect as done.
It keeps that work explicit so your application can choose the durability and
retry strategy.

## Failure modes

`factos_pog` separates common failure classes:

- `DomainError(error)`: your decider rejected the command.
- `StoreError(error)`: PostgreSQL or `pog` failed.
- `AppendConditionFailed(condition)`: the context or stream changed before append.
- `DecodeError(error)`: stored bytes could not be decoded by your codec.

This distinction matters operationally. A sold-out ticket is not a database
failure. A decode failure means stored history and current codec no longer agree.
An append-condition failure usually means the command should be retried from a
fresh context or rejected with newer information.

## Scaling model

The current `factos_pog` context dispatch path prioritizes correctness over write
throughput. It locks the event table while running `dispatch_with_query`, so
concurrent writers queue behind each other even if their contexts do not overlap.

That simple lock is what makes arbitrary event-type/tag append conditions
correct in this backend.

Use `dispatch` when one stream revision is the correct consistency boundary. Use
`dispatch_with_query` when the rule spans facts selected by event type and tag.
Future PostgreSQL backends can use more granular locking, but they must preserve
the same append-condition guarantee.

Read scaling and projection scaling are application concerns. You can recompute
views by replaying events, maintain your own materialized tables, or build
subscription workers on top of backend reads.

## Example

Run the PostgreSQL ticket-sale example:

```sh
cd examples/tickets_pog
docker compose up -d
gleam run
```

It starts many concurrent buyers for one event. Only 100 tickets can be accepted.
The backend stores the accepted `TicketSold` events, protects the tag-based
capacity context, and returns committed records for the reactor.

## Repository packages

This repository contains:

1. `factos`: core primitives and pure computations.
2. `factos_pog`: PostgreSQL backend using `pog`.
3. `factos_sqlight`: SQLite backend using `sqlight`.
4. `factos_kurrentdb_erlang`: KurrentDB backend for Erlang.
5. `factos_cf`: Cloudflare D1 backend.

The core concepts are shared. Storage behaviour and scaling tradeoffs are backend
specific.
