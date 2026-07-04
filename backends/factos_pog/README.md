# factos_pog

PostgreSQL backend for Factos using [`pog`](https://hex.pm/packages/pog).

This backend follows the "Simply Event Sourcing" shape used by Factos: accepted facts are stored in an append-only event table, command handlers select the facts relevant to their decision, and appends are accepted only when that command context is still stable.

## Design Notes

The event table stores opaque event bytes plus store-visible query metadata:
event type and tags. Tags are also mirrored into `factos_event_tags` so
context reads and context-stability checks can use indexed SQL predicates
instead of decoding unrelated rows in the application. PostgreSQL does not
need to understand payloads, but any payload value needed by future context
queries must be exposed as a tag when the event is written.

`dispatch_with_query` runs inside a PostgreSQL transaction and locks the event
table before reading, deciding, checking, and appending. This is intentionally
conservative. It makes arbitrary `FailIfEventsMatch(query, after)` checks
correct without trying to infer lock keys from dynamic query metadata. A
higher-throughput backend could replace the table lock with advisory locks or
more granular query-specific locks, but only if it preserves the same
context-stability guarantee.

`dispatch` is also available for applications where one stream revision really
is the intended consistency boundary. It is an implementation strategy, not the
definition of Event Sourcing.

Both dispatch functions return `factos_pog.Dispatch(event)`: the append metadata
plus the committed `factos.Recorded(event)` values for that dispatch. Application
code can feed those records into `factos.Reactor` values after the transaction
has accepted the facts.

## Usage

Start a `pog` pool in your application supervision tree, run `migrate`, build a
codec with `factos_pog.codec`, then call `dispatch_with_query` or `dispatch` with
your domain decider and command.

```gleam
let connection = pog.named_connection(pool_name)
let assert Ok(Nil) = factos_pog.migrate(connection)

let assert Ok(dispatch) =
  factos_pog.dispatch_with_query(
    connection,
    stream: "tickets",
    query: sale_query(),
    decider: ticket_decider(),
    codec: ticket_codec(),
    command: BuyTicket("renata"),
  )

let effects = factos.react_all(ticket_reactor(), dispatch.events)
```

Your codec owns event serialization. The backend only persists bytes and query
metadata. Your application owns any effects returned by reactors: it may run
them immediately, write them to durable infrastructure, retry them, or ignore
them during replay.
