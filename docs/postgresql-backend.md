# PostgreSQL Backend

`factos_pog` is the PostgreSQL backend for Factos. It uses
[`pog`](https://hex.pm/packages/pog) and implements the full read-decide-append
flow for context-first Event Sourcing.

Use it when PostgreSQL is your event store and command consistency should be
protected by event type and tag queries.

## Responsibilities

`factos_pog` owns storage mechanics:

- creating the event tables;
- reading contexts by event type and tag;
- reading streams by stream name;
- running PostgreSQL transactions;
- checking append conditions;
- assigning per-stream revisions and global positions;
- returning committed `factos.Recorded(event)` values.

Your application still owns the domain:

- command, event, state, and error types;
- deciders;
- event encoding and decoding;
- query tags;
- effect execution and retries.

It does not maintain materialized views. `factos.View` values are in-memory
folds, and applications decide where durable read models live.

It does not execute side effects. Successful dispatch returns committed records,
and applications decide how reactors/effect delivery should run.

## Schema

Run migrations once before dispatching commands:

```gleam
let connection = pog.named_connection(pool_name)
let assert Ok(Nil) = factos_pog.migrate(connection)
```

The backend creates two tables.

`factos_events` is the append-only log:

- `position`: global append order;
- `id`: application event id;
- `stream`: stream name;
- `revision`: per-stream revision;
- `type`: event type name;
- `version`: event version;
- `tags`: newline-encoded tags;
- `metadata`: newline-encoded metadata;
- `data`: opaque bytes.

`factos_event_tags` mirrors tags by event position. This gives PostgreSQL an
indexed shape for tag queries without decoding event payload bytes.

## Codecs

PostgreSQL does not understand your domain event payload. Your application
provides a codec:

```gleam
fn ticket_codec() -> factos_pog.EventCodec(Event) {
  factos_pog.codec(encode: encode_event, decode: decode_event)
}
```

The encoder turns a domain event into `Proposed(event)`:

```gleam
fn encode_event(event: Event) -> factos_pog.Proposed(Event) {
  case event {
    TicketSold(buyer) ->
      factos_pog.Proposed(
        id: "ticket-sold-" <> buyer,
        event: event,
        type_: factos.event_type("TicketSold"),
        version: 1,
        tags: [factos.tag("event:gleamconf-2026")],
        metadata: factos.empty_metadata(),
        data: bit_array.from_string(buyer),
      )
  }
}
```

The decoder turns a stored row into `factos.Decoded(event)`:

```gleam
fn decode_event(
  stored: factos_pog.StoredEvent,
) -> Result(factos.Decoded(Event), factos_pog.DecodeError) {
  case factos.event_type_name(stored.type_) {
    "TicketSold" -> {
      use buyer <- result.try(
        bit_array.to_string(stored.data)
        |> result.replace_error(factos_pog.InvalidData),
      )
      Ok(factos.Decoded(
        event: TicketSold(buyer),
        type_: stored.type_,
        version: stored.version,
        tags: stored.tags,
        metadata: stored.metadata,
      ))
    }
    _ -> Error(factos_pog.UnknownEvent)
  }
}
```

Decode errors stop read and dispatch flows with `factos_pog.DecodeError`. Library
code does not panic for malformed event data.

## Context dispatch

`dispatch_with_query` is the main context-first API:

```gleam
factos_pog.dispatch_with_query(
  connection,
  stream: buyer_stream(attempt),
  query: sale_query(),
  decider: ticket_decider(),
  codec: ticket_codec(),
  command: BuyTicket(buyer_name(attempt)),
)
```

It runs inside a PostgreSQL transaction and performs this sequence:

1. lock `factos_events` in exclusive mode;
2. select rows matching the query;
3. decode rows with the application codec;
4. fold those events into decision state;
5. run the decider;
6. check `FailIfEventsMatch(query, after: position)`;
7. insert the produced events;
8. mirror tags into `factos_event_tags`;
9. return `Dispatch(event)`.

The return value contains both append metadata and committed records:

```gleam
pub type Dispatch(event) {
  Dispatch(append: Append, events: List(factos.Recorded(event)))
}
```

Those records are the safe input for reactors because the transaction has already
accepted them.

## Why the table lock exists

PostgreSQL has row locks, advisory locks, serializable transactions, and unique
constraints, but it does not have a built-in primitive for:

> append these rows only if no row matching this arbitrary event-type/tag query
> appeared after position N.

`factos_pog` uses `lock table factos_events in exclusive mode` to make this
correct for every `factos.Query`. This is conservative. Concurrent writers queue
behind each other, even when their contexts do not overlap.

The tradeoff is deliberate for this backend: simple correctness before
throughput. A future PostgreSQL backend could use advisory locks or query-specific
lock keys, but only if it preserves the same context-stability guarantee.

## Stream dispatch

`dispatch` is available when one stream revision is the intended consistency
boundary:

```gleam
factos_pog.dispatch(
  connection,
  stream: "ticket-sale-renata",
  decider: ticket_decider(),
  codec: ticket_codec(),
  command: BuyTicket("renata"),
)
```

It loads the stream, folds state, runs the decider, and appends only if the
stream revision still matches the revision that was loaded.

Use stream dispatch for stream-shaped rules. Use context dispatch for rules that
need facts selected by event type and tag.

## Reads

`read_context` loads the facts selected by a query and returns a
`factos.Context` with folded state and an append condition.

`load_stream` loads one stream and returns a `factos.LoadedStream` with folded
state, decoded recorded events, and the current stream revision.

These functions are useful for tests, diagnostics, projections, and custom
application flows.

## Reacting to committed events

A successful dispatch returns the committed records for that dispatch:

```gleam
let assert Ok(dispatch) =
  factos_pog.dispatch_with_query(
    connection,
    stream: "ticket-sale-renata",
    query: sale_query(),
    decider: ticket_decider(),
    codec: ticket_codec(),
    command: BuyTicket("renata"),
  )

let effects = factos.react_all(ticket_reactor(), dispatch.events)
```

`factos_pog` does not execute effects. It exposes the accepted records so the
application can make a deliberate choice:

- run effects immediately;
- store effects durably in another table;
- send effects to a queue;
- retry failures;
- ignore reactions during replay.

## Errors

`factos_pog.Error(domain_error)` has four cases:

- `DomainError(domain_error)`: the decider rejected the command;
- `StoreError(pog.QueryError)`: PostgreSQL or `pog` failed;
- `AppendConditionFailed(factos.AppendCondition)`: the context or stream changed;
- `DecodeError(factos_pog.DecodeError)`: stored bytes could not be decoded.

This keeps business rejection separate from storage, concurrency, and decode
failures.

## Example

Run the ticket sale example:

```sh
cd examples/tickets_pog
docker compose up -d
gleam run
```

The example starts many concurrent buyers for the same event. Only 100 tickets
can be accepted. The backend serializes writes through the PostgreSQL transaction
lock, protects the `TicketSold` + `event:gleamconf-2026` context, and returns the
committed records so the example can run its reactor.
