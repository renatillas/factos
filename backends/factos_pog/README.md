# factos_pog

`factos_pog` is the PostgreSQL backend for Factos, implemented with
[`pog`](https://hex.pm/packages/pog).

It stores accepted facts in an append-only PostgreSQL event log, reads the facts
relevant to a command, runs your pure `factos.Decider`, and appends new facts
only if the relevant context is still stable.

Use this package when PostgreSQL is your event store and your consistency rules
are expressed with Factos event types and tags.

It persists event records only. It does not maintain materialized views and it
does not execute side effects. Applications build durable read models and effect
delivery on top of the committed records returned by dispatch.

## Install

```toml
[dependencies]
factos = ">= 1.0.0 and < 2.0.0"
factos_pog = ">= 1.0.0 and < 2.0.0"
pog = ">= 4.1.0 and < 5.0.0"
```

## Set up the schema

Start a `pog` pool in your application supervision tree, get a named connection,
and run the migration:

```gleam
let connection = pog.named_connection(pool_name)
let assert Ok(Nil) = factos_pog.migrate(connection)
```

The migration creates:

- `factos_events`: append-only event rows;
- `factos_event_tags`: indexed tag rows used for context reads.

## Define a codec

Your domain event type remains yours. PostgreSQL stores opaque bytes plus
queryable metadata, so the application provides an event codec.

```gleam
fn ticket_codec() -> factos_pog.EventCodec(Event) {
  factos_pog.codec(encode: encode_event, decode: decode_event)
}
```

The encoder prepares an event for persistence:

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

The decoder turns stored rows back into domain events:

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

Tags are the query contract. If future commands need to find an event by payload
value, expose that value as a tag when writing the event.

## Dispatch by context query

`dispatch_with_query` is the primary context-first write API.

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

The backend:

1. opens a PostgreSQL transaction;
2. locks `factos_events` in exclusive mode;
3. reads rows matching the query;
4. decodes and folds them into decision state;
5. runs the decider;
6. checks that no matching row appeared after the observed position;
7. inserts the new events;
8. returns append metadata and committed records.

The return type is:

```gleam
pub type Dispatch(event) {
  Dispatch(append: Append, events: List(factos.Recorded(event)))
}
```

`dispatch.events` contains the committed records inserted by this dispatch. Use
those records for reactors or durable effect adapters.

## Dispatch by stream

`dispatch` is also available when one stream revision is intentionally the
consistency boundary:

```gleam
factos_pog.dispatch(
  connection,
  stream: "ticket-sale-renata",
  decider: ticket_decider(),
  codec: ticket_codec(),
  command: BuyTicket("renata"),
)
```

Stream dispatch remains useful for stream-shaped rules, but context dispatch is
the better fit when a command depends on facts selected by event type and tag.

## React after commit

`factos_pog` does not run side effects. It returns committed records so the
application can react explicitly:

```gleam
let effects = factos.react_all(ticket_reactor(), dispatch.events)
```

A reactor is pure. It returns effect values as data. Your application decides how
to execute, persist, retry, or ignore those effects.

## Tradeoff: correctness before throughput

PostgreSQL does not provide a single native primitive for:

> append these events only if no row matching this arbitrary event-type/tag query
> appeared after position N.

`factos_pog` therefore locks the event table while running context dispatch. That
makes arbitrary Factos queries correct, but unrelated writers queue behind each
other. A future backend can use a more granular locking strategy if it preserves
the same append-condition guarantee.

## Example

Run the concurrent ticket sale example:

```sh
cd ../../examples/tickets_pog
docker compose up -d
gleam run
```

The example uses `dispatch_with_query` to protect a capacity rule across many
buyer streams. It then runs a reactor over the committed `TicketSold` records.
