# How `factos_pog` Works

`factos_pog` is a PostgreSQL event-store backend for the Factos core model.

It implements two write paths:

- `dispatch_with_query`: protect an arbitrary Factos event-type/tag context;
- `dispatch`: protect one stream revision.

It persists event records only. Views are computed by application folds over
events, and durable materialized views are application-owned tables or stores.
Reactors produce effect values, but this package does not execute or retry those
effects.

Both paths return `Dispatch(event)`, which includes append metadata and the
committed `factos.Recorded(event)` values inserted by the dispatch.

## Event rows

The append-only table is `factos_events`:

| Column | Meaning |
| --- | --- |
| `position` | Global append order. |
| `id` | Application event id. |
| `stream` | Stream name. |
| `revision` | Per-stream revision. |
| `type` | Store-visible event type name. |
| `version` | Event version from the application codec. |
| `tags` | Newline-encoded store-visible tags. |
| `metadata` | Newline-encoded application metadata. |
| `data` | Opaque application bytes. |

Tags are also mirrored into `factos_event_tags(position, tag)`. This table gives
PostgreSQL indexed predicates for context queries.

## Context dispatch flow

`dispatch_with_query` is for commands whose consistency boundary is a
`factos.Query`.

The transaction does this:

1. lock `factos_events` in exclusive mode;
2. select candidate rows with SQL generated from the query;
3. decode candidate rows using the application codec;
4. apply `factos.matches_query` semantics;
5. fold events with the decider's `evolve` function;
6. run the decider with the command;
7. check `FailIfEventsMatch(query, after)`;
8. insert produced events;
9. insert tag-index rows;
10. return `Dispatch(event)`.

The table lock is intentionally conservative. It is the simplest way to make the
append condition correct for arbitrary query shapes.

## Stream dispatch flow

`dispatch` is for commands whose consistency boundary is one stream.

The backend:

1. loads the stream ordered by revision;
2. decodes rows;
3. folds the stream state;
4. runs the decider;
5. checks that the stream revision still matches;
6. inserts the produced events;
7. returns `Dispatch(event)`.

Use stream dispatch only when the business rule really is protected by one
stream. If the rule needs event types and tags across streams, use
`dispatch_with_query`.

## Codec boundary

The backend never interprets event payload bytes. The application codec decides:

- how to encode event payloads;
- which event type name to store;
- which tags to expose for future queries;
- how to decode old stored rows;
- how to handle unknown or invalid data.

This keeps the backend generic and makes the query contract visible at write
time.

## Reactors and effects

`dispatch.events` contains committed records, not merely domain events. Records
include id, stream, revision, global position, type, version, tags, metadata, and
payload.

That makes them suitable for `factos.Reactor` values:

```gleam
let effects = factos.react_all(ticket_reactor(), dispatch.events)
```

`factos_pog` does not execute those effects. If effects need retry or durability,
model that in application code or a backend-specific adapter.
