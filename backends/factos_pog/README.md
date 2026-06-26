# factos_pog

PostgreSQL backend for Factos using [`pog`](https://hex.pm/packages/pog).

This backend follows the "Simply Event Sourcing" shape used by Factos: accepted facts are stored in an append-only event table, command handlers select the facts relevant to their decision, and appends are accepted only when that command context is still stable.

## Design Notes

The table stores opaque event bytes plus store-visible query metadata: event type and tags. This is a DCB-style tradeoff. PostgreSQL does not need to understand payloads, but any payload value needed by future context queries must be exposed as a tag when the event is written.

`dispatch_context` runs inside a PostgreSQL transaction and locks the event table before reading, deciding, checking, and appending. This is intentionally conservative. It makes arbitrary `FailIfEventsMatch(query, after)` checks correct without trying to infer lock keys from dynamic query metadata. A higher-throughput backend could replace the table lock with advisory locks or more granular query-specific locks, but only if it preserves the same context-stability guarantee.

`dispatch_stream` is also available for applications where one stream revision really is the intended consistency boundary. It is an implementation strategy, not the definition of Event Sourcing.

## Usage

Start a `pog` pool in your application supervision tree, run `migrate`, then call `dispatch_context` or `dispatch_stream` with your domain decider and codec.

```gleam
let connection = pog.named_connection(pool_name)
let assert Ok(Nil) = factos_pog.migrate(connection)
```

Your codec owns event serialization. The backend only persists bytes and query metadata.
