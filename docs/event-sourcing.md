# Event Logs and Command Dispatch

Factos stores events, and it also provides helpers for command dispatch.

Those two ideas are related but not identical:

- Event sourcing stores facts that happened: `TicketSold("renata")`.
- Command handling receives orders: `BuyTicket("renata")`.

The backend stores the accepted events. The `Decider` and `dispatch` helpers are a
standard way to process commands on top of that event log.

## What the backend persists

A backend such as `factos_pog` persists event records:

- event id;
- stream;
- stream revision;
- global position;
- event type;
- event version;
- tags;
- metadata;
- opaque payload bytes.

It does not persist commands. It does not persist materialized views. It does not
execute side effects.

## What dispatch does

A dispatch function combines an event log with a command handler:

```text
command + previous events -> new events or domain error
```

For `factos_pog.dispatch_with_query`, the previous events are selected by a
`factos.Query`:

```gleam
factos.query([
  factos.query_item(
    types: [factos.event_type("TicketSold")],
    tags: [factos.tag("event:gleamconf-2026")],
  ),
])
```

The backend reads those events, folds them into state, calls the decider, and
appends the resulting events only if the query context is still stable.

## What makes the append safe

A context read observes a global event-log position. The backend then protects the
append with:

```gleam
factos.FailIfEventsMatch(query, after: position)
```

That means:

> do not append these new events if another event matching the same query was
> accepted after the position used for the decision.

This is how Factos lets the consistency boundary follow the rule. The boundary
can be a tag, a set of event types, one stream, many streams, or all events.

## What views are

A `View` is not a durable projection table. It is a pure fold:

```gleam
factos.view(initial: 0, evolve: fn(count, event) {
  case event {
    TicketSold(_) -> count + 1
  }
})
```

You can run the fold over any list of events. To make a materialized view durable,
your application stores the folded result.

Views can be recomputed as long as the stored event history can still be decoded.
That makes event versioning and codec compatibility an application responsibility.

## What reactors are

A `Reactor` is also pure. It maps committed recorded events to effect values:

```gleam
factos.react_all(ticket_reactor(), dispatch.events)
```

Factos does not execute those effects. That is deliberate: replaying old events
should not accidentally resend emails, charge cards, or publish webhooks.

## Where Factos is opinionated

Factos is low-level about storage and high-level enough to standardize command
dispatch.

It is opinionated that:

- facts are stored as an append-only event log;
- command decisions should be pure;
- context reads should produce append conditions;
- backends should return committed records after append;
- projections and effects should remain explicit application code.

It is not opinionated about:

- your event names;
- your command names;
- payload encoding;
- projection storage;
- subscription infrastructure;
- effect retry policy;
- deployment topology.
