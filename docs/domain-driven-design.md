# Factos and Domain-Driven Design

Factos is not a Domain-Driven Design framework. It is a small event-sourcing
library whose shape fits DDD-style modelling.

The useful connection is this: DDD asks the code to express business decisions in
the language of the domain, and Factos asks each command to name the facts needed
for that decision.

## Domain language stays in the application

Your application defines the business language:

```gleam
pub type Command {
  BuyTicket(buyer: String)
}

pub type Event {
  TicketSold(buyer: String)
}

pub type DomainError {
  SoldOut(capacity: Int)
}
```

Factos does not provide generic `Command`, `Event`, or `Aggregate` interfaces.
Gleam custom types are clearer and give exhaustive pattern matching when the
model changes.

## Commands, facts, and state

DDD models often become clearer when intent, accepted facts, and decision state
are separated.

- A command is intent: `BuyTicket("renata")`.
- An event is an accepted fact: `TicketSold("renata")`.
- State is what the decision needs to know: `TicketWindow(capacity: 100, sold: 42)`.
- A domain error explains business rejection: `SoldOut(capacity: 100)`.

Factos represents this with a `Decider`:

```gleam
factos.decider(
  initial: TicketWindow(capacity: 100, sold: 0),
  decide: decide,
  evolve: evolve,
)
```

The decider is pure. It does not query a database, send emails, publish messages,
or mutate projections.

## Invariants define the context

An invariant is a rule that must remain true when the system accepts a change.

Examples:

- a ticket sale cannot exceed capacity;
- a username cannot be registered twice;
- an account cannot spend more than its available balance;
- an invoice cannot be paid after it was voided.

The key design question is:

> Which facts can change the answer to this command?

Factos calls that set of facts the command context.

For a ticket-sale capacity rule, the context can be all ticket-sale facts for one
event:

```gleam
factos.query([
  factos.query_item(
    types: [factos.event_type("TicketSold")],
    tags: [factos.tag("event:gleamconf-2026")],
  ),
])
```

That context is more precise than saying every command must belong to one
aggregate root.

## Bounded contexts and tags

DDD bounded contexts define where a model and its language are valid. Factos tags
are not a replacement for that modelling work, but they make the storage boundary
explicit.

If the ticketing context needs to protect event capacity, write tags in the
ticketing language:

```gleam
factos.tag("event:gleamconf-2026")
```

If the billing context needs account facts, use billing tags:

```gleam
factos.tag("account:acct_123")
```

The backend treats tags as strings, but the application should treat them as part
of the domain contract.

## Side effects stay outside decisions

A domain decision should not send email, call a payment gateway, write files, or
publish messages.

Factos gives two pure tools after facts exist:

- `View`: fold facts into read-side state;
- `Reactor`: turn committed recorded facts into effect values.

A reactor can say that a ticket-sale announcement is needed:

```gleam
pub type Effect {
  AnnounceTicketSale(buyer: String, position: factos.SequencePosition)
}
```

The application decides how to execute or persist that effect. This keeps replay,
retry, and rebuild logic outside the domain decision.

## What Factos does not decide for you

Factos does not tell you:

- how to split bounded contexts;
- what events should exist;
- what tag names your domain should use;
- how to version event payloads;
- where to store projections;
- how to deliver side effects;
- how to design retry or dead-letter policy.

Those are application design decisions. Factos provides the small set of types
and backend contracts that let those decisions stay explicit.
