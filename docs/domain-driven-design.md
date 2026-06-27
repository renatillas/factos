# What Is Domain-Driven Design?

Domain-Driven Design, often shortened to DDD, is an approach to building software
around the business domain it serves.

The main idea is simple: the most important parts of the code should speak the
same language as the people who understand the business. If the business talks
about registering users, reserving tickets, approving invoices, opening accounts,
or cancelling orders, the code should make those concepts visible instead of
hiding them behind generic database or framework terms.

DDD is not a library, framework, folder structure, or architecture pattern. It is
a way of designing software so that the model in the code matches the model used
by domain experts.

## The Domain

The domain is the area of knowledge the software is about.

For an accounting system, the domain includes invoices, payments, credits,
balances, tax rules, and financial periods. For a ticketing system, it includes
events, tickets, reservations, customers, seat maps, and sales rules.

DDD asks you to put this domain knowledge at the center of the design. Technical
details still matter, but they should support the domain model rather than
dominate it.

## Ubiquitous Language

A key DDD practice is building a ubiquitous language: a shared vocabulary used by
developers, domain experts, product people, and the code itself.

If the business says a username can be reserved, registered, or already taken,
the code should use names like these:

```gleam
pub type Command {
  RegisterUser(username: String)
}

pub type Event {
  UsernameReserved(username: String)
  UserRegistered(username: String)
}

pub type DomainError {
  UsernameAlreadyTaken
}
```

These names are not just labels. They document the business rules and make
conversations easier. A developer and a domain expert can discuss
`UsernameAlreadyTaken` without translating from implementation details such as
rows, tables, HTTP requests, or storage errors.

## Bounded Contexts

Large systems rarely have one perfect model for everything. The same word can
mean different things in different parts of a business.

For example, a `Customer` in billing may mean the legal entity that receives an
invoice. A `Customer` in support may mean the person who opened a ticket. Both
models can be correct inside their own part of the system.

DDD calls these boundaries bounded contexts. A bounded context defines where a
particular model and language are valid.

In Gleam, that often means keeping types and functions focused on one context at
a time. A billing command, billing event, and billing error type should not have
to carry every detail from support, inventory, or fulfilment.

## Entities, Values, and Rules

DDD distinguishes between different kinds of domain concepts.

Entities have identity over time. An order, account, or user may change while
remaining the same conceptual thing.

Value objects are defined by their contents. A money amount, email address,
date range, or seat number is usually meaningful because of its value rather than
because of a separate identity.

Business rules describe what is allowed. A username may be registered only if it
is available. A ticket may be sold only if capacity remains. An invoice may be
paid only once.

Gleam custom types are useful here because they let you model these concepts
directly and avoid many invalid states.

```gleam
pub type UsernameState {
  UsernameAvailable
  UsernameTaken
}
```

This is clearer than passing around a generic boolean whose meaning can be
forgotten or inverted.

## Invariants

An invariant is a rule that must remain true whenever the system accepts a
change.

Examples include:

1. A username cannot be registered twice.
2. A paid invoice cannot be paid again.
3. A ticket sale cannot exceed venue capacity.
4. A bank account cannot be closed while it has a non-zero balance.

DDD encourages making these rules explicit in the domain model. In Gleam, a rule
can be represented as a pure function that receives the relevant state and either
accepts the command or returns a domain error.

```gleam
pub fn decide(state: UsernameState, command: Command) -> Result(List(Event), DomainError) {
  case state, command {
    UsernameAvailable, RegisterUser(username) -> Ok([UserRegistered(username)])
    UsernameTaken, RegisterUser(_) -> Error(UsernameAlreadyTaken)
  }
}
```

The function describes the business rule without mentioning databases, JSON,
queues, web handlers, or transactions.

## How Factos Applies DDD

Factos supports DDD by keeping the domain model in the application.

Your application defines the commands, events, states, and errors using its own
business language. Factos provides small primitives for turning those definitions
into pure decision components and for connecting those decisions to event storage.

A Factos `Decider` is made from:

1. an initial state,
2. a function that decides whether a command is allowed,
3. a function that evolves state from accepted facts.

This keeps the important business rule easy to read, easy to test, and separate
from infrastructure.

## Why This Fits Gleam

Gleam works well for DDD because it encourages explicit, concrete modelling:

1. Custom types name the concepts in the domain.
2. Pattern matching makes business cases visible.
3. Exhaustive checks help when the model changes.
4. `Result` makes domain rejection explicit.
5. Pure functions keep rules independent from infrastructure.

DDD is mostly about clarity. Gleam helps by making that clarity part of the type
system instead of leaving it only in comments or diagrams.
