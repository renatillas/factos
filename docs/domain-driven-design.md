# What Is Domain-Driven Design?

Domain-Driven Design, often shortened to DDD, is an approach to building software
around the business domain it serves.

The central idea is that the most important code in a system should reflect the
real business concepts, rules, and language of the people who understand that
business. If the business talks about approving invoices, reserving seats,
opening accounts, settling payments, or cancelling bookings, those ideas should
be visible in the code.

DDD is not a framework, library, diagramming technique, or folder structure. It is
a way of designing software so that the model in the code and the model used by
domain experts stay close to each other.

## Why DDD Exists

Many systems start as technical models rather than domain models. The code is
organized around tables, controllers, endpoints, jobs, forms, or generic data
objects. That can work for simple CRUD applications, but it becomes painful when
the business rules are complex.

When the business is complex, bugs often come from misunderstandings:

1. Developers use a word differently from domain experts.
2. One part of the system assumes a rule that another part does not know about.
3. Important business states are represented as vague strings or booleans.
4. Rules are duplicated across handlers, database triggers, and background jobs.
5. Technical names hide the actual reason a change is allowed or rejected.

DDD tries to reduce this gap. It encourages teams to discover the domain language,
model the important rules directly, and keep technical details from overwhelming
the business model.

## The Domain

The domain is the area of knowledge the software is about.

For an accounting system, the domain may include invoices, payments, credits,
taxes, ledgers, balances, and accounting periods. For a ticketing system, it may
include venues, seats, shows, reservations, ticket sales, refunds, and capacity
rules. For a logistics system, it may include shipments, routes, warehouses,
carriers, customs declarations, and delivery promises.

The domain is not the database. It is not the UI. It is not the HTTP API. Those
things are implementation details around the domain.

DDD asks: what are the important concepts and rules of this business, and how can
the code express them clearly?

## Domain Experts

Domain experts are the people who understand the business problem. They may be
accountants, support agents, warehouse operators, doctors, insurance analysts,
lawyers, product managers, or experienced users.

They may not know how to write code, but they know the rules the system must
respect. DDD treats their language and distinctions as design input, not just as
requirements that are translated once and forgotten.

For example, a domain expert might explain that an order is not simply
"cancelled". It may be cancelled before payment, cancelled after payment but
before shipping, or returned after fulfilment. Those distinctions matter because
they lead to different business consequences.

Good DDD keeps asking for these distinctions until the software model can express
them.

## Ubiquitous Language

A ubiquitous language is a shared vocabulary used by developers, domain experts,
product people, documentation, tests, and code.

The goal is to avoid translation layers such as:

1. The business says "reservation", but the code says `TemporaryOrder`.
2. The business says "settled payment", but the database says `status = 4`.
3. The business says "invoice is overdue", but the code says `is_bad = true`.

These translations create confusion. When the code uses the same words as the
business, conversations become more precise and mistakes are easier to see.

For example, this model communicates business meaning:

```gleam
pub type InvoiceStatus {
  Draft
  Issued
  Paid
  Overdue
  Void
}
```

This model hides it:

```gleam
pub type Invoice {
  Invoice(status: Int)
}
```

Both can be stored in a database, but only the first one explains the business
states in the code itself.

## Models Are Purposeful

In DDD, a model is not a perfect copy of the real world. It is a useful
simplification for a particular purpose.

A delivery application does not need to model every physical detail of a parcel.
It may only need weight, dimensions, destination, customs category, and delivery
promise. A medical scheduling system may care about appointments, clinicians,
rooms, and patient eligibility, but not the internal details of billing.

A good model includes the distinctions needed to make correct business decisions.
It leaves out detail that does not matter for those decisions.

## Bounded Contexts

Large organizations rarely have one universal model. The same word can mean
different things in different parts of the business.

For example, `Customer` can mean:

1. the person receiving support,
2. the legal entity that receives an invoice,
3. the buyer placing an order,
4. the account holder with contractual obligations.

Trying to force all of these meanings into one universal `Customer` model often
creates confusion. Each team adds fields for its own needs, and the model becomes
large, vague, and hard to change.

DDD uses bounded contexts to avoid this. A bounded context defines where a model
and its language are valid.

Inside the billing context, `Customer` might mean the billable legal entity.
Inside the support context, `Customer` might mean the person asking for help. Both
models can be correct because they serve different purposes.

## Context Boundaries Matter

Bounded contexts are not only about code organization. They are about meaning.

Crossing a boundary often requires translation. A support case may refer to a
customer email address, while billing may require a billing account id. A shipping
context may know about parcels and labels, while sales may know about orders and
line items.

Keeping these models separate prevents accidental coupling. It also lets each
part of the system evolve with the language and rules of its own business area.

## Entities

An entity is a domain concept with identity over time.

For example:

1. an order,
2. a user account,
3. a bank account,
4. a shipment,
5. a support case.

An entity can change while remaining the same conceptual thing. An order may move
from `Draft` to `Placed` to `Shipped`. A bank account balance may change. A
support case may be reassigned. The identity is what lets the business say it is
still the same order, account, or case.

Entities should not become bags of data with every possible operation attached.
Their purpose is to protect the rules that belong to their identity and lifecycle.

## Value Objects

A value object is a domain concept defined by its contents rather than by a
separate identity.

Examples include:

1. money amount,
2. email address,
3. date range,
4. seat number,
5. geographic coordinate,
6. percentage discount.

Two value objects with the same contents are usually interchangeable. If two
prices are both `10 EUR`, they represent the same value. If two date ranges cover
the same dates, they represent the same range.

Value objects are useful because they give names and validation rules to concepts
that would otherwise be primitive strings, integers, or floats.

```gleam
pub type Money {
  Money(amount_in_cents: Int, currency: Currency)
}

pub type Currency {
  Eur
  Usd
  Gbp
}
```

This is clearer than passing separate integers and strings through the system and
hoping every function interprets them correctly.

## Invariants

An invariant is a rule that must remain true whenever the system accepts a
change.

Examples include:

1. a username cannot be registered twice,
2. a paid invoice cannot be paid again,
3. a ticket sale cannot exceed venue capacity,
4. a bank account cannot be closed while it has a non-zero balance,
5. a shipment cannot be marked delivered before it has been dispatched.

Invariants are central to DDD because they define what the model must protect.
They are different from ordinary validation.

Validation might check that an email address has a plausible shape. An invariant
checks whether a change is allowed by the current business state.

Each command should make clear which facts are needed to protect the invariant it
cares about. The consistency boundary can then follow the rule being checked
rather than a fixed object hierarchy.

## Commands, Events, and State

DDD does not require event sourcing, but many DDD models benefit from separating
intent, facts, and state.

A command represents intent. It asks the system to do something:

```gleam
pub type Command {
  IssueInvoice(customer_id: String, amount: Money)
  PayInvoice(invoice_id: String)
  VoidInvoice(invoice_id: String, reason: String)
}
```

An event represents something the business has accepted as true:

```gleam
pub type Event {
  InvoiceIssued(invoice_id: String, customer_id: String, amount: Money)
  InvoicePaid(invoice_id: String)
  InvoiceVoided(invoice_id: String, reason: String)
}
```

State represents what the model needs to know to make a decision:

```gleam
pub type InvoiceState {
  NoInvoice
  OpenInvoice(amount: Money)
  PaidInvoice
  VoidedInvoice
}
```

Keeping these concepts separate makes the model easier to reason about. A command
can be rejected. An event is already accepted. State is a derived view used for a
decision.

## Domain Errors

Domain errors explain why a business operation is not allowed.

They should use domain language rather than infrastructure language.

```gleam
pub type PaymentError {
  InvoiceDoesNotExist
  InvoiceAlreadyPaid
  InvoiceWasVoided
}
```

This is more useful than returning generic errors such as `BadRequest`,
`DatabaseError`, or `InvalidState` from the domain model. Technical errors can
still exist at the application or infrastructure boundary, but business rejection
should be described in business terms.

## Services and Side Effects

DDD separates domain rules from technical side effects.

The domain model should decide whether something is allowed. It should not usually
send emails, call payment providers, write files, publish messages, or make HTTP
requests directly.

Those effects belong in application or infrastructure code that coordinates the
use case. The domain should expose the business decision clearly enough that the
outer code knows what happened and what effects are needed.

For example:

1. The domain accepts `PayInvoice` and produces `InvoicePaid`.
2. Application code stores that fact.
3. A pure reactor inspects the committed recorded fact and returns effect values.
4. Application or infrastructure code sends a receipt email and updates a reporting view.

The receipt email is important, but sending it is not the same as deciding
whether the invoice may be paid. The reactor can describe the needed effect, but
it should not hide IO inside the domain rule.

## Strategic and Tactical DDD

DDD is often described in two parts: strategic design and tactical design.

Strategic design is about understanding the larger system:

1. What are the bounded contexts?
2. Which teams own which models?
3. Which contexts need to integrate?
4. Where is the core business complexity?
5. Which parts can be simpler supporting systems?

Tactical design is about modelling inside a context:

1. What are the entities?
2. What are the value objects?
3. What invariants must be protected?
4. What commands can be accepted?
5. What domain errors can happen?
6. Which facts are needed to protect each invariant?

Both matter. Tactical patterns without strategic boundaries can produce a large,
overcomplicated model. Strategic diagrams without concrete code can fail to
protect the real rules.

## How Gleam Helps

Gleam is a good fit for DDD because it encourages explicit modelling with small,
concrete types.

Custom types can name business states directly. Pattern matching makes business
cases visible. Exhaustive checks help when the model changes. `Result` makes
business rejection explicit. Pure functions keep domain decisions separate from
infrastructure.

DDD is mostly about clarity. Gleam helps make that clarity executable.

## How Factos Fits

Factos is not required to practice DDD. It is one small set of primitives for
applications that want to model domain decisions from accepted facts.

With Factos, the application still owns the domain language. The application
defines its commands, events, states, errors, business rules, and effect values.
Factos provides supporting types for pure decisions, pure projections, pure
reactors, and event-backed consistency, while storage and effect execution remain
outside the domain model.
