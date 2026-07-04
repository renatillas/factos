# Event Sourcing

Factos follows the interpretation described in Rico Fritzsche's
[Simply Event Sourcing](https://ricofritzsche.me/simply-event-sourcing/): Event
Sourcing means accepted facts are stored as the source of truth, and the facts
relevant to a decision are used before new facts are accepted.

Aggregates, CQRS, projections, stream-per-object storage, message brokers, and
microservices can be useful implementation choices, but they are not the
definition of Event Sourcing.

## Events Are Accepted Facts

An event is something the application has accepted as true. Current state is not
the authority; it is derived by folding the event history.

```gleam
pub type Event {
  UsernameReserved(username: String)
  UserRegistered(username: String)
  DisplayNameChanged(user_id: String, name: String)
}
```

In Gleam, each application defines its own event type. There is no need for a
base event interface or runtime inheritance. The type tells readers and the
compiler which facts exist in this part of the domain.

## Decisions Use Relevant History

To handle a command, load the relevant facts, fold them into a temporary state,
and run a pure decision function.

```gleam
pub fn evolve(state: State, event: Event) -> State {
  case state, event {
    UsernameAvailable, UsernameReserved(_) -> UsernameTaken
    UsernameAvailable, UserRegistered(_) -> UsernameTaken
    UsernameAvailable, DisplayNameChanged(_, _) -> UsernameAvailable
    UsernameTaken, UsernameReserved(_) -> UsernameTaken
    UsernameTaken, UserRegistered(_) -> UsernameTaken
    UsernameTaken, DisplayNameChanged(_, _) -> UsernameTaken
  }
}
```

That folded state is only the state needed for the decision. Read models,
reports, caches, and UI projections can be built separately with `View` values.

## Context-First Consistency

Factos models command context consistency with `Query`, `Context`, and
`AppendCondition`.

For username registration, the context may be all events of selected types tagged
with the requested username:

```gleam
pub fn username_context(username: String) -> factos.Query {
  factos.query([
    factos.query_item(
      types: [
        factos.event_type("UsernameReserved"),
        factos.event_type("UserRegistered"),
      ],
      tags: [factos.tag("username:" <> username)],
    ),
  ])
}
```

After the decision is made, the backend should append the new facts only if no
matching facts appeared after the position used for the decision. This protects
the invariant without forcing every command into a single aggregate stream.

## Applying the Idea in Gleam

The typical Factos flow is:

1. Model domain commands, events, states, and errors as Gleam custom types.
2. Write an `evolve` function that folds accepted events into decision state.
3. Write a `decide` function that returns `Result(List(Event), DomainError)`.
4. Define the command context with event types and tags.
5. Let a backend load matching facts and protect the append with the returned condition.
6. React to the committed recorded facts with pure reactors if application effects are needed.

This style keeps Event Sourcing concrete. The important parts are plain Gleam
functions and types, while storage backends handle persistence, codecs, append
guarantees, and the committed records that application code may react to.
