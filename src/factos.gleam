//// Store-independent event-sourcing domain primitives.
////
//// Factos keeps the domain model in the application. The core module models
//// facts, command contexts, pure decision components, and pure views. Concrete
//// storage concerns live in backend packages such as `factos_sqlight` and
//// `factos_kurrentdb_erlang`.
////
//// This package follows a context-first reading of Event Sourcing: accepted
//// facts are the authoritative state of the system, and the facts relevant to a
//// command are considered before new facts are accepted. Aggregates, stream-per-
//// object storage, CQRS, projections, and message brokers are implementation
//// choices rather than prerequisites.
////
//// The central flow is:
////
//// 1. Select a command context with `Query`.
//// 2. Fold the matching recorded events into a temporary decision state.
//// 3. Run a pure `Decider`.
//// 4. Append the produced facts only if the context is still stable.
////
//// Backends implement the storage-specific parts of that flow. This module keeps
//// the shared types and pure computations small and portable.

import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result

/// A store-visible event type name.
///
/// Event types are part of the query contract. A backend may use them for
/// efficient context reads, and applications should keep names stable enough
/// for stored history to remain decodable.
pub opaque type EventType {
  EventType(String)
}

/// A store-visible tag value.
///
/// Tags expose selected payload information to the event store so commands can
/// query the facts relevant to a decision. For example, an event payload may
/// contain `username: "renata"`, while the stored event also carries the tag
/// `username:renata`.
pub opaque type Tag {
  Tag(String)
}

pub type Query {
  /// Match every recorded event.
  AllEvents

  /// Match events using one or more query items.
  ///
  /// Query items are OR-combined. See `QueryItem` for the matching rules inside
  /// each item.
  Query(items: List(QueryItem))
}

pub type QueryItem {
  /// One branch of a command-context query.
  ///
  /// Within an item, event types are OR-combined and tags are AND-combined. Empty
  /// `types` means any event type matches. Empty `tags` means no tag constraint.
  ///
  /// A query item with `types: [UserRegistered, UsernameReserved]` and
  /// `tags: [username:renata]` means: events of either type that also have the
  /// `username:renata` tag.
  QueryItem(types: List(EventType), tags: List(Tag))
}

pub type SequencePosition {
  /// No global position was observed.
  NoPosition

  /// A backend-specific global sequence position.
  ///
  /// Positions are used by context append conditions to express "after this
  /// observed point in history". They are not stream revisions.
  SequencePosition(Int)
}

pub type AppendCondition {
  /// Append without an additional context condition.
  NoAppendCondition

  /// Append only if no event matching `query` appeared after `after`.
  ///
  /// This models Command Context Consistency. The decision was made from the
  /// matching facts visible at `after`, so the append must fail if that relevant
  /// context changed before the new facts are recorded.
  FailIfEventsMatch(query: Query, after: SequencePosition)
}

pub type Decider(command, state, event, domain_error) {
  /// A pure command-side domain component.
  ///
  /// `initial` is the empty decision state. `evolve` folds accepted events into
  /// state. `decide` applies a command to the folded state and either returns new
  /// events or a domain error.
  ///
  /// A decider has no dependency on storage, transactions, codecs, projections,
  /// subscriptions, or transports.
  Decider(
    initial: state,
    decide: fn(state, command) -> Result(List(event), domain_error),
    evolve: fn(state, event) -> state,
  )
}

pub type View(state, event) {
  /// A pure projection fold.
  ///
  /// Views derive read-side state from events. They are intentionally only the
  /// computation; persistence, delivery, rebuilds, and subscription management are
  /// outside the core library.
  View(initial: state, evolve: fn(state, event) -> state)
}

pub type Revision {
  /// A stream has no events.
  NoEvents

  /// The last known revision of a stream.
  CurrentRevision(Int)
}

pub type Decoded(event) {
  /// A domain event decoded from backend storage.
  ///
  /// Backends use codecs supplied by the application. The decoded value includes
  /// the domain event plus the event type and tags that should participate in
  /// query matching.
  Decoded(event: event, type_: EventType, tags: List(Tag))
}

pub type Recorded(event) {
  /// A stored event with backend metadata.
  ///
  /// `revision` is the per-stream revision. `position` is the global sequence
  /// position used for context consistency. `type_` and `tags` are store-visible
  /// query metadata.
  Recorded(
    id: String,
    stream: String,
    revision: Int,
    position: SequencePosition,
    type_: EventType,
    tags: List(Tag),
    event: event,
  )
}

pub type Context(event, state) {
  /// A command context read from history.
  ///
  /// The context contains the query that selected the relevant facts, the folded
  /// decision state, the matching recorded events, the highest observed position,
  /// and the append condition needed to protect the decision.
  Context(
    query: Query,
    state: state,
    events: List(Recorded(event)),
    position: SequencePosition,
    append_condition: AppendCondition,
  )
}

pub type LoadedStream(event, state) {
  /// A stream read from history.
  ///
  /// This is the stream-consistency counterpart to `Context`. It contains the
  /// folded state for one stream and its current revision.
  LoadedStream(
    stream: String,
    state: state,
    events: List(Recorded(event)),
    revision: Revision,
  )
}

/// Wrap an event type name.
pub fn event_type(name: String) -> EventType {
  EventType(name)
}

/// Unwrap an event type name.
pub fn event_type_name(event_type: EventType) -> String {
  let EventType(name) = event_type
  name
}

/// Wrap a tag value.
pub fn tag(value: String) -> Tag {
  Tag(value)
}

/// Unwrap a tag value.
pub fn tag_value(tag: Tag) -> String {
  let Tag(value) = tag
  value
}

/// Build a query from query items.
///
/// An empty list becomes `AllEvents`; otherwise the query contains the supplied
/// items. Query items are OR-combined by `matches_query`.
pub fn query(items: List(QueryItem)) -> Query {
  case items {
    [] -> AllEvents
    [_, ..] -> Query(items)
  }
}

/// Build one command-context query branch.
///
/// Event types are OR-combined. Tags are AND-combined. Empty lists act as wildcards
/// for that part of the item.
pub fn query_item(
  types types: List(EventType),
  tags tags: List(Tag),
) -> QueryItem {
  QueryItem(types:, tags:)
}

/// Build a pure command-side decider.
///
/// The supplied functions remain owned by the application domain. Factos only
/// stores them together so backends and tests can run the same read-decide-append
/// flow consistently.
pub fn decider(
  initial initial: state,
  decide decide: fn(state, command) -> Result(List(event), domain_error),
  evolve evolve: fn(state, event) -> state,
) -> Decider(command, state, event, domain_error) {
  Decider(initial:, decide:, evolve:)
}

/// Build a pure projection view.
///
/// A view folds events into read-side state. It does not prescribe where that
/// state is stored or how events are delivered.
pub fn view(
  initial initial: state,
  evolve evolve: fn(state, event) -> state,
) -> View(state, event) {
  View(initial:, evolve:)
}

/// Fold events with a decider and decide which new events a command produces.
///
/// This is useful for unit tests and for in-memory command handling. It does not
/// perform any append or consistency check.
pub fn compute_events(
  decider decider: Decider(command, state, event, domain_error),
  events events: List(event),
  command command: command,
) -> Result(List(event), domain_error) {
  let Decider(initial, decide, evolve) = decider
  decide(fold_events(initial, events, evolve), command)
}

/// Decide from an optional current state and return the state after produced events.
///
/// If `current` is `None`, the decider's initial state is used. The function first
/// runs the decider, then folds the produced events into the decision state.
pub fn compute_state(
  decider decider: Decider(command, state, event, domain_error),
  current current: Option(state),
  command command: command,
) -> Result(state, domain_error) {
  let Decider(initial, decide, evolve) = decider
  let state = case current {
    Some(state) -> state
    None -> initial
  }

  use events <- result.try(decide(state, command))
  Ok(fold_events(state, events, evolve))
}

/// Fold recorded events into state using a domain evolution function.
///
/// Backends use this after decoding stored events. Only the domain event payload is
/// passed to `evolve`; storage metadata is ignored for state computation.
pub fn evolve_recorded(
  initial initial: state,
  events events: List(Recorded(event)),
  evolve evolve: fn(state, event) -> state,
) -> state {
  use state, recorded <- list.fold(events, initial)
  evolve(state, recorded.event)
}

/// Project events from a view's initial state.
pub fn project(
  view view: View(state, event),
  events events: List(event),
) -> state {
  let View(initial, evolve) = view
  fold_events(initial, events, evolve)
}

/// Project events starting from an already materialized view state.
pub fn project_from(
  view view: View(state, event),
  state state: state,
  events events: List(event),
) -> state {
  let View(_, evolve) = view
  fold_events(state, events, evolve)
}

/// Merge two views that consume the same event type.
///
/// The resulting view keeps both states in a tuple and evolves both for every
/// event. This is a convenience for composing small pure projections.
pub fn merge_views(
  first first: View(first_state, event),
  second second: View(second_state, event),
) -> View(#(first_state, second_state), event) {
  let View(first_initial, first_evolve) = first
  let View(second_initial, second_evolve) = second

  use state, event <- View(initial: #(first_initial, second_initial))
  let #(first_state, second_state) = state
  #(first_evolve(first_state, event), second_evolve(second_state, event))
}

/// Run a command against a previously read context.
///
/// The returned tuple preserves the original context alongside the newly produced
/// events so a backend can append them with `context.append_condition`.
pub fn decide_context(
  context: Context(event, state),
  command: command,
  decider: Decider(command, state, event, domain_error),
) -> Result(#(Context(event, state), List(event)), domain_error) {
  let Decider(_, decide, _) = decider
  use events <- result.try(decide(context.state, command))
  Ok(#(context, events))
}

/// Test whether a recorded event belongs to a query-defined context.
pub fn matches_query(recorded: Recorded(event), query: Query) -> Bool {
  case query {
    AllEvents -> True
    Query(items) -> list.any(items, matches_item(recorded, _))
  }
}

/// Return the later of two sequence positions.
///
/// `NoPosition` acts as absence of an observed position. If both positions are
/// concrete, the larger integer is returned.
pub fn highest_position(
  left: SequencePosition,
  right: SequencePosition,
) -> SequencePosition {
  case left, right {
    NoPosition, position -> position
    position, NoPosition -> position
    SequencePosition(left), SequencePosition(right) ->
      case left >= right {
        True -> SequencePosition(left)
        False -> SequencePosition(right)
      }
  }
}

fn fold_events(
  initial: state,
  events: List(event),
  evolve: fn(state, event) -> state,
) -> state {
  use state, event <- list.fold(events, initial)
  evolve(state, event)
}

fn matches_item(recorded: Recorded(event), item: QueryItem) -> Bool {
  let QueryItem(types, tags) = item
  matches_types(recorded.type_, types) && matches_tags(recorded.tags, tags)
}

fn matches_types(event_type: EventType, types: List(EventType)) -> Bool {
  case types {
    [] -> True
    [_, ..] -> {
      use required <- list.any(types)
      event_type == required
    }
  }
}

fn matches_tags(event_tags: List(Tag), required_tags: List(Tag)) -> Bool {
  case required_tags {
    [] -> True
    [_, ..] -> {
      use required <- list.all(required_tags)
      use tag <- list.any(event_tags)
      tag == required
    }
  }
}
