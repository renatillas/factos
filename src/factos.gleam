//// Store-independent event-sourcing domain primitives.
////
//// Factos keeps the domain model in the application. The core module models
//// facts, command contexts, pure decision components, and pure views. Concrete
//// storage concerns live in backend packages such as `factos_sqlight` and
//// `factos_kurrentdb_erlang`.

import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result

pub type EventType {
  EventType(String)
}

pub type Tag {
  Tag(String)
}

pub type Query {
  AllEvents
  Query(items: List(QueryItem))
}

pub type QueryItem {
  QueryItem(types: List(EventType), tags: List(Tag))
}

pub type SequencePosition {
  NoPosition
  SequencePosition(Int)
}

pub type AppendCondition {
  NoAppendCondition
  FailIfEventsMatch(query: Query, after: SequencePosition)
}

pub type Decider(command, state, event, domain_error) {
  Decider(
    initial: state,
    decide: fn(state, command) -> Result(List(event), domain_error),
    evolve: fn(state, event) -> state,
  )
}

pub type View(state, event) {
  View(initial: state, evolve: fn(state, event) -> state)
}

pub type Revision {
  NoEvents
  CurrentRevision(Int)
}

pub type Decoded(event) {
  Decoded(event: event, type_: EventType, tags: List(Tag))
}

pub type Recorded(event) {
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
  Context(
    query: Query,
    state: state,
    events: List(Recorded(event)),
    position: SequencePosition,
    append_condition: AppendCondition,
  )
}

pub type LoadedStream(event, state) {
  LoadedStream(
    stream: String,
    state: state,
    events: List(Recorded(event)),
    revision: Revision,
  )
}

pub fn event_type(name: String) -> EventType {
  EventType(name)
}

pub fn event_type_name(event_type: EventType) -> String {
  let EventType(name) = event_type
  name
}

pub fn tag(value: String) -> Tag {
  Tag(value)
}

pub fn tag_value(tag: Tag) -> String {
  let Tag(value) = tag
  value
}

pub fn query(items: List(QueryItem)) -> Query {
  case items {
    [] -> AllEvents
    [_, ..] -> Query(items)
  }
}

pub fn query_item(
  types types: List(EventType),
  tags tags: List(Tag),
) -> QueryItem {
  QueryItem(types:, tags:)
}

pub fn decider(
  initial initial: state,
  decide decide: fn(state, command) -> Result(List(event), domain_error),
  evolve evolve: fn(state, event) -> state,
) -> Decider(command, state, event, domain_error) {
  Decider(initial:, decide:, evolve:)
}

pub fn view(
  initial initial: state,
  evolve evolve: fn(state, event) -> state,
) -> View(state, event) {
  View(initial:, evolve:)
}

pub fn compute_events(
  decider decider: Decider(command, state, event, domain_error),
  events events: List(event),
  command command: command,
) -> Result(List(event), domain_error) {
  let Decider(initial, decide, evolve) = decider
  decide(fold_events(initial, events, evolve), command)
}

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

pub fn evolve_recorded(
  initial initial: state,
  events events: List(Recorded(event)),
  evolve evolve: fn(state, event) -> state,
) -> state {
  use state, recorded <- list.fold(events, initial)
  evolve(state, recorded.event)
}

pub fn project(
  view view: View(state, event),
  events events: List(event),
) -> state {
  let View(initial, evolve) = view
  fold_events(initial, events, evolve)
}

pub fn project_from(
  view view: View(state, event),
  state state: state,
  events events: List(event),
) -> state {
  let View(_, evolve) = view
  fold_events(state, events, evolve)
}

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

pub fn decide_context(
  context: Context(event, state),
  command: command,
  decider: Decider(command, state, event, domain_error),
) -> Result(#(Context(event, state), List(event)), domain_error) {
  let Decider(_, decide, _) = decider
  use events <- result.try(decide(context.state, command))
  Ok(#(context, events))
}

pub fn matches_query(recorded: Recorded(event), query: Query) -> Bool {
  case query {
    AllEvents -> True
    Query(items) -> list.any(items, matches_item(recorded, _))
  }
}

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
