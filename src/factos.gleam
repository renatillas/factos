//// Context-first event-sourcing helpers for Gleam and KurrentDB.
////
//// Event Sourcing does not require aggregates. A command capability should read
//// the facts relevant to its decision, fold a temporary decision model, decide
//// which new facts to record, and then record those facts only when the relevant
//// context is still stable. This module models that API directly.
////
//// KurrentDB's regular append API can guarantee stream revision consistency. It
//// cannot, through the operations used here, atomically guarantee a DCB-style
//// "fail if events matching this query were appended after position X" check.
//// The API keeps those concepts separate instead of pretending they are the same.

import gleam/list
import gleam/result
import kurrentdb/operation/append_to_stream
import kurrentdb/operation/read_all
import kurrentdb/operation/read_stream
import kurrentdb_erlang
import youid/uuid.{type Uuid}

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
  SequencePosition(commit_position: Int, prepare_position: Int)
}

pub type AppendCondition {
  NoAppendCondition
  FailIfEventsMatch(query: Query, after: SequencePosition)
}

pub type Revision {
  NoEvents
  CurrentRevision(Int)
}

pub type Decoded(event) {
  Decoded(event: event, type_: EventType, tags: List(Tag))
}

pub type Proposed(event) {
  Proposed(
    event: event,
    type_: EventType,
    tags: List(Tag),
    message: append_to_stream.Event,
  )
}

pub type EventCodec(event, decode_error) {
  EventCodec(
    encode: fn(event) -> Proposed(event),
    decode: fn(read_stream.RecordedEvent) ->
      Result(Decoded(event), decode_error),
  )
}

pub type Recorded(event) {
  Recorded(
    id: Uuid,
    stream: String,
    revision: Int,
    position: SequencePosition,
    metadata: List(#(String, String)),
    custom_metadata: BitArray,
    data: BitArray,
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

pub type Error(domain_error, decode_error) {
  DomainError(domain_error)
  DecodeError(decode_error)
  ReadError(kurrentdb_erlang.Error(read_stream.ResponseError))
  AppendError(kurrentdb_erlang.Error(append_to_stream.ResponseError))
  ReadTimedOut
  UnsupportedAppendCondition(AppendCondition)
}

pub fn event_type(name: String) -> EventType {
  EventType(name)
}

pub fn tag(value: String) -> Tag {
  Tag(value)
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

pub fn read_context(
  connection: kurrentdb_erlang.Connection,
  query query: Query,
  initial initial: state,
  evolve evolve: fn(state, event) -> state,
  codec codec: EventCodec(event, decode_error),
  timeout timeout: Int,
) -> Result(Context(event, state), Error(domain_error, decode_error)) {
  let stream =
    kurrentdb_erlang.read_all(
      connection,
      config: read_all.configure()
        |> read_all.filter(query_to_read_all_filter(query)),
    )

  receive_context(
    stream,
    query,
    initial,
    evolve,
    codec,
    [],
    NoPosition,
    timeout,
  )
}

pub fn decide_context(
  context: Context(event, state),
  command: command,
  decide: fn(state, command) -> Result(List(event), domain_error),
) -> Result(
  #(Context(event, state), List(event)),
  Error(domain_error, decode_error),
) {
  use events <- result.try(
    decide(context.state, command)
    |> result.map_error(DomainError),
  )

  Ok(#(context, events))
}

pub fn append_with_condition(
  connection: kurrentdb_erlang.Connection,
  stream stream_name: String,
  events events: List(event),
  codec codec: EventCodec(event, decode_error),
  condition condition: AppendCondition,
  timeout timeout: Int,
) -> Result(append_to_stream.Append, Error(domain_error, decode_error)) {
  case condition {
    NoAppendCondition ->
      append_to_stream_with_config(
        connection,
        stream_name,
        events,
        codec,
        append_to_stream.configure() |> append_to_stream.any,
        timeout,
      )
    FailIfEventsMatch(_, _) -> Error(UnsupportedAppendCondition(condition))
  }
}

pub fn load_stream(
  connection: kurrentdb_erlang.Connection,
  stream stream_name: String,
  initial initial: state,
  evolve evolve: fn(state, event) -> state,
  codec codec: EventCodec(event, decode_error),
  timeout timeout: Int,
) -> Result(LoadedStream(event, state), Error(domain_error, decode_error)) {
  let stream =
    kurrentdb_erlang.read_stream(
      connection,
      stream: stream_name,
      config: read_stream.configure(),
    )

  receive_stream(
    stream,
    stream_name,
    initial,
    evolve,
    codec,
    [],
    NoEvents,
    timeout,
  )
}

pub fn dispatch_stream(
  connection: kurrentdb_erlang.Connection,
  stream stream_name: String,
  initial initial: state,
  decide decide: fn(state, command) -> Result(List(event), domain_error),
  evolve evolve: fn(state, event) -> state,
  codec codec: EventCodec(event, decode_error),
  command command: command,
  timeout timeout: Int,
) -> Result(append_to_stream.Append, Error(domain_error, decode_error)) {
  use loaded <- result.try(load_stream(
    connection,
    stream: stream_name,
    initial: initial,
    evolve: evolve,
    codec: codec,
    timeout: timeout,
  ))

  use events <- result.try(
    decide(loaded.state, command)
    |> result.map_error(DomainError),
  )

  append_stream_events(
    connection,
    stream: stream_name,
    events: events,
    codec: codec,
    expected: loaded.revision,
    timeout: timeout,
  )
}

pub fn append_stream_events(
  connection: kurrentdb_erlang.Connection,
  stream stream_name: String,
  events events: List(event),
  codec codec: EventCodec(event, decode_error),
  expected expected: Revision,
  timeout timeout: Int,
) -> Result(append_to_stream.Append, Error(domain_error, decode_error)) {
  append_to_stream_with_config(
    connection,
    stream_name,
    events,
    codec,
    append_config(expected),
    timeout,
  )
}

pub fn fold(
  initial: state,
  events: List(Recorded(event)),
  evolve: fn(state, event) -> state,
) -> state {
  use state, recorded <- list.fold(events, initial)
  evolve(state, recorded.event)
}

pub fn matches_query(recorded: Recorded(event), query: Query) -> Bool {
  case query {
    AllEvents -> True
    Query(items) -> list.any(items, fn(item) { matches_item(recorded, item) })
  }
}

pub fn highest_position(
  left: SequencePosition,
  right: SequencePosition,
) -> SequencePosition {
  case left, right {
    NoPosition, position -> position
    position, NoPosition -> position
    SequencePosition(left_commit, left_prepare),
      SequencePosition(right_commit, right_prepare)
    -> {
      case
        left_commit > right_commit
        || { left_commit == right_commit && left_prepare >= right_prepare }
      {
        True -> left
        False -> right
      }
    }
  }
}

fn append_to_stream_with_config(
  connection: kurrentdb_erlang.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, decode_error),
  config: append_to_stream.Configuration,
  timeout: Int,
) -> Result(append_to_stream.Append, Error(domain_error, decode_error)) {
  case events {
    [] ->
      Ok(append_to_stream.Append(
        current_revision: -1,
        position: append_to_stream.NoPositionReturned,
      ))
    [_, ..] -> {
      let EventCodec(encode, _) = codec
      let task =
        kurrentdb_erlang.append_to_stream(
          connection,
          stream: stream_name,
          events: list.map(events, fn(event) {
            let Proposed(message: message, ..) = encode(event)
            message
          }),
          config: config,
        )

      kurrentdb_erlang.await(task, within: timeout)
      |> result.map_error(AppendError)
    }
  }
}

fn receive_context(
  stream: kurrentdb_erlang.Stream,
  query: Query,
  state: state,
  evolve: fn(state, event) -> state,
  codec: EventCodec(event, decode_error),
  events: List(Recorded(event)),
  position: SequencePosition,
  timeout: Int,
) -> Result(Context(event, state), Error(domain_error, decode_error)) {
  case kurrentdb_erlang.receive(stream, within: timeout) {
    Error(Nil) -> {
      kurrentdb_erlang.close(stream)
      Error(ReadTimedOut)
    }
    Ok(kurrentdb_erlang.StreamFinished) -> {
      kurrentdb_erlang.close(stream)
      Ok(Context(
        query:,
        state:,
        events: list.reverse(events),
        position:,
        append_condition: FailIfEventsMatch(query, position),
      ))
    }
    Ok(kurrentdb_erlang.StreamFailed(error)) -> {
      kurrentdb_erlang.close(stream)
      Error(ReadError(error))
    }
    Ok(kurrentdb_erlang.ReadEvent(event)) ->
      receive_context_event(
        stream,
        query,
        state,
        evolve,
        codec,
        events,
        position,
        timeout,
        event,
      )
    Ok(kurrentdb_erlang.ReadMessage(read_stream.ReadEvent(event))) ->
      receive_context_event(
        stream,
        query,
        state,
        evolve,
        codec,
        events,
        position,
        timeout,
        event,
      )
    Ok(kurrentdb_erlang.ReadMessage(read_stream.LastAllStreamPosition(read_stream.Position(
      commit_position: commit_position,
      prepare_position: prepare_position,
    )))) ->
      receive_context(
        stream,
        query,
        state,
        evolve,
        codec,
        events,
        highest_position(
          position,
          SequencePosition(commit_position, prepare_position),
        ),
        timeout,
      )
    Ok(kurrentdb_erlang.ReadMessage(_)) ->
      receive_context(
        stream,
        query,
        state,
        evolve,
        codec,
        events,
        position,
        timeout,
      )
  }
}

fn receive_context_event(
  stream: kurrentdb_erlang.Stream,
  query: Query,
  state: state,
  evolve: fn(state, event) -> state,
  codec: EventCodec(event, decode_error),
  events: List(Recorded(event)),
  position: SequencePosition,
  timeout: Int,
  read_event: read_stream.ReadEvent,
) -> Result(Context(event, state), Error(domain_error, decode_error)) {
  use recorded <- result.try(decode_recorded(read_event, codec))
  let next_position = highest_position(position, recorded.position)

  case matches_query(recorded, query) {
    True ->
      receive_context(
        stream,
        query,
        evolve(state, recorded.event),
        evolve,
        codec,
        [recorded, ..events],
        next_position,
        timeout,
      )
    False ->
      receive_context(
        stream,
        query,
        state,
        evolve,
        codec,
        events,
        next_position,
        timeout,
      )
  }
}

fn receive_stream(
  stream: kurrentdb_erlang.Stream,
  stream_name: String,
  state: state,
  evolve: fn(state, event) -> state,
  codec: EventCodec(event, decode_error),
  events: List(Recorded(event)),
  revision: Revision,
  timeout: Int,
) -> Result(LoadedStream(event, state), Error(domain_error, decode_error)) {
  case kurrentdb_erlang.receive(stream, within: timeout) {
    Error(Nil) -> {
      kurrentdb_erlang.close(stream)
      Error(ReadTimedOut)
    }
    Ok(kurrentdb_erlang.StreamFinished) -> {
      kurrentdb_erlang.close(stream)
      Ok(LoadedStream(
        stream: stream_name,
        state:,
        events: list.reverse(events),
        revision:,
      ))
    }
    Ok(kurrentdb_erlang.StreamFailed(error)) -> {
      kurrentdb_erlang.close(stream)
      case error {
        kurrentdb_erlang.OperationError(read_stream.ReadStreamNotFound(_)) ->
          Ok(LoadedStream(
            stream: stream_name,
            state:,
            events: [],
            revision: NoEvents,
          ))
        _ -> Error(ReadError(error))
      }
    }
    Ok(kurrentdb_erlang.ReadEvent(event)) ->
      receive_stream_event(
        stream,
        stream_name,
        state,
        evolve,
        codec,
        events,
        timeout,
        event,
      )
    Ok(kurrentdb_erlang.ReadMessage(read_stream.ReadEvent(event))) ->
      receive_stream_event(
        stream,
        stream_name,
        state,
        evolve,
        codec,
        events,
        timeout,
        event,
      )
    Ok(kurrentdb_erlang.ReadMessage(_)) ->
      receive_stream(
        stream,
        stream_name,
        state,
        evolve,
        codec,
        events,
        revision,
        timeout,
      )
  }
}

fn receive_stream_event(
  stream: kurrentdb_erlang.Stream,
  stream_name: String,
  state: state,
  evolve: fn(state, event) -> state,
  codec: EventCodec(event, decode_error),
  events: List(Recorded(event)),
  timeout: Int,
  read_event: read_stream.ReadEvent,
) -> Result(LoadedStream(event, state), Error(domain_error, decode_error)) {
  use recorded <- result.try(decode_recorded(read_event, codec))

  receive_stream(
    stream,
    stream_name,
    evolve(state, recorded.event),
    evolve,
    codec,
    [recorded, ..events],
    CurrentRevision(recorded.revision),
    timeout,
  )
}

fn decode_recorded(
  read_event: read_stream.ReadEvent,
  codec: EventCodec(event, decode_error),
) -> Result(Recorded(event), Error(domain_error, decode_error)) {
  let recorded_event = case read_event {
    read_stream.Recorded(event) -> event
    read_stream.Resolved(event: event, ..) -> event
  }

  let EventCodec(_, decode) = codec
  use decoded <- result.try(
    decode(recorded_event)
    |> result.map_error(DecodeError),
  )

  let Decoded(event, type_, tags) = decoded
  Ok(Recorded(
    id: recorded_event.id,
    stream: recorded_event.stream,
    revision: recorded_event.revision,
    position: SequencePosition(
      recorded_event.commit_position,
      recorded_event.prepare_position,
    ),
    metadata: recorded_event.metadata,
    custom_metadata: recorded_event.custom_metadata,
    data: recorded_event.data,
    type_: type_,
    tags: tags,
    event: event,
  ))
}

fn query_to_read_all_filter(query: Query) -> read_all.Filter {
  let types = query_event_type_names(query)

  case types {
    [] -> read_all.NoFilter
    [_, ..] -> read_all.EventTypePrefix(types, window: read_all.FilterMax(1000))
  }
}

fn query_event_type_names(query: Query) -> List(String) {
  case query {
    AllEvents -> []
    Query(items) -> collect_type_names(items, [])
  }
}

fn collect_type_names(
  items: List(QueryItem),
  names: List(String),
) -> List(String) {
  case items {
    [] -> list.reverse(names)
    [QueryItem(types, _), ..rest] ->
      collect_type_names(rest, prepend_type_names(types, names))
  }
}

fn prepend_type_names(
  types: List(EventType),
  names: List(String),
) -> List(String) {
  case types {
    [] -> names
    [EventType(name), ..rest] -> prepend_type_names(rest, [name, ..names])
  }
}

fn matches_item(recorded: Recorded(event), item: QueryItem) -> Bool {
  let QueryItem(types, tags) = item
  matches_types(recorded.type_, types) && matches_tags(recorded.tags, tags)
}

fn matches_types(event_type: EventType, types: List(EventType)) -> Bool {
  case types {
    [] -> True
    [_, ..] -> list.any(types, fn(required) { event_type == required })
  }
}

fn matches_tags(event_tags: List(Tag), required_tags: List(Tag)) -> Bool {
  case required_tags {
    [] -> True
    [_, ..] ->
      list.all(required_tags, fn(required) {
        list.any(event_tags, fn(tag) { tag == required })
      })
  }
}

fn append_config(revision: Revision) -> append_to_stream.Configuration {
  case revision {
    NoEvents -> append_to_stream.configure() |> append_to_stream.no_stream
    CurrentRevision(revision) ->
      append_to_stream.configure()
      |> append_to_stream.expected_revision(revision)
  }
}
