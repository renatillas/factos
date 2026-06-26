//// KurrentDB Erlang backend for Factos.
////
//// This backend integrates Factos with the Erlang-target KurrentDB client. It
//// supports stream revision consistency and context reads from `$all`.
////
//// KurrentDB's regular append API can protect a stream with expected revision
//// checks. That is useful when the stream is the real consistency boundary.
//// However, a Factos context append condition is query-based:
//// `FailIfEventsMatch(query, after)`. KurrentDB's regular append operation cannot
//// atomically enforce an arbitrary event-type/tag query condition, so this backend
//// reports `UnsupportedAppendCondition` for context dispatch.
////
//// This distinction is intentional. Stream revision consistency is one valid
//// Event Sourcing implementation strategy; Command Context Consistency and DCB-
//// style tag contracts require stores or write paths that can protect the actual
//// command context.

import factos
import gleam/list
import gleam/result
import kurrentdb/operation/append_to_stream
import kurrentdb/operation/read_all
import kurrentdb/operation/read_stream
import kurrentdb_erlang
import youid/uuid

pub type Proposed(event) {
  /// A domain event prepared for KurrentDB persistence.
  ///
  /// The application codec creates this value. `event` is kept for domain-level
  /// typing, `type_` and `tags` are Factos query metadata, and `message` is the
  /// KurrentDB append event sent to the client.
  Proposed(
    event: event,
    type_: factos.EventType,
    tags: List(factos.Tag),
    metadata: factos.Metadata,
    message: append_to_stream.Event,
  )
}

pub type EventCodec(event, decode_error) {
  /// Application-owned KurrentDB event codec.
  ///
  /// `encode` converts a domain event into a KurrentDB append message plus Factos
  /// metadata. `decode` converts a KurrentDB recorded event into a
  /// `factos.Decoded` domain event. Decode failures are wrapped as `DecodeError`.
  EventCodec(
    encode: fn(event) -> Proposed(event),
    decode: fn(read_stream.RecordedEvent) ->
      Result(factos.Decoded(event), decode_error),
  )
}

pub type Error(domain_error, decode_error) {
  /// The decider rejected the command with a domain error.
  DomainError(domain_error)

  /// The application codec could not decode a stored event.
  DecodeError(decode_error)

  /// KurrentDB returned an error while reading a stream or `$all`.
  ReadError(kurrentdb_erlang.Error(read_stream.ResponseError))

  /// KurrentDB returned an error while appending to a stream.
  AppendError(kurrentdb_erlang.Error(append_to_stream.ResponseError))

  /// A read stream did not produce a message before the configured timeout.
  ReadTimedOut

  /// The requested append condition cannot be enforced by this backend.
  ///
  /// This is expected for `FailIfEventsMatch` because the regular KurrentDB append
  /// API protects stream revisions, not arbitrary Factos context queries.
  UnsupportedAppendCondition(factos.AppendCondition)
}

/// Read and fold a command context from KurrentDB `$all`.
///
/// Event types in the query are translated into a KurrentDB event-type prefix
/// filter where possible. Decoded events are then filtered locally with the full
/// Factos query, including tags. The returned context contains a
/// `FailIfEventsMatch(query, after)` append condition, but this backend cannot
/// enforce that condition during append.
pub fn read_context(
  connection: kurrentdb_erlang.Connection,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, decode_error),
  timeout timeout: Int,
) -> Result(factos.Context(event, state), Error(domain_error, decode_error)) {
  let factos.Decider(initial, _, evolve) = decider

  read_context_events(connection, query, initial, evolve, codec, timeout)
}

/// Attempt a full context-first dispatch flow.
///
/// This function reads the context and runs the decider, then delegates to
/// `append_with_condition`. Because normal KurrentDB appends cannot enforce
/// `FailIfEventsMatch`, context dispatch returns `UnsupportedAppendCondition` for
/// the context condition produced by `read_context`.
///
/// Use this function to make the storage limitation explicit. Prefer
/// `dispatch_stream` when a stream revision is the intended consistency boundary.
pub fn dispatch_context(
  connection: kurrentdb_erlang.Connection,
  stream stream_name: String,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, decode_error),
  command command: command,
  timeout timeout: Int,
) -> Result(append_to_stream.Append, Error(domain_error, decode_error)) {
  use context <- result.try(read_context(
    connection,
    query:,
    decider:,
    codec:,
    timeout:,
  ))
  use pair <- result.try(
    factos.decide_context(context, command, decider)
    |> result.map_error(DomainError),
  )
  let #(context, events) = pair

  append_with_condition(
    connection,
    stream_name,
    events,
    codec,
    context.append_condition,
    timeout,
  )
}

/// Load and fold one KurrentDB stream.
///
/// Missing streams are treated as empty streams and returned with
/// `factos.NoEvents`. Existing streams return their latest observed revision as
/// `factos.CurrentRevision(n)`.
pub fn load_stream(
  connection: kurrentdb_erlang.Connection,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, decode_error),
  timeout timeout: Int,
) -> Result(
  factos.LoadedStream(event, state),
  Error(domain_error, decode_error),
) {
  let factos.Decider(initial, _, evolve) = decider

  load_stream_events(connection, stream_name, initial, evolve, codec, timeout)
}

/// Run a stream-based read-decide-append command flow.
///
/// The backend loads the target stream, folds it into state, runs the decider, and
/// appends produced events with KurrentDB expected-revision checks. Use this when
/// one KurrentDB stream is the correct consistency boundary for the command.
pub fn dispatch_stream(
  connection: kurrentdb_erlang.Connection,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, decode_error),
  command command: command,
  timeout timeout: Int,
) -> Result(append_to_stream.Append, Error(domain_error, decode_error)) {
  let factos.Decider(initial, decide, evolve) = decider

  use loaded <- result.try(load_stream_events(
    connection,
    stream_name,
    initial,
    evolve,
    codec,
    timeout,
  ))

  use events <- result.try(
    decide(loaded.state, command)
    |> result.map_error(DomainError),
  )

  append_stream_events(
    connection,
    stream_name,
    events,
    codec,
    loaded.revision,
    timeout,
  )
}

fn append_with_condition(
  connection: kurrentdb_erlang.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, decode_error),
  condition: factos.AppendCondition,
  timeout: Int,
) -> Result(append_to_stream.Append, Error(domain_error, decode_error)) {
  case condition {
    factos.NoAppendCondition ->
      append_to_stream_with_config(
        connection,
        stream_name,
        events,
        codec,
        append_to_stream.configure() |> append_to_stream.any,
        timeout,
      )
    factos.FailIfEventsMatch(_, _) ->
      Error(UnsupportedAppendCondition(condition))
  }
}

fn read_context_events(
  connection: kurrentdb_erlang.Connection,
  query: factos.Query,
  initial: state,
  evolve: fn(state, event) -> state,
  codec: EventCodec(event, decode_error),
  timeout: Int,
) -> Result(factos.Context(event, state), Error(domain_error, decode_error)) {
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
    factos.NoPosition,
    timeout,
  )
}

fn load_stream_events(
  connection: kurrentdb_erlang.Connection,
  stream_name: String,
  initial: state,
  evolve: fn(state, event) -> state,
  codec: EventCodec(event, decode_error),
  timeout: Int,
) -> Result(
  factos.LoadedStream(event, state),
  Error(domain_error, decode_error),
) {
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
    factos.NoEvents,
    timeout,
  )
}

fn append_stream_events(
  connection: kurrentdb_erlang.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, decode_error),
  expected: factos.Revision,
  timeout: Int,
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

fn append_to_stream_with_config(
  connection: kurrentdb_erlang.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, decode_error),
  config: append_to_stream.Configuration,
  within: Int,
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
          events: list.map(events, fn(event) { encode(event).message }),
          config: config,
        )

      kurrentdb_erlang.await(task, within:)
      |> result.map_error(AppendError)
    }
  }
}

fn receive_context(
  stream: kurrentdb_erlang.Stream,
  query: factos.Query,
  state: state,
  evolve: fn(state, event) -> state,
  codec: EventCodec(event, decode_error),
  events: List(factos.Recorded(event)),
  position: factos.SequencePosition,
  within: Int,
) -> Result(factos.Context(event, state), Error(domain_error, decode_error)) {
  case kurrentdb_erlang.receive(stream, within:) {
    Error(Nil) -> {
      kurrentdb_erlang.close(stream)
      Error(ReadTimedOut)
    }
    Ok(kurrentdb_erlang.StreamFinished) -> {
      kurrentdb_erlang.close(stream)
      Ok(factos.Context(
        query:,
        state:,
        events: list.reverse(events),
        position:,
        append_condition: factos.FailIfEventsMatch(query, position),
      ))
    }
    Ok(kurrentdb_erlang.StreamFailed(error)) -> {
      kurrentdb_erlang.close(stream)
      Error(ReadError(error))
    }
    Ok(kurrentdb_erlang.ReadMessage(read_stream.ReadEvent(event))) ->
      receive_context_event(
        stream,
        query,
        state,
        evolve,
        codec,
        events,
        position,
        within,
        event,
      )
    Ok(kurrentdb_erlang.ReadMessage(read_stream.LastAllStreamPosition(read_stream.Position(
      commit_position: commit_position,
      prepare_position: _,
    )))) ->
      receive_context(
        stream,
        query,
        state,
        evolve,
        codec,
        events,
        factos.highest_position(
          position,
          factos.SequencePosition(commit_position),
        ),
        within,
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
        within,
      )
  }
}

fn receive_context_event(
  stream: kurrentdb_erlang.Stream,
  query: factos.Query,
  state: state,
  evolve: fn(state, event) -> state,
  codec: EventCodec(event, decode_error),
  events: List(factos.Recorded(event)),
  position: factos.SequencePosition,
  timeout: Int,
  read_event: read_stream.ReadEvent,
) -> Result(factos.Context(event, state), Error(domain_error, decode_error)) {
  use recorded <- result.try(decode_recorded(read_event, codec))
  let next_position = factos.highest_position(position, recorded.position)

  case factos.matches_query(recorded, query) {
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
  events: List(factos.Recorded(event)),
  revision: factos.Revision,
  within: Int,
) -> Result(
  factos.LoadedStream(event, state),
  Error(domain_error, decode_error),
) {
  case kurrentdb_erlang.receive(stream, within:) {
    Error(Nil) -> {
      kurrentdb_erlang.close(stream)
      Error(ReadTimedOut)
    }
    Ok(kurrentdb_erlang.StreamFinished) -> {
      kurrentdb_erlang.close(stream)
      Ok(factos.LoadedStream(
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
          Ok(factos.LoadedStream(
            stream: stream_name,
            state:,
            events: [],
            revision: factos.NoEvents,
          ))
        _ -> Error(ReadError(error))
      }
    }
    Ok(kurrentdb_erlang.ReadMessage(read_stream.ReadEvent(event))) ->
      receive_stream_event(
        stream,
        stream_name,
        state,
        evolve,
        codec,
        events,
        within,
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
        within,
      )
  }
}

fn receive_stream_event(
  stream: kurrentdb_erlang.Stream,
  stream_name: String,
  state: state,
  evolve: fn(state, event) -> state,
  codec: EventCodec(event, decode_error),
  events: List(factos.Recorded(event)),
  timeout: Int,
  read_event: read_stream.ReadEvent,
) -> Result(
  factos.LoadedStream(event, state),
  Error(domain_error, decode_error),
) {
  use recorded <- result.try(decode_recorded(read_event, codec))

  receive_stream(
    stream,
    stream_name,
    evolve(state, recorded.event),
    evolve,
    codec,
    [recorded, ..events],
    factos.CurrentRevision(recorded.revision),
    timeout,
  )
}

fn decode_recorded(
  read_event: read_stream.ReadEvent,
  codec: EventCodec(event, decode_error),
) -> Result(factos.Recorded(event), Error(domain_error, decode_error)) {
  let recorded_event = case read_event {
    read_stream.Recorded(event) -> event
    read_stream.Resolved(event: event, ..) -> event
  }

  let EventCodec(_, decode) = codec
  use decoded <- result.try(
    decode(recorded_event)
    |> result.map_error(DecodeError),
  )

  let factos.Decoded(event, type_, tags, metadata) = decoded
  Ok(factos.Recorded(
    id: uuid.to_string(recorded_event.id),
    stream: recorded_event.stream,
    revision: recorded_event.revision,
    position: factos.SequencePosition(recorded_event.commit_position),
    type_: type_,
    tags: tags,
    metadata: metadata,
    event: event,
  ))
}

fn query_to_read_all_filter(query: factos.Query) -> read_all.Filter {
  let types = query_event_type_names(query)

  case types {
    [] -> read_all.NoFilter
    [_, ..] -> read_all.EventTypePrefix(types, window: read_all.FilterMax(1000))
  }
}

fn query_event_type_names(query: factos.Query) -> List(String) {
  case query {
    factos.AllEvents -> []
    factos.Query(items) -> collect_type_names(items, [])
  }
}

fn collect_type_names(
  items: List(factos.QueryItem),
  names: List(String),
) -> List(String) {
  case items {
    [] -> list.reverse(names)
    [factos.QueryItem(types, _), ..rest] ->
      collect_type_names(rest, prepend_type_names(types, names))
  }
}

fn prepend_type_names(
  types: List(factos.EventType),
  names: List(String),
) -> List(String) {
  case types {
    [] -> names
    [type_, ..rest] ->
      prepend_type_names(rest, [factos.event_type_name(type_), ..names])
  }
}

fn append_config(revision: factos.Revision) -> append_to_stream.Configuration {
  case revision {
    factos.NoEvents ->
      append_to_stream.configure() |> append_to_stream.no_stream
    factos.CurrentRevision(revision) ->
      append_to_stream.configure()
      |> append_to_stream.expected_revision(revision)
  }
}
