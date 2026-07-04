//// SQLite backend for Factos using the `sqlight` package.
////
//// This backend stores events in an append-only `factos_events` table and uses
//// SQLite transactions to implement both supported dispatch styles:
////
//// 1. `dispatch_stream` protects one stream with a per-stream revision check.
//// 2. `dispatch_context` protects a command context with
////    `FailIfEventsMatch(query, after)`.
////
//// The context flow is the important part for Command Context Consistency. The
//// command reads the facts selected by a `factos.Query`, folds them into a
//// temporary decision state, decides new facts, and appends those facts only when
//// no matching facts appeared after the observed position. SQLite can enforce
//// that condition transactionally because the context check and append happen in
//// the same database transaction.
////
//// Event payload encoding is deliberately application-owned. The backend only
//// stores bytes plus query metadata (`EventType` and `Tag`).

import factos
import gleam/dynamic/decode
import gleam/list
import gleam/result
import gleam/string
import sqlight

pub type Proposed(event) {
  /// A domain event prepared for SQLite persistence.
  ///
  /// The application codec creates this value. `id` should identify the event for
  /// the application. `type_` and `tags` are store-visible query metadata. `data`
  /// is opaque bytes owned by the application codec.
  Proposed(
    id: String,
    event: event,
    type_: factos.EventType,
    version: Int,
    tags: List(factos.Tag),
    metadata: factos.Metadata,
    data: BitArray,
  )
}

pub type StoredEvent {
  /// A raw event row read from SQLite before domain decoding.
  ///
  /// Decoders receive this value so they can inspect the stored event type, tags,
  /// and bytes. `position` is the global append order. `revision` is the
  /// per-stream revision.
  StoredEvent(
    position: Int,
    id: String,
    stream: String,
    revision: Int,
    type_: factos.EventType,
    version: Int,
    tags: List(factos.Tag),
    metadata: factos.Metadata,
    data: BitArray,
  )
}

pub type EventCodec(event, decode_error) {
  /// Application-owned SQLite event codec.
  ///
  /// `encode` converts a domain event into bytes and metadata. `decode` converts a
  /// stored row back into a `factos.Decoded` domain event. Decode failures are kept
  /// in the application's own error type and wrapped as `DecodeError`.
  EventCodec(
    encode: fn(event) -> Proposed(event),
    decode: fn(StoredEvent) -> Result(factos.Decoded(event), decode_error),
  )
}

pub type Append {
  /// Result of a successful append.
  ///
  /// `current_revision` is the latest revision of the target stream after the
  /// append. `position` is the global position of the last inserted event, or
  /// `NoPosition` when no events were produced.
  Append(current_revision: Int, position: factos.SequencePosition)
}

pub type Dispatch(event) {
  /// Result of a successful dispatch.
  ///
  /// `append` has the stream revision and final global position. `events` are the
  /// committed events recorded by this dispatch, suitable for pure Factos
  /// reactors or backend-specific durable effect adapters.
  Dispatch(append: Append, events: List(factos.Recorded(event)))
}

pub type Error(domain_error, decode_error) {
  /// The decider rejected the command with a domain error.
  DomainError(domain_error)

  /// The application codec could not decode a stored event.
  DecodeError(decode_error)

  /// SQLite returned an error.
  StoreError(sqlight.Error)

  /// A stream revision or context append condition failed.
  AppendConditionFailed(factos.AppendCondition)
}

/// Create or update the SQLite schema required by this backend.
///
/// The schema is an append-only `factos_events` table with a global autoincrement
/// `position`, per-stream `revision`, event `type`, newline-encoded `tags`, and
/// opaque `data` bytes. It also creates indexes for stream/revision reads and
/// position-based context checks.
pub fn migrate(connection: sqlight.Connection) -> Result(Nil, Error(_, _)) {
  sqlight.exec(
    "
    create table if not exists factos_events (
      position integer primary key autoincrement,
      id text not null,
      stream text not null,
      revision integer not null,
      type text not null,
      version integer not null,
      tags text not null,
      metadata text not null default '',
      data blob not null,
      unique(stream, revision)
    );
    create index if not exists factos_events_stream_revision
      on factos_events(stream, revision);
    create index if not exists factos_events_position
      on factos_events(position);
    ",
    on: connection,
  )
  |> result.map_error(StoreError)
}

/// Read and fold the facts selected by a command-context query.
///
/// The backend reads stored rows, decodes them with the supplied codec, filters
/// them with `factos.matches_query`, folds matching events with the decider's
/// `evolve` function, and returns a `factos.Context` with a
/// `FailIfEventsMatch(query, after)` append condition.
pub fn read_context(
  connection: sqlight.Connection,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, decode_error),
) -> Result(factos.Context(event, state), Error(domain_error, decode_error)) {
  let factos.Decider(initial, _, evolve) = decider

  use events <- result.try(read_matching_events(connection, query, codec))
  let position = highest_recorded_position(events)

  Ok(factos.Context(
    query:,
    state: factos.evolve_recorded(
      initial: initial,
      events: events,
      evolve: evolve,
    ),
    events: events,
    position: position,
    append_condition: factos.FailIfEventsMatch(query, position),
  ))
}

/// Run a full context-first read-decide-append command flow.
///
/// This function starts `BEGIN IMMEDIATE`, reads the query context, runs the
/// decider, verifies that no matching events appeared after the context position,
/// appends produced events to `stream`, and commits. Any error rolls the
/// transaction back.
///
/// Use this when the command's real consistency boundary is the selected event
/// context rather than one predefined stream.
pub fn dispatch_context(
  connection: sqlight.Connection,
  stream stream_name: String,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, decode_error),
  command command: command,
) -> Result(Dispatch(event), Error(domain_error, decode_error)) {
  use _ <- result.try(
    sqlight.exec("begin immediate", on: connection)
    |> result.map_error(StoreError),
  )

  let result = {
    use context <- result.try(read_context(
      connection,
      query: query,
      decider: decider,
      codec: codec,
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
    )
  }

  finish_transaction(connection, result)
}

/// Load and fold one stream.
///
/// This supports classic stream-revision consistency. The returned
/// `factos.LoadedStream` contains the folded state, decoded recorded events, and
/// current stream revision.
pub fn load_stream(
  connection: sqlight.Connection,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, decode_error),
) -> Result(
  factos.LoadedStream(event, state),
  Error(domain_error, decode_error),
) {
  let factos.Decider(initial, _, evolve) = decider
  use events <- result.try(read_stream_events(connection, stream_name, codec))

  Ok(factos.LoadedStream(
    stream: stream_name,
    state: factos.evolve_recorded(
      initial: initial,
      events: events,
      evolve: evolve,
    ),
    events: events,
    revision: stream_revision(events),
  ))
}

/// Run a stream-based read-decide-append command flow.
///
/// The backend loads the target stream, folds it into state, runs the decider, and
/// appends produced events only if the stream revision still matches the loaded
/// revision. Use this when one stream is the intended consistency boundary.
pub fn dispatch_stream(
  connection: sqlight.Connection,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, decode_error),
  command command: command,
) -> Result(Dispatch(event), Error(domain_error, decode_error)) {
  use _ <- result.try(
    sqlight.exec("begin immediate", on: connection)
    |> result.map_error(StoreError),
  )

  let result = {
    use loaded <- result.try(load_stream(
      connection,
      stream: stream_name,
      decider: decider,
      codec: codec,
    ))
    let factos.Decider(_, decide, _) = decider
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
    )
  }

  finish_transaction(connection, result)
}

fn finish_transaction(
  connection: sqlight.Connection,
  result: Result(Dispatch(event), Error(domain_error, decode_error)),
) -> Result(Dispatch(event), Error(domain_error, decode_error)) {
  case result {
    Ok(dispatch) ->
      case sqlight.exec("commit", on: connection) {
        Ok(Nil) -> Ok(dispatch)
        Error(error) -> Error(StoreError(error))
      }
    Error(error) -> {
      let _ = sqlight.exec("rollback", on: connection)
      Error(error)
    }
  }
}

fn append_with_condition(
  connection: sqlight.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, decode_error),
  condition: factos.AppendCondition,
) -> Result(Dispatch(event), Error(domain_error, decode_error)) {
  case condition {
    factos.NoAppendCondition ->
      append_current_stream(connection, stream_name, events, codec)
    factos.FailIfEventsMatch(query, after) ->
      case has_matching_events_after(connection, query, after) {
        Error(error) -> Error(StoreError(error))
        Ok(True) -> Error(AppendConditionFailed(condition))
        Ok(False) ->
          append_current_stream(connection, stream_name, events, codec)
      }
  }
}

fn append_current_stream(
  connection: sqlight.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, decode_error),
) -> Result(Dispatch(event), Error(domain_error, decode_error)) {
  use revision <- result.try(
    current_revision(connection, stream_name)
    |> result.map_error(StoreError),
  )
  append_stream_events(
    connection,
    stream_name,
    events,
    codec,
    factos.CurrentRevision(revision),
  )
}

fn append_stream_events(
  connection: sqlight.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, decode_error),
  expected: factos.Revision,
) -> Result(Dispatch(event), Error(domain_error, decode_error)) {
  case events {
    [] -> {
      let append = Append(
        current_revision: revision_to_int(expected),
        position: factos.NoPosition,
      )
      Ok(Dispatch(append:, events: []))
    }
    [_, ..] -> {
      use current <- result.try(
        current_revision(connection, stream_name)
        |> result.map_error(StoreError),
      )
      case expected_matches(expected, current) {
        False -> Error(AppendConditionFailed(factos.NoAppendCondition))
        True ->
          insert_events(
            connection,
            stream_name,
            events,
            codec,
            current + 1,
            factos.NoPosition,
            [],
          )
      }
    }
  }
}

fn insert_events(
  connection: sqlight.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, decode_error),
  revision: Int,
  position: factos.SequencePosition,
  recorded_events: List(factos.Recorded(event)),
) -> Result(Dispatch(event), Error(domain_error, decode_error)) {
  case events {
    [] -> {
      let append = Append(current_revision: revision - 1, position: position)
      Ok(Dispatch(append:, events: list.reverse(recorded_events)))
    }
    [event, ..rest] -> {
      let EventCodec(encode, _) = codec
      let Proposed(id, _, type_, version, tags, metadata, data) = encode(event)
      use positions <- result.try(
        sqlight.query(
          "
          insert into factos_events (id, stream, revision, type, version, tags, metadata, data)
          values (?, ?, ?, ?, ?, ?, ?, ?)
          returning position
          ",
          on: connection,
          with: [
            sqlight.text(id),
            sqlight.text(stream_name),
            sqlight.int(revision),
            sqlight.text(factos.event_type_name(type_)),
            sqlight.int(version),
            sqlight.text(tags_to_text(tags)),
            sqlight.text(metadata_to_text(metadata)),
            sqlight.blob(data),
          ],
          expecting: int_field_decoder(),
        )
        |> result.map_error(StoreError),
      )
      let position = case positions {
        [position, ..] -> factos.SequencePosition(position)
        [] -> position
      }
      let recorded = factos.Recorded(
        id: id,
        stream: stream_name,
        revision: revision,
        position: position,
        type_: type_,
        version: version,
        tags: tags,
        metadata: metadata,
        event: event,
      )
      insert_events(
        connection,
        stream_name,
        rest,
        codec,
        revision + 1,
        position,
        [recorded, ..recorded_events],
      )
    }
  }
}

fn read_matching_events(
  connection: sqlight.Connection,
  query: factos.Query,
  codec: EventCodec(event, decode_error),
) -> Result(List(factos.Recorded(event)), Error(domain_error, decode_error)) {
  use rows <- result.try(
    sqlight.query(
      "select position, id, stream, revision, type, version, tags, metadata, data from factos_events order by position",
      on: connection,
      with: [],
      expecting: stored_event_decoder(),
    )
    |> result.map_error(StoreError),
  )
  decode_rows(rows, codec)
  |> result.map(list.filter(_, factos.matches_query(_, query)))
}

fn read_stream_events(
  connection: sqlight.Connection,
  stream_name: String,
  codec: EventCodec(event, decode_error),
) -> Result(List(factos.Recorded(event)), Error(domain_error, decode_error)) {
  use rows <- result.try(
    sqlight.query(
      "select position, id, stream, revision, type, version, tags, metadata, data from factos_events where stream = ? order by revision",
      on: connection,
      with: [sqlight.text(stream_name)],
      expecting: stored_event_decoder(),
    )
    |> result.map_error(StoreError),
  )
  decode_rows(rows, codec)
}

fn decode_rows(
  rows: List(StoredEvent),
  codec: EventCodec(event, decode_error),
) -> Result(List(factos.Recorded(event)), Error(domain_error, decode_error)) {
  case rows {
    [] -> Ok([])
    [row, ..rest] -> {
      use recorded <- result.try(decode_row(row, codec))
      use rest <- result.try(decode_rows(rest, codec))
      Ok([recorded, ..rest])
    }
  }
}

fn decode_row(
  row: StoredEvent,
  codec: EventCodec(event, decode_error),
) -> Result(factos.Recorded(event), Error(domain_error, decode_error)) {
  let EventCodec(_, decode_event) = codec
  use decoded <- result.try(decode_event(row) |> result.map_error(DecodeError))
  let factos.Decoded(event, type_, version, tags, metadata) = decoded
  let StoredEvent(position, id, stream, revision, _, _, _, _, _) = row

  Ok(factos.Recorded(
    id: id,
    stream: stream,
    revision: revision,
    position: factos.SequencePosition(position),
    type_: type_,
    version: version,
    tags: tags,
    metadata: metadata,
    event: event,
  ))
}

fn stored_event_decoder() -> decode.Decoder(StoredEvent) {
  use position <- decode.field(0, decode.int)
  use id <- decode.field(1, decode.string)
  use stream <- decode.field(2, decode.string)
  use revision <- decode.field(3, decode.int)
  use type_name <- decode.field(4, decode.string)
  use version <- decode.field(5, decode.int)
  use tags <- decode.field(6, decode.string)
  use metadata <- decode.field(7, decode.string)
  use data <- decode.field(8, decode.bit_array)
  decode.success(StoredEvent(
    position: position,
    id: id,
    stream: stream,
    revision: revision,
    type_: factos.event_type(type_name),
    version: version,
    tags: tags_from_text(tags),
    metadata: metadata_from_text(metadata),
    data: data,
  ))
}

fn current_revision(
  connection: sqlight.Connection,
  stream_name: String,
) -> Result(Int, sqlight.Error) {
  use rows <- result.map(sqlight.query(
    "select coalesce(max(revision), -1) from factos_events where stream = ?",
    on: connection,
    with: [sqlight.text(stream_name)],
    expecting: int_field_decoder(),
  ))

  case rows {
    [revision, ..] -> revision
    [] -> -1
  }
}

fn has_matching_events_after(
  connection: sqlight.Connection,
  query: factos.Query,
  after: factos.SequencePosition,
) -> Result(Bool, sqlight.Error) {
  let after_position = case after {
    factos.NoPosition -> -1
    factos.SequencePosition(position) -> position
  }
  sqlight.query(
    "select type, tags from factos_events where position > ?",
    on: connection,
    with: [sqlight.int(after_position)],
    expecting: query_match_decoder(),
  )
  |> result.map(
    list.any(_, fn(pair) {
      let #(type_, tags) = pair
      matches_query_parts(type_, tags, query)
    }),
  )
}

fn query_match_decoder() -> decode.Decoder(
  #(factos.EventType, List(factos.Tag)),
) {
  use type_name <- decode.field(0, decode.string)
  use tags <- decode.field(1, decode.string)
  decode.success(#(factos.event_type(type_name), tags_from_text(tags)))
}

fn int_field_decoder() -> decode.Decoder(Int) {
  use value <- decode.field(0, decode.int)
  decode.success(value)
}

fn matches_query_parts(
  type_: factos.EventType,
  tags: List(factos.Tag),
  query: factos.Query,
) -> Bool {
  factos.matches_query(
    factos.Recorded(
      id: "",
      stream: "",
      revision: 0,
      position: factos.NoPosition,
      type_: type_,
      version: 1,
      tags: tags,
      metadata: factos.empty_metadata(),
      event: Nil,
    ),
    query,
  )
}

fn stream_revision(events: List(factos.Recorded(event))) -> factos.Revision {
  case list.reverse(events) {
    [] -> factos.NoEvents
    [event, ..] -> factos.CurrentRevision(event.revision)
  }
}

fn highest_recorded_position(
  events: List(factos.Recorded(event)),
) -> factos.SequencePosition {
  case list.reverse(events) {
    [] -> factos.NoPosition
    [event, ..] -> event.position
  }
}

fn expected_matches(expected: factos.Revision, current: Int) -> Bool {
  case expected {
    factos.NoEvents -> current == -1
    factos.CurrentRevision(revision) -> current == revision
  }
}

fn revision_to_int(revision: factos.Revision) -> Int {
  case revision {
    factos.NoEvents -> -1
    factos.CurrentRevision(revision) -> revision
  }
}

fn tags_to_text(tags: List(factos.Tag)) -> String {
  tags
  |> list.map(factos.tag_value)
  |> string.join(with: "\n")
}

fn tags_from_text(tags: String) -> List(factos.Tag) {
  case string.is_empty(tags) {
    True -> []
    False -> tags |> string.split(on: "\n") |> list.map(factos.tag)
  }
}

fn metadata_to_text(metadata: factos.Metadata) -> String {
  metadata
  |> factos.metadata_entries
  |> list.map(fn(entry) { entry.0 <> "=" <> entry.1 })
  |> string.join(with: "\n")
}

fn metadata_from_text(metadata: String) -> factos.Metadata {
  case string.is_empty(metadata) {
    True -> factos.empty_metadata()
    False ->
      metadata
      |> string.split(on: "\n")
      |> list.filter_map(fn(entry) {
        case string.split(entry, on: "=") {
          [key, value] -> Ok(#(key, value))
          _ -> Error(Nil)
        }
      })
      |> factos.metadata
  }
}
