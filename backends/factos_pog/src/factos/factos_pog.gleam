//// PostgreSQL backend for Factos using the `pog` package.
////
//// This backend stores accepted facts in an append-only `factos_events` table.
//// The event history is the source of truth; projections and stream-shaped reads
//// are derived views over that history.
////
//// The context dispatch flow follows the Command Context Consistency idea from
//// "Simply Event Sourcing": a command selects the facts required for its decision,
//// folds them into temporary state, decides new facts, and appends those facts
//// only when no relevant facts appeared after the observed context position.
////
//// The query contract is intentionally tag-based. PostgreSQL stores opaque event
//// bytes, an event type, and tags. This keeps domain serialization outside the
//// backend, but it means any payload value needed for a selective consistency
//// query must be written as a tag.

import factos
import gleam/dynamic/decode
import gleam/list
import gleam/result
import gleam/string
import pog

pub type Proposed(event) {
  /// A domain event prepared for PostgreSQL persistence.
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
  /// A raw event row read from PostgreSQL before domain decoding.
  ///
  /// Decoders receive this value so they can inspect stored metadata and bytes.
  /// `position` is the global append order. `revision` is the per-stream revision.
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
  /// Application-owned PostgreSQL event codec.
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

pub type Error(domain_error, decode_error) {
  /// The decider rejected the command with a domain error.
  DomainError(domain_error)

  /// The application codec could not decode a stored event.
  DecodeError(decode_error)

  /// PostgreSQL or `pog` returned an error while running a query.
  StoreError(pog.QueryError)

  /// A stream revision or context append condition failed.
  AppendConditionFailed(factos.AppendCondition)
}

/// Create or update the PostgreSQL schema required by this backend.
///
/// The schema is an append-only `factos_events` table with a global identity
/// `position`, per-stream `revision`, event `type`, newline-encoded `tags`, and
/// opaque `data` bytes. The `(stream, revision)` uniqueness constraint supports
/// stream-revision consistency. Position indexes support context reads and checks.
pub fn migrate(connection: pog.Connection) -> Result(Nil, Error(_, _)) {
  // `pog` uses prepared statements, and PostgreSQL does not allow multiple SQL
  // commands in one prepared statement. Keep migrations split into individual
  // statements rather than relying on client-side SQL script execution.
  use _ <- result.try(execute_migration(
    connection,
    "
    create table if not exists factos_events (
      position bigint generated always as identity primary key,
      id text not null,
      stream text not null,
      revision integer not null,
      type text not null,
      version integer not null,
      tags text not null,
      metadata text not null,
      data bytea not null,
      unique(stream, revision)
    )
    ",
  ))
  use _ <- result.try(execute_migration(
    connection,
    "
    create index if not exists factos_events_stream_revision
      on factos_events(stream, revision)
    ",
  ))
  use _ <- result.try(execute_migration(
    connection,
    "
    create index if not exists factos_events_position
      on factos_events(position)
    ",
  ))
  Ok(Nil)
}

fn execute_migration(
  connection: pog.Connection,
  sql: String,
) -> Result(Nil, Error(_, _)) {
  pog.query(sql)
  |> pog.execute(on: connection)
  |> result.map(fn(_) { Nil })
  |> result.map_error(StoreError)
}

/// Read and fold the facts selected by a command-context query.
///
/// The backend reads stored rows, decodes them with the supplied codec, filters
/// them with `factos.matches_query`, folds matching events with the decider's
/// `evolve` function, and returns a `factos.Context` with a
/// `FailIfEventsMatch(query, after)` append condition.
pub fn read_context(
  connection: pog.Connection,
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
/// PostgreSQL does not have a native primitive for "append if no row matching this
/// arbitrary event-type/tag query appeared after position N". This backend uses a
/// transaction plus `lock table factos_events in exclusive mode` to make the read,
/// context check, and append atomic for all Factos queries.
///
/// That lock is the main throughput tradeoff: unrelated writers queue behind each
/// other even if their contexts do not overlap. It is deliberately simple and
/// correct. A future backend can use advisory locks or query-specific lock keys,
/// but only if it keeps the same context-stability guarantee.
pub fn dispatch_context(
  connection: pog.Connection,
  stream stream_name: String,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, decode_error),
  command command: command,
) -> Result(Append, Error(domain_error, decode_error)) {
  use transaction_connection <- run_locked_transaction(connection)
  use context <- result.try(read_context(
    transaction_connection,
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
    transaction_connection,
    stream_name,
    events,
    codec,
    context.append_condition,
  )
}

/// Load and fold one stream.
///
/// This supports classic stream-revision consistency. The returned
/// `factos.LoadedStream` contains the folded state, decoded recorded events, and
/// current stream revision.
pub fn load_stream(
  connection: pog.Connection,
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
/// Use this when one stream is intentionally the consistency boundary. It remains
/// useful, but it is not required by Event Sourcing. For command-specific rules,
/// prefer `dispatch_context` so the protected boundary follows the decision.
pub fn dispatch_stream(
  connection: pog.Connection,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, decode_error),
  command command: command,
) -> Result(Append, Error(domain_error, decode_error)) {
  use transaction_connection <- run_locked_transaction(connection)
  use loaded <- result.try(load_stream(
    transaction_connection,
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
    transaction_connection,
    stream_name,
    events,
    codec,
    loaded.revision,
  )
}

fn run_locked_transaction(
  connection: pog.Connection,
  work: fn(pog.Connection) -> Result(Append, Error(domain_error, decode_error)),
) -> Result(Append, Error(domain_error, decode_error)) {
  case
    {
      use transaction_connection <- pog.transaction(connection)
      use _ <- result.try(
        pog.query("lock table factos_events in exclusive mode")
        |> pog.execute(on: transaction_connection)
        |> result.map(fn(_) { Nil })
        |> result.map_error(StoreError),
      )
      work(transaction_connection)
    }
  {
    Ok(append) -> Ok(append)
    Error(pog.TransactionQueryError(error)) -> Error(StoreError(error))
    Error(pog.TransactionRolledBack(error)) -> Error(error)
  }
}

fn append_with_condition(
  connection: pog.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, decode_error),
  condition: factos.AppendCondition,
) -> Result(Append, Error(domain_error, decode_error)) {
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
  connection: pog.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, decode_error),
) -> Result(Append, Error(domain_error, decode_error)) {
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
  connection: pog.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, decode_error),
  expected: factos.Revision,
) -> Result(Append, Error(domain_error, decode_error)) {
  case events {
    [] ->
      Ok(Append(
        current_revision: revision_to_int(expected),
        position: factos.NoPosition,
      ))
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
          )
      }
    }
  }
}

fn insert_events(
  connection: pog.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, decode_error),
  revision: Int,
  position: factos.SequencePosition,
) -> Result(Append, Error(domain_error, decode_error)) {
  case events {
    [] -> Ok(Append(current_revision: revision - 1, position: position))
    [event, ..rest] -> {
      let EventCodec(encode, _) = codec
      let Proposed(id, _, type_, version, tags, metadata, data) = encode(event)
      use returned <- result.try(
        pog.query(
          "
          insert into factos_events (id, stream, revision, type, version, tags, metadata, data)
          values ($1, $2, $3, $4, $5, $6, $7, $8)
          returning position
          ",
        )
        |> pog.parameter(pog.text(id))
        |> pog.parameter(pog.text(stream_name))
        |> pog.parameter(pog.int(revision))
        |> pog.parameter(pog.text(factos.event_type_name(type_)))
        |> pog.parameter(pog.int(version))
        |> pog.parameter(pog.text(tags_to_text(tags)))
        |> pog.parameter(pog.text(metadata_to_text(metadata)))
        |> pog.parameter(pog.bytea(data))
        |> pog.returning(int_field_decoder())
        |> pog.execute(on: connection)
        |> result.map_error(StoreError),
      )
      let position = case returned.rows {
        [position, ..] -> factos.SequencePosition(position)
        [] -> position
      }
      insert_events(
        connection,
        stream_name,
        rest,
        codec,
        revision + 1,
        position,
      )
    }
  }
}

fn read_matching_events(
  connection: pog.Connection,
  query: factos.Query,
  codec: EventCodec(event, decode_error),
) -> Result(List(factos.Recorded(event)), Error(domain_error, decode_error)) {
  use rows <- result.try(
    pog.query(
      "select position, id, stream, revision, type, version, tags, metadata, data from factos_events order by position",
    )
    |> pog.returning(stored_event_decoder())
    |> pog.execute(on: connection)
    |> result.map(fn(returned) { returned.rows })
    |> result.map_error(StoreError),
  )
  decode_rows(rows, codec)
  |> result.map(list.filter(_, factos.matches_query(_, query)))
}

fn read_stream_events(
  connection: pog.Connection,
  stream_name: String,
  codec: EventCodec(event, decode_error),
) -> Result(List(factos.Recorded(event)), Error(domain_error, decode_error)) {
  use rows <- result.try(
    pog.query(
      "select position, id, stream, revision, type, version, tags, metadata, data from factos_events where stream = $1 order by revision",
    )
    |> pog.parameter(pog.text(stream_name))
    |> pog.returning(stored_event_decoder())
    |> pog.execute(on: connection)
    |> result.map(fn(returned) { returned.rows })
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
  connection: pog.Connection,
  stream_name: String,
) -> Result(Int, pog.QueryError) {
  use returned <- result.try(
    pog.query(
      "select coalesce(max(revision), -1) from factos_events where stream = $1",
    )
    |> pog.parameter(pog.text(stream_name))
    |> pog.returning(int_field_decoder())
    |> pog.execute(on: connection),
  )

  case returned.rows {
    [revision, ..] -> Ok(revision)
    [] -> Ok(-1)
  }
}

fn has_matching_events_after(
  connection: pog.Connection,
  query: factos.Query,
  after: factos.SequencePosition,
) -> Result(Bool, pog.QueryError) {
  let after_position = case after {
    factos.NoPosition -> -1
    factos.SequencePosition(position) -> position
  }
  pog.query("select type, tags from factos_events where position > $1")
  |> pog.parameter(pog.int(after_position))
  |> pog.returning(query_match_decoder())
  |> pog.execute(on: connection)
  |> result.map(fn(returned) {
    use pair <- list.any(returned.rows)
    let #(type_, tags) = pair
    matches_query_parts(type_, tags, query)
  })
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
