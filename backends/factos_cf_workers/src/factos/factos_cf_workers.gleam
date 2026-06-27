//// Cloudflare Workers D1 backend for Factos.
////
//// This backend stores accepted facts in an append-only D1 table. Event payload
//// serialization remains application-owned; the backend stores a string payload
//// plus Factos query metadata (`EventType` and `Tag`).
////
//// D1 operations are asynchronous, so public functions return
//// `Promise(Result(_, _))`. The stream and context dispatch functions use one
//// conditional `insert ... select ... returning` statement for appends. That keeps
//// the append condition and writes in the same SQLite statement, which is the
//// atomic boundary D1 exposes through prepared statements.
////
//// Side-effect outbox rows (derived from the codec's `side_effects` function) are
//// inserted atomically alongside events via `d1.batch`, preserving the invariant
//// that side effects are never lost after a successful event append.

import cf_workers/d1
import factos
import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/int
import gleam/javascript/array
import gleam/javascript/promise.{type Promise}
import gleam/list
import gleam/result
import gleam/string

pub type Proposed(event) {
  /// A domain event prepared for D1 persistence.
  ///
  /// The application codec creates this value. `id` should identify the event for
  /// the application. `type_` and `tags` are store-visible query metadata. `data`
  /// is an opaque string owned by the application codec, typically JSON.
  Proposed(
    id: String,
    event: event,
    type_: factos.EventType,
    version: Int,
    tags: List(factos.Tag),
    metadata: factos.Metadata,
    data: String,
  )
}

pub type StoredEvent {
  /// A raw event row read from D1 before domain decoding.
  StoredEvent(
    position: Int,
    id: String,
    stream: String,
    revision: Int,
    type_: factos.EventType,
    version: Int,
    tags: List(factos.Tag),
    metadata: factos.Metadata,
    data: String,
  )
}

/// An outbox row for a registered side effect.
///
/// The backend inserts these atomically alongside the events that produced them.
/// `event_type` identifies the kind of side effect. `stream` is the event stream
/// name. `payload` is application-owned opaque data, typically JSON with the
/// arguments the side-effect handler needs.
pub type OutboxRow {
  OutboxRow(event_type: String, stream: String, payload: String)
}

pub type EventCodec(event, state, decode_error) {
  /// Application-owned D1 event codec.
  ///
  /// `side_effects` maps each domain event to the outbox rows it triggers.
  /// It receives the event, the stream name, and the **pre‑event** decision state
  /// so that contextual data (e.g. customer email) can be captured for the
  /// side-effect handler without a second read.
  EventCodec(
    encode: fn(event) -> Proposed(event),
    decode: fn(StoredEvent) -> Result(factos.Decoded(event), decode_error),
    side_effects: fn(event, String, state) -> List(OutboxRow),
  )
}

pub type Append {
  /// Result of a successful append.
  ///
  /// `outbox_ids` contains the auto-generated IDs of side-effect outbox rows that
  /// were inserted atomically with the events. The caller may use these IDs to
  /// enqueue asynchronous processing of those side effects.
  Append(
    current_revision: Int,
    position: factos.SequencePosition,
    outbox_ids: List(Int),
  )
}

pub type Error(domain_error, decode_error) {
  /// The decider rejected the command with a domain error.
  DomainError(domain_error)

  /// The application codec could not decode a stored event.
  DecodeError(decode_error)

  /// D1 returned an error while running a query.
  StoreError(String)

  /// A D1 row did not match the backend's expected shape.
  RowDecodeError(List(decode.DecodeError))

  /// A stream revision or context append condition failed.
  AppendConditionFailed(factos.AppendCondition)
}

type AppendMode {
  CurrentStream
  ExpectedStream(factos.Revision)
  ContextCondition(factos.Query, factos.SequencePosition)
}

type QuerySql {
  QuerySql(sql: String, values: List(String))
}

/// Create or update the D1 schema required by this backend.
pub fn migrate(database: d1.Database) -> Promise(Result(Nil, Error(_, _))) {
  use _ <- promise.try_await(execute_migration(
    database,
    "
    create table if not exists factos_events (
      position integer primary key autoincrement,
      id text not null,
      stream text not null,
      revision integer not null,
      type text not null,
      version integer not null,
      tags text not null,
      metadata text not null,
      data text not null,
      unique(stream, revision)
    )
    ",
  ))
  use _ <- promise.try_await(execute_migration(
    database,
    "
    create index if not exists factos_events_stream_revision
      on factos_events(stream, revision)
    ",
  ))
  use _ <- promise.try_await(execute_migration(
    database,
    "
    create index if not exists factos_events_position
      on factos_events(position)
    ",
  ))
  execute_migration(
    database,
    "
    create table if not exists event_outbox (
      id integer primary key autoincrement,
      stream text not null,
      event_type text not null,
      payload text not null,
      status text not null default 'pending',
      error text,
      created_at integer not null default (unixepoch()),
      processed_at integer
    )
    ",
  )
}

fn execute_migration(
  database: d1.Database,
  sql: String,
) -> Promise(Result(Nil, Error(_, _))) {
  d1.prepare(database, sql)
  |> d1.run
  |> promise.map(fn(result) {
    result
    |> result.map(fn(_) { Nil })
    |> result.map_error(StoreError)
  })
}

/// Read and fold the facts selected by a command-context query.
pub fn read_context(
  database: d1.Database,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, state, decode_error),
) -> Promise(
  Result(factos.Context(event, state), Error(domain_error, decode_error)),
) {
  let factos.Decider(initial, _, evolve) = decider

  read_matching_events(database, query, codec)
  |> promise.map_try(fn(events) {
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
  })
}

/// Run a full context-first read-decide-append command flow.
pub fn dispatch_with_context(
  database: d1.Database,
  stream stream_name: String,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, state, decode_error),
  command command: command,
) -> Promise(Result(Append, Error(domain_error, decode_error))) {
  use context <- promise.try_await(read_context(
    database,
    query: query,
    decider: decider,
    codec: codec,
  ))
  case factos.decide_context(context, command, decider) {
    Error(error) -> promise.resolve(Error(DomainError(error)))
    Ok(pair) -> {
      let #(context, events) = pair
      let outbox_rows = generate_outbox_rows(events, stream_name, context.state, codec)
      append_with_condition(
        database,
        stream_name,
        events,
        codec,
        context.append_condition,
        outbox_rows,
      )
    }
  }
}

/// Load and fold one stream.
pub fn load_stream(
  database: d1.Database,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, state, decode_error),
) -> Promise(
  Result(factos.LoadedStream(event, state), Error(domain_error, decode_error)),
) {
  let factos.Decider(initial, _, evolve) = decider

  read_stream_events(database, stream_name, codec)
  |> promise.map_try(fn(events) {
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
  })
}

/// Run a stream-based read-decide-append command flow.
pub fn dispatch(
  database: d1.Database,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, state, decode_error),
  command command: command,
) -> Promise(Result(Append, Error(domain_error, decode_error))) {
  use loaded <- promise.try_await(load_stream(
    database,
    stream: stream_name,
    decider: decider,
    codec: codec,
  ))
  let factos.Decider(_, decide, _) = decider
  case decide(loaded.state, command) {
    Error(error) -> promise.resolve(Error(DomainError(error)))
    Ok(events) -> {
      let outbox_rows = generate_outbox_rows(events, stream_name, loaded.state, codec)
      append_stream_events(
        database,
        stream_name,
        events,
        codec,
        loaded.revision,
        outbox_rows,
      )
    }
  }
}

/// Run a stream-based read-decide-append command flow and return the loaded
/// state alongside the append result. This avoids a second read when the caller
/// needs both the appended metadata and the decision state (e.g., customer
/// details for a side effect).
pub fn dispatch_with_state(
  database: d1.Database,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, state, decode_error),
  command command: command,
) -> Promise(
  Result(#(Append, factos.LoadedStream(event, state)), Error(
    domain_error,
    decode_error,
  )),
) {
  use loaded <- promise.try_await(load_stream(
    database,
    stream: stream_name,
    decider: decider,
    codec: codec,
  ))
  let factos.Decider(_, decide, _) = decider
  let events = decide(loaded.state, command)

  use append <- promise.try_await(
    case events {
      Ok(events) -> {
        let outbox_rows = generate_outbox_rows(events, stream_name, loaded.state, codec)
        append_stream_events(
          database,
          stream_name,
          events,
          codec,
          loaded.revision,
          outbox_rows,
        )
        |> promise.map(fn(result) {
          result |> result.map(fn(append) { #(append, loaded) })
        })
      }
      Error(error) -> Error(DomainError(error)) |> promise.resolve
    },
  )

  Ok(append) |> promise.resolve
}

fn generate_outbox_rows(
  events: List(event),
  stream_name: String,
  state: state,
  codec: EventCodec(event, state, decode_error),
) -> List(OutboxRow) {
  let EventCodec(_, _, side_effects) = codec
  events
  |> list.flat_map(fn(event) { side_effects(event, stream_name, state) })
}

fn append_with_condition(
  database: d1.Database,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, state, decode_error),
  condition: factos.AppendCondition,
  outbox_rows: List(OutboxRow),
) -> Promise(Result(Append, Error(domain_error, decode_error))) {
  case condition {
    factos.NoAppendCondition ->
      append_events(database, stream_name, events, codec, CurrentStream, outbox_rows)
    factos.FailIfEventsMatch(query, after) ->
      append_events(
        database,
        stream_name,
        events,
        codec,
        ContextCondition(query, after),
        outbox_rows,
      )
  }
}

fn append_stream_events(
  database: d1.Database,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, state, decode_error),
  expected: factos.Revision,
  outbox_rows: List(OutboxRow),
) -> Promise(Result(Append, Error(domain_error, decode_error))) {
  append_events(
    database,
    stream_name,
    events,
    codec,
    ExpectedStream(expected),
    outbox_rows,
  )
}

fn append_events(
  database: d1.Database,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, state, decode_error),
  mode: AppendMode,
  outbox_rows: List(OutboxRow),
) -> Promise(Result(Append, Error(domain_error, decode_error))) {
  case events {
    [] ->
      current_revision(database, stream_name)
      |> promise.map(fn(result) {
        result
        |> result.map(fn(revision) {
          Append(current_revision: revision, position: factos.NoPosition, outbox_ids: [])
        })
      })
    [_, ..] -> {
      let #(sql, values) = append_sql(stream_name, events, codec, mode)
      let event_statement =
        d1.prepare(database, sql) |> d1.bind(values)

      case outbox_rows {
        [] ->
          d1.batch(database, [event_statement])
          |> decode_batch_result(events, mode)
        [_, ..] -> {
          let #(outbox_sql, outbox_values) = outbox_insert_sql(outbox_rows)
          let outbox_statement =
            d1.prepare(database, outbox_sql) |> d1.bind(outbox_values)
          d1.batch(database, [event_statement, outbox_statement])
          |> decode_batch_result(events, mode)
        }
      }
    }
  }
}

fn decode_batch_result(
  batch_result: Promise(Result(array.Array(d1.RunResult), String)),
  events: List(event),
  mode: AppendMode,
) -> Promise(Result(Append, Error(domain_error, decode_error))) {
  batch_result
  |> promise.map(fn(result) {
    use run_results <- result.try(result |> result.map_error(StoreError))
    use first_result <- result.try(
      array.get(run_results, 0)
      |> result.replace_error(StoreError("no event insert result in batch")),
    )
    let d1.RunResult(success: True, results: event_rows, ..) = first_result

    use appended <- result.try(decode_append_rows(event_rows))
    case list.length(appended) == list.length(events) {
      True -> {
        let outbox_ids = case array.length(run_results) {
          1 -> Ok([])
          _ -> {
            use outbox_result <- result.try(
              array.get(run_results, 1)
              |> result.replace_error(StoreError("no outbox insert result in batch")),
            )
            let d1.RunResult(success: True, results: outbox_rows, ..) = outbox_result
            decode_outbox_ids(outbox_rows)
          }
        }
        use outbox_ids <- result.try(outbox_ids)
        let #(position, revision) = last_append_row(appended)
        Ok(Append(
          current_revision: revision,
          position: factos.SequencePosition(position),
          outbox_ids: outbox_ids,
        ))
      }
      False -> Error(AppendConditionFailed(append_condition_for(mode)))
    }
  })
}

fn read_matching_events(
  database: d1.Database,
  query: factos.Query,
  codec: EventCodec(event, state, decode_error),
) -> Promise(
  Result(List(factos.Recorded(event)), Error(domain_error, decode_error)),
) {
  d1.prepare(
    database,
    "select position, id, stream, revision, type, version, tags, metadata, data from factos_events order by position",
  )
  |> d1.raw
  |> promise.map(fn(result) {
    use rows <- result.try(result |> result.map_error(StoreError))
    decode_rows(rows, codec)
    |> result.map(list.filter(_, factos.matches_query(_, query)))
  })
}

fn read_stream_events(
  database: d1.Database,
  stream_name: String,
  codec: EventCodec(event, state, decode_error),
) -> Promise(
  Result(List(factos.Recorded(event)), Error(domain_error, decode_error)),
) {
  d1.prepare(
    database,
    "select position, id, stream, revision, type, version, tags, metadata, data from factos_events where stream = ? order by revision",
  )
  |> d1.bind([stream_name])
  |> d1.raw
  |> promise.map(fn(result) {
    use rows <- result.try(result |> result.map_error(StoreError))
    decode_rows(rows, codec)
  })
}

fn current_revision(
  database: d1.Database,
  stream_name: String,
) -> Promise(Result(Int, Error(domain_error, decode_error))) {
  d1.prepare(
    database,
    "select coalesce(max(revision), -1) from factos_events where stream = ?",
  )
  |> d1.bind([stream_name])
  |> d1.raw
  |> promise.map(fn(result) {
    use rows <- result.try(result |> result.map_error(StoreError))
    case array.to_list(rows) {
      [row, ..] -> decode_int_field(row, 0)
      [] -> Ok(-1)
    }
  })
}

fn decode_rows(
  rows: array.Array(array.Array(Dynamic)),
  codec: EventCodec(event, state, decode_error),
) -> Result(List(factos.Recorded(event)), Error(domain_error, decode_error)) {
  rows
  |> array.to_list
  |> list.try_map(fn(row) { decode_row(row, codec) })
}

fn decode_row(
  row: array.Array(Dynamic),
  codec: EventCodec(event, state, decode_error),
) -> Result(factos.Recorded(event), Error(domain_error, decode_error)) {
  use stored <- result.try(decode_stored_event(row))
  let EventCodec(_, decode_event, _) = codec
  use decoded <- result.try(
    decode_event(stored) |> result.map_error(DecodeError),
  )
  let factos.Decoded(event, type_, version, tags, metadata) = decoded
  let StoredEvent(position, id, stream, revision, _, _, _, _, _) = stored

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

fn decode_stored_event(
  row: array.Array(Dynamic),
) -> Result(StoredEvent, Error(domain_error, decode_error)) {
  use position <- result.try(decode_int_field(row, 0))
  use id <- result.try(decode_string_field(row, 1))
  use stream <- result.try(decode_string_field(row, 2))
  use revision <- result.try(decode_int_field(row, 3))
  use type_name <- result.try(decode_string_field(row, 4))
  use version <- result.try(decode_int_field(row, 5))
  use tags <- result.try(decode_string_field(row, 6))
  use metadata <- result.try(decode_string_field(row, 7))
  use data <- result.try(decode_string_field(row, 8))

  Ok(StoredEvent(
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

fn decode_append_rows(
  rows: array.Array(Dynamic),
) -> Result(List(#(Int, Int)), Error(domain_error, decode_error)) {
  rows
  |> array.to_list
  |> list.try_map(fn(row) {
    use position <- result.try(
      decode.run(row, decode.field("position", decode.int))
      |> result.map_error(RowDecodeError),
    )
    use revision <- result.try(
      decode.run(row, decode.field("revision", decode.int))
      |> result.map_error(RowDecodeError),
    )
    Ok(#(position, revision))
  })
}

fn decode_outbox_ids(
  rows: array.Array(Dynamic),
) -> Result(List(Int), Error(domain_error, decode_error)) {
  rows
  |> array.to_list
  |> list.try_map(fn(row) {
    decode.run(row, decode.field("id", decode.int))
    |> result.map_error(RowDecodeError)
  })
}

fn decode_int_field(
  row: array.Array(Dynamic),
  index: Int,
) -> Result(Int, Error(domain_error, decode_error)) {
  use value <- result.try(
    array.get(row, index)
    |> result.replace_error(RowDecodeError([])),
  )
  decode.run(value, decode.int)
  |> result.map_error(RowDecodeError)
}

fn decode_string_field(
  row: array.Array(Dynamic),
  index: Int,
) -> Result(String, Error(domain_error, decode_error)) {
  use value <- result.try(
    array.get(row, index)
    |> result.replace_error(RowDecodeError([])),
  )
  decode.run(value, decode.string)
  |> result.map_error(RowDecodeError)
}

fn append_sql(
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, state, decode_error),
  mode: AppendMode,
) -> #(String, List(String)) {
  let rows =
    events
    |> list.index_map(fn(event, index) {
      append_select_sql(stream_name, event, codec, mode, index)
    })

  let sql =
    "insert into factos_events (id, stream, revision, type, version, tags, metadata, data) "
    <> string.join(list.map(rows, fn(row) { row.0 }), with: " union all ")
    <> " returning position, revision"

  let values = rows |> list.flat_map(fn(row) { row.1 })
  #(sql, values)
}

fn append_select_sql(
  stream_name: String,
  event: event,
  codec: EventCodec(event, state, decode_error),
  mode: AppendMode,
  index: Int,
) -> #(String, List(String)) {
  let EventCodec(encode, _, _) = codec
  let Proposed(id, _, type_, version, tags, metadata, data) = encode(event)
  let base_values = [
    id,
    stream_name,
    stream_name,
    int.to_string(index + 1),
    factos.event_type_name(type_),
    int.to_string(version),
    tags_to_text(tags),
    metadata_to_text(metadata),
    data,
  ]

  let #(condition, condition_values) = append_condition_sql(stream_name, mode)
  #(
    "select ?, ?, (select coalesce(max(revision), -1) from factos_events where stream = ?) + cast(? as integer), ?, cast(? as integer), ?, ?, ? where "
      <> condition,
    list.append(base_values, condition_values),
  )
}

fn outbox_insert_sql(
  outbox_rows: List(OutboxRow),
) -> #(String, List(String)) {
  let placeholders =
    list.repeat("(?, ?, ?)", list.length(outbox_rows))
    |> string.join(with: ", ")
  let values =
    outbox_rows
    |> list.flat_map(fn(OutboxRow(event_type, stream, payload)) {
      [stream, event_type, payload]
    })
  #(
    "insert into event_outbox (stream, event_type, payload) values "
    <> placeholders
    <> " returning id",
    values,
  )
}

fn append_condition_sql(
  stream_name: String,
  mode: AppendMode,
) -> #(String, List(String)) {
  case mode {
    CurrentStream -> #("1 = 1", [])
    ExpectedStream(expected) -> #(
      "(select coalesce(max(revision), -1) from factos_events where stream = ?) = cast(? as integer)",
      [stream_name, int.to_string(revision_to_int(expected))],
    )
    ContextCondition(query, after) -> {
      let QuerySql(sql, values) = matching_events_after_sql(query, after)
      #("not exists (" <> sql <> ")", values)
    }
  }
}

fn matching_events_after_sql(
  query: factos.Query,
  after: factos.SequencePosition,
) -> QuerySql {
  let after_position = case after {
    factos.NoPosition -> -1
    factos.SequencePosition(position) -> position
  }

  case query {
    factos.AllEvents ->
      QuerySql(
        sql: "select 1 from factos_events where position > cast(? as integer) limit 1",
        values: [int.to_string(after_position)],
      )
    factos.Query(items) -> {
      let item_sql = list.map(items, query_item_sql)
      QuerySql(
        sql: "select 1 from factos_events where position > cast(? as integer) and ("
          <> string.join(
          list.map(item_sql, fn(item) { item.sql }),
          with: " or ",
        )
          <> ") limit 1",
        values: [
          int.to_string(after_position),
          ..list.flat_map(item_sql, fn(item) { item.values })
        ],
      )
    }
  }
}

fn query_item_sql(item: factos.QueryItem) -> QuerySql {
  let factos.QueryItem(types, tags) = item
  let type_sql = case types {
    [] -> QuerySql(sql: "1 = 1", values: [])
    [_, ..] ->
      QuerySql(
        sql: "type in (" <> placeholders(list.length(types)) <> ")",
        values: list.map(types, factos.event_type_name),
      )
  }
  let tag_sql = case tags {
    [] -> QuerySql(sql: "1 = 1", values: [])
    [_, ..] ->
      QuerySql(
        sql: string.join(
          list.repeat("instr(tags, ?) > 0", list.length(tags)),
          with: " and ",
        ),
        values: list.map(tags, fn(tag) { "\n" <> factos.tag_value(tag) <> "\n" }),
      )
  }

  QuerySql(
    sql: "(" <> type_sql.sql <> " and " <> tag_sql.sql <> ")",
    values: list.append(type_sql.values, tag_sql.values),
  )
}

fn placeholders(count: Int) -> String {
  list.repeat("?", count) |> string.join(with: ", ")
}

fn append_condition_for(mode: AppendMode) -> factos.AppendCondition {
  case mode {
    CurrentStream -> factos.NoAppendCondition
    ExpectedStream(_) -> factos.NoAppendCondition
    ContextCondition(query, after) -> factos.FailIfEventsMatch(query, after)
  }
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

fn revision_to_int(revision: factos.Revision) -> Int {
  case revision {
    factos.NoEvents -> -1
    factos.CurrentRevision(revision) -> revision
  }
}

fn last_append_row(rows: List(#(Int, Int))) -> #(Int, Int) {
  case list.reverse(rows) {
    [row, ..] -> row
    [] -> #(-1, -1)
  }
}

fn tags_to_text(tags: List(factos.Tag)) -> String {
  case tags {
    [] -> ""
    [_, ..] ->
      "\n"
      <> { tags |> list.map(factos.tag_value) |> string.join(with: "\n") }
      <> "\n"
  }
}

fn tags_from_text(tags: String) -> List(factos.Tag) {
  case string.is_empty(tags) {
    True -> []
    False ->
      tags
      |> string.split(on: "\n")
      |> list.filter(fn(tag) { !string.is_empty(tag) })
      |> list.map(factos.tag)
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
