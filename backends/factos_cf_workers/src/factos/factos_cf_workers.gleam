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
    tags: List(factos.Tag),
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
    tags: List(factos.Tag),
    data: String,
  )
}

pub type EventCodec(event, decode_error) {
  /// Application-owned D1 event codec.
  EventCodec(
    encode: fn(event) -> Proposed(event),
    decode: fn(StoredEvent) -> Result(factos.Decoded(event), decode_error),
  )
}

pub type Append {
  /// Result of a successful append.
  Append(current_revision: Int, position: factos.SequencePosition)
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
      tags text not null,
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
  execute_migration(
    database,
    "
    create index if not exists factos_events_position
      on factos_events(position)
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
  codec codec: EventCodec(event, decode_error),
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
pub fn dispatch_context(
  database: d1.Database,
  stream stream_name: String,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, decode_error),
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
      append_with_condition(
        database,
        stream_name,
        events,
        codec,
        context.append_condition,
      )
    }
  }
}

/// Load and fold one stream.
pub fn load_stream(
  database: d1.Database,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, decode_error),
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
pub fn dispatch_stream(
  database: d1.Database,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, decode_error),
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
    Ok(events) ->
      append_stream_events(
        database,
        stream_name,
        events,
        codec,
        loaded.revision,
      )
  }
}

fn append_with_condition(
  database: d1.Database,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, decode_error),
  condition: factos.AppendCondition,
) -> Promise(Result(Append, Error(domain_error, decode_error))) {
  case condition {
    factos.NoAppendCondition ->
      append_events(database, stream_name, events, codec, CurrentStream)
    factos.FailIfEventsMatch(query, after) ->
      append_events(
        database,
        stream_name,
        events,
        codec,
        ContextCondition(query, after),
      )
  }
}

fn append_stream_events(
  database: d1.Database,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, decode_error),
  expected: factos.Revision,
) -> Promise(Result(Append, Error(domain_error, decode_error))) {
  append_events(database, stream_name, events, codec, ExpectedStream(expected))
}

fn append_events(
  database: d1.Database,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, decode_error),
  mode: AppendMode,
) -> Promise(Result(Append, Error(domain_error, decode_error))) {
  case events {
    [] ->
      current_revision(database, stream_name)
      |> promise.map(fn(result) {
        result
        |> result.map(fn(revision) {
          Append(current_revision: revision, position: factos.NoPosition)
        })
      })
    [_, ..] -> {
      let #(sql, values) = append_sql(stream_name, events, codec, mode)
      d1.prepare(database, sql)
      |> d1.bind(values)
      |> d1.raw
      |> promise.map(fn(result) {
        use rows <- result.try(result |> result.map_error(StoreError))
        use appended <- result.try(decode_append_rows(rows))
        case list.length(appended) == list.length(events) {
          True -> {
            let #(position, revision) = last_append_row(appended)
            Ok(Append(
              current_revision: revision,
              position: factos.SequencePosition(position),
            ))
          }
          False -> Error(AppendConditionFailed(append_condition_for(mode)))
        }
      })
    }
  }
}

fn read_matching_events(
  database: d1.Database,
  query: factos.Query,
  codec: EventCodec(event, decode_error),
) -> Promise(
  Result(List(factos.Recorded(event)), Error(domain_error, decode_error)),
) {
  d1.prepare(
    database,
    "select position, id, stream, revision, type, tags, data from factos_events order by position",
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
  codec: EventCodec(event, decode_error),
) -> Promise(
  Result(List(factos.Recorded(event)), Error(domain_error, decode_error)),
) {
  d1.prepare(
    database,
    "select position, id, stream, revision, type, tags, data from factos_events where stream = ? order by revision",
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
  codec: EventCodec(event, decode_error),
) -> Result(List(factos.Recorded(event)), Error(domain_error, decode_error)) {
  rows
  |> array.to_list
  |> list.try_map(fn(row) { decode_row(row, codec) })
}

fn decode_row(
  row: array.Array(Dynamic),
  codec: EventCodec(event, decode_error),
) -> Result(factos.Recorded(event), Error(domain_error, decode_error)) {
  use stored <- result.try(decode_stored_event(row))
  let EventCodec(_, decode_event) = codec
  use decoded <- result.try(
    decode_event(stored) |> result.map_error(DecodeError),
  )
  let factos.Decoded(event, type_, tags) = decoded
  let StoredEvent(position, id, stream, revision, _, _, _) = stored

  Ok(factos.Recorded(
    id: id,
    stream: stream,
    revision: revision,
    position: factos.SequencePosition(position),
    type_: type_,
    tags: tags,
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
  use tags <- result.try(decode_string_field(row, 5))
  use data <- result.try(decode_string_field(row, 6))

  Ok(StoredEvent(
    position: position,
    id: id,
    stream: stream,
    revision: revision,
    type_: factos.event_type(type_name),
    tags: tags_from_text(tags),
    data: data,
  ))
}

fn decode_append_rows(
  rows: array.Array(array.Array(Dynamic)),
) -> Result(List(#(Int, Int)), Error(domain_error, decode_error)) {
  rows
  |> array.to_list
  |> list.try_map(fn(row) {
    use position <- result.try(decode_int_field(row, 0))
    use revision <- result.try(decode_int_field(row, 1))
    Ok(#(position, revision))
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
  codec: EventCodec(event, decode_error),
  mode: AppendMode,
) -> #(String, List(String)) {
  let rows =
    events
    |> list.index_map(fn(event, index) {
      append_select_sql(stream_name, event, codec, mode, index)
    })

  let sql =
    "insert into factos_events (id, stream, revision, type, tags, data) "
    <> string.join(list.map(rows, fn(row) { row.0 }), with: " union all ")
    <> " returning position, revision"

  let values = rows |> list.flat_map(fn(row) { row.1 })
  #(sql, values)
}

fn append_select_sql(
  stream_name: String,
  event: event,
  codec: EventCodec(event, decode_error),
  mode: AppendMode,
  index: Int,
) -> #(String, List(String)) {
  let EventCodec(encode, _) = codec
  let Proposed(id, _, type_, tags, data) = encode(event)
  let base_values = [
    id,
    stream_name,
    stream_name,
    int.to_string(index + 1),
    factos.event_type_name(type_),
    tags_to_text(tags),
    data,
  ]

  let #(condition, condition_values) = append_condition_sql(stream_name, mode)
  #(
    "select ?, ?, (select coalesce(max(revision), -1) from factos_events where stream = ?) + cast(? as integer), ?, ?, ? where "
      <> condition,
    list.append(base_values, condition_values),
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
