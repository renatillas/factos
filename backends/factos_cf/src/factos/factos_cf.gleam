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
//// Side effects (extracted from the codec's `side_effects` list) run
//// **after** a successful append, receiving the domain events that were
//// just persisted. Dispatch functions wait for side effects to finish before
//// resolving so callers can safely depend on side-effect continuations such as
//// outbox inserts and queue notifications.

import cf/d1
import factos
import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/int
import gleam/javascript/array
import gleam/javascript/promise.{type Promise}
import gleam/list
import gleam/pair
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

/// A raw event row read from D1 before domain decoding.
///
/// Applications normally do not construct this directly. The backend reads
/// `StoredEvent` values from the `factos_events` table and passes them to the
/// application's `EventCodec.decode` function.
///
/// The fields map directly to stored columns:
///
/// - `position` is the global append-only sequence position.
/// - `id` is the application-provided event identifier.
/// - `stream` is the logical stream name.
/// - `revision` is the zero-based stream revision.
/// - `type_`, `version`, `tags`, and `metadata` are query and compatibility
///   metadata used by Factos.
/// - `data` is the application-owned serialized payload.
pub type StoredEvent {
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

/// Backend client bundling a D1 database binding.
///
/// Create this once from the Worker environment's D1 binding and pass it to the
/// backend functions. Keeping the database inside `Client` makes APIs stable if
/// more Cloudflare bindings or runtime configuration are needed later.
pub opaque type Client {
  Client(database: d1.Database)
}

pub fn new(database: d1.Database) -> Client {
  Client(database)
}

/// Application-owned codec and side-effect configuration for this D1 backend.
///
pub opaque type EventCodec(event, state, decode_error) {
  EventCodec(
    encode: fn(event) -> Proposed(event),
    decode: fn(StoredEvent) -> Result(factos.Decoded(event), decode_error),
    side_effects: List(fn(List(event)) -> Promise(Nil)),
  )
}

/// Create a new codec
/// 
/// `encode` turns a domain event into a `Proposed` event ready for persistence.
/// This is where the application chooses event IDs, event types, schema
/// versions, tags, metadata, and payload serialization.
///
/// `decode` turns a stored row back into a domain event. Decode failures are
/// returned as `DecodeError` and stop load/read flows rather than panicking.
///
/// `side_effects` are async hooks that run after a successful append. Each hook
/// receives the exact domain events that were just persisted. Dispatch functions
/// await all hooks before resolving, which is important for outbox workflows:
/// a side effect can insert `factos/outbox.Entry` rows and enqueue the returned
/// outbox IDs before the command response completes.
///
/// Side effects are not part of the append transaction. Events are persisted
/// first; side effects run after the append succeeds. If a side effect can fail
/// and must be retried, prefer writing an outbox row and processing it from a
/// queue or scheduled worker.
pub fn codec(
  encode encode: fn(event) -> Proposed(event),
  decode decode: fn(StoredEvent) -> Result(factos.Decoded(event), decode_error),
  side_effects side_effects: List(fn(List(event)) -> Promise(Nil)),
) -> EventCodec(event, state, decode_error) {
  EventCodec(encode:, decode:, side_effects:)
}

/// Result of a successful append.
///
/// `current_revision` is the new stream revision after the append. For an empty
/// append it is the current revision that was observed.
///
/// `position` is the global event position assigned to the last appended event.
/// Empty appends use `factos.NoPosition` because no new event row was written.
pub type Append {
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
///
/// This function is idempotent and safe to run during application startup or in
/// tests. It creates:
///
/// - `factos_events`, the append-only event store.
/// - indexes used by stream reads, context queries, and projection cursors.
/// - `event_outbox`, the optional outbox table used by `factos/outbox`.
///
/// The outbox table is included here because side-effect hooks commonly need to
/// insert outbox rows immediately after successful appends. Keeping both tables
/// in one migration function prevents consumers from accidentally deploying the
/// event store without the side-effect infrastructure.
pub fn migrate(client: Client) -> Promise(Result(Nil, Error(_, _))) {
  let Client(database:) = client
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
///
/// This is the read half of a context-first command flow. It selects all events
/// matching `query`, decodes them with `codec`, and folds them through the
/// decider's `evolve` function starting from `decider.initial`.
///
/// The returned `factos.Context` includes an append condition that protects the
/// caller from stale decisions. Pass the same query to `dispatch_with_context`
/// when you want the backend to perform the full read-decide-append flow.
pub fn read_context(
  client: Client,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, state, decode_error),
) -> Promise(
  Result(factos.Context(event, state), Error(domain_error, decode_error)),
) {
  use events <- promise.map_try(read_matching_events(
    client.database,
    query,
    codec,
  ))
  let position = highest_recorded_position(events)
  Ok(factos.Context(
    query:,
    state: factos.evolve_recorded(
      initial: decider.initial,
      events: events,
      evolve: decider.evolve,
    ),
    events: events,
    position: position,
    append_condition: factos.FailIfEventsMatch(query, position),
  ))
}

/// Run a full context-first read-decide-append command flow.
///
/// This function:
///
/// - reads all events matching `query`,
/// - folds them into state,
/// - asks the decider to produce new events,
/// - appends those events only if no matching events were added meanwhile,
/// - runs and awaits codec side effects after a successful append.
///
/// Use this for commands whose validity depends on facts outside a single
/// stream. If the context changed between the read and the append, the function
/// returns `AppendConditionFailed`.
pub fn dispatch_with_query(
  client: Client,
  stream stream_name: String,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, state, decode_error),
  command command: command,
) -> Promise(Result(Append, Error(domain_error, decode_error))) {
  use context <- promise.try_await(read_context(
    client,
    query:,
    decider:,
    codec:,
  ))

  use #(context, events) <- promise.try_await(
    factos.decide_context(context, command, decider)
    |> result.map_error(DomainError)
    |> promise.resolve(),
  )

  use append <- promise.try_await(append_with_condition(
    client.database,
    stream_name,
    events,
    codec,
    context.append_condition,
  ))

  use _nil_list <- promise.await(run_side_effects(codec, events))

  promise.resolve(Ok(append))
}

/// Load and fold one stream.
///
/// Reads every event from `stream_name`, decodes the rows with `codec`, and
/// folds them through `decider.evolve`. This does not append events and does not
/// run side effects.
pub fn load_stream(
  client: Client,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, state, decode_error),
) -> Promise(
  Result(factos.LoadedStream(event, state), Error(domain_error, decode_error)),
) {
  use events <- promise.map_try(read_stream_events(
    client.database,
    stream_name,
    codec,
  ))
  Ok(factos.LoadedStream(
    stream: stream_name,
    state: factos.evolve_recorded(
      initial: decider.initial,
      events:,
      evolve: decider.evolve,
    ),
    events: events,
    revision: stream_revision(events),
  ))
}

/// Run a stream-based read-decide-append command flow.
///
/// This function loads one stream, asks the decider to handle `command`, and
/// appends the produced events with an expected-revision condition. If another
/// write has advanced the stream, the append fails with `AppendConditionFailed`.
///
/// After a successful append, all codec side effects are awaited before the
/// returned promise resolves.
pub fn dispatch(
  client: Client,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event, state, decode_error),
  command command: command,
) -> Promise(Result(Append, Error(domain_error, decode_error))) {
  let Client(database:) = client
  use loaded <- promise.try_await(load_stream(
    client,
    stream: stream_name,
    decider: decider,
    codec: codec,
  ))
  let factos.Decider(_, decide, _) = decider
  case decide(loaded.state, command) {
    Error(error) -> promise.resolve(Error(DomainError(error)))
    Ok(events) -> {
      use result <- promise.await(append_stream_events(
        database,
        stream_name,
        events,
        codec,
        loaded.revision,
      ))
      case result {
        Ok(_) -> {
          use _ <- promise.await(run_side_effects(codec, events))
          promise.resolve(result)
        }
        Error(_) -> promise.resolve(result)
      }
    }
  }
}

fn run_side_effects(
  codec: EventCodec(event, state, decode_error),
  events: List(event),
) -> promise.Promise(List(Nil)) {
  promise.await_list(list.map(codec.side_effects, fn(f) { f(events) }))
}

fn append_with_condition(
  database: d1.Database,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, state, decode_error),
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
  codec: EventCodec(event, state, decode_error),
  expected: factos.Revision,
) -> Promise(Result(Append, Error(domain_error, decode_error))) {
  append_events(database, stream_name, events, codec, ExpectedStream(expected))
}

fn append_events(
  database: d1.Database,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event, state, decode_error),
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
      let event_statement = d1.prepare(database, sql) |> d1.bind(values)

      d1.batch(database, [event_statement])
      |> decode_batch_result(events, mode)
    }
  }
}

fn decode_batch_result(
  batch_result: Promise(Result(array.Array(d1.RunResult), String)),
  events: List(event),
  mode: AppendMode,
) -> Promise(Result(Append, Error(domain_error, decode_error))) {
  use result <- promise.map(batch_result)
  use run_results <- result.try(result |> result.map_error(StoreError))
  let run_result_list = array.to_list(run_results)
  use first_result <- result.try(
    list.first(run_result_list)
    |> result.replace_error(StoreError("no event insert result in batch")),
  )
  let d1.RunResult(success: success, results: event_rows, ..) = first_result
  use _ <- result.try(case success {
    True -> Ok(Nil)
    False -> Error(StoreError("event insert failed"))
  })
  use appended <- result.try(decode_append_rows(event_rows))
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
}

fn read_matching_events(
  database: d1.Database,
  query: factos.Query,
  codec: EventCodec(event, state, decode_error),
) -> Promise(
  Result(List(factos.Recorded(event)), Error(domain_error, decode_error)),
) {
  let #(where_sql, params) = query_to_sql(query)

  d1.prepare(
    database,
    "select position, id, stream, revision, type, version, tags, metadata, data
     from factos_events " <> where_sql <> " order by position",
  )
  |> d1.bind(params)
  |> d1.raw
  |> promise.map(fn(result) {
    use rows <- result.try(result |> result.map_error(StoreError))
    decode_rows(rows, codec)
  })
}

fn query_to_sql(query: factos.Query) {
  case query {
    factos.AllEvents -> #("", [])

    factos.Query(items) -> {
      case items {
        [] -> #("where 1 = 0", [])
        [_, ..] -> {
          let built_items =
            items
            |> list.map(query_item_to_sql)

          let where_sql =
            built_items
            |> list.map(pair.first)
            |> string.join(" or ")

          let params =
            built_items
            |> list.flat_map(pair.second)

          #("where " <> where_sql, params)
        }
      }
    }
  }
}

fn query_item_to_sql(item: factos.QueryItem) {
  let #(types_sql, types_params) = types_to_sql(item.types)
  let #(tag_sql, tag_params) = tags_to_sql(item.tags)

  #(
    "(" <> types_sql <> " and " <> tag_sql <> ")",
    list.append(types_params, tag_params),
  )
}

fn types_to_sql(types: List(factos.EventType)) {
  case types {
    [] -> #("1 = 1", [])

    [_, ..] -> {
      let placeholders =
        types
        |> list.map(fn(_) { "?" })
        |> string.join(", ")

      let params =
        types
        |> list.map(factos.event_type_name)

      #("type in (" <> placeholders <> ")", params)
    }
  }
}

fn tags_to_sql(tags: List(factos.Tag)) {
  case tags {
    [] -> #("1 = 1", [])

    [_, ..] -> {
      let clauses =
        tags
        |> list.map(fn(_) { "instr(tags, char(10) || ? || char(10)) > 0" })
        |> string.join(" and ")

      let params =
        tags
        |> list.map(factos.tag_value)

      #("(" <> clauses <> ")", params)
    }
  }
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
      decode.run(row, append_row_decoder())
      |> result.map_error(RowDecodeError),
    )
    Ok(position)
  })
}

fn append_row_decoder() -> decode.Decoder(#(Int, Int)) {
  use position <- decode.field("position", decode.int)
  use revision <- decode.field("revision", decode.int)
  decode.success(#(position, revision))
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

pub fn error_to_string(
  error: Error(domain_error, decode_error),
  domain_error_to_string: fn(domain_error) -> String,
  decode_error_to_string: fn(domain_error) -> String,
) -> String {
  case error {
    DecodeError(_) -> todo
    DomainError(error) -> domain_error_to_string(error)
    StoreError(error) -> "store error: " <> error
    RowDecodeError(decode_error) ->
      "database row decode error: "
      <> list.map(decode_error, decode_error_to_string) |> string.join(", ")
    AppendConditionFailed(factos.NoAppendCondition) ->
      "append to event failed: No append condition"
    AppendConditionFailed(factos.FailIfEventsMatch(query: _, after: _)) ->
      "append to event failed: Events matched"
  }
}

fn decode_error_to_string(decode_error: decode.DecodeError) -> String {
  case decode_error {
    decode.DecodeError(expected:, found:, path:) ->
      "expected: "
      <> expected
      <> ", found: "
      <> found
      <> ", "
      <> "path: ["
      <> path |> string.join(",")
      <> "]"
  }
}
