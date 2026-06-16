import cqrs
import gleam/dynamic/decode
import gleam/json
import gleam/list
import gleam/result
import pog

const create_event_table_query: String = "
CREATE TABLE IF NOT EXISTS cqrs_event (
  global_position bigint PRIMARY KEY,
  stream_id text NOT NULL,
  stream_position bigint NOT NULL,
  event_type text NOT NULL,
  payload text NOT NULL,
  metadata text NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (stream_id, stream_position)
);
"

const lock_event_table_query: String = "
SELECT 1
FROM pg_advisory_xact_lock(hashtext('cqrs_event'));
"

const select_next_global_position_query: String = "
SELECT COALESCE(MAX(global_position), 0) + 1
FROM cqrs_event;
"

const select_next_stream_position_query: String = "
SELECT COALESCE(MAX(stream_position), 0) + 1
FROM cqrs_event
WHERE stream_id = $1;
"

const insert_event_query: String = "
INSERT INTO cqrs_event
  (global_position, stream_id, stream_position, event_type, payload, metadata)
VALUES
  ($1, $2, $3, $4, $5, $6);
"

const load_all_query: String = "
SELECT global_position, stream_id, stream_position, event_type, payload, metadata
FROM cqrs_event
ORDER BY global_position ASC;
"

const load_stream_query: String = "
SELECT global_position, stream_id, stream_position, event_type, payload, metadata
FROM cqrs_event
WHERE stream_id = $1
ORDER BY stream_position ASC;
"

pub opaque type PostgresStore(event) {
  PostgresStore(
    connection: pog.Connection,
    event_to_json: fn(event) -> json.Json,
    event_decoder: decode.Decoder(event),
  )
}

pub type PostgresStoreError {
  QueryFailed(reason: pog.QueryError)
  TransactionFailed(reason: pog.QueryError)
  PayloadCouldNotBeDecoded(event_type: String, reason: json.DecodeError)
  MetadataCouldNotBeDecoded(reason: json.DecodeError)
  ExpectedSingleRow(query: String)
}

type EventRow {
  EventRow(
    global_position: Int,
    stream_id: String,
    stream_position: Int,
    event_type: String,
    payload: String,
    metadata: String,
  )
}

pub fn new(
  connection: pog.Connection,
  event_to_json: fn(event) -> json.Json,
  event_decoder: decode.Decoder(event),
) -> cqrs.EventStore(PostgresStore(event), event, PostgresStoreError) {
  cqrs.EventStore(
    store: PostgresStore(
      connection: connection,
      event_to_json: event_to_json,
      event_decoder: event_decoder,
    ),
    append: append,
    load_all: load_all,
    load_stream: load_stream,
  )
}

pub fn create_table(
  connection: pog.Connection,
) -> Result(Nil, PostgresStoreError) {
  create_event_table_query
  |> pog.query
  |> pog.returning(nil_decoder())
  |> pog.execute(connection)
  |> result.replace(Nil)
  |> result.map_error(QueryFailed)
}

fn append(
  store: PostgresStore(event),
  stream_id: String,
  events: List(cqrs.PendingEvent(event)),
  metadata: List(#(String, String)),
) -> Result(
  #(PostgresStore(event), List(cqrs.RecordedEvent(event))),
  PostgresStoreError,
) {
  case
    pog.transaction(store.connection, fn(transaction) {
      use _ <- result.try(lock_event_table(transaction))
      use next_global_position <- result.try(next_global_position(transaction))
      use next_stream_position <- result.try(next_stream_position(
        transaction,
        stream_id,
      ))

      let recorded_events =
        record_events(
          events,
          stream_id,
          metadata,
          next_global_position,
          next_stream_position,
        )

      use _ <- result.try(insert_events(transaction, store, recorded_events))
      Ok(recorded_events)
    })
  {
    Ok(recorded_events) -> Ok(#(store, recorded_events))
    Error(pog.TransactionQueryError(reason)) -> Error(TransactionFailed(reason))
    Error(pog.TransactionRolledBack(reason)) -> Error(reason)
  }
}

fn load_all(
  store: PostgresStore(event),
) -> Result(List(cqrs.RecordedEvent(event)), PostgresStoreError) {
  load_all_query
  |> pog.query
  |> pog.returning(event_row_decoder())
  |> pog.execute(store.connection)
  |> result.map_error(QueryFailed)
  |> result.try(fn(returned) {
    let pog.Returned(_, rows) = returned
    decode_rows(rows, store)
  })
}

fn load_stream(
  store: PostgresStore(event),
  stream_id: String,
) -> Result(List(cqrs.RecordedEvent(event)), PostgresStoreError) {
  load_stream_query
  |> pog.query
  |> pog.parameter(pog.text(stream_id))
  |> pog.returning(event_row_decoder())
  |> pog.execute(store.connection)
  |> result.map_error(QueryFailed)
  |> result.try(fn(returned) {
    let pog.Returned(_, rows) = returned
    decode_rows(rows, store)
  })
}

fn lock_event_table(
  connection: pog.Connection,
) -> Result(Nil, PostgresStoreError) {
  lock_event_table_query
  |> pog.query
  |> pog.returning(int_row_decoder())
  |> pog.execute(connection)
  |> result.map(fn(_) { Nil })
  |> result.map_error(QueryFailed)
}

fn next_global_position(
  connection: pog.Connection,
) -> Result(Int, PostgresStoreError) {
  select_next_global_position_query
  |> pog.query
  |> pog.returning(int_row_decoder())
  |> pog.execute(connection)
  |> result.map_error(QueryFailed)
  |> result.try(fn(returned) {
    single_row(returned, "select_next_global_position")
  })
}

fn next_stream_position(
  connection: pog.Connection,
  stream_id: String,
) -> Result(Int, PostgresStoreError) {
  select_next_stream_position_query
  |> pog.query
  |> pog.parameter(pog.text(stream_id))
  |> pog.returning(int_row_decoder())
  |> pog.execute(connection)
  |> result.map_error(QueryFailed)
  |> result.try(fn(returned) {
    single_row(returned, "select_next_stream_position")
  })
}

fn insert_events(
  connection: pog.Connection,
  store: PostgresStore(event),
  events: List(cqrs.RecordedEvent(event)),
) -> Result(Nil, PostgresStoreError) {
  list.try_each(events, fn(event) {
    insert_event_query
    |> pog.query
    |> pog.parameter(pog.int(event.global_position))
    |> pog.parameter(pog.text(event.stream_id))
    |> pog.parameter(pog.int(event.stream_position))
    |> pog.parameter(pog.text(event.event_type))
    |> pog.parameter(
      pog.text(json.to_string(store.event_to_json(event.payload))),
    )
    |> pog.parameter(pog.text(metadata_to_json(event.metadata)))
    |> pog.returning(nil_decoder())
    |> pog.execute(connection)
    |> result.replace(Nil)
    |> result.map_error(QueryFailed)
  })
}

fn record_events(
  events: List(cqrs.PendingEvent(event)),
  stream_id: String,
  metadata: List(#(String, String)),
  next_global_position: Int,
  next_stream_position: Int,
) -> List(cqrs.RecordedEvent(event)) {
  let #(recorded_events, _, _) =
    list.fold(
      events,
      #([], next_global_position, next_stream_position),
      fn(accumulator, pending_event) {
        let #(recorded_events, global_position, stream_position) = accumulator
        let cqrs.PendingEvent(event_type, payload) = pending_event

        #(
          [
            cqrs.RecordedEvent(
              global_position: global_position,
              stream_id: stream_id,
              stream_position: stream_position,
              event_type: event_type,
              payload: payload,
              metadata: metadata,
            ),
            ..recorded_events
          ],
          global_position + 1,
          stream_position + 1,
        )
      },
    )

  list.reverse(recorded_events)
}

fn decode_rows(
  rows: List(EventRow),
  store: PostgresStore(event),
) -> Result(List(cqrs.RecordedEvent(event)), PostgresStoreError) {
  list.try_map(rows, fn(row) { decode_row(row, store) })
}

fn decode_row(
  row: EventRow,
  store: PostgresStore(event),
) -> Result(cqrs.RecordedEvent(event), PostgresStoreError) {
  use payload <- result.try(
    json.parse(row.payload, store.event_decoder)
    |> result.map_error(fn(reason) {
      PayloadCouldNotBeDecoded(event_type: row.event_type, reason: reason)
    }),
  )

  use metadata <- result.try(
    json.parse(row.metadata, metadata_decoder())
    |> result.map_error(MetadataCouldNotBeDecoded),
  )

  Ok(cqrs.RecordedEvent(
    global_position: row.global_position,
    stream_id: row.stream_id,
    stream_position: row.stream_position,
    event_type: row.event_type,
    payload: payload,
    metadata: metadata,
  ))
}

fn metadata_to_json(metadata: List(#(String, String))) -> String {
  metadata
  |> json.array(fn(pair) {
    let #(key, value) = pair
    json.object([#("key", json.string(key)), #("value", json.string(value))])
  })
  |> json.to_string
}

fn metadata_decoder() -> decode.Decoder(List(#(String, String))) {
  decode.list({
    use key <- decode.field("key", decode.string)
    use value <- decode.field("value", decode.string)
    decode.success(#(key, value))
  })
}

fn event_row_decoder() -> decode.Decoder(EventRow) {
  use global_position <- decode.field(0, decode.int)
  use stream_id <- decode.field(1, decode.string)
  use stream_position <- decode.field(2, decode.int)
  use event_type <- decode.field(3, decode.string)
  use payload <- decode.field(4, decode.string)
  use metadata <- decode.field(5, decode.string)

  decode.success(EventRow(
    global_position: global_position,
    stream_id: stream_id,
    stream_position: stream_position,
    event_type: event_type,
    payload: payload,
    metadata: metadata,
  ))
}

fn int_row_decoder() -> decode.Decoder(Int) {
  decode.field(0, decode.int, decode.success)
}

fn nil_decoder() -> decode.Decoder(Nil) {
  decode.map(decode.dynamic, fn(_) { Nil })
}

fn single_row(
  returned: pog.Returned(Int),
  query: String,
) -> Result(Int, PostgresStoreError) {
  case returned {
    pog.Returned(_, [row]) -> Ok(row)
    pog.Returned(_, []) -> Error(ExpectedSingleRow(query))
    pog.Returned(_, [_, ..]) -> Error(ExpectedSingleRow(query))
  }
}
