//// Generic event outbox helpers for PostgreSQL.
////
//// Provides insertion, read, status-update, and decoding helpers for rows in
//// the `event_outbox` table created by `factos_pog.migrate`.

import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/option
import gleam/result
import gleam/time/timestamp
import pog

/// A full row from the `event_outbox` table.
pub type OutboxRecord {
  OutboxRecord(
    id: Int,
    stream: String,
    event_type: String,
    payload: String,
    status: String,
    error: String,
    created_at: Int,
    processed_at: Int,
  )
}

/// Errors returned by outbox reads and status updates.
pub type Error {
  StoreError(pog.QueryError)
  RowDecodeError(List(decode.DecodeError))
}

/// A pending side-effect row to insert into the outbox.
pub opaque type Entry {
  Entry(stream: String, event_type: String, payload: String)
}

/// Construct an outbox entry for later insertion.
pub fn entry(
  stream stream: String,
  event_type event_type: String,
  payload payload: String,
) -> Entry {
  Entry(stream:, event_type:, payload:)
}

/// Insert outbox entries and return their generated IDs.
pub fn insert(
  connection: pog.Connection,
  entries: List(Entry),
) -> Result(List(Int), Error) {
  insert_entries(connection, entries, [])
}

/// Fetch a single pending outbox row by ID.
pub fn read_pending_outbox(
  connection: pog.Connection,
  id: Int,
) -> Result(option.Option(OutboxRecord), Error) {
  use returned <- result.try(
    pog.query(
      "select id, stream, event_type, payload, status, error, created_at, processed_at
       from event_outbox
       where id = $1 and status = 'pending'
       limit 1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.returning(outbox_record_decoder())
    |> pog.execute(on: connection)
    |> result.map_error(StoreError),
  )

  case returned.rows {
    [] -> Ok(option.None)
    [row, ..] -> Ok(option.Some(row))
  }
}

/// Mark an outbox row as successfully processed.
pub fn mark_outbox_sent(
  connection: pog.Connection,
  id: Int,
  processed_at: timestamp.Timestamp,
) -> Result(Nil, Error) {
  mark_outbox_processed(connection, id, "sent", "", processed_at)
}

/// Mark an outbox row as failed with an error message.
pub fn mark_outbox_failed(
  connection: pog.Connection,
  id: Int,
  error_message: String,
  processed_at: timestamp.Timestamp,
) -> Result(Nil, Error) {
  mark_outbox_processed(connection, id, "failed", error_message, processed_at)
}

fn insert_entries(
  connection: pog.Connection,
  entries: List(Entry),
  ids: List(Int),
) -> Result(List(Int), Error) {
  case entries {
    [] -> Ok(list.reverse(ids))
    [entry, ..rest] -> {
      let Entry(stream, event_type, payload) = entry
      use returned <- result.try(
        pog.query(
          "insert into event_outbox (stream, event_type, payload)
           values ($1, $2, $3)
           returning id",
        )
        |> pog.parameter(pog.text(stream))
        |> pog.parameter(pog.text(event_type))
        |> pog.parameter(pog.text(payload))
        |> pog.returning(int_field_decoder())
        |> pog.execute(on: connection)
        |> result.map_error(StoreError),
      )
      let ids = case returned.rows {
        [] -> ids
        [id, ..] -> [id, ..ids]
      }
      insert_entries(connection, rest, ids)
    }
  }
}

fn mark_outbox_processed(
  connection: pog.Connection,
  id: Int,
  status: String,
  error_message: String,
  processed_at: timestamp.Timestamp,
) -> Result(Nil, Error) {
  let processed_at_seconds =
    processed_at
    |> timestamp.to_unix_seconds_and_nanoseconds
    |> fn(pair) { pair.0 }

  pog.query(
    "update event_outbox
     set status = $1, error = $2, processed_at = $3
     where id = $4",
  )
  |> pog.parameter(pog.text(status))
  |> pog.parameter(pog.text(error_message))
  |> pog.parameter(pog.int(processed_at_seconds))
  |> pog.parameter(pog.int(id))
  |> pog.execute(on: connection)
  |> result.map(fn(_) { Nil })
  |> result.map_error(StoreError)
}

fn outbox_record_decoder() -> decode.Decoder(OutboxRecord) {
  use id <- decode.field(0, decode.int)
  use stream <- decode.field(1, decode.string)
  use event_type <- decode.field(2, decode.string)
  use payload <- decode.field(3, decode.string)
  use status <- decode.field(4, decode.string)
  use error <- decode.field(5, decode.optional(decode.string))
  use created_at <- decode.field(6, decode.int)
  use processed_at <- decode.field(7, decode.optional(decode.int))
  decode.success(OutboxRecord(
    id:,
    stream:,
    event_type:,
    payload:,
    status:,
    error: option.unwrap(error, ""),
    created_at:,
    processed_at: option.unwrap(processed_at, 0),
  ))
}

fn int_field_decoder() -> decode.Decoder(Int) {
  use value <- decode.field(0, decode.int)
  decode.success(value)
}

pub fn error_to_string(error: Error) -> String {
  case error {
    StoreError(_) -> "store error"
    RowDecodeError(errors) ->
      "database row decode error: " <> int.to_string(list.length(errors))
  }
}
