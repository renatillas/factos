//// Generic event outbox helpers for Cloudflare Workers D1.
////
//// Provides insertion, read, status-update, and decoding helpers for rows in
//// the `event_outbox` table created by `factos_cf_workers.migrate`.
////
//// The intended workflow is:
////
//// 1. A Factos side effect converts domain events into `Entry` values.
//// 2. The side effect calls `insert`, receiving the inserted outbox IDs.
//// 3. The application sends those IDs to a Cloudflare Queue.
//// 4. A queue consumer calls `read_pending_outbox` by ID.
//// 5. After processing, the consumer calls `mark_outbox_sent` or
////    `mark_outbox_failed`.
////
//// The module intentionally stores opaque payload strings. Applications decide
//// how to encode and decode payloads for each `event_type`.

import cf/d1
import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/int
import gleam/javascript/array
import gleam/javascript/promise.{type Promise}
import gleam/list
import gleam/option.{type Option}
import gleam/pair
import gleam/result
import gleam/time/timestamp

/// A full row from the `event_outbox` table.
///
/// `id` is the database-generated identifier used to enqueue and later read a
/// pending side effect.
///
/// `stream` is application-owned context. Many consumers use the aggregate or
/// stream ID here so related side effects can be traced.
///
/// `event_type` identifies what kind of side effect should be processed. Queue
/// consumers commonly dispatch on this value.
///
/// `payload` is an opaque application-owned string, typically JSON.
///
/// `status` is expected to be `pending`, `sent`, or `failed` by the helpers in
/// this module. The database default is `pending`.
///
/// `error` contains a failure message for failed rows. It is decoded as an empty
/// string when the database value is null.
///
/// `created_at` and `processed_at` are Unix timestamps. `processed_at` is
/// decoded as `0` when the database value is null.
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
///
/// `StoreError` wraps D1 execution failures. `RowDecodeError` means D1 returned
/// a row shape that did not match the columns requested by this module.
pub type Error {
  StoreError(String)
  RowDecodeError(List(decode.DecodeError))
}

/// A pending side-effect row to insert into the outbox.
///
/// The type is opaque so callers construct valid entries through `entry` rather
/// than depending on the table representation. This keeps the insert API small:
/// status, timestamps, errors, and IDs are owned by the database workflow.
pub opaque type Entry {
  Entry(stream: String, event_type: String, payload: String)
}

/// Construct an outbox entry for later insertion.
///
/// `stream` should be the domain stream or aggregate identifier related to the
/// side effect.
///
/// `event_type` should be stable and specific enough for queue consumers to
/// dispatch safely, such as `purchase.fulfillment_email_requested`.
///
/// `payload` is intentionally a string. Use JSON or another application-owned
/// format and decode it in the queue consumer after `read_pending_outbox`.
pub fn entry(
  stream stream: String,
  event_type event_type: String,
  payload payload: String,
) {
  Entry(stream:, event_type:, payload:)
}

/// Insert outbox entries and return their generated IDs.
///
/// The inserts are batched through D1, preserving all-or-nothing behaviour for
/// the supplied entries. The SQL uses `returning id`, and the returned IDs are
/// intended to be sent to a queue for asynchronous processing.
///
/// Passing an empty list is valid and returns `Ok([])` without calling
/// `d1.batch`. This matters because Cloudflare D1 rejects empty batches with
/// `No SQL statements detected`.
///
/// Failures are collapsed to `Error(Nil)` because callers generally only need
/// to know whether IDs were produced. Use D1 logs for lower-level diagnostics.
pub fn insert(
  database: d1.Database,
  entries: List(Entry),
) -> Promise(Result(List(Int), Nil)) {
  case entries {
    [] -> promise.resolve(Ok([]))
    _ -> {
      let results =
        {
          use entry <- list.map(entries)

          d1.prepare(
            database,
            "insert into event_outbox (stream, event_type, payload) values (?, ?, ?) returning id;",
          )
          |> d1.bind([entry.stream, entry.event_type, entry.payload])
        }
        |> d1.batch(database, _)

      use results <- promise.await(results)

      case results {
        Ok(results) -> {
          results
          |> array.to_list
          |> list.map(fn(result) {
            case result {
              d1.RunResult(success: True, results:, ..) ->
                decode_ids_from_rows(results)
              d1.RunResult(success: False, ..) -> []
            }
          })
          |> list.flatten
          |> Ok
          |> promise.resolve()
        }
        Error(_) -> promise.resolve(Error(Nil))
      }
    }
  }
}

/// Fetch a single pending outbox row by ID.
///
/// Returns `Ok(option.None)` if the row does not exist or is no longer pending.
/// Queue consumers should treat that as already processed or not actionable.
///
/// Only pending rows are returned so retrying a queue message after successful
/// processing does not re-run the side effect.
pub fn read_pending_outbox(
  database: d1.Database,
  id: Int,
) -> Promise(Result(Option(OutboxRecord), Error)) {
  d1.prepare(
    database,
    "select id, stream, event_type, payload, status, error, created_at, processed_at from event_outbox where id = ? and status = 'pending' limit 1",
  )
  |> d1.bind([int.to_string(id)])
  |> d1.raw
  |> promise.map(fn(result) {
    use rows <- result.try(result |> result.map_error(StoreError))
    case rows |> array.to_list {
      [] -> Ok(option.None)
      [row, ..] -> row |> decode_row |> result.map(option.Some)
    }
  })
}

/// Mark an outbox row as successfully processed.
///
/// `processed_at` should be a Unix timestamp chosen by the application. The row
/// status is changed to `sent`, and future `read_pending_outbox` calls for the
/// same ID will return `option.None`.
pub fn mark_outbox_sent(
  database: d1.Database,
  id: Int,
  processed_at: timestamp.Timestamp,
) -> Promise(Result(Nil, Error)) {
  d1.prepare(
    database,
    "update event_outbox set status = 'sent', processed_at = ? where id = ?",
  )
  |> d1.bind([
    processed_at
      |> timestamp.to_unix_seconds_and_nanoseconds
      |> pair.first
      |> int.to_string,
    int.to_string(id),
  ])
  |> d1.run
  |> promise.map(fn(result) {
    result
    |> result.map(constant_nil)
    |> result.map_error(StoreError)
  })
}

/// Mark an outbox row as failed with an error message.
///
/// `error_message` is stored for operational debugging. The helper also records
/// `processed_at`, allowing consumers to distinguish unprocessed pending rows
/// from failed rows that were attempted.
pub fn mark_outbox_failed(
  database: d1.Database,
  id: Int,
  error_message: String,
  processed_at: timestamp.Timestamp,
) -> Promise(Result(Nil, Error)) {
  use result <- promise.map(
    d1.prepare(
      database,
      "update event_outbox set status = 'failed', error = ?, processed_at = ? where id = ?",
    )
    |> d1.bind([
      error_message,
      processed_at
        |> timestamp.to_unix_seconds_and_nanoseconds
        |> pair.first
        |> int.to_string,
      int.to_string(id),
    ])
    |> d1.run,
  )
  result
  |> result.map(constant_nil)
  |> result.map_error(StoreError)
}

fn decode_row(row: array.Array(Dynamic)) -> Result(OutboxRecord, Error) {
  use id <- result.try(decode_int_field(row, 0))
  use stream <- result.try(decode_string_field(row, 1))
  use event_type <- result.try(decode_string_field(row, 2))
  use payload <- result.try(decode_string_field(row, 3))
  use status <- result.try(decode_string_field(row, 4))
  use error <- result.try(decode_nullable_string_field(row, 5))
  use created_at <- result.try(decode_int_field(row, 6))
  use processed_at <- result.try(decode_nullable_int_field(row, 7))

  Ok(OutboxRecord(
    id:,
    stream:,
    event_type:,
    payload:,
    status:,
    error:,
    created_at:,
    processed_at:,
  ))
}

fn decode_int_field(
  row: array.Array(Dynamic),
  index: Int,
) -> Result(Int, Error) {
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
) -> Result(String, Error) {
  use value <- result.try(
    array.get(row, index)
    |> result.replace_error(RowDecodeError([])),
  )
  decode.run(value, decode.string)
  |> result.map_error(RowDecodeError)
}

fn decode_nullable_string_field(
  row: array.Array(Dynamic),
  index: Int,
) -> Result(String, Error) {
  use value <- result.try(
    array.get(row, index)
    |> result.replace_error(RowDecodeError([])),
  )
  decode.run(value, decode.optional(decode.string))
  |> result.map_error(RowDecodeError)
  |> result.map(fn(opt) { option.unwrap(opt, "") })
}

fn decode_nullable_int_field(
  row: array.Array(Dynamic),
  index: Int,
) -> Result(Int, Error) {
  use value <- result.try(
    array.get(row, index)
    |> result.replace_error(RowDecodeError([])),
  )
  decode.run(value, decode.optional(decode.int))
  |> result.map_error(RowDecodeError)
  |> result.map(fn(opt) { option.unwrap(opt, 0) })
}

fn decode_ids_from_rows(rows: array.Array(dynamic.Dynamic)) -> List(Int) {
  use row <- list.filter_map(array.to_list(rows))

  let decoder = decode.field("id", decode.int, decode.success)
  decode.run(row, decoder) |> result.map_error(constant_nil)
}

fn constant_nil(_: a) -> Nil {
  Nil
}
