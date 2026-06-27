import cf/d1
import cf/miniflare
import cf/miniflare/bindings
import factos
import factos/factos_cf
import factos/factos_cf/outbox
import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/int
import gleam/javascript/promise.{type Promise}
import gleam/list
import gleam/option
import gleam/result
import gleeunit

pub fn main() {
  gleeunit.main()
}

const worker_name = "test"

const database_binding = "DB"

type Command {
  Reserve(name: String)
}

type Event {
  Reserved(name: String)
}

type DomainError {
  AlreadyReserved(name: String)
}

type DecodeError {
  UnknownEvent(String)
}

type TestDatabase {
  TestDatabase(miniflare: miniflare.Miniflare, database: d1.Database)
}

fn test_client(database: d1.Database) -> factos_cf.Client {
  factos_cf.new(database)
}

pub fn dispatch_stream_appends_and_loads_events_test() -> Promise(Nil) {
  use test_database <- promise.await(new_test_database())
  let client = test_client(test_database.database)
  use migrate_result <- promise.await(factos_cf.migrate(client))
  case migrate_result {
    Ok(Nil) -> Nil
    Error(error) -> {
      let message = "migration failed: " <> error_to_string(error)
      panic as message
    }
  }
  use clear_result <- promise.await(clear_events(test_database.database))
  case clear_result {
    Ok(Nil) -> Nil
    Error(error) -> {
      let message = "clear events failed: " <> error
      panic as message
    }
  }

  use append_result <- promise.await(factos_cf.dispatch(
    client,
    stream: "reservation-renata",
    decider: reservation_decider(),
    codec: codec(),
    command: Reserve("renata"),
  ))
  case append_result {
    Ok(factos_cf.Append(
      current_revision: 0,
      position: factos.SequencePosition(_),
    )) -> Nil
    Ok(append) -> {
      let message =
        "stream append returned unexpected metadata: "
        <> append_to_string(append)
      panic as message
    }
    Error(error) -> {
      let message = "stream append failed: " <> error_to_string(error)
      panic as message
    }
  }

  use loaded_result <- promise.await(factos_cf.load_stream(
    client,
    stream: "reservation-renata",
    decider: reservation_decider(),
    codec: codec(),
  ))
  let loaded = case loaded_result {
    Ok(loaded) -> loaded
    Error(_) -> panic as "stream load failed"
  }
  case loaded.state {
    ["renata"] -> Nil
    _ -> panic as "unexpected loaded state"
  }
  case loaded.revision {
    factos.CurrentRevision(0) -> Nil
    _ -> panic as "unexpected stream revision"
  }
  case list.length(loaded.events) {
    1 -> Nil
    _ -> panic as "unexpected loaded event count"
  }

  use Nil <- promise.await(dispose(test_database))
  promise.resolve(Nil)
}

pub fn dispatch_context_rejects_changed_context_test() -> Promise(Nil) {
  use test_database <- promise.await(new_test_database())
  let client = test_client(test_database.database)
  use migrate_result <- promise.await(factos_cf.migrate(client))
  case migrate_result {
    Ok(Nil) -> Nil
    Error(error) -> {
      let message = "migration failed: " <> error_to_string(error)
      panic as message
    }
  }
  use clear_result <- promise.await(clear_events(test_database.database))
  case clear_result {
    Ok(Nil) -> Nil
    Error(error) -> {
      let message = "clear events failed: " <> error
      panic as message
    }
  }

  let query =
    factos.query([
      factos.query_item(types: [factos.event_type("reserved")], tags: [
        factos.tag("name:context-renata"),
      ]),
    ])

  use first_result <- promise.await(factos_cf.dispatch_with_query(
    client,
    stream: "reservation-renata",
    query: query,
    decider: reservation_decider(),
    codec: codec(),
    command: Reserve("context-renata"),
  ))
  case first_result {
    Ok(factos_cf.Append(
      current_revision: 0,
      position: factos.SequencePosition(_),
    )) -> Nil
    Ok(append) -> {
      let message =
        "context append returned unexpected metadata: "
        <> append_to_string(append)
      panic as message
    }
    Error(error) -> {
      let message = "context append failed: " <> error_to_string(error)
      panic as message
    }
  }

  use second_result <- promise.await(factos_cf.dispatch_with_query(
    client,
    stream: "reservation-renata-duplicate",
    query: query,
    decider: reservation_decider(),
    codec: codec(),
    command: Reserve("context-renata"),
  ))
  case second_result {
    Error(factos_cf.DomainError(AlreadyReserved("context-renata"))) -> Nil
    _ -> panic as "duplicate context command should fail"
  }

  use context_result <- promise.await(factos_cf.read_context(
    client,
    query: query,
    decider: reservation_decider(),
    codec: codec(),
  ))
  let context = case context_result {
    Ok(context) -> context
    Error(_) -> panic as "context read failed"
  }
  case context.state {
    ["context-renata"] -> Nil
    _ -> panic as "unexpected context state"
  }
  case context.position {
    factos.SequencePosition(_) -> Nil
    _ -> panic as "unexpected context position"
  }

  use Nil <- promise.await(dispose(test_database))
  promise.resolve(Nil)
}

pub fn dispatch_awaits_side_effects_test() -> Promise(Nil) {
  use test_database <- promise.await(new_test_database())
  let client = test_client(test_database.database)
  use Nil <- promise.await(migrate_or_panic(client))
  use Nil <- promise.await(clear_events_or_panic(test_database.database))
  use Nil <- promise.await(create_side_effect_marker_table(
    test_database.database,
  ))

  use append_result <- promise.await(factos_cf.dispatch(
    client,
    stream: "reservation-side-effect",
    decider: reservation_decider(),
    codec: codec_with_side_effect(test_database.database),
    command: Reserve("side-effect"),
  ))
  case append_result {
    Ok(_) -> Nil
    Error(error) -> {
      let message = "stream append failed: " <> error_to_string(error)
      panic as message
    }
  }

  use marker_written <- promise.await(side_effect_marker_written(
    test_database.database,
  ))
  case marker_written {
    True -> Nil
    False -> panic as "dispatch returned before side effect finished"
  }

  use Nil <- promise.await(dispose(test_database))
  promise.resolve(Nil)
}

pub fn dispatch_with_state_awaits_side_effects_test() -> Promise(Nil) {
  use test_database <- promise.await(new_test_database())
  let client = test_client(test_database.database)
  use Nil <- promise.await(migrate_or_panic(client))
  use Nil <- promise.await(clear_events_or_panic(test_database.database))
  use Nil <- promise.await(create_side_effect_marker_table(
    test_database.database,
  ))

  use append_result <- promise.await(factos_cf.dispatch(
    client,
    stream: "reservation-side-effect-state",
    decider: reservation_decider(),
    codec: codec_with_side_effect(test_database.database),
    command: Reserve("side-effect-state"),
  ))
  case append_result {
    Ok(_) -> Nil
    Error(error) -> {
      let message = "stream append failed: " <> error_to_string(error)
      panic as message
    }
  }

  use marker_written <- promise.await(side_effect_marker_written(
    test_database.database,
  ))
  case marker_written {
    True -> Nil
    False -> panic as "dispatch_with_state returned before side effect finished"
  }

  use Nil <- promise.await(dispose(test_database))
  promise.resolve(Nil)
}

pub fn outbox_insert_with_no_entries_returns_empty_ids_test() -> Promise(Nil) {
  use test_database <- promise.await(new_test_database())
  let client = test_client(test_database.database)
  use Nil <- promise.await(migrate_or_panic(client))

  use ids <- promise.await(outbox.insert(test_database.database, []))
  case ids {
    Ok([]) -> Nil
    Ok(_) -> panic as "empty outbox insert returned ids"
    Error(_) -> panic as "empty outbox insert failed"
  }

  use Nil <- promise.await(dispose(test_database))
  promise.resolve(Nil)
}

fn new_test_database() -> Promise(TestDatabase) {
  let worker =
    miniflare.worker(
      worker_name,
      "export default { fetch() { return new Response('ok') } }",
      "2026-06-26",
    )
    |> miniflare.with_d1_database(database_binding)

  let miniflare = miniflare.new([worker])

  use Nil <- promise.await(miniflare.ready(miniflare))
  use database <- promise.await(bindings.get_database(
    miniflare,
    database_binding,
    option.Some(worker_name),
  ))

  promise.resolve(TestDatabase(miniflare:, database:))
}

fn dispose(test_database: TestDatabase) -> Promise(Nil) {
  miniflare.dispose(test_database.miniflare)
}

fn migrate_or_panic(client: factos_cf.Client) -> Promise(Nil) {
  use migrate_result <- promise.await(factos_cf.migrate(client))
  case migrate_result {
    Ok(Nil) -> promise.resolve(Nil)
    Error(error) -> {
      let message = "migration failed: " <> error_to_string(error)
      panic as message
    }
  }
}

fn clear_events_or_panic(database: d1.Database) -> Promise(Nil) {
  use clear_result <- promise.await(clear_events(database))
  case clear_result {
    Ok(Nil) -> promise.resolve(Nil)
    Error(error) -> {
      let message = "clear events failed: " <> error
      panic as message
    }
  }
}

fn clear_events(database: d1.Database) -> Promise(Result(Nil, String)) {
  d1.prepare(database, "delete from factos_events")
  |> d1.run
  |> promise.map(fn(run_result) { run_result |> result.map(fn(_) { Nil }) })
}

fn create_side_effect_marker_table(database: d1.Database) -> Promise(Nil) {
  use result <- promise.await(d1.exec(
    database,
    "create table side_effect_marker (id integer primary key autoincrement);",
  ))
  case result {
    Ok(_) -> promise.resolve(Nil)
    Error(error) -> panic as error
  }
}

fn insert_side_effect_marker(database: d1.Database) -> Promise(Nil) {
  use Nil <- promise.await(promise.wait(50))
  use result <- promise.await(
    d1.prepare(database, "insert into side_effect_marker default values")
    |> d1.run,
  )
  case result {
    Ok(_) -> promise.resolve(Nil)
    Error(error) -> panic as error
  }
}

fn side_effect_marker_written(database: d1.Database) -> Promise(Bool) {
  use result <- promise.await(
    d1.prepare(database, "select count(*) as count from side_effect_marker")
    |> d1.first,
  )
  case result {
    Ok(row) -> promise.resolve(decode_count(row) > 0)
    Error(error) -> panic as error
  }
}

fn decode_count(row: Dynamic) -> Int {
  let decoder = decode.field("count", decode.int, decode.success)
  case decode.run(row, decoder) {
    Ok(count) -> count
    Error(_) -> 0
  }
}

fn error_to_string(error: factos_cf.Error(DomainError, DecodeError)) -> String {
  case error {
    factos_cf.DomainError(_) -> "domain error"
    factos_cf.DecodeError(_) -> "decode error"
    factos_cf.StoreError(error) -> error
    factos_cf.RowDecodeError(_) -> "row decode error"
    factos_cf.AppendConditionFailed(_) -> "append condition failed"
  }
}

fn append_to_string(append: factos_cf.Append) -> String {
  let factos_cf.Append(current_revision:, position:) = append
  "current_revision="
  <> int.to_string(current_revision)
  <> ", position="
  <> position_to_string(position)
}

fn position_to_string(position: factos.SequencePosition) -> String {
  case position {
    factos.NoPosition -> "none"
    factos.SequencePosition(position) -> int.to_string(position)
  }
}

fn reservation_decider() -> factos.Decider(
  Command,
  List(String),
  Event,
  DomainError,
) {
  factos.decider(initial: [], decide: decide, evolve: evolve)
}

fn decide(
  state: List(String),
  command: Command,
) -> Result(List(Event), DomainError) {
  case command {
    Reserve(name) ->
      case list.contains(state, name) {
        True -> Error(AlreadyReserved(name))
        False -> Ok([Reserved(name)])
      }
  }
}

fn evolve(state: List(String), event: Event) -> List(String) {
  case event {
    Reserved(name) -> [name, ..state]
  }
}

fn codec() -> factos_cf.EventCodec(Event, List(String), DecodeError) {
  factos_cf.codec(encode:, decode:, side_effects: [])
}

fn codec_with_side_effect(
  database: d1.Database,
) -> factos_cf.EventCodec(Event, List(String), DecodeError) {
  factos_cf.codec(encode: encode, decode: decode, side_effects: [
    fn(_) { insert_side_effect_marker(database) },
  ])
}

fn encode(event: Event) -> factos_cf.Proposed(Event) {
  case event {
    Reserved(name) ->
      factos_cf.Proposed(
        id: "event-" <> name,
        event: event,
        type_: factos.event_type("reserved"),
        version: 1,
        tags: [factos.tag("name:" <> name)],
        metadata: factos.empty_metadata(),
        data: name,
      )
  }
}

fn decode(
  stored: factos_cf.StoredEvent,
) -> Result(factos.Decoded(Event), DecodeError) {
  case factos.event_type_name(stored.type_) {
    "reserved" ->
      Ok(factos.Decoded(
        event: Reserved(stored.data),
        type_: stored.type_,
        version: stored.version,
        tags: stored.tags,
        metadata: stored.metadata,
      ))
    other -> Error(UnknownEvent(other))
  }
}
