import cf_workers/d1
import cf_workers/miniflare
import cf_workers/miniflare/d1 as miniflare_d1
import factos
import factos/factos_cf_workers as backend
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

pub fn dispatch_stream_appends_and_loads_events_test() -> Promise(Nil) {
  use test_database <- promise.await(new_test_database())
  use migrate_result <- promise.await(backend.migrate(test_database.database))
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

  use append_result <- promise.await(backend.dispatch_stream(
    test_database.database,
    stream: "reservation-renata",
    decider: reservation_decider(),
    codec: codec(),
    command: Reserve("renata"),
  ))
  case append_result {
    Ok(backend.Append(current_revision: 0, position: factos.SequencePosition(_))) ->
      Nil
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

  use loaded_result <- promise.await(backend.load_stream(
    test_database.database,
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
  use migrate_result <- promise.await(backend.migrate(test_database.database))
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

  use first_result <- promise.await(backend.dispatch_context(
    test_database.database,
    stream: "reservation-renata",
    query: query,
    decider: reservation_decider(),
    codec: codec(),
    command: Reserve("context-renata"),
  ))
  case first_result {
    Ok(backend.Append(current_revision: 0, position: factos.SequencePosition(_))) ->
      Nil
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

  use second_result <- promise.await(backend.dispatch_context(
    test_database.database,
    stream: "reservation-renata-duplicate",
    query: query,
    decider: reservation_decider(),
    codec: codec(),
    command: Reserve("context-renata"),
  ))
  case second_result {
    Error(backend.DomainError(AlreadyReserved("context-renata"))) -> Nil
    _ -> panic as "duplicate context command should fail"
  }

  use context_result <- promise.await(backend.read_context(
    test_database.database,
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
  use database <- promise.await(miniflare_d1.get_database(
    miniflare,
    database_binding,
    option.Some(worker_name),
  ))

  promise.resolve(TestDatabase(miniflare:, database:))
}

fn dispose(test_database: TestDatabase) -> Promise(Nil) {
  miniflare.dispose(test_database.miniflare)
}

fn clear_events(database: d1.Database) -> Promise(Result(Nil, String)) {
  d1.prepare(database, "delete from factos_events")
  |> d1.run
  |> promise.map(fn(run_result) { run_result |> result.map(fn(_) { Nil }) })
}

fn error_to_string(error: backend.Error(DomainError, DecodeError)) -> String {
  case error {
    backend.DomainError(_) -> "domain error"
    backend.DecodeError(_) -> "decode error"
    backend.StoreError(error) -> error
    backend.RowDecodeError(_) -> "row decode error"
    backend.AppendConditionFailed(_) -> "append condition failed"
  }
}

fn append_to_string(append: backend.Append) -> String {
  let backend.Append(current_revision, position) = append
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

fn codec() -> backend.EventCodec(Event, DecodeError) {
  backend.EventCodec(encode: encode, decode: decode)
}

fn encode(event: Event) -> backend.Proposed(Event) {
  case event {
    Reserved(name) ->
      backend.Proposed(
        id: "event-" <> name,
        event: event,
        type_: factos.event_type("reserved"),
        tags: [factos.tag("name:" <> name)],
        metadata: factos.empty_metadata(),
        data: name,
      )
  }
}

fn decode(
  stored: backend.StoredEvent,
) -> Result(factos.Decoded(Event), DecodeError) {
  case factos.event_type_name(stored.type_) {
    "reserved" ->
      Ok(factos.Decoded(
        event: Reserved(stored.data),
        type_: stored.type_,
        tags: stored.tags,
        metadata: stored.metadata,
      ))
    other -> Error(UnknownEvent(other))
  }
}
