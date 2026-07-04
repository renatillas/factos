import cf/d1
import cf/miniflare
import cf/miniflare/bindings
import factos
import factos/factos_cf
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
    Ok(dispatch) -> {
      let assert factos_cf.Append(
        current_revision: 0,
        position: factos.SequencePosition(_),
      ) = dispatch.append
      let assert [recorded] = dispatch.events
      assert recorded.event == Reserved("renata")
      Nil
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
    Ok(dispatch) -> {
      let assert factos_cf.Append(
        current_revision: 0,
        position: factos.SequencePosition(_),
      ) = dispatch.append
      let assert [recorded] = dispatch.events
      assert recorded.event == Reserved("context-renata")
      Nil
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

fn clear_events(database: d1.Database) -> Promise(Result(Nil, String)) {
  d1.prepare(database, "delete from factos_events")
  |> d1.run
  |> promise.map(fn(run_result) { run_result |> result.map(fn(_) { Nil }) })
}

fn error_to_string(error: factos_cf.Error(DomainError)) -> String {
  case error {
    factos_cf.DomainError(_) -> "domain error"
    factos_cf.EventDecodeError(_) -> "decode error"
    factos_cf.StoreError(error) -> error
    factos_cf.RowDecodeError(_) -> "row decode error"
    factos_cf.AppendConditionFailed(_) -> "append condition failed"
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

fn codec() -> factos_cf.EventCodec(Event, List(String)) {
  factos_cf.codec(encode:, decode:)
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
) -> Result(factos.Decoded(Event), factos_cf.EventDecodeError) {
  case factos.event_type_name(stored.type_) {
    "reserved" ->
      Ok(factos.Decoded(
        event: Reserved(stored.data),
        type_: stored.type_,
        version: stored.version,
        tags: stored.tags,
        metadata: stored.metadata,
      ))
    other -> Error(factos_cf.UnknownEventType(other))
  }
}
