import cqrs
import cqrs/stores/postgres_store
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/result
import gleeunit
import global_value
import pog

const database_url: String = "postgres://cqrs:cqrs@localhost:54329/cqrs_test"

const stress_appends: Int = 300

const stress_queries: Int = 300

pub fn main() -> Nil {
  gleeunit.main()
}

type TestEvent {
  UserRegistered(id: String, email: String)
}

type TestGlobalData {
  TestGlobalData(connection: pog.Connection)
}

pub fn appends_and_loads_all_events_test() {
  let system = start_system()

  let assert Ok(events) =
    cqrs.append(
      system,
      "append-user-1",
      [cqrs.PendingEvent("user_registered", UserRegistered("user-1", "a@b.c"))],
      [#("request_id", "request-1")],
    )

  assert events
    == [
      cqrs.RecordedEvent(
        global_position: events |> first_global_position,
        stream_id: "append-user-1",
        stream_position: 1,
        event_type: "user_registered",
        payload: UserRegistered("user-1", "a@b.c"),
        metadata: [#("request_id", "request-1")],
      ),
    ]

  let assert Ok(stored_events) = cqrs.load_stream(system, "append-user-1")
  assert stored_events == events
  cqrs.stop(system)
}

pub fn load_stream_only_returns_matching_stream_test() {
  let system = start_system()

  let assert Ok(_) =
    cqrs.append(
      system,
      "load-user-1",
      [cqrs.PendingEvent("user_registered", UserRegistered("user-1", "a@b.c"))],
      [],
    )

  let assert Ok(_) =
    cqrs.append(
      system,
      "load-user-2",
      [cqrs.PendingEvent("user_registered", UserRegistered("user-2", "b@c.d"))],
      [],
    )

  let assert Ok(events) = cqrs.load_stream(system, "load-user-2")

  assert events
    == [
      cqrs.RecordedEvent(
        global_position: events |> first_global_position,
        stream_id: "load-user-2",
        stream_position: 1,
        event_type: "user_registered",
        payload: UserRegistered("user-2", "b@c.d"),
        metadata: [],
      ),
    ]

  cqrs.stop(system)
}

pub fn works_with_cqrs_system_test() {
  let system = start_system()

  let assert Ok(events) =
    cqrs.append(
      system,
      "system-user-3",
      [cqrs.PendingEvent("user_registered", UserRegistered("user-3", "c@d.e"))],
      [],
    )

  assert events
    == [
      cqrs.RecordedEvent(
        global_position: events |> first_global_position,
        stream_id: "system-user-3",
        stream_position: 1,
        event_type: "user_registered",
        payload: UserRegistered("user-3", "c@d.e"),
        metadata: [],
      ),
    ]

  let assert Ok(stored_events) = cqrs.load_stream(system, "system-user-3")
  assert stored_events == events
  cqrs.stop(system)
}

pub fn simultaneous_appends_and_queries_stress_test() {
  let system = start_system()
  let results = process.new_subject()

  int.range(0, stress_appends, Nil, fn(_, index) {
    let _pid =
      process.spawn(fn() {
        let result =
          cqrs.append(
            system,
            "stress-stream",
            [
              cqrs.PendingEvent(
                "user_registered",
                UserRegistered(
                  "stress-" <> int.to_string(index),
                  "stress-" <> int.to_string(index) <> "@example.com",
                ),
              ),
            ],
            [#("index", int.to_string(index))],
          )

        process.send(results, AppendCompleted(result))
      })

    Nil
  })

  int.range(0, stress_queries, Nil, fn(_, _index) {
    let _pid =
      process.spawn(fn() {
        process.send(results, QueryCompleted(cqrs.load_all(system)))
      })

    Nil
  })

  let outcomes =
    receive_stress_outcomes(results, stress_appends + stress_queries, [])
  let append_results = append_results(outcomes)
  let query_results = query_results(outcomes)

  assert list.length(append_results) == stress_appends
  assert list.length(query_results) == stress_queries
  assert ok_count(append_results) == stress_appends
  assert ok_count(query_results) == stress_queries

  let assert Ok(stored_events) = cqrs.load_stream(system, "stress-stream")
  assert list.length(stored_events) == stress_appends
  assert positions(stored_events, fn(event) { event.stream_position })
    == expected_positions(stress_appends)
  cqrs.stop(system)
}

fn global_data() -> TestGlobalData {
  global_value.create_with_unique_name("cqrs_postgres_test.global_data", fn() {
    let pool_name = process.new_name("cqrs_postgres_test_pool")
    let assert Ok(config) = pog.url_config(pool_name, database_url)
    let assert Ok(started) =
      config
      |> pog.pool_size(20)
      |> pog.start

    process.unlink(started.pid)
    let connection = started.data
    let assert Ok(Nil) = postgres_store.create_table(connection)
    reset_database(connection)
    TestGlobalData(connection:)
  })
}

type StressOutcome {
  AppendCompleted(
    Result(
      List(cqrs.RecordedEvent(TestEvent)),
      cqrs.Error(Nil, postgres_store.PostgresStoreError),
    ),
  )
  QueryCompleted(
    Result(
      List(cqrs.RecordedEvent(TestEvent)),
      cqrs.Error(Nil, postgres_store.PostgresStoreError),
    ),
  )
}

fn receive_stress_outcomes(
  results: process.Subject(StressOutcome),
  remaining: Int,
  outcomes: List(StressOutcome),
) -> List(StressOutcome) {
  case remaining {
    0 -> outcomes
    _ -> {
      let assert Ok(outcome) = process.receive(results, 30_000)
      receive_stress_outcomes(results, remaining - 1, [outcome, ..outcomes])
    }
  }
}

fn append_results(
  outcomes: List(StressOutcome),
) -> List(
  Result(
    List(cqrs.RecordedEvent(TestEvent)),
    cqrs.Error(Nil, postgres_store.PostgresStoreError),
  ),
) {
  outcomes
  |> list.filter_map(fn(outcome) {
    case outcome {
      AppendCompleted(result) -> Ok(result)
      QueryCompleted(_) -> Error(Nil)
    }
  })
}

fn query_results(
  outcomes: List(StressOutcome),
) -> List(
  Result(
    List(cqrs.RecordedEvent(TestEvent)),
    cqrs.Error(Nil, postgres_store.PostgresStoreError),
  ),
) {
  outcomes
  |> list.filter_map(fn(outcome) {
    case outcome {
      AppendCompleted(_) -> Error(Nil)
      QueryCompleted(result) -> Ok(result)
    }
  })
}

fn ok_count(results: List(Result(a, b))) -> Int {
  results
  |> list.filter(result.is_ok)
  |> list.length
}

fn positions(
  events: List(cqrs.RecordedEvent(event)),
  get_position: fn(cqrs.RecordedEvent(event)) -> Int,
) -> List(Int) {
  events
  |> list.map(get_position)
  |> list.sort(by: int.compare)
}

fn expected_positions(count: Int) -> List(Int) {
  int.range(1, count + 1, [], list.prepend)
  |> list.reverse
}

fn start_system() -> cqrs.System(TestEvent, postgres_store.PostgresStoreError) {
  let globals = global_data()
  let assert Ok(started) =
    cqrs.start(
      process.new_name("cqrs_postgres_registry"),
      timeout: 30_000,
      event_store: store(globals.connection),
    )

  started.data
}

fn first_global_position(events: List(cqrs.RecordedEvent(event))) -> Int {
  let assert [event] = events
  event.global_position
}

fn reset_database(connection: pog.Connection) -> Nil {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })
  let assert Ok(_) =
    "TRUNCATE TABLE cqrs_event;"
    |> pog.query
    |> pog.returning(decoder)
    |> pog.execute(connection)

  Nil
}

fn store(
  connection: pog.Connection,
) -> cqrs.EventStore(
  postgres_store.PostgresStore(TestEvent),
  TestEvent,
  postgres_store.PostgresStoreError,
) {
  postgres_store.new(connection, event_to_json, event_decoder())
}

fn event_to_json(event: TestEvent) -> json.Json {
  case event {
    UserRegistered(id, email) ->
      json.object([
        #("type", json.string("user_registered")),
        #("id", json.string(id)),
        #("email", json.string(email)),
      ])
  }
}

fn event_decoder() -> decode.Decoder(TestEvent) {
  use event_type <- decode.field("type", decode.string)
  case event_type {
    "user_registered" -> {
      use id <- decode.field("id", decode.string)
      use email <- decode.field("email", decode.string)
      decode.success(UserRegistered(id, email))
    }
    _ -> decode.failure(UserRegistered("", ""), expected: "TestEvent")
  }
}
