import factos
import factos/factos_pog
import gleam/bit_array
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{Some}
import gleam/result
import gleeunit
import global_value
import pog

pub fn main() -> Nil {
  gleeunit.main()
}

type Command {
  RegisterUser(username: String)
}

type Event {
  UserRegistered(username: String)
}

type State {
  Available
  Taken
}

type DomainError {
  AlreadyTaken
}

type DecodeError {
  UnknownEvent
  InvalidData
}

type CounterCommand {
  Increment
}

type CounterEvent {
  Incremented(value: Int)
}

type CounterState {
  CounterState(total: Int)
}

type TestGlobalData {
  TestGlobalData(connection: pog.Connection)
}

pub fn dispatch_stream_persists_events_test() {
  let TestGlobalData(connection) = global_data()
  reset_schema(connection)

  let assert Ok(factos_pog.Append(current_revision: 0, position: _)) =
    factos_pog.dispatch_stream(
      connection,
      stream: "user-renata",
      decider: decider(),
      codec: codec(),
      command: RegisterUser("renata"),
    )

  let assert Ok(loaded) =
    factos_pog.load_stream(
      connection,
      stream: "user-renata",
      decider: decider(),
      codec: codec(),
    )

  assert loaded.state == Taken
  assert loaded.revision == factos.CurrentRevision(0)
}

pub fn dispatch_context_reads_by_event_type_and_tags_test() {
  let TestGlobalData(connection) = global_data()
  reset_schema(connection)

  let query = username_query("renata")

  let assert Ok(factos_pog.Append(
    current_revision: 0,
    position: factos.SequencePosition(_),
  )) =
    factos_pog.dispatch_context(
      connection,
      stream: "user-renata",
      query: query,
      decider: decider(),
      codec: codec(),
      command: RegisterUser("renata"),
    )

  let assert Ok(context) =
    factos_pog.read_context(
      connection,
      query: query,
      decider: decider(),
      codec: codec(),
    )

  assert context.state == Taken
  assert list.length(context.events) == 1
  assert context.position != factos.NoPosition
}

pub fn dispatch_context_handles_many_streams_test() {
  let TestGlobalData(connection) = global_data()
  reset_schema(connection)

  let query =
    factos.query([
      factos.query_item(types: [factos.event_type("Incremented")], tags: [
        factos.tag("counter:load"),
      ]),
    ])

  let assert Ok(factos_pog.Append(
    current_revision: 0,
    position: factos.SequencePosition(_),
  )) = dispatch_counter_context_many(connection, query, 25)

  let assert Ok(context) =
    factos_pog.read_context(
      connection,
      query: query,
      decider: counter_decider(),
      codec: counter_codec(),
    )

  assert context.state == CounterState(25)
  assert list.length(context.events) == 25
}

fn global_data() -> TestGlobalData {
  global_value.create_with_unique_name("factos_pog_test.global.data", fn() {
    TestGlobalData(connection: start_test_connection())
  })
}

fn start_test_connection() -> pog.Connection {
  let pool_name = process.new_name("factos_pog_test")
  let config =
    pog.default_config(pool_name)
    |> pog.host("127.0.0.1")
    |> pog.port(5432)
    |> pog.database("factos_pog")
    |> pog.user("postgres")
    |> pog.password(Some("postgres"))
    |> pog.ssl(pog.SslDisabled)

  let assert Ok(_) = pog.start(config)
  process.sleep(100)
  pog.named_connection(pool_name)
}

fn reset_schema(connection: pog.Connection) -> Nil {
  let assert Ok(_) =
    pog.query("drop table if exists factos_events")
    |> pog.execute(on: connection)
  let assert Ok(Nil) = factos_pog.migrate(connection)
  Nil
}

fn username_query(username: String) -> factos.Query {
  factos.query([
    factos.query_item(types: [factos.event_type("UserRegistered")], tags: [
      factos.tag("username:" <> username),
    ]),
  ])
}

fn decider() -> factos.Decider(Command, State, Event, DomainError) {
  factos.decider(initial: Available, decide:, evolve:)
}

fn decide(state: State, command: Command) -> Result(List(Event), DomainError) {
  case state, command {
    Available, RegisterUser(username) -> Ok([UserRegistered(username)])
    Taken, RegisterUser(_) -> Error(AlreadyTaken)
  }
}

fn evolve(_state: State, _event: Event) -> State {
  Taken
}

fn codec() -> factos_pog.EventCodec(Event, DecodeError) {
  factos_pog.EventCodec(encode:, decode:)
}

fn encode(event: Event) -> factos_pog.Proposed(Event) {
  factos_pog.Proposed(
    id: "event-" <> event.username,
    event: event,
    type_: factos.event_type("UserRegistered"),
    version: 1,
    tags: [factos.tag("username:" <> event.username)],
    metadata: factos.empty_metadata(),
    data: bit_array.from_string(event.username),
  )
}

fn decode(
  stored: factos_pog.StoredEvent,
) -> Result(factos.Decoded(Event), DecodeError) {
  case factos.event_type_name(stored.type_) {
    "UserRegistered" -> {
      use username <- result.try(
        bit_array.to_string(stored.data)
        |> result.replace_error(InvalidData),
      )
      Ok(factos.Decoded(
        event: UserRegistered(username),
        type_: stored.type_,
        version: stored.version,
        tags: stored.tags,
        metadata: stored.metadata,
      ))
    }
    _ -> Error(UnknownEvent)
  }
}

fn dispatch_counter_context_many(
  connection: pog.Connection,
  query: factos.Query,
  remaining: Int,
) -> Result(factos_pog.Append, factos_pog.Error(Nil, DecodeError)) {
  case remaining {
    0 ->
      factos_pog.dispatch_context(
        connection,
        stream: "counter-context-0",
        query: query,
        decider: counter_decider(),
        codec: counter_codec(),
        command: Increment,
      )
    _ -> {
      let stream_name = "counter-context-" <> int.to_string(remaining)
      let result =
        factos_pog.dispatch_context(
          connection,
          stream: stream_name,
          query: query,
          decider: counter_decider(),
          codec: counter_codec(),
          command: Increment,
        )
      case remaining, result {
        1, _ -> result
        _, Ok(_) ->
          dispatch_counter_context_many(connection, query, remaining - 1)
        _, Error(error) -> Error(error)
      }
    }
  }
}

fn counter_decider() -> factos.Decider(
  CounterCommand,
  CounterState,
  CounterEvent,
  Nil,
) {
  factos.decider(
    initial: CounterState(0),
    decide: counter_decide,
    evolve: counter_evolve,
  )
}

fn counter_decide(
  state: CounterState,
  command: CounterCommand,
) -> Result(List(CounterEvent), Nil) {
  let CounterState(total) = state
  case command {
    Increment -> Ok([Incremented(total + 1)])
  }
}

fn counter_evolve(state: CounterState, event: CounterEvent) -> CounterState {
  let CounterState(total) = state
  case event {
    Incremented(_) -> CounterState(total + 1)
  }
}

fn counter_codec() -> factos_pog.EventCodec(CounterEvent, DecodeError) {
  factos_pog.EventCodec(
    encode: encode_counter_event,
    decode: decode_counter_event,
  )
}

fn encode_counter_event(
  event: CounterEvent,
) -> factos_pog.Proposed(CounterEvent) {
  case event {
    Incremented(value) ->
      factos_pog.Proposed(
        id: "counter-event-" <> int.to_string(value),
        event: event,
        type_: factos.event_type("Incremented"),
        version: 1,
        tags: [factos.tag("counter:load")],
        metadata: factos.empty_metadata(),
        data: bit_array.from_string(int.to_string(value)),
      )
  }
}

fn decode_counter_event(
  stored: factos_pog.StoredEvent,
) -> Result(factos.Decoded(CounterEvent), DecodeError) {
  case factos.event_type_name(stored.type_) {
    "Incremented" -> {
      use text <- result.try(
        bit_array.to_string(stored.data)
        |> result.replace_error(InvalidData),
      )
      use value <- result.try(
        int.parse(text)
        |> result.replace_error(InvalidData),
      )
      Ok(factos.Decoded(
        event: Incremented(value),
        type_: stored.type_,
        version: stored.version,
        tags: stored.tags,
        metadata: stored.metadata,
      ))
    }
    _ -> Error(UnknownEvent)
  }
}
