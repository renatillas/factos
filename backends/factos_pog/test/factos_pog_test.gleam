import factos
import factos/factos_pog
import gleam/bit_array
import gleam/erlang/application
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import gleeunit
import pog
import simplifile
import testcontainer
import testcontainer/error as testcontainer_error
import testcontainer_formulas/postgres

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

type CounterCommand {
  Increment
}

type CounterEvent {
  Incremented(value: Int)
}

type CounterState {
  CounterState(total: Int)
}

pub type Timeout(a) {
  Timeout(time: Int, function: fn() -> a)
}

pub fn dispatch_alias_persists_events_test_() -> Timeout(Nil) {
  use <- Timeout(120)
  let assert Ok(Nil) =
    with_test_connection(fn(connection) {
      reset_schema(connection)

      let assert Ok(dispatch) =
        factos_pog.dispatch(
          connection,
          stream: "user-renata",
          decider: decider(),
          codec: codec(),
          command: RegisterUser("renata"),
        )

      let assert factos_pog.Append(
        current_revision: 0,
        position: factos.SequencePosition(_),
      ) = dispatch.append
      let assert [recorded] = dispatch.events
      assert_user_recorded(
        recorded,
        stream: "user-renata",
        revision: 0,
        position: dispatch.append.position,
        username: "renata",
      )
      let reactor = factos.reactor(react: fn(recorded) { [recorded.event] })
      assert factos.react_all(reactor: reactor, events: dispatch.events)
        == [
          UserRegistered("renata"),
        ]

      let assert Ok(loaded) =
        factos_pog.load_stream(
          connection,
          stream: "user-renata",
          decider: decider(),
          codec: codec(),
        )

      assert loaded.state == Taken
      assert loaded.revision == factos.CurrentRevision(0)
      Nil
    })
  Nil
}

pub fn dispatch_with_query_filters_before_decoding_unknown_events_test_() -> Timeout(
  Nil,
) {
  use <- Timeout(120)
  let assert Ok(Nil) =
    with_test_connection(fn(connection) {
      reset_schema(connection)
      insert_unknown_event(connection)

      let query = username_query("renata")

      let assert Ok(dispatch) =
        factos_pog.dispatch_with_query(
          connection,
          stream: "user-renata",
          query: query,
          decider: decider(),
          codec: codec(),
          command: RegisterUser("renata"),
        )

      let assert factos_pog.Append(
        current_revision: 0,
        position: factos.SequencePosition(_),
      ) = dispatch.append
      let assert [recorded] = dispatch.events
      assert_user_recorded(
        recorded,
        stream: "user-renata",
        revision: 0,
        position: dispatch.append.position,
        username: "renata",
      )
      let reactor = factos.reactor(react: fn(recorded) { [recorded.event] })
      assert factos.react_all(reactor: reactor, events: dispatch.events)
        == [
          UserRegistered("renata"),
        ]

      let assert Ok(context) =
        factos_pog.read_context(
          connection,
          query: query,
          decider: decider(),
          codec: codec(),
        )

      let assert [event] = context.events
      assert event.event == UserRegistered("renata")
      assert context.state == Taken
      assert context.position != factos.NoPosition
      Nil
    })
  Nil
}

pub fn dispatch_with_query_handles_many_streams_test_() -> Timeout(Nil) {
  use <- Timeout(120)
  let assert Ok(Nil) =
    with_test_connection(fn(connection) {
      reset_schema(connection)

      let query =
        factos.query([
          factos.query_item(types: [factos.event_type("Incremented")], tags: [
            factos.tag("counter:load"),
          ]),
        ])

      let assert Ok(dispatch) =
        dispatch_counter_context_many(connection, query, 25)
      let assert factos_pog.Append(
        current_revision: 0,
        position: factos.SequencePosition(_),
      ) = dispatch.append
      let assert [recorded] = dispatch.events
      assert_counter_recorded(
        recorded,
        stream: "counter-context-1",
        revision: 0,
        position: dispatch.append.position,
        value: 25,
        type_: factos.event_type("Incremented"),
      )
      let reactor = factos.reactor(react: fn(recorded) { [recorded.event] })
      assert factos.react_all(reactor: reactor, events: dispatch.events)
        == [
          Incremented(25),
        ]

      let assert Ok(context) =
        factos_pog.read_context(
          connection,
          query: query,
          decider: counter_decider(),
          codec: counter_codec(),
        )

      assert context.state == CounterState(25)
      assert list.length(context.events) == 25
      Nil
    })
  Nil
}

fn with_test_connection(
  body: fn(pog.Connection) -> Nil,
) -> Result(Nil, testcontainer_error.Error) {
  use postgres_container <- testcontainer.with_formula(
    postgres.new() |> postgres.formula(),
  )
  let #(pool_pid, connection) = start_test_connection(postgres_container)
  body(connection)
  process.send_exit(pool_pid)
  process.sleep(100)
  Ok(Nil)
}

fn start_test_connection(
  postgres_container: postgres.PostgresContainer,
) -> #(process.Pid, pog.Connection) {
  let postgres.PostgresContainer(host:, port:, database:, username:, ..) =
    postgres_container
  let pool_name = process.new_name("factos_pog_test")
  let config =
    pog.default_config(pool_name)
    |> pog.host(host)
    |> pog.port(port)
    |> pog.database(database)
    |> pog.user(username)
    |> pog.password(Some("postgres"))
    |> pog.ssl(pog.SslDisabled)

  let assert Ok(actor.Started(pid:, ..)) = pog.start(config)
  process.sleep(100)
  #(pid, pog.named_connection(pool_name))
}

fn reset_schema(connection: pog.Connection) -> Nil {
  let assert Ok(_) =
    pog.query("drop table if exists factos_event_tags")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("drop table if exists factos_events")
    |> pog.execute(on: connection)
  execute_migration_file(connection)
}

fn execute_migration_file(connection: pog.Connection) -> Nil {
  let assert Ok(priv_directory) = application.priv_directory("factos_pog")
  let assert Ok(sql) = simplifile.read(priv_directory <> "/migrations.sql")

  sql
  |> string.split(";")
  |> list.each(fn(statement) {
    case string.trim(statement) {
      "" -> Nil
      statement -> {
        let assert Ok(_) = pog.query(statement) |> pog.execute(on: connection)
        Nil
      }
    }
  })
}

fn insert_unknown_event(connection: pog.Connection) -> Nil {
  let tag = "username:intruder"
  let assert Ok(_) =
    pog.query(
      "
      with inserted as (
        insert into factos_events (id, stream, revision, type, version, tags, metadata, data)
        values ($1, $2, $3, $4, $5, $6, $7, $8)
        returning position
      )
      insert into factos_event_tags(position, tag)
      select position, $9 from inserted
      ",
    )
    |> pog.parameter(pog.text("event-unknown"))
    |> pog.parameter(pog.text("unknown-stream"))
    |> pog.parameter(pog.int(0))
    |> pog.parameter(pog.text("UnknownEventType"))
    |> pog.parameter(pog.int(1))
    |> pog.parameter(pog.text("\n" <> tag <> "\n"))
    |> pog.parameter(pog.text(""))
    |> pog.parameter(pog.bytea(bit_array.from_string("unknown")))
    |> pog.parameter(pog.text(tag))
    |> pog.execute(on: connection)
  Nil
}

fn username_query(username: String) -> factos.Query {
  factos.query([
    factos.query_item(types: [factos.event_type("UserRegistered")], tags: [
      factos.tag("username:" <> username),
    ]),
  ])
}

fn assert_user_recorded(
  recorded: factos.Recorded(Event),
  stream stream_name: String,
  revision revision: Int,
  position position: factos.SequencePosition,
  username username: String,
) -> Nil {
  assert recorded.id == "event-" <> username
  assert recorded.stream == stream_name
  assert recorded.revision == revision
  assert recorded.position == position
  assert recorded.type_ == factos.event_type("UserRegistered")
  assert recorded.version == 1
  assert recorded.tags == [factos.tag("username:" <> username)]
  assert recorded.metadata == factos.empty_metadata()
  assert recorded.event == UserRegistered(username)
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

fn codec() -> factos_pog.EventCodec(Event) {
  factos_pog.codec(encode:, decode:)
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
) -> Result(factos.Decoded(Event), factos_pog.DecodeError) {
  case factos.event_type_name(stored.type_) {
    "UserRegistered" -> {
      use username <- result.try(
        bit_array.to_string(stored.data)
        |> result.replace_error(factos_pog.InvalidData),
      )
      Ok(factos.Decoded(
        event: UserRegistered(username),
        type_: stored.type_,
        version: stored.version,
        tags: stored.tags,
        metadata: stored.metadata,
      ))
    }
    _ -> Error(factos_pog.UnknownEvent)
  }
}

fn dispatch_counter_context_many(
  connection: pog.Connection,
  query: factos.Query,
  remaining: Int,
) -> Result(factos_pog.Dispatch(CounterEvent), factos_pog.Error(Nil)) {
  case remaining {
    0 ->
      factos_pog.dispatch_with_query(
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
        factos_pog.dispatch_with_query(
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

fn counter_codec() -> factos_pog.EventCodec(CounterEvent) {
  factos_pog.codec(encode: encode_counter_event, decode: decode_counter_event)
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
) -> Result(factos.Decoded(CounterEvent), factos_pog.DecodeError) {
  case factos.event_type_name(stored.type_) {
    "Incremented" -> {
      use text <- result.try(
        bit_array.to_string(stored.data)
        |> result.replace_error(factos_pog.InvalidData),
      )
      use value <- result.try(
        int.parse(text)
        |> result.replace_error(factos_pog.InvalidData),
      )
      Ok(factos.Decoded(
        event: Incremented(value),
        type_: stored.type_,
        version: stored.version,
        tags: stored.tags,
        metadata: stored.metadata,
      ))
    }
    _ -> Error(factos_pog.UnknownEvent)
  }
}

fn assert_counter_recorded(
  recorded: factos.Recorded(CounterEvent),
  stream stream_name: String,
  revision revision: Int,
  position position: factos.SequencePosition,
  value value: Int,
  type_ type_: factos.EventType,
) -> Nil {
  assert recorded.id == "counter-event-" <> int.to_string(value)
  assert recorded.stream == stream_name
  assert recorded.revision == revision
  assert recorded.position == position
  assert recorded.type_ == type_
  assert recorded.version == 1
  assert recorded.tags == [factos.tag("counter:load")]
  assert recorded.metadata == factos.empty_metadata()
  assert recorded.event == Incremented(value)
}
