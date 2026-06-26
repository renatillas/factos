import factos
import factos/sqlight as factos_sqlight
import gleam/bit_array
import gleam/int
import gleam/list
import gleam/result
import gleeunit
import sqlight

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

pub fn dispatch_stream_persists_events_test() {
  use connection <- sqlight.with_connection(":memory:")
  let assert Ok(Nil) = factos_sqlight.migrate(connection)

  let assert Ok(factos_sqlight.Append(current_revision: 0, position: _)) =
    factos_sqlight.dispatch_stream(
      connection,
      stream: "user-renata",
      decider: decider(),
      codec: codec(),
      command: RegisterUser("renata"),
    )

  let assert Ok(loaded) =
    factos_sqlight.load_stream(
      connection,
      stream: "user-renata",
      decider: decider(),
      codec: codec(),
    )

  assert loaded.state == Taken
  assert loaded.revision == factos.CurrentRevision(0)
}

pub fn dispatch_stream_handles_many_events_test() {
  use connection <- sqlight.with_connection(":memory:")
  let assert Ok(Nil) = factos_sqlight.migrate(connection)

  let assert Ok(factos_sqlight.Append(
    current_revision: 249,
    position: factos.SequencePosition(_),
  )) = dispatch_counter_stream_many(connection, 250)

  let assert Ok(loaded) =
    factos_sqlight.load_stream(
      connection,
      stream: "counter-load",
      decider: counter_decider(),
      codec: counter_codec(),
    )

  assert loaded.state == CounterState(250)
  assert loaded.revision == factos.CurrentRevision(249)
  assert list.length(loaded.events) == 250
}

pub fn dispatch_context_handles_many_streams_test() {
  use connection <- sqlight.with_connection(":memory:")
  let assert Ok(Nil) = factos_sqlight.migrate(connection)

  let query =
    factos.query([
      factos.query_item(types: [factos.event_type("Incremented")], tags: [
        factos.tag("counter:load"),
      ]),
    ])

  let assert Ok(factos_sqlight.Append(
    current_revision: 0,
    position: factos.SequencePosition(_),
  )) = dispatch_counter_context_many(connection, query, 100)

  let assert Ok(context) =
    factos_sqlight.read_context(
      connection,
      query: query,
      decider: counter_decider(),
      codec: counter_codec(),
    )

  assert context.state == CounterState(100)
  assert list.length(context.events) == 100
  assert context.position != factos.NoPosition
}

fn decider() -> factos.Decider(Command, State, Event, DomainError) {
  factos.decider(initial: Available, decide: decide, evolve: evolve)
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

fn codec() -> factos_sqlight.EventCodec(Event, DecodeError) {
  factos_sqlight.EventCodec(encode: encode, decode: decode_event)
}

fn encode(event: Event) -> factos_sqlight.Proposed(Event) {
  case event {
    UserRegistered(username) ->
      factos_sqlight.Proposed(
        id: "event-" <> username,
        event: event,
        type_: factos.event_type("UserRegistered"),
        tags: [factos.tag("username:" <> username)],
        data: bit_array.from_string(username),
      )
  }
}

fn decode_event(
  stored: factos_sqlight.StoredEvent,
) -> Result(factos.Decoded(Event), DecodeError) {
  case factos.event_type_name(stored.type_) {
    "UserRegistered" -> {
      use username <- result.try(
        bit_array.to_string(stored.data)
        |> result.map_error(fn(_) { InvalidData }),
      )
      Ok(factos.Decoded(
        event: UserRegistered(username),
        type_: stored.type_,
        tags: stored.tags,
      ))
    }
    _ -> Error(UnknownEvent)
  }
}

fn dispatch_counter_stream_many(
  connection: sqlight.Connection,
  remaining: Int,
) -> Result(factos_sqlight.Append, factos_sqlight.Error(Nil, DecodeError)) {
  case remaining {
    0 ->
      factos_sqlight.dispatch_stream(
        connection,
        stream: "counter-load",
        decider: counter_decider(),
        codec: counter_codec(),
        command: Increment,
      )
    _ -> {
      let result =
        factos_sqlight.dispatch_stream(
          connection,
          stream: "counter-load",
          decider: counter_decider(),
          codec: counter_codec(),
          command: Increment,
        )
      case remaining, result {
        1, _ -> result
        _, Ok(_) -> dispatch_counter_stream_many(connection, remaining - 1)
        _, Error(error) -> Error(error)
      }
    }
  }
}

fn dispatch_counter_context_many(
  connection: sqlight.Connection,
  query: factos.Query,
  remaining: Int,
) -> Result(factos_sqlight.Append, factos_sqlight.Error(Nil, DecodeError)) {
  case remaining {
    0 ->
      factos_sqlight.dispatch_context(
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
        factos_sqlight.dispatch_context(
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

fn counter_codec() -> factos_sqlight.EventCodec(CounterEvent, DecodeError) {
  factos_sqlight.EventCodec(
    encode: encode_counter_event,
    decode: decode_counter_event,
  )
}

fn encode_counter_event(
  event: CounterEvent,
) -> factos_sqlight.Proposed(CounterEvent) {
  case event {
    Incremented(value) ->
      factos_sqlight.Proposed(
        id: "counter-event-" <> int.to_string(value),
        event: event,
        type_: factos.event_type("Incremented"),
        tags: [factos.tag("counter:load")],
        data: bit_array.from_string(int.to_string(value)),
      )
  }
}

fn decode_counter_event(
  stored: factos_sqlight.StoredEvent,
) -> Result(factos.Decoded(CounterEvent), DecodeError) {
  case factos.event_type_name(stored.type_) {
    "Incremented" -> {
      use text <- result.try(
        bit_array.to_string(stored.data)
        |> result.map_error(fn(_) { InvalidData }),
      )
      use value <- result.try(
        int.parse(text)
        |> result.map_error(fn(_) { InvalidData }),
      )
      Ok(factos.Decoded(
        event: Incremented(value),
        type_: stored.type_,
        tags: stored.tags,
      ))
    }
    _ -> Error(UnknownEvent)
  }
}
