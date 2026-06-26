import factos
import factos/kurrentdb as factos_kurrentdb
import gleam/bit_array
import gleam/int
import gleam/list
import gleam/option
import gleam/result
import gleeunit
import global_value
import kurrentdb
import kurrentdb/operation/append_to_stream
import kurrentdb/operation/read_stream
import kurrentdb_erlang
import youid/uuid

pub fn main() -> Nil {
  gleeunit.main()
}

const connection_string = "kurrentdb://admin:changeit@localhost:2113?tls=true"

const timeout = 10_000

type CounterCommand {
  Increment
}

type CounterEvent {
  Incremented(value: Int)
}

type CounterState {
  CounterState(total: Int)
}

type DecodeError {
  UnknownEvent
  InvalidData
}

pub fn backend_package_compiles_test() {
  let module_name = "factos/kurrentdb"
  assert module_name == "factos/kurrentdb"
}

pub fn dispatch_stream_handles_many_events_integration_test() {
  let stream_name = unique_name("counter-stream")
  let event_type = unique_name("FactosCounterIncremented")

  let assert Ok(append_to_stream.Append(current_revision: 99, position: _)) =
    dispatch_counter_stream_many(stream_name, event_type, 100)

  let assert Ok(loaded) =
    factos_kurrentdb.load_stream(
      connection(),
      stream: stream_name,
      decider: counter_decider(),
      codec: counter_codec(event_type),
      timeout: timeout,
    )

  assert loaded.state == CounterState(100)
  assert loaded.revision == factos.CurrentRevision(99)
  assert list.length(loaded.events) == 100
}

pub fn read_context_handles_many_streams_integration_test() {
  let event_type = unique_name("FactosCounterContextIncremented")
  let query =
    factos.query([
      factos.query_item(types: [factos.event_type(event_type)], tags: [
        factos.tag("counter:load"),
      ]),
    ])

  let assert Ok(append_to_stream.Append(current_revision: 0, position: _)) =
    dispatch_counter_context_streams_many(event_type, 50)

  let assert Ok(context) =
    factos_kurrentdb.read_context(
      connection(),
      query: query,
      decider: counter_decider(),
      codec: counter_codec(event_type),
      timeout: timeout,
    )

  assert context.state == CounterState(50)
  assert list.length(context.events) == 50
  assert context.position != factos.NoPosition
}

fn connection() -> kurrentdb_erlang.Connection {
  global_value.create_with_unique_name(
    "factos_kurrentdb_erlang.global.data",
    fn() {
      let assert Ok(client) =
        kurrentdb.from_connection_string(connection_string)

      let assert Ok(connection) =
        kurrentdb_erlang.new(client)
        |> kurrentdb_erlang.verify_ca_certificate_file("certs/ca.crt")
        |> kurrentdb_erlang.start(option.None)

      connection
    },
  )
}

fn dispatch_counter_stream_many(
  stream_name: String,
  event_type: String,
  remaining: Int,
) -> Result(append_to_stream.Append, factos_kurrentdb.Error(Nil, DecodeError)) {
  let result =
    factos_kurrentdb.dispatch_stream(
      connection(),
      stream: stream_name,
      decider: counter_decider(),
      codec: counter_codec(event_type),
      command: Increment,
      timeout: timeout,
    )

  case remaining, result {
    1, _ -> result
    _, Ok(_) ->
      dispatch_counter_stream_many(stream_name, event_type, remaining - 1)
    _, Error(error) -> Error(error)
  }
}

fn dispatch_counter_context_streams_many(
  event_type: String,
  remaining: Int,
) -> Result(append_to_stream.Append, factos_kurrentdb.Error(Nil, DecodeError)) {
  let result =
    factos_kurrentdb.dispatch_stream(
      connection(),
      stream: unique_name("counter-context"),
      decider: counter_decider(),
      codec: counter_codec(event_type),
      command: Increment,
      timeout: timeout,
    )

  case remaining, result {
    1, _ -> result
    _, Ok(_) -> dispatch_counter_context_streams_many(event_type, remaining - 1)
    _, Error(error) -> Error(error)
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

fn counter_codec(
  event_type: String,
) -> factos_kurrentdb.EventCodec(CounterEvent, DecodeError) {
  factos_kurrentdb.EventCodec(
    encode: encode_counter_event(_, event_type),
    decode: decode_counter_event(_, event_type),
  )
}

fn encode_counter_event(
  event: CounterEvent,
  event_type: String,
) -> factos_kurrentdb.Proposed(CounterEvent) {
  case event {
    Incremented(value) ->
      factos_kurrentdb.Proposed(
        event: event,
        type_: factos.event_type(event_type),
        tags: [factos.tag("counter:load")],
        message: append_to_stream.binary_event(
          uuid: uuid.v4(),
          type_: event_type,
          data: bit_array.from_string(int.to_string(value)),
        ),
      )
  }
}

fn decode_counter_event(
  stored: read_stream.RecordedEvent,
  event_type: String,
) -> Result(factos.Decoded(CounterEvent), DecodeError) {
  case list.key_find(stored.metadata, "type") {
    Ok(type_name) if type_name == event_type -> {
      use text <- result.try(
        bit_array.to_string(stored.data)
        |> result.map_error(fn(_) { InvalidData }),
      )
      use value <- result.try(
        int.parse(text)
        |> result.map_error(fn(_) { InvalidData }),
      )
      Ok(
        factos.Decoded(
          event: Incremented(value),
          type_: factos.event_type(event_type),
          tags: [factos.tag("counter:load")],
        ),
      )
    }
    Ok(_) | Error(_) -> Error(UnknownEvent)
  }
}

fn unique_name(prefix: String) -> String {
  prefix <> "-" <> uuid.to_string(uuid.v4())
}
