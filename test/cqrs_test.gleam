import cqrs
import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/result
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

type Command {
  RegisterUser(id: String, email: String)
  RejectRegistration(reason: String)
}

type Event {
  UserRegistered(id: String, email: String)
}

type DomainError {
  RegistrationRejected(reason: String)
}

pub fn execute_maps_command_to_events_test() {
  let assert Ok(started) = start_system()
  let system = started.data

  let assert Ok(events) =
    cqrs.execute(
      system,
      "user-2",
      RegisterUser("user-2", "b@c.d"),
      [],
      handle_command,
    )

  assert events
    == [
      cqrs.RecordedEvent(
        global_position: 1,
        stream_id: "user-2",
        stream_position: 1,
        event_type: "user_registered",
        payload: UserRegistered("user-2", "b@c.d"),
        metadata: [],
      ),
    ]

  cqrs.stop(system)
}

pub fn execute_returns_domain_error_without_committing_test() {
  let assert Ok(started) = start_system()
  let system = started.data

  let assert Error(cqrs.CommandRejected(RegistrationRejected("closed"))) =
    cqrs.execute(
      system,
      "user-3",
      RejectRegistration("closed"),
      [],
      handle_command,
    )

  let assert Ok(events) = cqrs.load_all(system)
  assert events == []
  cqrs.stop(system)
}

pub fn load_stream_filters_recorded_events_test() {
  let assert Ok(started) = start_system()
  let system = started.data

  let assert Ok(_) =
    cqrs.append(
      system,
      "user-4",
      [
        cqrs.PendingEvent("user_registered", UserRegistered("user-4", "d@e.f")),
      ],
      [],
    )

  let assert Ok(_) =
    cqrs.append(
      system,
      "user-5",
      [
        cqrs.PendingEvent("user_registered", UserRegistered("user-5", "e@f.g")),
      ],
      [],
    )

  let assert Ok(events) = cqrs.load_stream(system, "user-5")

  assert events
    == [
      cqrs.RecordedEvent(
        global_position: 2,
        stream_id: "user-5",
        stream_position: 1,
        event_type: "user_registered",
        payload: UserRegistered("user-5", "e@f.g"),
        metadata: [],
      ),
    ]

  cqrs.stop(system)
}

pub fn subscribers_receive_committed_events_test() {
  let assert Ok(started) = start_system()
  let system = started.data
  let all_events = cqrs.subscribe(system, cqrs.AllEvents)
  let stream_events = cqrs.subscribe(system, cqrs.StreamEvents("user-6"))

  let assert Ok(committed_events) =
    cqrs.append(
      system,
      "user-6",
      [cqrs.PendingEvent("user_registered", UserRegistered("user-6", "f@g.h"))],
      [],
    )

  assert process.receive(all_events, 1000)
    == Ok(cqrs.EventsCommitted(committed_events))
  assert process.receive(stream_events, 1000)
    == Ok(cqrs.EventsCommitted(committed_events))

  cqrs.stop(system)
}

pub fn custom_event_store_is_used_for_persistence_test() {
  let assert Ok(started) =
    cqrs.start(
      process.new_name("cqrs_registry"),
      timeout: cqrs.default_timeout,
      event_store: failing_load_store(),
    )

  let system = started.data

  let assert Ok(_) =
    cqrs.append(
      system,
      "user-7",
      [cqrs.PendingEvent("user_registered", UserRegistered("user-7", "g@h.i"))],
      [],
    )

  let assert Error(cqrs.EventStoreFailed("load_all failed")) =
    cqrs.load_all(system)
  cqrs.stop(system)
}

pub fn started_event_handler_receives_committed_events_test() {
  let assert Ok(started) = start_system()
  let system = started.data
  let received = process.new_subject()
  let assert Ok(handler) =
    cqrs.start_handler(system, "projection", cqrs.AllEvents, fn(events) {
      process.send(received, events)
      Ok(Nil)
    })

  let assert Ok(committed_events) =
    cqrs.append(
      system,
      "user-8",
      [cqrs.PendingEvent("user_registered", UserRegistered("user-8", "h@i.j"))],
      [],
    )

  assert process.receive(received, 1000) == Ok(committed_events)
  cqrs.stop_handler(handler.data)
  cqrs.stop(system)
}

pub fn event_handler_failures_are_observable_test() {
  let assert Ok(started) = start_system()
  let system = started.data
  let assert Ok(handler) =
    cqrs.start_handler(system, "projection", cqrs.AllEvents, fn(_events) {
      Error("projection failed")
    })

  let assert Ok(_) =
    cqrs.append(
      system,
      "user-9",
      [cqrs.PendingEvent("user_registered", UserRegistered("user-9", "i@j.k"))],
      [],
    )

  process.sleep(50)

  assert cqrs.handler_failures(handler.data, timeout: cqrs.default_timeout)
    == Ok([
      cqrs.HandlerFailure(handler: "projection", reason: "projection failed"),
    ])

  cqrs.stop_handler(handler.data)
  cqrs.stop(system)
}

pub fn supervised_system_can_run_under_static_supervisor_test() {
  let spec =
    cqrs.supervised(
      registry_name: process.new_name("cqrs_registry"),
      timeout: cqrs.default_timeout,
      event_store: cqrs.memory_store(),
    )

  let assert Ok(started) = spec.start()
  let system = started.data

  let assert Ok(events) =
    cqrs.append(
      system,
      "supervised-user",
      [
        cqrs.PendingEvent(
          "user_registered",
          UserRegistered("supervised-user", "supervised@example.com"),
        ),
      ],
      [],
    )

  assert events
    == [
      cqrs.RecordedEvent(
        global_position: 1,
        stream_id: "supervised-user",
        stream_position: 1,
        event_type: "user_registered",
        payload: UserRegistered("supervised-user", "supervised@example.com"),
        metadata: [],
      ),
    ]

  let supervised_spec =
    cqrs.supervised(
      registry_name: process.new_name("cqrs_supervised_registry"),
      timeout: cqrs.default_timeout,
      event_store: cqrs.memory_store(),
    )

  let assert Ok(started) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(supervised_spec)
    |> static_supervisor.start

  assert process.is_alive(started.pid)
}

fn start_system() -> actor.StartResult(cqrs.System(Event, Nil)) {
  cqrs.start(
    process.new_name("cqrs_registry"),
    timeout: cqrs.default_timeout,
    event_store: cqrs.memory_store(),
  )
}

fn handle_command(
  command: Command,
) -> Result(List(cqrs.PendingEvent(Event)), DomainError) {
  case command {
    RegisterUser(id, email) ->
      Ok([cqrs.PendingEvent("user_registered", UserRegistered(id, email))])
    RejectRegistration(reason) -> Error(RegistrationRejected(reason))
  }
}

fn failing_load_store() -> cqrs.EventStore(
  cqrs.MemoryStore(Event),
  Event,
  String,
) {
  let memory_store = cqrs.memory_store()

  cqrs.EventStore(
    store: memory_store.store,
    append: fn(store, stream_id, events, metadata) {
      memory_store.append(store, stream_id, events, metadata)
      |> result.map_error(fn(_reason) { "append failed" })
    },
    load_all: fn(_store) { Error("load_all failed") },
    load_stream: fn(_store, _stream_id) { Error("load_stream failed") },
  )
}
