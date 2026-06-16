import gleam/dict.{type Dict}
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import group_registry

pub type Metadata =
  List(#(String, String))

pub type PendingEvent(event) {
  PendingEvent(event_type: String, payload: event)
}

pub type RecordedEvent(event) {
  RecordedEvent(
    global_position: Int,
    stream_id: String,
    stream_position: Int,
    event_type: String,
    payload: event,
    metadata: Metadata,
  )
}

pub type Subscription {
  AllEvents
  StreamEvents(stream_id: String)
}

pub type SubscriberMessage(event) {
  EventsCommitted(events: List(RecordedEvent(event)))
}

pub type HandlerFailure(handler_error) {
  HandlerFailure(handler: String, reason: handler_error)
}

pub type Error(domain_error, store_error) {
  CommandRejected(reason: domain_error)
  EventStoreFailed(reason: store_error)
  NoEventsToCommit
  EventLogTimedOut(operation: String, timeout_ms: Int)
}

pub type CommandHandler(command, event, domain_error) =
  fn(command) -> Result(List(PendingEvent(event)), domain_error)

pub type EventHandler(event, handler_error) =
  fn(List(RecordedEvent(event))) -> Result(Nil, handler_error)

pub type EventStore(store, event, store_error) {
  EventStore(
    store: store,
    append: fn(store, String, List(PendingEvent(event)), Metadata) ->
      Result(#(store, List(RecordedEvent(event))), store_error),
    load_all: fn(store) -> Result(List(RecordedEvent(event)), store_error),
    load_stream: fn(store, String) ->
      Result(List(RecordedEvent(event)), store_error),
  )
}

pub opaque type MemoryStore(event) {
  MemoryStore(
    events: List(RecordedEvent(event)),
    stream_positions: Dict(String, Int),
    next_global_position: Int,
  )
}

pub opaque type EventLog(store, event, store_error) {
  EventLog(event_store: EventStore(store, event, store_error))
}

pub opaque type System(event, store_error) {
  System(
    event_log: process.Subject(Message(event, store_error)),
    registry: group_registry.GroupRegistry(SubscriberMessage(event)),
    timeout: Int,
  )
}

type SystemMessage {
  StopSystem
}

pub type Message(event, store_error) {
  Append(
    stream_id: String,
    events: List(PendingEvent(event)),
    metadata: Metadata,
    reply_to: process.Subject(Result(List(RecordedEvent(event)), store_error)),
  )
  LoadAll(
    reply_to: process.Subject(Result(List(RecordedEvent(event)), store_error)),
  )
  LoadStream(
    stream_id: String,
    reply_to: process.Subject(Result(List(RecordedEvent(event)), store_error)),
  )
  Stop
}

pub type HandlerMessage(event, handler_error) {
  HandleEvents(events: List(RecordedEvent(event)))
  GetHandlerFailures(
    reply_to: process.Subject(List(HandlerFailure(handler_error))),
  )
  StopHandler
}

pub const default_timeout: Int = 100

pub fn memory_store() -> EventStore(MemoryStore(event), event, Nil) {
  EventStore(
    store: MemoryStore(
      events: [],
      stream_positions: dict.new(),
      next_global_position: 1,
    ),
    append: append_to_memory_store,
    load_all: fn(store) { Ok(store.events) },
    load_stream: fn(store, stream_id) {
      Ok(list.filter(store.events, fn(event) { event.stream_id == stream_id }))
    },
  )
}

pub fn start(
  registry_name: process.Name(group_registry.Message(SubscriberMessage(event))),
  timeout timeout_value: Int,
  event_store event_store: EventStore(store, event, store_error),
) -> actor.StartResult(System(event, store_error)) {
  actor.new_with_initialiser(default_timeout, fn(_subject) {
    use registry <- result.try(
      group_registry.start(registry_name)
      |> result.replace_error("CQRS registry could not be started"),
    )
    use event_log <- result.try(
      start_event_log(event_store)
      |> result.replace_error("CQRS event log could not be started"),
    )

    actor.initialised(Nil)
    |> actor.returning(System(
      event_log: event_log.data,
      registry: registry.data,
      timeout: timeout_value,
    ))
    |> Ok
  })
  |> actor.on_message(handle_system_message)
  |> actor.start
}

pub fn supervised(
  registry_name registry_name: process.Name(
    group_registry.Message(SubscriberMessage(event)),
  ),
  timeout timeout_value: Int,
  event_store event_store: EventStore(store, event, store_error),
) -> supervision.ChildSpecification(System(event, store_error)) {
  supervision.worker(fn() {
    start(registry_name, timeout: timeout_value, event_store: event_store)
  })
}

fn start_event_log(
  event_store: EventStore(store, event, store_error),
) -> actor.StartResult(process.Subject(Message(event, store_error))) {
  actor.new(EventLog(event_store: event_store))
  |> actor.on_message(handle_message)
  |> actor.start
}

fn handle_system_message(
  _state: Nil,
  message: SystemMessage,
) -> actor.Next(Nil, SystemMessage) {
  case message {
    StopSystem -> actor.stop()
  }
}

pub fn subscribe(
  system: System(event, store_error),
  subscription: Subscription,
) -> process.Subject(SubscriberMessage(event)) {
  group_registry.join(system.registry, group_name(subscription), process.self())
}

pub fn start_handler(
  system: System(event, store_error),
  name: String,
  subscription: Subscription,
  handler: EventHandler(event, handler_error),
) -> actor.StartResult(process.Subject(HandlerMessage(event, handler_error))) {
  actor.new_with_initialiser(default_timeout, fn(subject) {
    let events =
      group_registry.join(
        system.registry,
        group_name(subscription),
        process.self(),
      )

    actor.initialised([])
    |> actor.returning(subject)
    |> actor.selecting(
      process.new_selector()
      |> process.select(subject)
      |> process.select_map(events, fn(message) {
        case message {
          EventsCommitted(events) -> HandleEvents(events)
        }
      }),
    )
    |> Ok
  })
  |> actor.on_message(handle_handler_message(name, handler))
  |> actor.start
}

pub fn handler_failures(
  handler: process.Subject(HandlerMessage(event, handler_error)),
  timeout timeout_value: Int,
) -> Result(List(HandlerFailure(handler_error)), Nil) {
  let reply_to = process.new_subject()
  process.send(handler, GetHandlerFailures(reply_to))
  process.receive(reply_to, timeout_value)
}

pub fn stop_handler(
  handler: process.Subject(HandlerMessage(event, handler_error)),
) -> Nil {
  process.send(handler, StopHandler)
}

@internal
pub fn append(
  system: System(event, store_error),
  stream_id: String,
  events: List(PendingEvent(event)),
  metadata: Metadata,
) -> Result(List(RecordedEvent(event)), Error(domain_error, store_error)) {
  case events {
    [] -> Error(NoEventsToCommit)
    [_, ..] -> {
      let reply_to = process.new_subject()
      process.send(
        system.event_log,
        Append(stream_id, events, metadata, reply_to),
      )

      case process.receive(reply_to, system.timeout) {
        Ok(Ok(recorded_events)) -> {
          publish(system, recorded_events)
          Ok(recorded_events)
        }
        Ok(Error(reason)) -> Error(EventStoreFailed(reason))
        Error(Nil) -> Error(EventLogTimedOut("append", system.timeout))
      }
    }
  }
}

pub fn execute(
  system: System(event, store_error),
  stream_id: String,
  command: command,
  metadata: Metadata,
  handle: CommandHandler(command, event, domain_error),
) -> Result(List(RecordedEvent(event)), Error(domain_error, store_error)) {
  case handle(command) {
    Ok(events) -> append(system, stream_id, events, metadata)
    Error(reason) -> Error(CommandRejected(reason))
  }
}

pub fn load_all(
  system: System(event, store_error),
) -> Result(List(RecordedEvent(event)), Error(domain_error, store_error)) {
  let reply_to = process.new_subject()
  process.send(system.event_log, LoadAll(reply_to))

  case process.receive(reply_to, system.timeout) {
    Ok(Ok(events)) -> Ok(events)
    Ok(Error(reason)) -> Error(EventStoreFailed(reason))
    Error(Nil) -> Error(EventLogTimedOut("load_all", system.timeout))
  }
}

pub fn load_stream(
  system: System(event, store_error),
  stream_id: String,
) -> Result(List(RecordedEvent(event)), Error(domain_error, store_error)) {
  let reply_to = process.new_subject()
  process.send(system.event_log, LoadStream(stream_id, reply_to))

  case process.receive(reply_to, system.timeout) {
    Ok(Ok(events)) -> Ok(events)
    Ok(Error(reason)) -> Error(EventStoreFailed(reason))
    Error(Nil) -> Error(EventLogTimedOut("load_stream", system.timeout))
  }
}

pub fn stop(system: System(event, store_error)) -> Nil {
  process.send(system.event_log, Stop)
}

fn handle_message(
  event_log: EventLog(store, event, store_error),
  message: Message(event, store_error),
) -> actor.Next(
  EventLog(store, event, store_error),
  Message(event, store_error),
) {
  case message {
    Append(_stream_id, [], _metadata, reply_to) -> {
      process.send(reply_to, Ok([]))
      actor.continue(event_log)
    }
    Append(stream_id, [_, ..] as events, metadata, reply_to) -> {
      case
        event_log.event_store.append(
          event_log.event_store.store,
          stream_id,
          events,
          metadata,
        )
      {
        Ok(#(store, recorded_events)) -> {
          let event_store = EventStore(..event_log.event_store, store: store)
          process.send(reply_to, Ok(recorded_events))
          actor.continue(EventLog(event_store: event_store))
        }
        Error(reason) -> {
          process.send(reply_to, Error(reason))
          actor.continue(event_log)
        }
      }
    }
    LoadAll(reply_to) -> {
      let events = event_log.event_store.load_all(event_log.event_store.store)
      process.send(reply_to, events)
      actor.continue(event_log)
    }
    LoadStream(stream_id, reply_to) -> {
      let events =
        event_log.event_store.load_stream(
          event_log.event_store.store,
          stream_id,
        )
      process.send(reply_to, events)
      actor.continue(event_log)
    }
    Stop -> actor.stop()
  }
}

fn handle_handler_message(
  name: String,
  handler: EventHandler(event, handler_error),
) -> fn(
  List(HandlerFailure(handler_error)),
  HandlerMessage(event, handler_error),
) ->
  actor.Next(
    List(HandlerFailure(handler_error)),
    HandlerMessage(event, handler_error),
  ) {
  fn(failures, message) {
    case message {
      HandleEvents(events) -> {
        case handler(events) {
          Ok(Nil) -> actor.continue(failures)
          Error(reason) ->
            actor.continue([
              HandlerFailure(handler: name, reason: reason),
              ..failures
            ])
        }
      }
      GetHandlerFailures(reply_to) -> {
        process.send(reply_to, list.reverse(failures))
        actor.continue(failures)
      }
      StopHandler -> actor.stop()
    }
  }
}

fn append_to_memory_store(
  memory_store: MemoryStore(event),
  stream_id: String,
  events: List(PendingEvent(event)),
  metadata: Metadata,
) -> Result(#(MemoryStore(event), List(RecordedEvent(event))), Nil) {
  let stream_position =
    memory_store.stream_positions
    |> dict.get(stream_id)
    |> result.unwrap(0)

  let #(recorded_events, next_global_position, next_stream_position) =
    record_events(
      events,
      stream_id,
      metadata,
      memory_store.next_global_position,
      stream_position,
    )

  let memory_store =
    MemoryStore(
      events: list.append(memory_store.events, recorded_events),
      stream_positions: dict.insert(
        memory_store.stream_positions,
        stream_id,
        next_stream_position,
      ),
      next_global_position: next_global_position,
    )

  Ok(#(memory_store, recorded_events))
}

fn record_events(
  events: List(PendingEvent(event)),
  stream_id: String,
  metadata: Metadata,
  next_global_position: Int,
  stream_position: Int,
) -> #(List(RecordedEvent(event)), Int, Int) {
  let #(recorded_events, next_global_position, next_stream_position) =
    list.fold(
      events,
      #([], next_global_position, stream_position),
      fn(accumulator, pending_event) {
        let #(recorded_events, global_position, stream_position) = accumulator
        let PendingEvent(event_type, payload) = pending_event
        let stream_position = stream_position + 1
        let recorded_event =
          RecordedEvent(
            global_position: global_position,
            stream_id: stream_id,
            stream_position: stream_position,
            event_type: event_type,
            payload: payload,
            metadata: metadata,
          )

        #(
          [recorded_event, ..recorded_events],
          global_position + 1,
          stream_position,
        )
      },
    )

  #(list.reverse(recorded_events), next_global_position, next_stream_position)
}

fn publish(
  system: System(event, store_error),
  events: List(RecordedEvent(event)),
) -> Nil {
  case events {
    [] -> Nil
    [first_event, ..] -> {
      notify(system, AllEvents, events)
      notify(system, StreamEvents(first_event.stream_id), events)
    }
  }
}

fn notify(
  system: System(event, store_error),
  subscription: Subscription,
  events: List(RecordedEvent(event)),
) -> Nil {
  group_registry.members(system.registry, group_name(subscription))
  |> list.each(fn(subscriber) {
    process.send(subscriber, EventsCommitted(events))
  })
}

fn group_name(subscription: Subscription) -> String {
  case subscription {
    AllEvents -> "cqrs$all"
    StreamEvents(stream_id) -> "cqrs$stream:" <> stream_id
  }
}

pub fn recorded_event_to_string(event: RecordedEvent(a)) -> String {
  int.to_string(event.global_position)
  <> "@"
  <> event.stream_id
  <> ":"
  <> int.to_string(event.stream_position)
  <> "/"
  <> event.event_type
}
