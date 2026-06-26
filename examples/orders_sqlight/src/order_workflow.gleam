import factos
import factos/factos_sqlight
import gleam/bit_array
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/result
import gleam/string
import simplifile
import sqlight

const order_count = 40

const concurrency = 8

const receive_timeout = 30_000

const write_retries = 200

pub type Command {
  OpenOrder(table: Int)
  AddItem(sku: String, name: String, price: Int)
  RemoveItem(sku: String)
  SubmitOrder
  StartPreparing
  MarkReady
  Serve
  Pay(amount: Int)
  CancelOrder(reason: String)
}

pub type Event {
  OrderOpened(table: Int)
  ItemAdded(sku: String, name: String, price: Int)
  ItemRemoved(sku: String)
  OrderSubmitted
  PreparationStarted
  OrderMarkedReady
  OrderServed
  PaymentReceived(amount: Int)
  OrderCancelled(reason: String)
}

pub type Item {
  Item(sku: String, name: String, price: Int)
}

pub type State {
  NoOrder
  Draft(table: Int, items: List(Item))
  Submitted(table: Int, items: List(Item))
  Preparing(table: Int, items: List(Item))
  Ready(table: Int, items: List(Item))
  Served(table: Int, items: List(Item))
  Paid(table: Int, items: List(Item), amount: Int)
  Cancelled(reason: String)
}

pub type DomainError {
  OrderAlreadyOpen
  OrderNotOpen
  DuplicateItem(sku: String)
  ItemNotFound(sku: String)
  EmptyOrder
  PaymentTooLow(required: Int, paid: Int)
  WorkerTimedOut(remaining: Int)
  InvalidTransition(action: String, state: String)
}

pub type DecodeError {
  UnknownEventType(String)
  InvalidPayload(String)
}

pub type KitchenSummary {
  KitchenSummary(
    opened: Int,
    submitted: Int,
    preparing: Int,
    ready: Int,
    served: Int,
    paid: Int,
    cancelled: Int,
    revenue: Int,
  )
}

pub type ExampleResult {
  ExampleResult(
    order_id: String,
    final_state: State,
    kitchen_summary: KitchenSummary,
    recorded_events: Int,
  )
}

pub type StressResult {
  StressResult(
    orders: Int,
    paid_orders: Int,
    cancelled_orders: Int,
    recorded_events: Int,
    revenue: Int,
  )
}

type WorkerResult {
  WorkerResult(final_state: State, recorded_events: Int, revenue: Int)
}

type WorkerMessage {
  WorkerFinished(
    order_number: Int,
    result: Result(WorkerResult, factos_sqlight.Error(DomainError, DecodeError)),
  )
}

pub fn main() -> Nil {
  case run() {
    Ok(result) ->
      io.println(
        "restaurant stress workflow completed: "
        <> int.to_string(result.orders)
        <> " orders, "
        <> int.to_string(result.recorded_events)
        <> " events",
      )
    Error(_) -> io.println("restaurant stress workflow failed")
  }
}

pub fn run() -> Result(
  StressResult,
  factos_sqlight.Error(DomainError, DecodeError),
) {
  let database_path = "/tmp/factos_examples_stress.sqlite3"
  log("reset database " <> database_path)
  let _ = simplifile.delete_file(database_path)

  log("prepare database")
  use _ <- result.try(prepare_database(database_path))

  let workers = process.new_subject()

  let _ =
    int.range(from: 1, to: concurrency + 1, with: Nil, run: fn(_, order_number) {
      spawn_order(workers, database_path, order_number)
      Nil
    })

  collect_workers(
    workers,
    database_path: database_path,
    remaining: order_count,
    next_order: concurrency + 1,
    summary: StressResult(0, 0, 0, 0, 0),
  )
}

fn spawn_order(
  workers: process.Subject(WorkerMessage),
  database_path: String,
  order_number: Int,
) -> Nil {
  log("spawn order " <> int.to_string(order_number))
  let _ =
    process.spawn(fn() {
      log("order " <> int.to_string(order_number) <> " started")
      let result = run_order(database_path, order_number)
      log(
        "order "
        <> int.to_string(order_number)
        <> " finished with "
        <> result_to_string(result),
      )
      process.send(workers, WorkerFinished(order_number, result))
    })
  Nil
}

fn prepare_database(
  database_path: String,
) -> Result(Nil, factos_sqlight.Error(DomainError, DecodeError)) {
  use connection <- sqlight.with_connection(database_path)
  use _ <- result.try(configure_connection(connection))
  factos_sqlight.migrate(connection)
}

fn configure_connection(
  connection: sqlight.Connection,
) -> Result(Nil, factos_sqlight.Error(DomainError, DecodeError)) {
  sqlight.exec(
    "pragma journal_mode = wal; pragma busy_timeout = 50",
    on: connection,
  )
  |> result.map_error(factos_sqlight.StoreError)
}

fn run_order(
  database_path: String,
  order_number: Int,
) -> Result(WorkerResult, factos_sqlight.Error(DomainError, DecodeError)) {
  use connection <- sqlight.with_connection(database_path)
  use _ <- result.try(configure_connection(connection))

  let order_id = "stress-" <> int.to_string(order_number)
  use _ <- result.try(dispatch_commands(
    connection,
    order_number,
    order_id,
    workflow(order_number),
  ))

  log("order " <> int.to_string(order_number) <> " loading stream")
  use loaded <- result.try(factos_sqlight.load_stream(
    connection,
    stream: order_stream(order_id),
    decider: order_decider(),
    codec: order_codec(),
  ))

  Ok(WorkerResult(
    final_state: loaded.state,
    recorded_events: list.length(loaded.events),
    revenue: revenue(loaded.state),
  ))
}

fn collect_workers(
  workers: process.Subject(WorkerMessage),
  database_path database_path: String,
  remaining remaining: Int,
  next_order next_order: Int,
  summary summary: StressResult,
) -> Result(StressResult, factos_sqlight.Error(DomainError, DecodeError)) {
  case remaining {
    0 -> Ok(summary)
    _ ->
      case process.receive(workers, within: receive_timeout) {
        Ok(WorkerFinished(order_number, Ok(result))) -> {
          log("collector received order " <> int.to_string(order_number))
          case next_order <= order_count {
            True -> spawn_order(workers, database_path, next_order)
            False -> Nil
          }
          collect_workers(
            workers,
            database_path: database_path,
            remaining: remaining - 1,
            next_order: next_order + 1,
            summary: add_worker_result(summary, result),
          )
        }
        Ok(WorkerFinished(order_number, Error(error))) -> {
          log(
            "collector received error from order "
            <> int.to_string(order_number)
            <> ": "
            <> store_error_to_string(error),
          )
          Error(error)
        }
        Error(Nil) -> {
          log(
            "collector timed out with "
            <> int.to_string(remaining)
            <> " remaining",
          )
          Error(factos_sqlight.DomainError(WorkerTimedOut(remaining)))
        }
      }
  }
}

fn add_worker_result(
  summary: StressResult,
  result: WorkerResult,
) -> StressResult {
  let #(paid_orders, cancelled_orders) = case result.final_state {
    Paid(_, _, _) -> #(summary.paid_orders + 1, summary.cancelled_orders)
    Cancelled(_) -> #(summary.paid_orders, summary.cancelled_orders + 1)
    NoOrder
    | Draft(_, _)
    | Submitted(_, _)
    | Preparing(_, _)
    | Ready(_, _)
    | Served(_, _) -> #(summary.paid_orders, summary.cancelled_orders)
  }

  StressResult(
    orders: summary.orders + 1,
    paid_orders: paid_orders,
    cancelled_orders: cancelled_orders,
    recorded_events: summary.recorded_events + result.recorded_events,
    revenue: summary.revenue + result.revenue,
  )
}

fn workflow(order_number: Int) -> List(Command) {
  let draft_commands = [
    OpenOrder(table: order_number),
    AddItem(sku: "burger", name: "House Burger", price: 16),
    AddItem(sku: "fries", name: "Fries", price: 6),
    AddItem(sku: "shake", name: "Vanilla Shake", price: 8),
    RemoveItem(sku: "shake"),
    SubmitOrder,
  ]

  case should_cancel(order_number) {
    True ->
      list.append(draft_commands, [
        CancelOrder(reason: "guest left before kitchen started"),
      ])
    False ->
      list.append(draft_commands, [
        StartPreparing,
        MarkReady,
        Serve,
        Pay(amount: 25),
      ])
  }
}

fn should_cancel(order_number: Int) -> Bool {
  int.modulo(order_number, by: 5) == Ok(0)
}

fn dispatch_commands(
  connection: sqlight.Connection,
  order_number: Int,
  order_id: String,
  commands: List(Command),
) -> Result(Nil, factos_sqlight.Error(DomainError, DecodeError)) {
  case commands {
    [] -> Ok(Nil)
    [command, ..rest] -> {
      log(
        "order "
        <> int.to_string(order_number)
        <> " dispatch "
        <> command_to_string(command),
      )
      use _ <- result.try(dispatch_with_retry(
        connection,
        order_number,
        order_id,
        command,
        attempts: write_retries,
      ))
      dispatch_commands(connection, order_number, order_id, rest)
    }
  }
}

fn dispatch_with_retry(
  connection: sqlight.Connection,
  order_number: Int,
  order_id: String,
  command: Command,
  attempts attempts: Int,
) -> Result(
  factos_sqlight.Append,
  factos_sqlight.Error(DomainError, DecodeError),
) {
  let result = dispatch(connection, order_id, command)

  case attempts > 0, result {
    _, Ok(append) -> Ok(append)
    True, Error(factos_sqlight.StoreError(_)) -> {
      log(
        "order "
        <> int.to_string(order_number)
        <> " retry "
        <> command_to_string(command)
        <> " after SQLite store error; attempts left "
        <> int.to_string(attempts - 1),
      )
      process.sleep(retry_delay(order_number, attempts))
      dispatch_with_retry(
        connection,
        order_number,
        order_id,
        command,
        attempts: attempts - 1,
      )
    }
    _, Error(error) -> {
      log(
        "order "
        <> int.to_string(order_number)
        <> " failed "
        <> command_to_string(command)
        <> " with "
        <> store_error_to_string(error),
      )
      Error(error)
    }
  }
}

fn retry_delay(order_number: Int, attempts: Int) -> Int {
  case int.modulo(order_number + attempts, by: 10) {
    Ok(offset) -> 5 + offset
    Error(Nil) -> 5
  }
}

fn dispatch(
  connection: sqlight.Connection,
  order_id: String,
  command: Command,
) -> Result(
  factos_sqlight.Append,
  factos_sqlight.Error(DomainError, DecodeError),
) {
  factos_sqlight.dispatch_stream(
    connection,
    stream: order_stream(order_id),
    decider: order_decider(),
    codec: order_codec(),
    command:,
  )
}

fn order_stream(order_id: String) -> String {
  "restaurant-order-" <> order_id
}

fn revenue(state: State) -> Int {
  case state {
    Paid(_, _, amount) -> amount
    NoOrder
    | Draft(_, _)
    | Submitted(_, _)
    | Preparing(_, _)
    | Ready(_, _)
    | Served(_, _)
    | Cancelled(_) -> 0
  }
}

pub fn order_decider() -> factos.Decider(Command, State, Event, DomainError) {
  factos.decider(initial: NoOrder, decide:, evolve:)
}

fn decide(state: State, command: Command) -> Result(List(Event), DomainError) {
  case state, command {
    NoOrder, OpenOrder(table) -> Ok([OrderOpened(table)])
    NoOrder, _ -> Error(OrderNotOpen)

    Draft(_, _), OpenOrder(_) -> Error(OrderAlreadyOpen)
    Draft(_, items), AddItem(sku, name, price) ->
      case has_item(items, sku) {
        True -> Error(DuplicateItem(sku))
        False -> Ok([ItemAdded(sku, name, price)])
      }
    Draft(_, items), RemoveItem(sku) ->
      case has_item(items, sku) {
        True -> Ok([ItemRemoved(sku)])
        False -> Error(ItemNotFound(sku))
      }
    Draft(_, items), SubmitOrder ->
      case list.is_empty(items) {
        True -> Error(EmptyOrder)
        False -> Ok([OrderSubmitted])
      }
    Draft(_, _), CancelOrder(reason) -> Ok([OrderCancelled(reason)])
    Draft(_, _), StartPreparing
    | Draft(_, _), MarkReady
    | Draft(_, _), Serve
    | Draft(_, _), Pay(_)
    -> invalid(command, state)

    Submitted(_, _), StartPreparing -> Ok([PreparationStarted])
    Submitted(_, _), CancelOrder(reason) -> Ok([OrderCancelled(reason)])
    Submitted(_, _), OpenOrder(_)
    | Submitted(_, _), AddItem(_, _, _)
    | Submitted(_, _), RemoveItem(_)
    | Submitted(_, _), SubmitOrder
    | Submitted(_, _), MarkReady
    | Submitted(_, _), Serve
    | Submitted(_, _), Pay(_)
    -> invalid(command, state)

    Preparing(_, _), MarkReady -> Ok([OrderMarkedReady])
    Preparing(_, _), OpenOrder(_)
    | Preparing(_, _), AddItem(_, _, _)
    | Preparing(_, _), RemoveItem(_)
    | Preparing(_, _), SubmitOrder
    | Preparing(_, _), StartPreparing
    | Preparing(_, _), Serve
    | Preparing(_, _), Pay(_)
    | Preparing(_, _), CancelOrder(_)
    -> invalid(command, state)

    Ready(_, _), Serve -> Ok([OrderServed])
    Ready(_, _), OpenOrder(_)
    | Ready(_, _), AddItem(_, _, _)
    | Ready(_, _), RemoveItem(_)
    | Ready(_, _), SubmitOrder
    | Ready(_, _), StartPreparing
    | Ready(_, _), MarkReady
    | Ready(_, _), Pay(_)
    | Ready(_, _), CancelOrder(_)
    -> invalid(command, state)

    Served(_, items), Pay(amount) -> {
      let required = total(items)
      case amount >= required {
        True -> Ok([PaymentReceived(amount)])
        False -> Error(PaymentTooLow(required: required, paid: amount))
      }
    }
    Served(_, _), OpenOrder(_)
    | Served(_, _), AddItem(_, _, _)
    | Served(_, _), RemoveItem(_)
    | Served(_, _), SubmitOrder
    | Served(_, _), StartPreparing
    | Served(_, _), MarkReady
    | Served(_, _), Serve
    | Served(_, _), CancelOrder(_)
    -> invalid(command, state)

    Paid(_, _, _), _ -> invalid(command, state)
    Cancelled(_), _ -> invalid(command, state)
  }
}

fn evolve(state: State, event: Event) -> State {
  case event {
    OrderOpened(table) -> Draft(table: table, items: [])
    ItemAdded(sku, name, price) ->
      add_item_to_state(state, Item(sku, name, price))
    ItemRemoved(sku) -> remove_item_from_state(state, sku)
    OrderSubmitted -> move_to_submitted(state)
    PreparationStarted -> move_to_preparing(state)
    OrderMarkedReady -> move_to_ready(state)
    OrderServed -> move_to_served(state)
    PaymentReceived(amount) -> move_to_paid(state, amount)
    OrderCancelled(reason) -> Cancelled(reason)
  }
}

fn add_item_to_state(state: State, item: Item) -> State {
  case state {
    Draft(table, items) -> Draft(table: table, items: [item, ..items])
    NoOrder
    | Submitted(_, _)
    | Preparing(_, _)
    | Ready(_, _)
    | Served(_, _)
    | Paid(_, _, _)
    | Cancelled(_) -> state
  }
}

fn remove_item_from_state(state: State, sku: String) -> State {
  case state {
    Draft(table, items) ->
      Draft(
        table: table,
        items: list.filter(items, fn(item) { item.sku != sku }),
      )
    NoOrder
    | Submitted(_, _)
    | Preparing(_, _)
    | Ready(_, _)
    | Served(_, _)
    | Paid(_, _, _)
    | Cancelled(_) -> state
  }
}

fn move_to_submitted(state: State) -> State {
  case state {
    Draft(table, items) -> Submitted(table: table, items: items)
    NoOrder
    | Submitted(_, _)
    | Preparing(_, _)
    | Ready(_, _)
    | Served(_, _)
    | Paid(_, _, _)
    | Cancelled(_) -> state
  }
}

fn move_to_preparing(state: State) -> State {
  case state {
    Submitted(table, items) -> Preparing(table: table, items: items)
    NoOrder
    | Draft(_, _)
    | Preparing(_, _)
    | Ready(_, _)
    | Served(_, _)
    | Paid(_, _, _)
    | Cancelled(_) -> state
  }
}

fn move_to_ready(state: State) -> State {
  case state {
    Preparing(table, items) -> Ready(table: table, items: items)
    NoOrder
    | Draft(_, _)
    | Submitted(_, _)
    | Ready(_, _)
    | Served(_, _)
    | Paid(_, _, _)
    | Cancelled(_) -> state
  }
}

fn move_to_served(state: State) -> State {
  case state {
    Ready(table, items) -> Served(table: table, items: items)
    NoOrder
    | Draft(_, _)
    | Submitted(_, _)
    | Preparing(_, _)
    | Served(_, _)
    | Paid(_, _, _)
    | Cancelled(_) -> state
  }
}

fn move_to_paid(state: State, amount: Int) -> State {
  case state {
    Served(table, items) -> Paid(table: table, items: items, amount: amount)
    NoOrder
    | Draft(_, _)
    | Submitted(_, _)
    | Preparing(_, _)
    | Ready(_, _)
    | Paid(_, _, _)
    | Cancelled(_) -> state
  }
}

pub fn kitchen_summary_view() -> factos.View(KitchenSummary, Event) {
  factos.view(
    initial: KitchenSummary(0, 0, 0, 0, 0, 0, 0, 0),
    evolve: evolve_summary,
  )
}

fn evolve_summary(summary: KitchenSummary, event: Event) -> KitchenSummary {
  case event {
    OrderOpened(_) -> KitchenSummary(..summary, opened: summary.opened + 1)
    ItemAdded(_, _, _) | ItemRemoved(_) -> summary
    OrderSubmitted ->
      KitchenSummary(..summary, submitted: summary.submitted + 1)
    PreparationStarted ->
      KitchenSummary(..summary, preparing: summary.preparing + 1)
    OrderMarkedReady -> KitchenSummary(..summary, ready: summary.ready + 1)
    OrderServed -> KitchenSummary(..summary, served: summary.served + 1)
    PaymentReceived(amount) ->
      KitchenSummary(
        ..summary,
        paid: summary.paid + 1,
        revenue: summary.revenue + amount,
      )
    OrderCancelled(_) ->
      KitchenSummary(..summary, cancelled: summary.cancelled + 1)
  }
}

pub fn order_codec() -> factos_sqlight.EventCodec(Event, DecodeError) {
  factos_sqlight.EventCodec(encode: encode_event, decode: decode_event)
}

fn encode_event(event: Event) -> factos_sqlight.Proposed(Event) {
  case event {
    OrderOpened(table) ->
      proposed(event, "OrderOpened", [int.to_string(table)], [])
    ItemAdded(sku, name, price) ->
      proposed(event, "ItemAdded", [sku, name, int.to_string(price)], [
        factos.tag("sku:" <> sku),
      ])
    ItemRemoved(sku) ->
      proposed(event, "ItemRemoved", [sku], [
        factos.tag("sku:" <> sku),
      ])
    OrderSubmitted -> proposed(event, "OrderSubmitted", [], [])
    PreparationStarted -> proposed(event, "PreparationStarted", [], [])
    OrderMarkedReady -> proposed(event, "OrderMarkedReady", [], [])
    OrderServed -> proposed(event, "OrderServed", [], [])
    PaymentReceived(amount) ->
      proposed(event, "PaymentReceived", [int.to_string(amount)], [])
    OrderCancelled(reason) -> proposed(event, "OrderCancelled", [reason], [])
  }
}

fn proposed(
  event: Event,
  type_name: String,
  fields: List(String),
  tags: List(factos.Tag),
) -> factos_sqlight.Proposed(Event) {
  factos_sqlight.Proposed(
    id: "example-" <> type_name <> "-" <> fields_to_payload(fields),
    event: event,
    type_: factos.event_type(type_name),
    version: 1,
    tags: [factos.tag("restaurant"), ..tags],
    metadata: factos.empty_metadata(),
    data: bit_array.from_string(fields_to_payload(fields)),
  )
}

fn decode_event(
  stored: factos_sqlight.StoredEvent,
) -> Result(factos.Decoded(Event), DecodeError) {
  let type_name = factos.event_type_name(stored.type_)
  let fields =
    stored.data
    |> bit_array.to_string
    |> result.replace_error(InvalidPayload(type_name))
    |> result.map(payload_to_fields)

  use event <- result.try(decode_fields(type_name, fields))
  Ok(factos.Decoded(
    event: event,
    type_: stored.type_,
    version: stored.version,
    tags: stored.tags,
    metadata: stored.metadata,
  ))
}

fn decode_fields(
  type_name: String,
  fields_result: Result(List(String), DecodeError),
) -> Result(Event, DecodeError) {
  use fields <- result.try(fields_result)
  case type_name, fields {
    "OrderOpened", [table] -> {
      use table <- result.try(parse_int(table, type_name))
      Ok(OrderOpened(table))
    }
    "ItemAdded", [sku, name, price] -> {
      use price <- result.try(parse_int(price, type_name))
      Ok(ItemAdded(sku, name, price))
    }
    "ItemRemoved", [sku] -> Ok(ItemRemoved(sku))
    "OrderSubmitted", [] -> Ok(OrderSubmitted)
    "PreparationStarted", [] -> Ok(PreparationStarted)
    "OrderMarkedReady", [] -> Ok(OrderMarkedReady)
    "OrderServed", [] -> Ok(OrderServed)
    "PaymentReceived", [amount] -> {
      use amount <- result.try(parse_int(amount, type_name))
      Ok(PaymentReceived(amount))
    }
    "OrderCancelled", [reason] -> Ok(OrderCancelled(reason))
    _, _ -> Error(UnknownEventType(type_name))
  }
}

fn parse_int(value: String, type_name: String) -> Result(Int, DecodeError) {
  int.parse(value)
  |> result.replace_error(InvalidPayload(type_name))
}

fn fields_to_payload(fields: List(String)) -> String {
  string.join(fields, with: "|")
}

fn payload_to_fields(payload: String) -> List(String) {
  case string.is_empty(payload) {
    True -> []
    False -> string.split(payload, on: "|")
  }
}

fn has_item(items: List(Item), sku: String) -> Bool {
  list.any(items, fn(item) { item.sku == sku })
}

fn total(items: List(Item)) -> Int {
  list.fold(items, 0, fn(total, item) { total + item.price })
}

fn invalid(command: Command, state: State) -> Result(List(Event), DomainError) {
  Error(InvalidTransition(command_to_string(command), state_to_string(state)))
}

fn command_to_string(command: Command) -> String {
  case command {
    OpenOrder(_) -> "OpenOrder"
    AddItem(_, _, _) -> "AddItem"
    RemoveItem(_) -> "RemoveItem"
    SubmitOrder -> "SubmitOrder"
    StartPreparing -> "StartPreparing"
    MarkReady -> "MarkReady"
    Serve -> "Serve"
    Pay(_) -> "Pay"
    CancelOrder(_) -> "CancelOrder"
  }
}

fn result_to_string(
  result: Result(WorkerResult, factos_sqlight.Error(DomainError, DecodeError)),
) -> String {
  case result {
    Ok(worker_result) ->
      "ok "
      <> state_to_string(worker_result.final_state)
      <> " events="
      <> int.to_string(worker_result.recorded_events)
    Error(error) -> "error " <> store_error_to_string(error)
  }
}

fn store_error_to_string(
  error: factos_sqlight.Error(DomainError, DecodeError),
) -> String {
  case error {
    factos_sqlight.DomainError(error) ->
      "domain:" <> domain_error_to_string(error)
    factos_sqlight.DecodeError(error) ->
      "decode:" <> decode_error_to_string(error)
    factos_sqlight.StoreError(sqlight.SqlightError(code, message, _)) ->
      "sqlite(code="
      <> int.to_string(sqlight.error_code_to_int(code))
      <> ", message="
      <> message
      <> ")"
    factos_sqlight.AppendConditionFailed(_) -> "append-condition-failed"
  }
}

fn domain_error_to_string(error: DomainError) -> String {
  case error {
    OrderAlreadyOpen -> "OrderAlreadyOpen"
    OrderNotOpen -> "OrderNotOpen"
    DuplicateItem(sku) -> "DuplicateItem(" <> sku <> ")"
    ItemNotFound(sku) -> "ItemNotFound(" <> sku <> ")"
    EmptyOrder -> "EmptyOrder"
    PaymentTooLow(required, paid) ->
      "PaymentTooLow(required="
      <> int.to_string(required)
      <> ", paid="
      <> int.to_string(paid)
      <> ")"
    WorkerTimedOut(remaining) ->
      "WorkerTimedOut(remaining=" <> int.to_string(remaining) <> ")"
    InvalidTransition(action, state) ->
      "InvalidTransition(" <> action <> ", " <> state <> ")"
  }
}

fn decode_error_to_string(error: DecodeError) -> String {
  case error {
    UnknownEventType(type_name) -> "UnknownEventType(" <> type_name <> ")"
    InvalidPayload(type_name) -> "InvalidPayload(" <> type_name <> ")"
  }
}

fn log(message: String) -> Nil {
  io.println("[factos-example] " <> message)
}

fn state_to_string(state: State) -> String {
  case state {
    NoOrder -> "NoOrder"
    Draft(_, _) -> "Draft"
    Submitted(_, _) -> "Submitted"
    Preparing(_, _) -> "Preparing"
    Ready(_, _) -> "Ready"
    Served(_, _) -> "Served"
    Paid(_, _, _) -> "Paid"
    Cancelled(_) -> "Cancelled"
  }
}
