import factos
import factos/factos_pog
import gleam/bit_array
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{Some}
import gleam/result
import global_value
import pog

const event_id = "gleamconf-2026"

const ticket_capacity = 100

const purchase_attempts = 300

const concurrency = 128

const postgres_pool_size = 64

const receive_timeout = 60_000

pub type Command {
  BuyTicket(buyer: String)
}

pub type Event {
  TicketSold(buyer: String)
}

type Effect {
  AnnounceTicketSale(buyer: String, position: factos.SequencePosition)
}

pub type State {
  TicketWindow(capacity: Int, sold: Int)
}

pub type DomainError {
  SoldOut(capacity: Int)
}

pub type SaleSummary {
  SaleSummary(attempts: Int, accepted: Int, sold_out: Int, recorded_events: Int)
}

type TestGlobalData {
  TestGlobalData(connection: pog.Connection)
}

type PurchaseMessage {
  PurchaseFinished(
    attempt: Int,
    result: Result(factos_pog.Dispatch(Event), factos_pog.Error(DomainError)),
  )
}

pub fn main() -> Nil {
  case run() {
    Ok(summary) ->
      io.println(
        "ticket sale completed: "
        <> int.to_string(summary.accepted)
        <> " accepted, "
        <> int.to_string(summary.sold_out)
        <> " sold out, "
        <> int.to_string(summary.recorded_events)
        <> " recorded events",
      )
    Error(_) -> io.println("ticket sale failed")
  }
}

pub fn run() -> Result(SaleSummary, factos_pog.Error(DomainError)) {
  let TestGlobalData(connection) = global_data()
  use _ <- result.try(reset_schema(connection))

  let workers = process.new_subject()
  let initial =
    SaleSummary(attempts: 0, accepted: 0, sold_out: 0, recorded_events: 0)

  {
    use _, attempt <- int.range(from: 1, to: concurrency + 1, with: Nil)
    spawn_purchase(workers, connection, attempt)
  }

  collect_purchases(
    workers,
    connection: connection,
    remaining: purchase_attempts,
    next_attempt: concurrency + 1,
    summary: initial,
  )
}

fn global_data() -> TestGlobalData {
  global_value.create_with_unique_name("tickets_pog.global.data", fn() {
    TestGlobalData(connection: start_connection())
  })
}

fn start_connection() -> pog.Connection {
  let pool_name = process.new_name("tickets_pog")
  let config =
    pog.default_config(pool_name)
    |> pog.host("127.0.0.1")
    |> pog.port(5433)
    |> pog.database("tickets_pog")
    |> pog.user("postgres")
    |> pog.password(Some("postgres"))
    |> pog.ssl(pog.SslDisabled)
    |> pog.pool_size(postgres_pool_size)

  let assert Ok(_) = pog.start(config)
  process.sleep(100)
  pog.named_connection(pool_name)
}

fn reset_schema(
  connection: pog.Connection,
) -> Result(Nil, factos_pog.Error(DomainError)) {
  use _ <- result.try(
    pog.query("drop table if exists factos_event_tags")
    |> pog.execute(on: connection)
    |> result.map(fn(_) { Nil })
    |> result.map_error(factos_pog.StoreError),
  )
  use _ <- result.try(
    pog.query("drop table if exists factos_events")
    |> pog.execute(on: connection)
    |> result.map(fn(_) { Nil })
    |> result.map_error(factos_pog.StoreError),
  )
  factos_pog.migrate(connection)
}

fn spawn_purchase(
  workers: process.Subject(PurchaseMessage),
  connection: pog.Connection,
  attempt: Int,
) -> Nil {
  process.spawn(fn() {
    process.send(
      workers,
      PurchaseFinished(attempt, purchase(connection, attempt)),
    )
  })
  Nil
}

fn purchase(
  connection: pog.Connection,
  attempt: Int,
) -> Result(factos_pog.Dispatch(Event), factos_pog.Error(DomainError)) {
  // Each buyer races through the same event-context query. PostgreSQL receives a
  // large amount of concurrent work via the pool, while the backend's transaction
  // lock preserves the capacity invariant for this arbitrary tag-based context.
  factos_pog.dispatch_with_query(
    connection,
    stream: buyer_stream(attempt),
    query: sale_query(),
    decider: ticket_decider(),
    codec: ticket_codec(),
    command: BuyTicket(buyer_name(attempt)),
  )
}

fn collect_purchases(
  workers: process.Subject(PurchaseMessage),
  connection connection: pog.Connection,
  remaining remaining: Int,
  next_attempt next_attempt: Int,
  summary summary: SaleSummary,
) -> Result(SaleSummary, factos_pog.Error(DomainError)) {
  case remaining {
    0 -> finalize_summary(connection, summary)
    _ ->
      case process.receive(workers, within: receive_timeout) {
        Ok(PurchaseFinished(_, Ok(dispatch))) -> {
          run_effects(factos.react_all(ticket_reactor(), dispatch.events))
          case next_attempt <= purchase_attempts {
            True -> spawn_purchase(workers, connection, next_attempt)
            False -> Nil
          }
          collect_purchases(
            workers,
            connection: connection,
            remaining: remaining - 1,
            next_attempt: next_attempt + 1,
            summary: SaleSummary(
              attempts: summary.attempts + 1,
              accepted: summary.accepted + 1,
              sold_out: summary.sold_out,
              recorded_events: summary.recorded_events,
            ),
          )
        }
        Ok(PurchaseFinished(_, Error(factos_pog.DomainError(SoldOut(_))))) -> {
          case next_attempt <= purchase_attempts {
            True -> spawn_purchase(workers, connection, next_attempt)
            False -> Nil
          }
          collect_purchases(
            workers,
            connection: connection,
            remaining: remaining - 1,
            next_attempt: next_attempt + 1,
            summary: SaleSummary(
              attempts: summary.attempts + 1,
              accepted: summary.accepted,
              sold_out: summary.sold_out + 1,
              recorded_events: summary.recorded_events,
            ),
          )
        }
        Ok(PurchaseFinished(_, Error(error))) -> Error(error)
        Error(Nil) -> Error(factos_pog.DomainError(SoldOut(summary.accepted)))
      }
  }
}

fn finalize_summary(
  connection: pog.Connection,
  summary: SaleSummary,
) -> Result(SaleSummary, factos_pog.Error(DomainError)) {
  use context <- result.try(factos_pog.read_context(
    connection,
    query: sale_query(),
    decider: ticket_decider(),
    codec: ticket_codec(),
  ))

  Ok(SaleSummary(
    attempts: summary.attempts,
    accepted: summary.accepted,
    sold_out: summary.sold_out,
    recorded_events: list.length(context.events),
  ))
}

fn sale_query() -> factos.Query {
  factos.query([
    factos.query_item(types: [factos.event_type("TicketSold")], tags: [
      factos.tag("event:" <> event_id),
    ]),
  ])
}

fn ticket_decider() -> factos.Decider(Command, State, Event, DomainError) {
  factos.decider(
    initial: TicketWindow(capacity: ticket_capacity, sold: 0),
    decide: decide,
    evolve: evolve,
  )
}

fn decide(state: State, command: Command) -> Result(List(Event), DomainError) {
  let TicketWindow(capacity, sold) = state
  case command {
    BuyTicket(buyer) ->
      case sold < capacity {
        True -> Ok([TicketSold(buyer)])
        False -> Error(SoldOut(capacity))
      }
  }
}

fn evolve(state: State, event: Event) -> State {
  let TicketWindow(capacity, sold) = state
  case event {
    TicketSold(_) -> TicketWindow(capacity: capacity, sold: sold + 1)
  }
}

fn ticket_reactor() -> factos.Reactor(Event, Effect) {
  factos.reactor(fn(recorded) {
    case recorded.event {
      TicketSold(buyer) -> [
        AnnounceTicketSale(buyer: buyer, position: recorded.position),
      ]
    }
  })
}

fn run_effects(effects: List(Effect)) -> Nil {
  use effect <- list.each(effects)
  case effect {
    AnnounceTicketSale(buyer:, position:) ->
      io.println(
        "reactor: ticket sold to "
        <> buyer
        <> " at position "
        <> position_to_string(position),
      )
  }
}

fn position_to_string(position: factos.SequencePosition) -> String {
  case position {
    factos.NoPosition -> "none"
    factos.SequencePosition(position) -> int.to_string(position)
  }
}

fn ticket_codec() -> factos_pog.EventCodec(Event) {
  factos_pog.codec(encode: encode_event, decode: decode_event)
}

fn encode_event(event: Event) -> factos_pog.Proposed(Event) {
  case event {
    TicketSold(buyer) ->
      factos_pog.Proposed(
        id: "ticket-sold-" <> buyer,
        event: event,
        type_: factos.event_type("TicketSold"),
        version: 1,
        tags: [factos.tag("event:" <> event_id)],
        metadata: factos.empty_metadata(),
        data: bit_array.from_string(buyer),
      )
  }
}

fn decode_event(
  stored: factos_pog.StoredEvent,
) -> Result(factos.Decoded(Event), factos_pog.DecodeError) {
  case factos.event_type_name(stored.type_) {
    "TicketSold" -> {
      use buyer <- result.try(
        bit_array.to_string(stored.data)
        |> result.replace_error(factos_pog.InvalidData),
      )
      Ok(factos.Decoded(
        event: TicketSold(buyer),
        type_: stored.type_,
        version: stored.version,
        tags: stored.tags,
        metadata: stored.metadata,
      ))
    }
    _ -> Error(factos_pog.UnknownEvent)
  }
}

fn buyer_name(attempt: Int) -> String {
  "buyer-" <> int.to_string(attempt)
}

fn buyer_stream(attempt: Int) -> String {
  "ticket-buyer-" <> int.to_string(attempt)
}
