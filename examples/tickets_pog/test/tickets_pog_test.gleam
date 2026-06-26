import gleeunit
import ticket_sale

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn ticket_sale_preserves_capacity_under_high_concurrency_test() {
  let assert Ok(ticket_sale.SaleSummary(
    attempts: 300,
    accepted: 100,
    sold_out: 200,
    recorded_events: 100,
  )) = ticket_sale.run()
}
