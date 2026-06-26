import gleeunit
import order_workflow

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn restaurant_order_example_runs_concurrently_under_stress_test() {
  let assert Ok(order_workflow.StressResult(
    orders: 40,
    paid_orders: 32,
    cancelled_orders: 8,
    recorded_events: 376,
    revenue: 800,
  )) = order_workflow.run()
}
