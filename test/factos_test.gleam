import factos
import gleam/int
import gleam/option.{None, Some}
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

type Event {
  UsernameReserved(username: String)
  UserRegistered(username: String)
  DisplayNameChanged(name: String)
}

type State {
  UsernameAvailable
  UsernameTaken
}

type Command {
  RegisterUser(username: String)
}

type DomainError {
  UsernameAlreadyTaken
}

pub fn decide_context_uses_decider_test() {
  let context =
    factos.Context(
      query: factos.AllEvents,
      state: UsernameAvailable,
      events: [],
      position: factos.NoPosition,
      append_condition: factos.NoAppendCondition,
    )

  assert factos.decide_context(
      context,
      RegisterUser("renata"),
      username_decider(),
    )
    == Ok(#(context, [UserRegistered("renata")]))
}

pub fn query_matches_by_type_and_tags_test() {
  let username_query =
    factos.query([
      factos.query_item(
        types: [
          factos.event_type("UsernameReserved"),
          factos.event_type("UserRegistered"),
        ],
        tags: [factos.tag("username:renata")],
      ),
    ])

  let matching =
    recorded(
      UsernameReserved("renata"),
      [factos.tag("username:renata")],
      revision: 0,
    )
  let wrong_tag =
    recorded(
      UsernameReserved("lucy"),
      [factos.tag("username:lucy")],
      revision: 1,
    )
  let wrong_type =
    recorded(
      DisplayNameChanged("Renata"),
      [factos.tag("username:renata")],
      revision: 2,
    )

  assert factos.matches_query(matching, username_query)
  assert !factos.matches_query(wrong_tag, username_query)
  assert !factos.matches_query(wrong_type, username_query)
}

pub fn query_items_are_or_combined_test() {
  let query =
    factos.query([
      factos.query_item(types: [factos.event_type("UserRegistered")], tags: [
        factos.tag("user:1"),
      ]),
      factos.query_item(types: [factos.event_type("DisplayNameChanged")], tags: [
        factos.tag("user:2"),
      ]),
    ])

  let event =
    recorded(DisplayNameChanged("R"), [factos.tag("user:2")], revision: 0)

  assert factos.matches_query(event, query)
}

pub fn empty_query_matches_all_events_test() {
  let query = factos.query([])
  let event = recorded(DisplayNameChanged("R"), [], revision: 0)

  assert factos.matches_query(event, query)
}

pub fn highest_position_keeps_later_position_test() {
  let early = factos.SequencePosition(10)
  let later = factos.SequencePosition(11)

  assert factos.highest_position(early, later) == later
  assert factos.highest_position(factos.NoPosition, early) == early
}

pub fn revision_models_empty_and_loaded_streams_test() {
  assert revision_label(factos.NoEvents) == "empty"
  assert revision_label(factos.CurrentRevision(2)) == "revision:2"
}

pub fn decider_computes_events_from_history_test() {
  let decider = username_decider()

  assert factos.compute_events(
      decider: decider,
      events: [],
      command: RegisterUser("renata"),
    )
    == Ok([UserRegistered("renata")])

  assert factos.compute_events(
      decider: decider,
      events: [UsernameReserved("renata")],
      command: RegisterUser("renata"),
    )
    == Error(UsernameAlreadyTaken)
}

pub fn decider_computes_state_from_optional_state_test() {
  let decider = username_decider()

  assert factos.compute_state(
      decider: decider,
      current: None,
      command: RegisterUser("renata"),
    )
    == Ok(UsernameTaken)

  assert factos.compute_state(
      decider: decider,
      current: Some(UsernameTaken),
      command: RegisterUser("renata"),
    )
    == Error(UsernameAlreadyTaken)
}

pub fn view_projects_events_test() {
  let view =
    factos.view(initial: 0, evolve: fn(count, event) {
      case event {
        UserRegistered(_) -> count + 1
        UsernameReserved(_) -> count
        DisplayNameChanged(_) -> count
      }
    })

  assert factos.project(view: view, events: [
      UsernameReserved("renata"),
      UserRegistered("renata"),
      UserRegistered("lucy"),
    ])
    == 2
}

pub fn merge_views_projects_same_events_into_tuple_state_test() {
  let registration_count =
    factos.view(initial: 0, evolve: fn(count, event) {
      case event {
        UserRegistered(_) -> count + 1
        UsernameReserved(_) -> count
        DisplayNameChanged(_) -> count
      }
    })
  let display_name_count =
    factos.view(initial: 0, evolve: fn(count, event) {
      case event {
        DisplayNameChanged(_) -> count + 1
        UsernameReserved(_) -> count
        UserRegistered(_) -> count
      }
    })

  let merged = factos.merge_views(registration_count, display_name_count)

  assert factos.project(view: merged, events: [
      UserRegistered("renata"),
      DisplayNameChanged("Renata"),
      DisplayNameChanged("Rena"),
    ])
    == #(1, 2)
}

pub fn react_maps_one_recorded_event_into_effect_values_test() -> Nil {
  let event =
    recorded(
      UserRegistered("renata"),
      [factos.tag("username:renata")],
      revision: 5,
    )
  let reactor =
    factos.reactor(react: fn(recorded) {
      case recorded.event {
        UserRegistered(username) -> [
          recorded.id
          <> ":"
          <> int.to_string(recorded.revision)
          <> ":"
          <> username,
        ]
        UsernameReserved(_) -> []
        DisplayNameChanged(_) -> []
      }
    })

  assert factos.react(reactor: reactor, event: event) == ["event-5:5:renata"]
}

pub fn react_all_flattens_reactions_in_recorded_event_order_test() -> Nil {
  let reactor =
    factos.reactor(react: fn(recorded) {
      case recorded.event {
        UserRegistered(username) -> [
          "registered:" <> username,
          "welcome:" <> username,
        ]
        UsernameReserved(_) -> []
        DisplayNameChanged(name) -> ["display:" <> name]
      }
    })

  assert factos.react_all(reactor: reactor, events: [
      recorded(UserRegistered("renata"), [], revision: 0),
      recorded(UsernameReserved("renata"), [], revision: 1),
      recorded(DisplayNameChanged("Rena"), [], revision: 2),
      recorded(UserRegistered("lucy"), [], revision: 3),
    ])
    == [
      "registered:renata",
      "welcome:renata",
      "display:Rena",
      "registered:lucy",
      "welcome:lucy",
    ]
}

pub fn merge_reactors_combines_outputs_for_the_same_recorded_event_test() -> Nil {
  let event =
    recorded(
      UserRegistered("renata"),
      [factos.tag("username:renata")],
      revision: 7,
    )
  let audit =
    factos.reactor(react: fn(recorded) {
      case recorded.event {
        UserRegistered(username) -> [
          "audit:" <> recorded.id <> ":" <> username,
        ]
        UsernameReserved(_) -> []
        DisplayNameChanged(_) -> []
      }
    })
  let notification =
    factos.reactor(react: fn(recorded) {
      case recorded.event {
        UserRegistered(username) -> [
          "notify:" <> int.to_string(recorded.revision) <> ":" <> username,
        ]
        UsernameReserved(_) -> []
        DisplayNameChanged(_) -> []
      }
    })

  let merged = factos.merge_reactors(audit, notification)

  assert factos.react(reactor: merged, event: event)
    == ["audit:event-7:renata", "notify:7:renata"]
}

fn evolve(state: State, event: Event) -> State {
  case state, event {
    UsernameAvailable, UsernameReserved(_) -> UsernameTaken
    UsernameAvailable, UserRegistered(_) -> UsernameTaken
    UsernameAvailable, DisplayNameChanged(_) -> state
    UsernameTaken, UsernameReserved(_) -> state
    UsernameTaken, UserRegistered(_) -> state
    UsernameTaken, DisplayNameChanged(_) -> state
  }
}

fn decide(state: State, command: Command) -> Result(List(Event), DomainError) {
  case state, command {
    UsernameAvailable, RegisterUser(username) -> Ok([UserRegistered(username)])
    UsernameTaken, RegisterUser(_) -> Error(UsernameAlreadyTaken)
  }
}

fn username_decider() -> factos.Decider(Command, State, Event, DomainError) {
  factos.decider(initial: UsernameAvailable, decide: decide, evolve: evolve)
}

fn recorded(
  event: Event,
  tags tags: List(factos.Tag),
  revision revision: Int,
) -> factos.Recorded(Event) {
  let type_ = case event {
    UsernameReserved(_) -> factos.event_type("UsernameReserved")
    UserRegistered(_) -> factos.event_type("UserRegistered")
    DisplayNameChanged(_) -> factos.event_type("DisplayNameChanged")
  }

  factos.Recorded(
    id: "event-" <> int.to_string(revision),
    stream: "user-1",
    revision: revision,
    position: factos.SequencePosition(revision),
    type_: type_,
    version: 1,
    tags: tags,
    metadata: factos.empty_metadata(),
    event: event,
  )
}

fn revision_label(revision: factos.Revision) -> String {
  case revision {
    factos.NoEvents -> "empty"
    factos.CurrentRevision(revision) -> "revision:" <> int.to_string(revision)
  }
}
