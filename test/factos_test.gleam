import factos
import gleam/int
import gleeunit
import youid/uuid

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

pub fn fold_replays_recorded_events_test() {
  let events = [
    recorded(
      UsernameReserved("renata"),
      [factos.tag("username:renata")],
      revision: 0,
    ),
    recorded(
      UserRegistered("renata"),
      [factos.tag("username:renata")],
      revision: 1,
    ),
  ]

  assert factos.fold(UsernameAvailable, events, evolve) == UsernameTaken
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
  let early = factos.SequencePosition(commit_position: 10, prepare_position: 10)
  let later = factos.SequencePosition(commit_position: 11, prepare_position: 0)

  assert factos.highest_position(early, later) == later
  assert factos.highest_position(factos.NoPosition, early) == early
}

pub fn revision_models_empty_and_loaded_streams_test() {
  assert revision_label(factos.NoEvents) == "empty"
  assert revision_label(factos.CurrentRevision(2)) == "revision:2"
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
    id: uuid.v7(),
    stream: "user-1",
    revision: revision,
    position: factos.SequencePosition(
      commit_position: revision,
      prepare_position: revision,
    ),
    metadata: [],
    custom_metadata: <<>>,
    data: <<>>,
    type_: type_,
    tags: tags,
    event: event,
  )
}

fn revision_label(revision: factos.Revision) -> String {
  case revision {
    factos.NoEvents -> "empty"
    factos.CurrentRevision(revision) -> "revision:" <> int.to_string(revision)
  }
}
