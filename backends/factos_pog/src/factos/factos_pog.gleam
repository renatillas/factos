//// PostgreSQL backend for Factos using the `pog` package.
////
//// This backend stores accepted facts in an append-only `factos_events` table.
//// The event history is the source of truth; projections and stream-shaped reads
//// are derived views over that history.
////
//// The context dispatch flow follows the Command Context Consistency idea from
//// "Simply Event Sourcing": a command selects the facts required for its decision,
//// folds them into temporary state, decides new facts, and appends those facts
//// only when no relevant facts appeared after the observed context position.
////
//// The query contract is intentionally tag-based. PostgreSQL stores opaque event
//// bytes, an event type, and tags. This keeps domain serialization outside the
//// backend, but it means any payload value needed for a selective consistency
//// query must be written as a tag.

import factos
import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/result
import gleam/string
import pog

pub type Proposed(event) {
  /// A domain event prepared for PostgreSQL persistence.
  ///
  /// The application codec creates this value. `id` should identify the event for
  /// the application. `type_` and `tags` are store-visible query metadata. `data`
  /// is opaque bytes owned by the application codec.
  Proposed(
    id: String,
    event: event,
    type_: factos.EventType,
    version: Int,
    tags: List(factos.Tag),
    metadata: factos.Metadata,
    data: BitArray,
  )
}

pub type StoredEvent {
  /// A raw event row read from PostgreSQL before domain decoding.
  ///
  /// Decoders receive this value so they can inspect stored metadata and bytes.
  /// `position` is the global append order. `revision` is the per-stream revision.
  StoredEvent(
    position: Int,
    id: String,
    stream: String,
    revision: Int,
    type_: factos.EventType,
    version: Int,
    tags: List(factos.Tag),
    metadata: factos.Metadata,
    data: BitArray,
  )
}

pub type EventCodec(event) {
  /// Application-owned PostgreSQL event codec.
  ///
  /// `encode` converts a domain event into bytes and metadata. `decode` converts a
  /// stored row back into a `factos.Decoded` domain event. Decode failures are kept
  /// in the application's own error type and wrapped as `DecodeError`.
  EventCodec(
    encode: fn(event) -> Proposed(event),
    decode: fn(StoredEvent) -> Result(factos.Decoded(event), DecodeError),
  )
}

pub type Append {
  /// Result of a successful append.
  ///
  /// `current_revision` is the latest revision of the target stream after the
  /// append. `position` is the global position of the last inserted event, or
  /// `NoPosition` when no events were produced.
  Append(current_revision: Int, position: factos.SequencePosition)
}

pub type Dispatch(event) {
  /// Result of a successful dispatch.
  ///
  /// `append` has the stream revision and final global position. `events` are the
  /// committed events recorded by this dispatch, suitable for pure Factos
  /// reactors or backend-specific durable effect adapters.
  Dispatch(append: Append, events: List(factos.Recorded(event)))
}

pub type ProposedEffect(effect) {
  /// A durable integration effect prepared for outbox persistence.
  ///
  /// Applications own the effect payload and type names. Factos stores the
  /// delivery envelope so workers can lease, retry, and acknowledge effects
  /// without knowing the domain payload.
  ProposedEffect(
    effect: effect,
    consumer: String,
    key: String,
    target: String,
    type_: String,
    metadata: factos.Metadata,
    payload: BitArray,
  )
}

pub type EffectCodec(effect) {
  /// Application-owned outbox effect codec.
  EffectCodec(encode: fn(effect) -> ProposedEffect(effect))
}

pub type OutboxMessage {
  /// A leased outbox message ready for delivery by an application worker.
  OutboxMessage(
    id: Int,
    source_position: factos.SequencePosition,
    source_event_id: String,
    source_context: String,
    consumer: String,
    key: String,
    target: String,
    type_: String,
    metadata: factos.Metadata,
    payload: BitArray,
  )
}

pub type Error(domain_error) {
  /// The decider rejected the command with a domain error.
  DomainError(domain_error)

  /// PostgreSQL or `pog` returned an error while running a query.
  StoreError(pog.QueryError)

  /// A stream revision or context append condition failed.
  AppendConditionFailed(factos.AppendCondition)
  DecodeError(DecodeError)
}

pub type DecodeError {
  UnknownEvent
  InvalidData
}

type QuerySql {
  QuerySql(sql: String, parameters: List(QueryParameter))
}

type QueryParameter {
  IntParameter(Int)
  TextParameter(String)
}

/// Create a new codec.
///
/// `encode` turns a domain event into a `Proposed` event ready for persistence.
/// `decode` turns a stored row back into a domain event. Decode failures are
/// returned as `DecodeError` and stop load/read flows rather than panicking.
pub fn codec(
  encode encode: fn(event) -> Proposed(event),
  decode decode: fn(StoredEvent) -> Result(factos.Decoded(event), DecodeError),
) -> EventCodec(event) {
  EventCodec(encode:, decode:)
}

/// Create an effect codec.
pub fn effect_codec(
  encode encode: fn(effect) -> ProposedEffect(effect),
) -> EffectCodec(effect) {
  EffectCodec(encode:)
}

/// Deprecated: read `priv/migrations.sql` from the `factos_pog` application
/// with `gleam/erlang/application.priv_directory` and run it with your
/// application's migration tool instead.
/// The schema is an append-only `factos_events` table with a global identity
/// `position`, per-stream `revision`, event `type`, newline-encoded `tags`, and
/// opaque `data` bytes. Queryable tags are also mirrored into
/// `factos_event_tags`, which gives event-type/tag context reads indexed SQL
/// plans instead of loading the whole event table into the application.
@deprecated("Use the SQL file at factos_pog/priv/migrations.sql instead.")
pub fn migrate(connection: pog.Connection) -> Result(Nil, Error(_)) {
  // `pog` uses prepared statements, and PostgreSQL does not allow multiple SQL
  // commands in one prepared statement. Keep migrations split into individual
  // statements rather than relying on client-side SQL script execution.
  use _ <- result.try(execute_migration(
    connection,
    "
    create table if not exists factos_events (
      position bigint generated always as identity primary key,
      id text not null,
      stream text not null,
      revision integer not null,
      type text not null,
      version integer not null,
      tags text not null,
      metadata text not null,
      data bytea not null,
      unique(stream, revision)
    )
    ",
  ))
  use _ <- result.try(execute_migration(
    connection,
    "
    create index if not exists factos_events_stream_revision
      on factos_events(stream, revision)
    ",
  ))
  use _ <- result.try(execute_migration(
    connection,
    "
    create index if not exists factos_events_position
      on factos_events(position)
    ",
  ))
  use _ <- result.try(execute_migration(
    connection,
    "
    create table if not exists factos_event_tags (
      position bigint not null references factos_events(position) on delete cascade,
      tag text not null,
      primary key(position, tag)
    )
    ",
  ))
  use _ <- result.try(execute_migration(
    connection,
    "
    create index if not exists factos_events_type_position
      on factos_events(type, position)
    ",
  ))
  use _ <- result.try(execute_migration(
    connection,
    "
    create index if not exists factos_event_tags_tag_position
      on factos_event_tags(tag, position)
    ",
  ))
  use _ <- result.try(execute_migration(
    connection,
    "
    insert into factos_event_tags(position, tag)
    select factos_events.position, split_tags.tag
    from factos_events
    cross join lateral regexp_split_to_table(factos_events.tags, E'\\n') as split_tags(tag)
    where split_tags.tag <> ''
    on conflict do nothing
    ",
  ))
  use _ <- result.try(execute_migration(
    connection,
    "
    create table if not exists factos_outbox (
      id bigint generated always as identity primary key,
      source_position bigint not null references factos_events(position),
      source_event_id text not null,
      source_context text not null,
      consumer text not null,
      effect_key text not null,
      target text not null,
      type text not null,
      metadata text not null,
      payload bytea not null,
      status text not null default 'pending',
      attempts integer not null default 0,
      available_at timestamptz not null default now(),
      locked_until timestamptz,
      last_error text,
      created_at timestamptz not null default now(),
      delivered_at timestamptz,
      unique(consumer, effect_key)
    )
    ",
  ))
  use _ <- result.try(execute_migration(
    connection,
    "
    create index if not exists factos_outbox_pending
      on factos_outbox(status, available_at, id)
    ",
  ))
  Ok(Nil)
}

fn execute_migration(
  connection: pog.Connection,
  sql: String,
) -> Result(Nil, Error(_)) {
  pog.query(sql)
  |> pog.execute(on: connection)
  |> result.map(fn(_) { Nil })
  |> result.map_error(StoreError)
}

/// Read and fold the facts selected by a command-context query.
///
/// The backend reads stored rows, decodes them with the supplied codec, filters
/// them with `factos.matches_query`, folds matching events with the decider's
/// `evolve` function, and returns a `factos.Context` with a
/// `FailIfEventsMatch(query, after)` append condition.
pub fn read_context(
  connection: pog.Connection,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event),
) -> Result(factos.Context(event, state), Error(domain_error)) {
  let factos.Decider(initial, _, evolve) = decider

  use events <- result.try(read_matching_events(connection, query, codec))
  let position = highest_recorded_position(events)

  Ok(factos.Context(
    query:,
    state: factos.evolve_recorded(
      initial: initial,
      events: events,
      evolve: evolve,
    ),
    events: events,
    position: position,
    append_condition: factos.FailIfEventsMatch(query, position),
  ))
}

/// Read committed events after a global sequence position.
///
/// This is the bounded polling primitive used by durable subscriptions and
/// process managers. Events are returned in global append order.
pub fn read_events_after(
  connection: pog.Connection,
  query query: factos.Query,
  after after: factos.SequencePosition,
  limit limit: Int,
  codec codec: EventCodec(event),
) -> Result(List(factos.Recorded(event)), Error(domain_error)) {
  case limit <= 0 {
    True -> Ok([])
    False -> {
      let QuerySql(where_sql, parameters) =
        matching_events_after_sql(query, after)
      use rows <- result.try(
        pog.query(
          "select position, id, stream, revision, type, version, tags, metadata, data
           from factos_events
           "
          <> where_sql
          <> "
           order by position
           limit "
          <> int.to_string(limit),
        )
        |> with_parameters(parameters)
        |> pog.returning(stored_event_decoder())
        |> pog.execute(on: connection)
        |> result.map(fn(returned) { returned.rows })
        |> result.map_error(StoreError),
      )
      decode_rows(rows, codec)
    }
  }
}

/// Run a full context-first read-decide-append command flow.
///
/// PostgreSQL does not have a native primitive for "append if no row matching this
/// arbitrary event-type/tag query appeared after position N". This backend uses a
/// transaction plus `lock table factos_events in exclusive mode` to make the read,
/// context check, and append atomic for all Factos queries.
///
/// That lock is the main throughput tradeoff: unrelated writers queue behind each
/// other even if their contexts do not overlap. It is deliberately simple and
/// correct. A future backend can use advisory locks or query-specific lock keys,
/// but only if it keeps the same context-stability guarantee.
pub fn dispatch_with_query(
  connection: pog.Connection,
  stream stream_name: String,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event),
  command command: command,
) -> Result(Dispatch(event), Error(domain_error)) {
  use transaction_connection <- run_locked_transaction(connection)
  use context <- result.try(read_context(
    transaction_connection,
    query: query,
    decider: decider,
    codec: codec,
  ))
  use pair <- result.try(
    factos.decide_context(context, command, decider)
    |> result.map_error(DomainError),
  )
  let #(context, events) = pair

  append_with_condition(
    transaction_connection,
    stream_name,
    events,
    codec,
    context.append_condition,
  )
}

/// Run a context-first command and durably persist reactor effects atomically.
///
/// Effects are inserted into `factos_outbox` in the same transaction as the
/// domain events that produced them. If effect persistence fails, the event
/// append rolls back too.
pub fn dispatch_with_reactor(
  connection: pog.Connection,
  stream stream_name: String,
  query query: factos.Query,
  decider decider: factos.Decider(command, state, event, domain_error),
  event_codec event_codec: EventCodec(event),
  command command: command,
  reactor reactor: factos.Reactor(event, effect),
  effect_codec effect_codec: EffectCodec(effect),
) -> Result(Dispatch(event), Error(domain_error)) {
  use transaction_connection <- run_locked_transaction(connection)
  use context <- result.try(read_context(
    transaction_connection,
    query: query,
    decider: decider,
    codec: event_codec,
  ))
  use pair <- result.try(
    factos.decide_context(context, command, decider)
    |> result.map_error(DomainError),
  )
  let #(context, events) = pair
  use dispatch <- result.try(append_with_condition(
    transaction_connection,
    stream_name,
    events,
    event_codec,
    context.append_condition,
  ))
  use _ <- result.try(insert_outbox_effects(
    transaction_connection,
    dispatch.events,
    reactor,
    effect_codec,
  ))

  Ok(dispatch)
}

/// Load and fold one stream.
///
/// This supports classic stream-revision consistency. The returned
/// `factos.LoadedStream` contains the folded state, decoded recorded events, and
/// current stream revision.
pub fn load_stream(
  connection: pog.Connection,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event),
) -> Result(factos.LoadedStream(event, state), Error(domain_error)) {
  let factos.Decider(initial, _, evolve) = decider
  use events <- result.try(read_stream_events(connection, stream_name, codec))

  Ok(factos.LoadedStream(
    stream: stream_name,
    state: factos.evolve_recorded(
      initial: initial,
      events: events,
      evolve: evolve,
    ),
    events: events,
    revision: stream_revision(events),
  ))
}

/// Run a stream-based read-decide-append command flow.
///
/// Use this when one stream is intentionally the consistency boundary. It remains
/// useful, but it is not required by Event Sourcing. For command-specific rules,
/// prefer `dispatch_with_query` so the protected boundary follows the decision.
pub fn dispatch(
  connection: pog.Connection,
  stream stream_name: String,
  decider decider: factos.Decider(command, state, event, domain_error),
  codec codec: EventCodec(event),
  command command: command,
) -> Result(Dispatch(event), Error(domain_error)) {
  use transaction_connection <- run_locked_transaction(connection)
  use loaded <- result.try(load_stream(
    transaction_connection,
    stream: stream_name,
    decider: decider,
    codec: codec,
  ))
  let factos.Decider(_, decide, _) = decider
  use events <- result.try(
    decide(loaded.state, command)
    |> result.map_error(DomainError),
  )

  append_stream_events(
    transaction_connection,
    stream_name,
    events,
    codec,
    loaded.revision,
  )
}

fn run_locked_transaction(
  connection: pog.Connection,
  work: fn(pog.Connection) -> Result(Dispatch(event), Error(domain_error)),
) -> Result(Dispatch(event), Error(domain_error)) {
  case
    {
      use transaction_connection <- pog.transaction(connection)
      use _ <- result.try(
        pog.query("lock table factos_events in exclusive mode")
        |> pog.execute(on: transaction_connection)
        |> result.map(fn(_) { Nil })
        |> result.map_error(StoreError),
      )
      work(transaction_connection)
    }
  {
    Ok(dispatch) -> Ok(dispatch)
    Error(pog.TransactionQueryError(error)) -> Error(StoreError(error))
    Error(pog.TransactionRolledBack(error)) -> Error(error)
  }
}

fn append_with_condition(
  connection: pog.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event),
  condition: factos.AppendCondition,
) -> Result(Dispatch(event), Error(domain_error)) {
  case condition {
    factos.NoAppendCondition ->
      append_current_stream(connection, stream_name, events, codec)
    factos.FailIfEventsMatch(query, after) ->
      case has_matching_events_after(connection, query, after) {
        Error(error) -> Error(StoreError(error))
        Ok(True) -> Error(AppendConditionFailed(condition))
        Ok(False) ->
          append_current_stream(connection, stream_name, events, codec)
      }
  }
}

fn append_current_stream(
  connection: pog.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event),
) -> Result(Dispatch(event), Error(domain_error)) {
  use revision <- result.try(
    current_revision(connection, stream_name)
    |> result.map_error(StoreError),
  )
  append_stream_events(
    connection,
    stream_name,
    events,
    codec,
    factos.CurrentRevision(revision),
  )
}

fn append_stream_events(
  connection: pog.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event),
  expected: factos.Revision,
) -> Result(Dispatch(event), Error(domain_error)) {
  case events {
    [] -> {
      let append =
        Append(
          current_revision: revision_to_int(expected),
          position: factos.NoPosition,
        )
      Ok(Dispatch(append:, events: []))
    }
    [_, ..] -> {
      use current <- result.try(
        current_revision(connection, stream_name)
        |> result.map_error(StoreError),
      )
      case expected_matches(expected, current) {
        False -> Error(AppendConditionFailed(factos.NoAppendCondition))
        True ->
          insert_events(
            connection,
            stream_name,
            events,
            codec,
            current + 1,
            factos.NoPosition,
            [],
          )
      }
    }
  }
}

fn insert_events(
  connection: pog.Connection,
  stream_name: String,
  events: List(event),
  codec: EventCodec(event),
  revision: Int,
  position: factos.SequencePosition,
  recorded_events: List(factos.Recorded(event)),
) -> Result(Dispatch(event), Error(domain_error)) {
  case events {
    [] -> {
      let append = Append(current_revision: revision - 1, position: position)
      Ok(Dispatch(append:, events: list.reverse(recorded_events)))
    }
    [event, ..rest] -> {
      let EventCodec(encode, _) = codec
      let Proposed(id, _, type_, version, tags, metadata, data) = encode(event)
      use returned <- result.try(
        pog.query(
          "
          insert into factos_events (id, stream, revision, type, version, tags, metadata, data)
          values ($1, $2, $3, $4, $5, $6, $7, $8)
          returning position
          ",
        )
        |> pog.parameter(pog.text(id))
        |> pog.parameter(pog.text(stream_name))
        |> pog.parameter(pog.int(revision))
        |> pog.parameter(pog.text(factos.event_type_name(type_)))
        |> pog.parameter(pog.int(version))
        |> pog.parameter(pog.text(tags_to_text(tags)))
        |> pog.parameter(pog.text(metadata_to_text(metadata)))
        |> pog.parameter(pog.bytea(data))
        |> pog.returning(int_field_decoder())
        |> pog.execute(on: connection)
        |> result.map_error(StoreError),
      )
      let position = case returned.rows {
        [position, ..] -> factos.SequencePosition(position)
        [] -> position
      }
      let recorded =
        factos.Recorded(
          id: id,
          stream: stream_name,
          revision: revision,
          position: position,
          type_: type_,
          version: version,
          tags: tags,
          metadata: metadata,
          event: event,
        )
      use _ <- result.try(insert_event_tags(connection, position, tags))
      insert_events(
        connection,
        stream_name,
        rest,
        codec,
        revision + 1,
        position,
        [recorded, ..recorded_events],
      )
    }
  }
}

fn insert_outbox_effects(
  connection: pog.Connection,
  events: List(factos.Recorded(event)),
  reactor: factos.Reactor(event, effect),
  effect_codec: EffectCodec(effect),
) -> Result(Nil, Error(domain_error)) {
  case events {
    [] -> Ok(Nil)
    [event, ..rest] -> {
      use _ <- result.try(insert_outbox_effects_for_event(
        connection,
        event,
        factos.react(reactor, event),
        effect_codec,
      ))
      insert_outbox_effects(connection, rest, reactor, effect_codec)
    }
  }
}

fn insert_outbox_effects_for_event(
  connection: pog.Connection,
  event: factos.Recorded(event),
  effects: List(effect),
  effect_codec: EffectCodec(effect),
) -> Result(Nil, Error(domain_error)) {
  case effects {
    [] -> Ok(Nil)
    [effect, ..rest] -> {
      use _ <- result.try(insert_outbox_effect(
        connection,
        event,
        effect,
        effect_codec,
      ))
      insert_outbox_effects_for_event(connection, event, rest, effect_codec)
    }
  }
}

fn insert_outbox_effect(
  connection: pog.Connection,
  event: factos.Recorded(event),
  effect: effect,
  effect_codec: EffectCodec(effect),
) -> Result(Nil, Error(domain_error)) {
  let EffectCodec(encode) = effect_codec
  let ProposedEffect(_, consumer, key, target, type_, metadata, payload) =
    encode(effect)
  let source_position = case event.position {
    factos.NoPosition -> -1
    factos.SequencePosition(position) -> position
  }

  pog.query(
    "
    insert into factos_outbox (
      source_position,
      source_event_id,
      source_context,
      consumer,
      effect_key,
      target,
      type,
      metadata,
      payload
    )
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    on conflict (consumer, effect_key) do nothing
    ",
  )
  |> pog.parameter(pog.int(source_position))
  |> pog.parameter(pog.text(event.id))
  |> pog.parameter(pog.text(event.stream))
  |> pog.parameter(pog.text(consumer))
  |> pog.parameter(pog.text(key))
  |> pog.parameter(pog.text(target))
  |> pog.parameter(pog.text(type_))
  |> pog.parameter(pog.text(metadata_to_text(metadata)))
  |> pog.parameter(pog.bytea(payload))
  |> pog.execute(on: connection)
  |> result.map(fn(_) { Nil })
  |> result.map_error(StoreError)
}

fn read_matching_events(
  connection: pog.Connection,
  query: factos.Query,
  codec: EventCodec(event),
) -> Result(List(factos.Recorded(event)), Error(domain_error)) {
  let QuerySql(where_sql, parameters) = query_to_sql(query, 1)
  use rows <- result.try(
    pog.query(
      "select position, id, stream, revision, type, version, tags, metadata, data
       from factos_events
       "
      <> where_sql
      <> "
       order by position",
    )
    |> with_parameters(parameters)
    |> pog.returning(stored_event_decoder())
    |> pog.execute(on: connection)
    |> result.map(fn(returned) { returned.rows })
    |> result.map_error(StoreError),
  )
  decode_rows(rows, codec)
}

fn read_stream_events(
  connection: pog.Connection,
  stream_name: String,
  codec: EventCodec(event),
) -> Result(List(factos.Recorded(event)), Error(domain_error)) {
  use rows <- result.try(
    pog.query(
      "select position, id, stream, revision, type, version, tags, metadata, data from factos_events where stream = $1 order by revision",
    )
    |> pog.parameter(pog.text(stream_name))
    |> pog.returning(stored_event_decoder())
    |> pog.execute(on: connection)
    |> result.map(fn(returned) { returned.rows })
    |> result.map_error(StoreError),
  )
  decode_rows(rows, codec)
}

fn decode_rows(
  rows: List(StoredEvent),
  codec: EventCodec(event),
) -> Result(List(factos.Recorded(event)), Error(domain_error)) {
  case rows {
    [] -> Ok([])
    [row, ..rest] -> {
      use recorded <- result.try(decode_row(row, codec))
      use rest <- result.try(decode_rows(rest, codec))
      Ok([recorded, ..rest])
    }
  }
}

fn decode_row(
  row: StoredEvent,
  codec: EventCodec(event),
) -> Result(factos.Recorded(event), Error(domain_error)) {
  let EventCodec(_, decode_event) = codec
  use decoded <- result.try(decode_event(row) |> result.map_error(DecodeError))
  let factos.Decoded(event, type_, version, tags, metadata) = decoded
  let StoredEvent(position, id, stream, revision, _, _, _, _, _) = row

  Ok(factos.Recorded(
    id: id,
    stream: stream,
    revision: revision,
    position: factos.SequencePosition(position),
    type_: type_,
    version: version,
    tags: tags,
    metadata: metadata,
    event: event,
  ))
}

fn stored_event_decoder() -> decode.Decoder(StoredEvent) {
  use position <- decode.field(0, decode.int)
  use id <- decode.field(1, decode.string)
  use stream <- decode.field(2, decode.string)
  use revision <- decode.field(3, decode.int)
  use type_name <- decode.field(4, decode.string)
  use version <- decode.field(5, decode.int)
  use tags <- decode.field(6, decode.string)
  use metadata <- decode.field(7, decode.string)
  use data <- decode.field(8, decode.bit_array)
  decode.success(StoredEvent(
    position: position,
    id: id,
    stream: stream,
    revision: revision,
    type_: factos.event_type(type_name),
    version: version,
    tags: tags_from_text(tags),
    metadata: metadata_from_text(metadata),
    data: data,
  ))
}

fn current_revision(
  connection: pog.Connection,
  stream_name: String,
) -> Result(Int, pog.QueryError) {
  use returned <- result.try(
    pog.query(
      "select coalesce(max(revision), -1) from factos_events where stream = $1",
    )
    |> pog.parameter(pog.text(stream_name))
    |> pog.returning(int_field_decoder())
    |> pog.execute(on: connection),
  )

  case returned.rows {
    [revision, ..] -> Ok(revision)
    [] -> Ok(-1)
  }
}

fn has_matching_events_after(
  connection: pog.Connection,
  query: factos.Query,
  after: factos.SequencePosition,
) -> Result(Bool, pog.QueryError) {
  let QuerySql(where_sql, parameters) = matching_events_after_sql(query, after)
  pog.query("select 1
     from factos_events
     " <> where_sql <> "
     limit 1")
  |> with_parameters(parameters)
  |> pog.returning(int_field_decoder())
  |> pog.execute(on: connection)
  |> result.map(fn(returned) {
    case returned.rows {
      [] -> False
      [_, ..] -> True
    }
  })
}

fn int_field_decoder() -> decode.Decoder(Int) {
  use value <- decode.field(0, decode.int)
  decode.success(value)
}

fn stream_revision(events: List(factos.Recorded(event))) -> factos.Revision {
  case list.reverse(events) {
    [] -> factos.NoEvents
    [event, ..] -> factos.CurrentRevision(event.revision)
  }
}

fn highest_recorded_position(
  events: List(factos.Recorded(event)),
) -> factos.SequencePosition {
  case list.reverse(events) {
    [] -> factos.NoPosition
    [event, ..] -> event.position
  }
}

fn expected_matches(expected: factos.Revision, current: Int) -> Bool {
  case expected {
    factos.NoEvents -> current == -1
    factos.CurrentRevision(revision) -> current == revision
  }
}

fn revision_to_int(revision: factos.Revision) -> Int {
  case revision {
    factos.NoEvents -> -1
    factos.CurrentRevision(revision) -> revision
  }
}

fn insert_event_tags(
  connection: pog.Connection,
  position: factos.SequencePosition,
  tags: List(factos.Tag),
) -> Result(Nil, Error(domain_error)) {
  case position, tags {
    factos.NoPosition, _ -> Ok(Nil)
    _, [] -> Ok(Nil)
    factos.SequencePosition(position), [_, ..] -> {
      use _ <- result.try(
        pog.query(
          "
          insert into factos_event_tags(position, tag)
          select $1, unnest($2::text[])
          on conflict do nothing
          ",
        )
        |> pog.parameter(pog.int(position))
        |> pog.parameter(pog.array(
          fn(tag) { pog.text(factos.tag_value(tag)) },
          tags,
        ))
        |> pog.execute(on: connection)
        |> result.map_error(StoreError),
      )
      Ok(Nil)
    }
  }
}

fn query_to_sql(query: factos.Query, parameter_index: Int) -> QuerySql {
  case query {
    factos.AllEvents -> QuerySql(sql: "", parameters: [])
    factos.Query(items) -> {
      case items {
        [] -> QuerySql(sql: "where 1 = 0", parameters: [])
        [_, ..] -> {
          let #(sql, parameters, _) =
            build_query_items_sql(items, parameter_index, [], [])
          QuerySql(
            sql: "where " <> string.join(list.reverse(sql), with: " or "),
            parameters: list.reverse(parameters),
          )
        }
      }
    }
  }
}

fn matching_events_after_sql(
  query: factos.Query,
  after: factos.SequencePosition,
) -> QuerySql {
  let after_position = case after {
    factos.NoPosition -> -1
    factos.SequencePosition(position) -> position
  }

  case query {
    factos.AllEvents ->
      QuerySql(sql: "where position > $1", parameters: [
        IntParameter(after_position),
      ])
    factos.Query(items) -> {
      case items {
        [] -> QuerySql(sql: "where 1 = 0", parameters: [])
        [_, ..] -> {
          let #(sql, parameters, _) =
            build_query_items_sql(items, 2, [], [IntParameter(after_position)])
          QuerySql(
            sql: "where position > $1 and ("
              <> string.join(list.reverse(sql), with: " or ")
              <> ")",
            parameters: list.reverse(parameters),
          )
        }
      }
    }
  }
}

fn build_query_items_sql(
  items: List(factos.QueryItem),
  parameter_index: Int,
  sql: List(String),
  parameters: List(QueryParameter),
) -> #(List(String), List(QueryParameter), Int) {
  case items {
    [] -> #(sql, parameters, parameter_index)
    [item, ..rest] -> {
      let QuerySql(item_sql, item_parameters) =
        query_item_to_sql(item, parameter_index)
      build_query_items_sql(
        rest,
        parameter_index + list.length(item_parameters),
        [item_sql, ..sql],
        list.append(list.reverse(item_parameters), parameters),
      )
    }
  }
}

fn query_item_to_sql(item: factos.QueryItem, parameter_index: Int) -> QuerySql {
  let factos.QueryItem(types, tags) = item
  let QuerySql(type_sql, type_parameters) = types_to_sql(types, parameter_index)
  let QuerySql(tag_sql, tag_parameters) =
    tags_to_sql(tags, parameter_index + list.length(type_parameters))

  QuerySql(
    sql: "(" <> type_sql <> " and " <> tag_sql <> ")",
    parameters: list.append(type_parameters, tag_parameters),
  )
}

fn types_to_sql(
  types: List(factos.EventType),
  parameter_index: Int,
) -> QuerySql {
  case types {
    [] -> QuerySql(sql: "1 = 1", parameters: [])
    [_, ..] ->
      QuerySql(
        sql: "type in ("
          <> placeholders(parameter_index, list.length(types))
          <> ")",
        parameters: list.map(types, fn(type_) {
          TextParameter(factos.event_type_name(type_))
        }),
      )
  }
}

fn tags_to_sql(tags: List(factos.Tag), parameter_index: Int) -> QuerySql {
  case tags {
    [] -> QuerySql(sql: "1 = 1", parameters: [])
    [_, ..] -> {
      let clauses =
        tags
        |> list.index_map(fn(_tag, index) { "exists (
            select 1 from factos_event_tags
            where factos_event_tags.position = factos_events.position
              and factos_event_tags.tag = $" <> int.to_string(
            parameter_index + index,
          ) <> "
          )" })
      QuerySql(
        sql: "(" <> string.join(clauses, with: " and ") <> ")",
        parameters: list.map(tags, fn(tag) {
          TextParameter(factos.tag_value(tag))
        }),
      )
    }
  }
}

fn placeholders(start: Int, count: Int) -> String {
  placeholder_indices(start, count, [])
  |> list.map(int.to_string)
  |> list.map(fn(index) { "$" <> index })
  |> string.join(with: ", ")
}

fn placeholder_indices(
  start: Int,
  remaining: Int,
  acc: List(Int),
) -> List(Int) {
  case remaining <= 0 {
    True -> list.reverse(acc)
    False -> placeholder_indices(start + 1, remaining - 1, [start, ..acc])
  }
}

fn with_parameters(
  query: pog.Query(row),
  parameters: List(QueryParameter),
) -> pog.Query(row) {
  case parameters {
    [] -> query
    [parameter, ..rest] -> {
      let query = case parameter {
        IntParameter(value) -> pog.parameter(query, pog.int(value))
        TextParameter(value) -> pog.parameter(query, pog.text(value))
      }
      with_parameters(query, rest)
    }
  }
}

/// Lease pending outbox messages for a consumer and target.
///
/// Leased messages stay in `pending` status but are hidden from competing
/// workers until `locked_until` expires.
pub fn lease_outbox(
  connection: pog.Connection,
  consumer consumer: String,
  target target: String,
  limit limit: Int,
  lease_for_milliseconds lease_for_milliseconds: Int,
) -> Result(List(OutboxMessage), Error(_)) {
  case limit <= 0 {
    True -> Ok([])
    False ->
      pog.query(
        "
        update factos_outbox
        set
          locked_until = now() + ($4::integer * interval '1 millisecond'),
          attempts = attempts + 1
        where id in (
          select id
          from factos_outbox
          where consumer = $1
            and target = $2
            and status = 'pending'
            and available_at <= now()
            and (locked_until is null or locked_until <= now())
          order by id
          limit $3
          for update skip locked
        )
        returning
          id,
          source_position,
          source_event_id,
          source_context,
          consumer,
          effect_key,
          target,
          type,
          metadata,
          payload
        ",
      )
      |> pog.parameter(pog.text(consumer))
      |> pog.parameter(pog.text(target))
      |> pog.parameter(pog.int(limit))
      |> pog.parameter(pog.int(lease_for_milliseconds))
      |> pog.returning(outbox_message_decoder())
      |> pog.execute(on: connection)
      |> result.map(fn(returned) { returned.rows })
      |> result.map_error(StoreError)
  }
}

/// Mark an outbox message as delivered.
pub fn ack_outbox(
  connection: pog.Connection,
  id id: Int,
) -> Result(Nil, Error(_)) {
  pog.query(
    "
    update factos_outbox
    set status = 'delivered',
        delivered_at = now(),
        locked_until = null
    where id = $1
    ",
  )
  |> pog.parameter(pog.int(id))
  |> pog.execute(on: connection)
  |> result.map(fn(_) { Nil })
  |> result.map_error(StoreError)
}

/// Release an outbox message for retry after a delay.
pub fn nack_outbox(
  connection: pog.Connection,
  id id: Int,
  error error: String,
  retry_after_milliseconds retry_after_milliseconds: Int,
) -> Result(Nil, Error(_)) {
  pog.query(
    "
    update factos_outbox
    set locked_until = null,
        last_error = $2,
        available_at = now() + ($3::integer * interval '1 millisecond')
    where id = $1
    ",
  )
  |> pog.parameter(pog.int(id))
  |> pog.parameter(pog.text(error))
  |> pog.parameter(pog.int(retry_after_milliseconds))
  |> pog.execute(on: connection)
  |> result.map(fn(_) { Nil })
  |> result.map_error(StoreError)
}

fn outbox_message_decoder() -> decode.Decoder(OutboxMessage) {
  use id <- decode.field(0, decode.int)
  use source_position <- decode.field(1, decode.int)
  use source_event_id <- decode.field(2, decode.string)
  use source_context <- decode.field(3, decode.string)
  use consumer <- decode.field(4, decode.string)
  use key <- decode.field(5, decode.string)
  use target <- decode.field(6, decode.string)
  use type_ <- decode.field(7, decode.string)
  use metadata <- decode.field(8, decode.string)
  use payload <- decode.field(9, decode.bit_array)
  decode.success(OutboxMessage(
    id: id,
    source_position: factos.SequencePosition(source_position),
    source_event_id: source_event_id,
    source_context: source_context,
    consumer: consumer,
    key: key,
    target: target,
    type_: type_,
    metadata: metadata_from_text(metadata),
    payload: payload,
  ))
}

fn tags_to_text(tags: List(factos.Tag)) -> String {
  case tags {
    [] -> ""
    [_, ..] ->
      "\n"
      <> { tags |> list.map(factos.tag_value) |> string.join(with: "\n") }
      <> "\n"
  }
}

fn tags_from_text(tags: String) -> List(factos.Tag) {
  case string.is_empty(tags) {
    True -> []
    False ->
      tags
      |> string.split(on: "\n")
      |> list.filter(fn(tag) { !string.is_empty(tag) })
      |> list.map(factos.tag)
  }
}

fn metadata_to_text(metadata: factos.Metadata) -> String {
  metadata
  |> factos.metadata_entries
  |> list.map(fn(entry) { entry.0 <> "=" <> entry.1 })
  |> string.join(with: "\n")
}

fn metadata_from_text(metadata: String) -> factos.Metadata {
  case string.is_empty(metadata) {
    True -> factos.empty_metadata()
    False ->
      metadata
      |> string.split(on: "\n")
      |> list.filter_map(fn(entry) {
        case string.split(entry, on: "=") {
          [key, value] -> Ok(#(key, value))
          _ -> Error(Nil)
        }
      })
      |> factos.metadata
  }
}

pub fn error_to_string(
  error: Error(domain_error),
  domain_error_to_string: fn(domain_error) -> String,
) -> String {
  case error {
    DomainError(error) -> domain_error_to_string(error)
    StoreError(error) -> "store error: " <> query_error_to_string(error)
    AppendConditionFailed(factos.NoAppendCondition) ->
      "append to event failed: No append condition"
    AppendConditionFailed(factos.FailIfEventsMatch(query: _, after: _)) ->
      "append to event failed: Events matched"
    DecodeError(UnknownEvent) -> "unknown event decoded"
    DecodeError(InvalidData) -> "invalid data stored in database"
  }
}

fn query_error_to_string(error: pog.QueryError) -> String {
  case error {
    pog.ConstraintViolated(message:, constraint:, detail:) ->
      "constraint violated: " <> constraint <> ": " <> message <> " " <> detail
    pog.PostgresqlError(code:, name:, message:) ->
      "postgresql error " <> code <> " " <> name <> ": " <> message
    pog.UnexpectedArgumentCount(expected:, got:) ->
      "unexpected argument count: expected "
      <> int.to_string(expected)
      <> ", got "
      <> int.to_string(got)
    pog.UnexpectedArgumentType(expected:, got:) ->
      "unexpected argument type: expected " <> expected <> ", got " <> got
    pog.UnexpectedResultType(errors) ->
      "unexpected result type: " <> int.to_string(list.length(errors))
    pog.QueryTimeout -> "query timeout"
    pog.ConnectionUnavailable -> "connection unavailable"
  }
}
