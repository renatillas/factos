-- migrate:up
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
);

create index if not exists factos_outbox_pending
  on factos_outbox(status, available_at, id);

-- migrate:down
drop table if exists factos_outbox;
