create table if not exists factos_events (
  position integer primary key autoincrement,
  id text not null,
  stream text not null,
  revision integer not null,
  type text not null,
  version integer not null,
  tags text not null,
  metadata text not null default '',
  data blob not null,
  unique(stream, revision)
);

create index if not exists factos_events_stream_revision
  on factos_events(stream, revision);

create index if not exists factos_events_position
  on factos_events(position);
