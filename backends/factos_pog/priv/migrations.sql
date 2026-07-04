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
);

create index if not exists factos_events_stream_revision
  on factos_events(stream, revision);

create index if not exists factos_events_position
  on factos_events(position);

create table if not exists factos_event_tags (
  position bigint not null references factos_events(position) on delete cascade,
  tag text not null,
  primary key(position, tag)
);

create index if not exists factos_events_type_position
  on factos_events(type, position);

create index if not exists factos_event_tags_tag_position
  on factos_event_tags(tag, position);

insert into factos_event_tags(position, tag)
select factos_events.position, split_tags.tag
from factos_events
cross join lateral regexp_split_to_table(factos_events.tags, E'\n') as split_tags(tag)
where split_tags.tag <> ''
on conflict do nothing;
