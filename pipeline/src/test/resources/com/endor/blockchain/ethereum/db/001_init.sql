-- table declarations :
create table summaries (
    data blob not null,
    id bigint not null,
    blockNumber bigint not null
  );
-- indexes on summaries
create unique index idx20ac04d1 on summaries (id);
