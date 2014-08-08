create table impression_stats_daily(
  impressions integer default 0,
  clicks integer default 0,
  pinned integer default 0,
  blocked integer default 0,
  tile_id integer not null,
  day date not null,
  position integer not null,
  locale char(5) not null,
  country_code char(2) not null,
  os varchar(64) not null,
  browser varchar(64) not null,
  version varchar(64) not null
);

create table unique_counts_daily(
  id serial not null,
  impression boolean default true,
  tile_id integer not null,
  day date default CURRENT_DATE,
  locale char(5) default 'en-us',
  country_code char(2) default 'US'
);

create table unique_hlls(
  unique_counts_daily_id integer not null,
  index smallint,
  value smallint
);