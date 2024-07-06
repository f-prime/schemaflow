create extension pg_trgm;

create table person (
  id serial primary key,
  created timestamp default now(),
  updated timestamp default now()
);

create table name (
  id integer references person(id) not null,
  name text not null,
  created timestamp default now()
);

create table age (
  id integer references person(id) not null ,
  age integer not null,
  created timestamp default now()
);
