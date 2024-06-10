create table person (
  id serial primary key
);

create table name (
  id integer references person(id) not null,
  name text not null
);

create table age (
  id integer references person(id) not null ,
  age integer not null
);
