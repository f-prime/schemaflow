create table person (
    id serial primary key,
    created timestamp default now()
);

create table person_name (
    person_id bigint references person(id) not null,
    created timestamp default now(),
    name text
);
