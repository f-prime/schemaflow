CREATE TABLE person (id serial PRIMARY KEY);
CREATE TABLE person_name (person_id bigint REFERENCES person (id) NOT NULL, name text);