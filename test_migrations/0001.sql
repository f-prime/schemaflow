ALTER TABLE person ADD COLUMN created timestamp default now();
ALTER TABLE person_name ADD COLUMN created timestamp default now();
