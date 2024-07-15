# SchemaFlow

A database schema management tool for SQL-first workflows. It bridges the gap between traditional ORM migration patterns and direct SQL schema authoring. By adapting the familiar "model-migrate-apply" process to pure SQL environments, SchemaFlow streamlines schema evolution for developers who prefer writing raw SQL over using ORM abstractions.

Join our [Discord Server](https://discord.gg/q7nQPxYQuE)

## What is SchemaFlow?

Database schemas evolve over time, often becoming complex and difficult to understand through migration files alone. ORMs address this by separating schema definition (models) from migrations, allowing developers to modify models that represent the database structure. These changes are then compiled into migration files and executed, making it easier to comprehend the schema without external visualization tools.

However, SQL-first migration tools typically lack this separation, leading to potential schema drift. As a result, individual migration files may not accurately reflect the current database state, requiring developers to either read through all migrations or rely on external tools for a complete understanding.

SchemaFlow bridges this gap for SQL-first, code-first developers. It introduces a clear distinction between schema definitions and migrations, bringing the benefits of ORM-style workflows to those who prefer writing raw SQL. This approach allows for easier schema management and understanding, even as the database structure grows more complex over time.

## Install

#### From Source

1. `git clone https://github.com/f-prime/schemaflow`
2. `cd schemaflow`
3. `go mod download`
4. `go build`
5. `./schemaflow help`

#### Docker Install

1. `docker pull fprime1/schemaflow`
2. `docker run --rm fprime1/schemaflow help`

## Usage

```
SchemaFlow

Usage
  schemaflow [options] [cmd]

Options
  --host              Database host name
  --port              Database port number
  --user              Database user
  --password          Database user password
  --db                Database name
  --sql-path          The path to your database schema files
  --migrations-path   The path where your migration files will be generated.

Commands
  make          Compute schema changes in --sql-path and generate a new migration file. New migrations will be placed in the --migration-path
  migrate       Run unexecuted migration files in the --migration-path
  help          Open this menu

Examples
  schemaflow --host=127.0.0.1 --port=5432 --user=postgres --password=postgres --db=example --sql-path=/path/to/my/schema/sql --migration-path=./project/migrations make
  schemaflow help
```

### Make 

The `make` command walks the `--sql-path` directory, parses the code, and extracts the individual statements. It then performs change detection to see which of those statements have been created, updated, or deleted. It will then create a new migration file inside of your `--migrations-path` directory. What it writes to this file depends on the result of the change detection.  

There is no need to name your files in any special way inside of the `--sql-path`. SchemaFlow will build a dependency graph from your schema files and order the resulting migrations accordingly.

SchemaFlow does not automatically generation the migration code for you. Instead, it generates a statement diff comment that you then have to replace with the appropriate statement for the given change. See `An example flow` below for a complete example.

### Migrate

The `migrate` command will execute all of the migrations inside of `--migration-path` that have not yet been executed. 

### An example flow

In this example I have a schema path `schema/` and a migrations path of `migrations/`

Let's say that I have the following schema defined in `schema/tables.sql` 

```
create table person (
    id serial primary key,
);

create table person_name (
    person_id bigint references person(id) not null,
    name text
);
```

When I run `./schemaflow make` a new file (`migrations/0000.sql`) will be generated. Since both `person` and `person_name` are brand new tables, the resulting migration will look nearly identical to what is in `schema/tables.sql`.

Now, when I run `./schemaflow migrate` two new tables (`person`, and `person_name`) will appear in my database.

Now I want to modify `person` and `person_name` to both contain a new column `created` that is a timestamp with a default of `now()`, so I modify `schema/tables.sql` to the following.

```
create table person (
    id serial primary key,
    created timestamp default now()
);

create table person_name (
    person_id bigint references person(id) not null,
    created timestamp default now(),
    name text
);
```

When `./schemaflow make` is run now, a new file (`migrations/0001.sql`) is generated with the following two comments:

```
/*
--- REMOVE WHEN MIGRATION RESOLVED ---
---------- CURRENT VERSION ----------
CREATE TABLE person (id serial PRIMARY KEY);
----------   CHANGED TO    ----------
CREATE TABLE person (id serial PRIMARY KEY, created timestamp DEFAULT now());
----------   CHANGE DIFF   ----------
@@@ CREATE TABLE person (id serial PRIMARY KEY
+++ , created timestamp DEFAULT now()
@@@ );
*/
/*
--- REMOVE WHEN MIGRATION RESOLVED ---
---------- CURRENT VERSION ----------
CREATE TABLE person_name (person_id bigint REFERENCES person (id) NOT NULL, name text);
----------   CHANGED TO    ----------
CREATE TABLE person_name (person_id bigint REFERENCES person (id) NOT NULL, created timestamp DEFAULT now(), name text);
----------   CHANGE DIFF   ----------
@@@ CREATE TABLE person_name (person_id bigint REFERENCES person (id) NOT NULL, 
+++ created timestamp DEFAULT now(), 
@@@ name text);
*/
```

SchemaFlow generated two comments describing the two changes that occurred in `person` and `person_name`. Until these migrations are resolved (and the comments removed) SchemaFlow will refuse to create new migrations or execute new migrations.

To resolve these two migrations, we can modify `migrations/0001.sql` to the following two lines:

```
ALTER TABLE person ADD COLUMN created timestamp default now();
ALTER TABLE person_name ADD COLUMN created timestamp default now();
```

To finish we'll run `./schemaflow migrate` and the changes will be applied.

## **WARNING**

SchemaFlow is still in early development. As a result, it is lacking support for many popular databases. At this time, only PostgreSQL is supported. Support for other databases is actively being worked on.
