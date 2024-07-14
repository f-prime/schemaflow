package core

import "os"

const MIGRATION_SCHEMA = `

create schema if not exists schemaflow;

create table if not exists schemaflow.migrations (
  file_name text primary key not null,
  file_hash text not null,
  created timestamp default now()
);

create table if not exists schemaflow.statements (
  id serial primary key,
  stmt text not null,
  stmt_hash text unique not null,
  stmt_type integer not null,
  stmt_name text default null,
  created timestamp default now(),
  updated timestamp default now()
);

create index on schemaflow.statements(stmt_hash);
`

func initializeMigrationsSchema(ctx *Context) {
  _, err := ctx.Db.Exec(MIGRATION_SCHEMA)
  perr(err)
}

func initializeMigrationsFolder(ctx *Context) {
  if !DoesPathExist(ctx.MigrationPath) {
    os.MkdirAll(ctx.MigrationPath, 0755)
  }
}

func Initialize(ctx *Context) {
  initializeMigrationsFolder(ctx)
  initializeMigrationsSchema(ctx)
}
