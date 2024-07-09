package main

import (
	"database/sql"
	"fmt"
)

type DiffableSchema struct {
  current_schemas []PgSchema
  new_schemas []PgSchema
}

func (x *DiffableSchema) GetCurrentNames() []string {
  return build_string_array[PgSchema](x.current_schemas, func(x PgSchema) string {
    return x.nspname
  })
}

func (x *DiffableSchema) GetNewNames() []string {
  return build_string_array[PgSchema](x.new_schemas, func(x PgSchema) string {
    return x.nspname
  });
}

func (x *DiffableSchema) GenerateDropStmts(ctx *Context) []string {
  return build_string_array[string](compute_removed_objects(x), func(x string) string {
    return fmt.Sprintf("DROP SCHEMA IF EXISTS %s;", x)
  })
}

func (x *DiffableSchema) GenerateAddStmts(ctx *Context) []string {
  return build_string_array[string](compute_added_objects(x), func(x string) string {
    return fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", x)
  })
}

func (x *DiffableSchema) GenerateUpdateStmts(ctx *Context) []string {
  var results []string
  return results
}

type PgSchema struct {
  nspname string
}

func get_list_of_schemas(db *sql.DB) []PgSchema {
  var schemas []PgSchema
  r, err := db.Query(`
    select nspname from pg_catalog.pg_namespace
  `);
  perr(err)
  defer r.Close()

  for r.Next() {
    var nspname string
    r.Scan(&nspname)
    schemas = append(schemas, PgSchema { nspname: nspname })
  }

  return schemas
}
