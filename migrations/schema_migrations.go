package migrations

import (
	"database/sql"
        "schemaflow/core"
	"fmt"
)

type DiffableSchema struct {
  current_schemas []PgSchema
  new_schemas []PgSchema
}

func (x *DiffableSchema) GetCurrentNames() []string {
  return buildStringArray[PgSchema](x.current_schemas, func(x PgSchema) string {
    return x.nspname
  })
}

func (x *DiffableSchema) GetNewNames() []string {
  return buildStringArray[PgSchema](x.new_schemas, func(x PgSchema) string {
    return x.nspname
  });
}

func (x *DiffableSchema) GenerateDropStmts(ctx *core.Context) []string {
  return buildStringArray[string](computeRemovedObjects(x), func(x string) string {
    return fmt.Sprintf("DROP SCHEMA IF EXISTS %s;", x)
  })
}

func (x *DiffableSchema) GenerateAddStmts(ctx *core.Context) []string {
  return buildStringArray[string](computeAddedObjects(x), func(x string) string {
    return fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", x)
  })
}

func (x *DiffableSchema) GenerateUpdateStmts(ctx *core.Context) []string {
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
