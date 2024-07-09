package main

import (
	"database/sql"
	"fmt"
)

type DiffableExtensions struct {
  current_extensions []PgExtension
  new_extensions []PgExtension
}

func (x *DiffableExtensions) GetCurrentNames() []string {
  return build_string_array[PgExtension](x.current_extensions, func(x PgExtension) string {
    return x.extname
  })
}

func (x *DiffableExtensions) GetNewNames() []string {
  return build_string_array[PgExtension](x.new_extensions, func(x PgExtension) string {
    return x.extname
  })
}

func (x *DiffableExtensions) GenerateDropStmts(ctx *Context) []string {
  return build_string_array[string](
    compute_removed_objects(x),
    func(x string) string {
      return fmt.Sprintf("DROP EXTENSION IF EXISTS %s;", x)
    },
  )
}

func (x *DiffableExtensions) GenerateAddStmts(ctx *Context) []string {
  return build_string_array[string](
    compute_added_objects(x),
    func(x string) string {
      return fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s;", x)
    },
  )
}

func (x *DiffableExtensions) GenerateUpdateStmts(ctx *Context) []string {
  var updates []string
  return updates
}

type PgExtension struct {
  extname string
}


func get_list_of_extensions(db *sql.DB) []PgExtension {
  var extensions []PgExtension
  r, err := db.Query(`
    select extname from pg_catalog.pg_extension
  `)

  defer r.Close()

  perr(err)

  for r.Next() {
    var extname string
    r.Scan(&extname)
    extensions = append(extensions, PgExtension { extname: extname })
  }

  return extensions
}

