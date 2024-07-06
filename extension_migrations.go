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
  var names []string

  for _, n := range x.current_extensions {
    names = append(names, n.extname)
  }

  return names
}

func (x *DiffableExtensions) GetNewNames() []string {
  var names []string

  for _, n := range x.new_extensions {
    names = append(names, n.extname)
  }

  return names
}

func (x *DiffableExtensions) GenerateDropStmts(ctx *Context) []string {
  var drops []string

  for _, v := range compute_not_in_new(x) {
    drops = append(drops, fmt.Sprintf("DROP EXTENSION IF EXISTS %s;", v))
  }

  return drops
}

func (x *DiffableExtensions) GenerateAddStmts(ctx *Context) []string {
  var adds []string

  for _, v := range compute_not_in_old(x) {
    adds = append(adds, fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s;", v))
  }

  return adds
}

func (x *DiffableExtensions) GenerateUpdateStmts(ctx *Context) []string {
  var updates []string
  return updates
}

type PgExtension struct {
  extname string
}

func (x *PgExtension) Rep() string {
  return x.extname
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

