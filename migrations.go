package main

import (
	"fmt"
	"log"
)

func compute_not_in_old(d Diffable) []string {
  var new_in_old []string

  for _, hs := range d.GetCurrentNames() {
    found := false
    for _, ns := range d.GetNewNames() {
      if hs == ns {
        found = true
        break
      }
    }

    if !found {
      new_in_old = append(new_in_old, hs) 
    }
  }

  return new_in_old
}

func compute_not_in_new(d Diffable) []string {
  var new_not_in_old []string

  for _, hs := range d.GetNewNames() {
    found := false
    for _, ns := range d.GetCurrentNames() {
      if hs == ns {
        found = true
        break
      }
    }

    if !found {
      new_not_in_old = append(new_not_in_old, hs) 
    }
  }

  return new_not_in_old 
}

type Diffable interface {
  GenerateDropStmts(ctx *Context) []string
  GenerateAddStmts(ctx *Context) []string
  GenerateUpdateStmts(ctx *Context) []string
  GetCurrentNames() []string
  GetNewNames() []string
}

func build_migrations_for(ctx *Context, x Diffable) []string {
  var migrations []string

  migrations = append(migrations, x.GenerateDropStmts(ctx)...)
  migrations = append(migrations, x.GenerateAddStmts(ctx)...)
  migrations = append(migrations, x.GenerateUpdateStmts(ctx)...)

  return migrations
}


func generate_extension_migrations(ctx *Context) []string {
  list_current := get_list_of_extensions(ctx.db)
  list_new := get_list_of_extensions(ctx.migration_db)

  diffable_exts := DiffableExtensions {
    current_extensions: list_current,
    new_extensions: list_new,
  }

  return build_migrations_for(ctx, &diffable_exts)
}

func generate_schema_migrations(ctx *Context) []string {
  var migrations []string
  return migrations
}

func generate_role_migrations(ctx *Context) []string {
  var migrations []string
  return migrations
}

func generate_sequence_migrations(ctx *Context) []string {
  var migrations []string
  return migrations
}

func generate_table_migrations(ctx *Context) []string {
  var migrations []string
  return migrations
}

func generate_columns_migrations(ctx *Context) []string {
  var migrations []string
  return migrations
}

func generate_indexes_migrations(ctx *Context) []string {
  var migrations []string
  return migrations
}

func generate_views_migrations(ctx *Context) []string {
  var migrations []string

  //list_current := get_list_of_views(ctx.db)
  //list_new := get_list_of_views(ctx.migration_db)

  return migrations
}

func generate_matviews_migrations(ctx *Context) []string {
  var migrations []string
  return migrations
}

func generate_foreign_keys_migrations(ctx *Context) []string {
  var migrations []string
  return migrations
}

func generate_functions_migrations(ctx *Context) []string {
  var migrations []string
  return migrations
}

func generate_triggers_migrations(ctx *Context) []string {
  var migrations []string
  return migrations
}

func generate_owners_migrations(ctx *Context) []string {
  var migrations []string
  return migrations
}

func generate_grant_relationships_migrations(ctx *Context) []string {
  var migrations []string
  return migrations
}

func generate_grant_attributes_migrations(ctx *Context) []string {
  var migrations []string
  return migrations
}

func generate_migrations(ctx *Context) []string {
  var migrations []string

  log.Println("Generating migrations...")

  migrations = append(migrations, generate_extension_migrations(ctx)...)
  migrations = append(migrations, generate_schema_migrations(ctx)...)

  migrations = append(migrations, generate_views_migrations(ctx)...)

  fmt.Printf("migrations: %v\n", migrations)

  return migrations
}
