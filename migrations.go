package main

import (
	"fmt"
	"log"
)

/* 

UNIVERSAL STRUCTURES

- Roles
- Tablespaces

*/

func build_string_array[T any](vals []T, f func(T) string) []string {
  var result []string

  for _, v := range vals {
    result = append(result, f(v))
  }

  return result
}

func compute_added_objects(d Diffable) []string {
  var added_items []string

  for _, n := range d.GetNewNames() {
    found := false
    for _, c := range d.GetCurrentNames() {
      if c == n {
        found = true
        break
      }
    }

    if !found {
      added_items = append(added_items, n)
    }
  }

  return added_items 
}

func compute_removed_objects(d Diffable) []string {
  var removed_items []string

  for _, n := range d.GetCurrentNames() {
    found := false
    for _, c := range d.GetNewNames() {
      if c == n {
        found = true
        break
      }
    }

    if !found {
      removed_items = append(removed_items, n)
    }
  }

  return removed_items 
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
  list_current := get_list_of_schemas(ctx.db)
  list_new := get_list_of_schemas(ctx.migration_db)

  diffable_schemas := DiffableSchema {
    current_schemas: list_current,
    new_schemas: list_new,
  }

  return build_migrations_for(ctx, &diffable_schemas)
}

func generate_role_migrations(ctx *Context) []string {
  var roles []string
  return roles
}

func generate_sequence_migrations(ctx *Context) []string {
  var migrations []string
  return migrations
}

func generate_table_migrations(ctx *Context) []string {
  list_current := get_list_of_tables(ctx.db)
  list_new := get_list_of_tables(ctx.migration_db)

  current_map := make(map[string]*PgTable)
  new_map := make(map[string]*PgTable)

  for _, c := range list_current {
    current_map[c.relnamespace + "." + c.relname] = &c
  }

  for _, c := range list_new {
    new_map[c.relnamespace + "." + c.relname] = &c
  }

  diffable_tables := DiffableTable {
    current_tables: list_current,
    new_tables: list_new,
    current_tables_map: current_map,
    new_tables_map: new_map,
  }

  return build_migrations_for(ctx, &diffable_tables)
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
  // migrations = append(migrations, generate_role_migrations(ctx)...)
  migrations = append(migrations, generate_table_migrations(ctx)...)

  fmt.Println("MIGRATIONS:")
  for _, m := range migrations {
    fmt.Printf("%v\n", m)
  }

  return migrations
}
