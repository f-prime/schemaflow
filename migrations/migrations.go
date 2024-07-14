package migrations

import (
	"fmt"
	"log"
        "schemaflow/core"
)

/* 

UNIVERSAL STRUCTURES

- Roles
- Tablespaces

*/

func perr(err error) {
  core.Perr(err)
}

func buildStringArray[T any](vals []T, f func(T) string) []string {
  var result []string

  for _, v := range vals {
    result = append(result, f(v))
  }

  return result
}

func computeAddedObjects(d Diffable) []string {
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

func computeRemovedObjects(d Diffable) []string {
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
  GenerateDropStmts(ctx *core.Context) []string
  GenerateAddStmts(ctx *core.Context) []string
  GenerateUpdateStmts(ctx *core.Context) []string
  GetCurrentNames() []string
  GetNewNames() []string
}

func buildMigrationsFor(ctx *core.Context, x Diffable) []string {
  var migrations []string

  migrations = append(migrations, x.GenerateDropStmts(ctx)...)
  migrations = append(migrations, x.GenerateAddStmts(ctx)...)
  migrations = append(migrations, x.GenerateUpdateStmts(ctx)...)

  return migrations
}


func generateExtensionMigrations(ctx *core.Context) []string {
  list_current := get_list_of_extensions(ctx.Db)
  list_new := get_list_of_extensions(ctx.MigrationDb)

  diffable_exts := DiffableExtensions {
    current_extensions: list_current,
    new_extensions: list_new,
  }

  return buildMigrationsFor(ctx, &diffable_exts)
}

func generateSchemaMigrations(ctx *core.Context) []string {
  list_current := get_list_of_schemas(ctx.Db)
  list_new := get_list_of_schemas(ctx.MigrationDb)

  diffable_schemas := DiffableSchema {
    current_schemas: list_current,
    new_schemas: list_new,
  }

  return buildMigrationsFor(ctx, &diffable_schemas)
}

func generateRoleMigrations(ctx *core.Context) []string {
  var roles []string
  return roles
}

func generateSequenceMigrations(ctx *core.Context) []string {
  var migrations []string
  return migrations
}

func generateTableMigrations(ctx *core.Context) []string {
  list_current := get_list_of_tables(ctx.Db)
  list_new := get_list_of_tables(ctx.MigrationDb)

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

  return buildMigrationsFor(ctx, &diffable_tables)
}

func generateColumnsMigrations(ctx *core.Context) []string {
  var migrations []string
  return migrations
}

func generateIndexesMigrations(ctx *core.Context) []string {
  var migrations []string
  return migrations
}

func generateViewsMigrations(ctx *core.Context) []string {
  var migrations []string

  //list_current := get_list_of_views(ctx.db)
  //list_new := get_list_of_views(ctx.migration_db)

  return migrations
}

func generateMatviewsMigrations(ctx *core.Context) []string {
  var migrations []string
  return migrations
}

func generateForeignKeysMigrations(ctx *core.Context) []string {
  var migrations []string
  return migrations
}

func generateFunctionsMigrations(ctx *core.Context) []string {
  var migrations []string
  return migrations
}

func generateTriggersMigrations(ctx *core.Context) []string {
  var migrations []string
  return migrations
}

func generateOwnersMigrations(ctx *core.Context) []string {
  var migrations []string
  return migrations
}

func generateGrantRelationshipsMigrations(ctx *core.Context) []string {
  var migrations []string
  return migrations
}

func generateGrantAttributesMigrations(ctx *core.Context) []string {
  var migrations []string
  return migrations
}

func GenerateMigrations(ctx *core.Context) []string {
  var migrations []string

  log.Println("Generating migrations...")

  migrations = append(migrations, generateExtensionMigrations(ctx)...)
  migrations = append(migrations, generateSchemaMigrations(ctx)...)
  // migrations = append(migrations, generate_role_migrations(ctx)...)
  migrations = append(migrations, generateTableMigrations(ctx)...)

  fmt.Println("MIGRATIONS:")
  for _, m := range migrations {
    fmt.Printf("%v\n", m)
  }

  return migrations
}
