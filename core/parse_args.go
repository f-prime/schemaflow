package core

import (
	"flag"
	"log"
)

func ParseArgs() *Context {
  host := flag.String("host", "127.0.0.1", "host")
  port := flag.Int("port", 5432, "port") 
  user := flag.String("user", "postgres", "user")
  password := flag.String("password", "postgres", "password")
  db_name := flag.String("db", "", "db") 

  sql_path := flag.String("sql-path", "./", "sql-path")
  migration_path := flag.String("migrations-path", "./schemaflow_migrations", "migrations-path")

  action := flag.String("action", "", "action")

  flag.Parse()

  var action_enum ActionType

  if *db_name == "" {
    log.Fatalln("'db' is required.")
  }

  if *action == "" {
    log.Fatalln("'action' is required.")
  } else if *action == ACTION_CLEAN {
    action_enum = CLEAN
  } else if *action == ACTION_MIGRATE {
    action_enum = MIGRATE 
  } else if *action == ACTION_MAKE_MIGRATIONS {
    action_enum = MAKEMIGRATIONS
  } else {
    log.Fatalf("Unknown action %s\n", *action)
  }

  ctx := new(Context);

  ctx.DbContext = &DbContext{
    *host,
    *port,
    *user,
    *password,
    *db_name,
  }

  ctx.SqlPath = *sql_path
  ctx.Action = action_enum
  ctx.MigrationPath = *migration_path

  return ctx
}
