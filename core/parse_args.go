package core

import (
	"flag"
	"fmt"
	"log"
	"os"
)

const HELP_TEXT = `SchemaFlow

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
`

func showHelp() {
  fmt.Print(HELP_TEXT)
  os.Exit(0)
}

func ParseArgs() *Context {
  host := flag.String("host", "127.0.0.1", "host")
  port := flag.Int("port", 5432, "port") 
  user := flag.String("user", "postgres", "user")
  password := flag.String("password", "postgres", "password")
  db_name := flag.String("db", "", "db") 

  sql_path := flag.String("sql-path", "./", "sql-path")
  migration_path := flag.String("migrations-path", "./schemaflow_migrations", "migrations-path")

  flag.Parse()

  actions := flag.Args()

  if len(actions) == 0 {
    showHelp()
  }

  action := actions[0]

  if action == "help" {
    showHelp()
  }

  var action_enum ActionType

  if *db_name == "" {
    log.Fatalln("'db' is required.")
  }

  if action == "" {
    log.Fatalln("'action' is required.")
  } else if action == ACTION_CLEAN {
    action_enum = CLEAN
  } else if action == ACTION_MIGRATE {
    action_enum = MIGRATE 
  } else if action == ACTION_MAKE_MIGRATIONS {
    action_enum = MAKEMIGRATIONS
  } else {
    showHelp()
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
