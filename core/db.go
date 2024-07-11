package core

import (
	"database/sql"
        _ "github.com/lib/pq"
	"fmt"
	"log"
)

func CreateDbConnections(db_ctx *DbContext) *sql.DB {
  db_conn_str := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", db_ctx.PgHost, db_ctx.PgPort, db_ctx.PgUser, db_ctx.PgPassword, db_ctx.PgDbName);

  db_conn, err := sql.Open("postgres", db_conn_str)
  perr(err)
  db_conn.SetMaxOpenConns(20)
  perr(db_conn.Ping())

  return db_conn
}

func CreateMigrationsDbConn(ctx *Context, db_ctx *DbContext) *sql.DB {
  migration_db_conn_str := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", db_ctx.PgHost, db_ctx.PgPort, db_ctx.PgUser, db_ctx.PgPassword, MIGRATIONS_DB);

  // Resets the ephemeral db
  _, err := ctx.Db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", MIGRATIONS_DB))
  perr(err)
  _, err = ctx.Db.Exec(fmt.Sprintf("CREATE DATABASE %s", MIGRATIONS_DB))
  perr(err)

  migration_db_conn, err := sql.Open("postgres", migration_db_conn_str)
  migration_db_conn.SetMaxOpenConns(20)
  perr(err)
  perr(migration_db_conn.Ping())

  return migration_db_conn
}

func LoadSchemaIntoEphemeralDb(ctx *Context, stmts []*ParsedStmt) {
  log.Println("Loading schema into ephemeral db...")
  for _, stmt := range stmts {
    _, err := ctx.MigrationDbTx.Exec(stmt.Deparsed)
    perr(err)
  }
  err := ctx.MigrationDbTx.Commit()
  perr(err)
}
