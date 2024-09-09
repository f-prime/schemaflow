package core

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

func CreateDbConnections(db_ctx *DbContext) *sql.DB {
  db_conn_str := fmt.Sprintf("port=%d sslmode=disable", db_ctx.PgPort);

  if db_ctx.PgHost != "" {
    db_conn_str = fmt.Sprintf("%s host=%s", db_conn_str, db_ctx.PgHost)
  }

  if db_ctx.PgUser != "" {
    db_conn_str = fmt.Sprintf("%s user=%s", db_conn_str, db_ctx.PgUser)
  }

  if db_ctx.PgPassword != "" {
    db_conn_str = fmt.Sprintf("%s password=%s", db_conn_str, db_ctx.PgPassword) 
  }

  if db_ctx.PgDbName != "" {
    db_conn_str = fmt.Sprintf("%s dbname=%s", db_conn_str, db_ctx.PgDbName)
  }

  db_conn, err := sql.Open("postgres", db_conn_str)
  perr(err)
  db_conn.SetMaxOpenConns(20)
  perr(db_conn.Ping())

  return db_conn
}
