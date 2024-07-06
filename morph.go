package main

import (
	sql "database/sql"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/lib/pq"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

type StmtType int

// THE ORDER OF THIS ENUM IS THE SORT BUCKET PRIORITY ORDER
const (
  DATABASE StmtType = iota
  SERVER
  SCHEMA 
  EXTENSION
  USER
  ROLE
  VARIABLE
  CAST
  ACCESS_METHOD
  FOREIGN_SERVER
  OPERATOR
  OPERATOR_CLASS
  OPERATOR_FAMILY
  STATISTICS
  TEXT_SEARCH_CONFIGURATION
  TEXT_SEARCH_DICTIONARY
  TEXT_SEARCH_PARSER
  TEXT_SEARCH_TEMPLATE
  FUNCTION
  DOMAIN
  DOMAIN_CONSTRAINT
  TYPE
  GENERIC_TYPE // This is an ambiguous type. Could be a domain or a type.
  AGGREGATE
  COLLATION
  LANGUAGE
  ENUM

  FOREIGN_DATA_WRAPPER
  FOREIGN_TABLE
  TABLE 
  VIEW
  MATERIALIZED_VIEW
  INDEX
  COLUMN
  CASE
  CONVERSION
  SEQUENCE
  LARGE_OBJECT
  ROUTINE
  TRANSFORM
  SELECT
  PROCEDURE
  COMMENT
  GRANT
  GRANT_ROLE
  UPDATE
  ALTER_DEFAULT_PRIVILEGES
  ALTER_POLICY
  ALTER_TABLE
  EVENT_TRIGGER
  TRIGGER
  RULE
  CONSTRAINT
  TABLE_CONSTRAINT
  TABLESPACE
  GROUP
  POLICY
  PUBLICATION
  SUBSCRIPTION
  INSERT
  DROP_OWNED
  DROP
  DO
  UNKNOWN_TYPE
)

type DbContext struct { 
  pg_host string 
  pg_port int
  pg_user string
  pg_password string 
  pg_db_name string
}

type Context struct {
  migration_db_context DbContext
  migration_db_tx *sql.Tx
  migration_db *sql.DB
  db_context DbContext
  db_tx *sql.Tx
  db *sql.DB
  sql_path string
  stmts *[]*ParsedStmt
}

type Dependency struct {
  stmt_type StmtType
  stmt_name string
  dependency *ParsedStmt
}

type ParsedStmt struct {
  stmt *pg_query.RawStmt
  has_name bool
  name string
  deparsed string
  json string
  hash string
  stmt_type StmtType
  dependencies []*Dependency
  handled bool
  removed bool
}

const MIGRATIONS_DB = "morph_ephemeral_migration_db"

func perr(e error) {
  if e != nil {
    panic(e)
  }
}

func list_all_files_in_path(path string) []string {
  var files []string
  err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
    if err != nil {
      return err
    }

    if !info.IsDir() && strings.HasSuffix(path, ".sql") {
      files = append(files, path)
    }

    return nil 
  })

  perr(err)
  return files
}

func create_db_connections(db_ctx DbContext) (*sql.DB, *sql.DB) {
  db_conn_str := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", db_ctx.pg_host, db_ctx.pg_port, db_ctx.pg_user, db_ctx.pg_password, db_ctx.pg_db_name);
  migration_db_conn_str := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", db_ctx.pg_host, db_ctx.pg_port, db_ctx.pg_user, db_ctx.pg_password, MIGRATIONS_DB);

  db_conn, err := sql.Open("postgres", db_conn_str)
  db_conn.SetMaxOpenConns(20)
  perr(err)
  perr(db_conn.Ping())

  // Resets the ephemeral db
  _, err = db_conn.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", MIGRATIONS_DB))
  perr(err)
  _, err = db_conn.Exec(fmt.Sprintf("CREATE DATABASE %s", MIGRATIONS_DB))
  perr(err)

  migration_db_conn, err := sql.Open("postgres", migration_db_conn_str)
  migration_db_conn.SetMaxOpenConns(20)
  perr(err)
  perr(migration_db_conn.Ping())

  return db_conn, migration_db_conn
}

func parse_args() *Context {
  pg_host := flag.String("pg-host", "", "pg-host")
  pg_port := flag.Int("pg-port", 5432, "pg-port") 
  pg_user := flag.String("pg-user", "", "pg-user")
  pg_password := flag.String("pg-password", "", "pg-password")
  pg_db_name := flag.String("pg-db-name", "", "pg-db-name") 

  sql_path := flag.String("sql-path", "./", "sql-path")

  flag.Parse()

  ctx := new(Context);

  ctx.db_context = DbContext{
    *pg_host,
    *pg_port,
    *pg_user,
    *pg_password,
    *pg_db_name,
  }

  ctx.sql_path = *sql_path

  return ctx
}

func deparse_raw_stmt(x *pg_query.RawStmt) (string, error) {
  pr := new(pg_query.ParseResult)
  pr.Stmts = make([]*pg_query.RawStmt, 1)
  pr.Stmts[0] = x 

  deparsed, err := pg_query.Deparse(pr)

  return deparsed + ";", err
}


func parse_sql(code string) (*pg_query.ParseResult, error) {
  pr, err := pg_query.Parse(code) 
  return pr, err
}

func process_sql_files(ctx *Context) []*ParsedStmt {
  var ps []*ParsedStmt

  err := filepath.Walk(ctx.sql_path, func(path string, info fs.FileInfo, err error) error {
    perr(err)

    if !strings.HasSuffix(info.Name(), ".sql") {
      return nil
    }

    log.Printf("Processing file %s\n", path)

    fdata, err := os.ReadFile(path)

    perr(err)

    parsed_file, parse_err := parse_sql(string(fdata))

    if parse_err != nil {
      log.Panicf("Syntax Error in %v:\n\n %v\n", path, parse_err)
    }

    extracted := extract_stmts(parsed_file)

    ps = append(ps, extracted...)

    return nil
  })


  log.Println("Building dependency graph...");
  hydrate_dependencies(ps)

  perr(err)

  return ps
}

func load_schema_into_ephemeral_db(ctx *Context, stmts []*ParsedStmt) {
  log.Println("Loading schema into ephemeral db...")
  for _, stmt := range stmts {
    _, err := ctx.migration_db_tx.Exec(stmt.deparsed)
    perr(err)
  }
  err := ctx.migration_db_tx.Commit()
  perr(err)
}

func main() {
  ctx := parse_args()
  db_conn, migration_db_conn := create_db_connections(ctx.db_context)

  defer db_conn.Close()
  defer migration_db_conn.Close()

  ctx.db = db_conn
  db_tx, te := ctx.db.Begin()
  ctx.db_tx = db_tx
  perr(te)

  ctx.migration_db = migration_db_conn
  mdb_tx, te := ctx.migration_db.Begin()
  ctx.migration_db_tx = mdb_tx
  perr(te)

  stmts := process_sql_files(ctx)
  stmts = sort_stmts_by_priority(stmts)

  ctx.stmts = &stmts

  load_schema_into_ephemeral_db(ctx, stmts)
  generate_migrations(ctx)

  perr(ctx.db_tx.Commit())

  log.Println("Done.")
}
