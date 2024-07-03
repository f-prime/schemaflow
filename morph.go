package main

import (
	"crypto/sha1"
	sql "database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

const MIGRATION_FOLDER = "migrations"
const MIGRATION_REQUIRED = "-- REMOVE WHEN MIGRATION IS RESOLVED --"
const MAKE_MIGRATIONS_CMD = "makemigrations"
const CLEAR_MIGRATIONS_CMD = "clearmigrations"
const MIGRATE_CMD = "migrate"

const MIGRATION_SCHEMA = `

create schema if not exists morph;

create table if not exists morph.migrations (
  file_name text primary key not null,
  file_hash text not null,
  created timestamp default now()
);

create table if not exists morph.statements (
  id serial primary key,
  stmt text not null,
  stmt_hash text unique not null,
  stmt_type integer not null,
  stmt_name text default null,
  created timestamp default now(),
  updated timestamp default now()
);

create index on morph.statements(stmt_hash);

`

type StmtStatus int

const (
  UNKNOWN StmtStatus = iota
  NEW
  CHANGED
  UNCHANGED
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
  cmd string
  db_context DbContext
  db_tx *sql.Tx
  db *sql.DB
  sql_path string
  migration_file *os.File
  migration_file_path string
}

type Dependency struct {
  stmt_type StmtType
  stmt_name string
  dependency *ParsedStmt
}

type ParsedStmt struct {
  prev_version_stmt *pg_query.RawStmt
  stmt *pg_query.RawStmt
  has_name bool
  name string
  deparsed string
  json string
  hash string
  status StmtStatus
  stmt_type StmtType
  dependencies []*Dependency
  handled bool
  removed bool
}

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

func create_db_connection(db_ctx DbContext) *sql.DB {
  conn_str := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", db_ctx.pg_host, db_ctx.pg_port, db_ctx.pg_user, db_ctx.pg_password, db_ctx.pg_db_name);
  dc, err := sql.Open("postgres", conn_str)
  dc.SetMaxOpenConns(20)

  perr(err)
  perr(dc.Ping())

  return dc
}

func parse_args() *Context {
  cmd := flag.String("cmd", "", "cmd")
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
  ctx.cmd = *cmd

  return ctx
}

func deparse_raw_stmt(x *pg_query.RawStmt) (string, error) {
  pr := new(pg_query.ParseResult)
  pr.Stmts = make([]*pg_query.RawStmt, 1)
  pr.Stmts[0] = x 

  deparsed, err := pg_query.Deparse(pr)

  return deparsed + ";", err
}

func get_current_version_of_stmt(ctx *Context, stmt *ParsedStmt) (*pg_query.RawStmt, error) {
  if stmt.has_name {
    var code string
    err := ctx.db.QueryRow("select stmt from morph.statements where stmt_type=$1 and stmt_name=$2", stmt.stmt_type, stmt.name).Scan(&code); 
    if err != nil {
      return nil, err 
    }

    result, err2 := pg_query.Parse(code)

    if err2 != nil {
      return nil, err2 
    }

    stmts := result.GetStmts()

    if len(stmts) == 0 {
      return nil, errors.New("Statements array is empty.")
    }

    return result.GetStmts()[0], nil
  }

  return nil, errors.New(fmt.Sprintf("Could not get current version of statement. %v\n", stmt));
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


  fmt.Println("Hydrating dependencies...");
  hydrate_dependencies(ps)

  perr(err)

  return ps
}

func init_migration_schema(ctx *Context) {
  _, e := ctx.db.Query(MIGRATION_SCHEMA) 
  perr(e)
}

func init_migrations_folder(ctx *Context) {
  _, err := os.Stat(MIGRATION_FOLDER)
  if os.IsNotExist(err) {
    os.Mkdir("migrations", os.FileMode(0777))
  }
}

func hash_string(s string) string {
  h := sha1.New()
  h.Write([]byte(s))
  r := h.Sum(nil)
  return hex.EncodeToString(r)
}

func hash_file(p string) string {
  data, e := os.ReadFile(p)
  perr(e)
  sdata := string(data)
  return hash_string(sdata) 
}

func execute_sql_file(ctx *Context, fname string) error {
  fd, e := os.ReadFile(fname)
  perr(e)

  _, e = ctx.db_tx.Exec(string(fd))
  return e
}

func mark_migration_as_executed(ctx *Context, fname string) error {
  _, e := ctx.db_tx.Exec("insert into morph.migrations (file_name, file_hash) values ($1, $2)", fname, hash_file(fname)) 
  return e
}

func does_migration_with_file_name_exist_in_db(ctx *Context, file_name string) bool {
  r, e := ctx.db.Query("select true from morph.migrations where file_name=$1", file_name)
  perr(e)
  return r.Next()
}

func does_migration_with_hash_exist_in_db(ctx *Context, hash string) bool {
  r, e := ctx.db.Query("select true from morph.migrations where file_hash=$1", hash)
  perr(e)
  return r.Next()
}

func get_all_executed_migration_files(ctx *Context) []string {
  var files []string

  for _, f := range list_all_files_in_path(MIGRATION_FOLDER) {
    if does_migration_with_file_name_exist_in_db(ctx, f) {
      files = append(files, f) 
    }
  }

  return files
}

func extract_number_from_migration_file_name(fname string) int {
  fname = strings.ReplaceAll(fname, MIGRATION_FOLDER + "/", "")
  fi, e1 := strconv.Atoi(strings.Split(fname, ".")[0])
  perr(e1)

  return fi
}

func get_all_unexecuted_migration_files(ctx *Context) []string {
  var files []string

  for _, f := range list_all_files_in_path(MIGRATION_FOLDER) {
    if !does_migration_with_file_name_exist_in_db(ctx, f) {
      files = append(files, f) 
    }
  }

  sort.Slice(files, func(i int, j int) bool {
    fi := extract_number_from_migration_file_name(files[i])
    fj := extract_number_from_migration_file_name(files[j])

    return fj > fi
  });

  return files
}

func is_migration_resolved(ctx *Context, fname string) bool {
  fn, e := os.ReadFile(fname)
  perr(e)
  for _, line := range strings.Split(string(fn), "\n") {
    if strings.Compare(line, MIGRATION_REQUIRED) == 0 {
      return false
    }
  }

  return true
}

func have_all_migrations_been_executed(ctx *Context) bool {
  return len(get_all_unexecuted_migration_files(ctx)) == 0
}

func have_all_migrations_been_resolved(ctx *Context) bool {
  for _, m := range get_all_unexecuted_migration_files(ctx) {
    if !is_migration_resolved(ctx, m) {
      return false
    }
  }

  return true
}

func verify_all_migration_files(ctx *Context) {
  migration_files := get_all_executed_migration_files(ctx)

  for _, f := range migration_files {
    if !does_migration_with_hash_exist_in_db(ctx, hash_file(f)) {
      log.Panicf("Migration file %s has does not match the checksum in the database.\n", f)
    }
  }
}

func is_stmt_hash_found_in_db(ctx *Context, stmt *ParsedStmt) bool {
  r, e := ctx.db.Query("select * from morph.statements where stmt_hash=$1", stmt.hash)
  defer r.Close()
  perr(e)
  return r.Next()
}

func is_stmt_name_found_in_db(ctx *Context, stmt *ParsedStmt) bool {
  if !stmt.has_name {
    return false
  }

  r, e := ctx.db.Query("select * from morph.statements where stmt_name=$1 and stmt_type=$2", stmt.name, stmt.stmt_type);
  defer r.Close()
  perr(e)
  return r.Next()
}

func get_prev_stmt_version(ctx *Context, stmt *ParsedStmt) *pg_query.RawStmt {
  var prev_stmt_text string
  e := ctx.db.QueryRow("select stmt from morph.statements where stmt_name=$1 and stmt_type=$2", stmt.name, stmt.stmt_type).Scan(&prev_stmt_text);
  perr(e)
  parsed, e := pg_query.Parse(prev_stmt_text)
  perr(e)

  stmts := parsed.GetStmts()

  if len(stmts) == 0 {
    return nil
  }

  return stmts[0]
}

func set_stmt_status(ctx *Context, stmts []*ParsedStmt) []*ParsedStmt {
  for _, stmt := range stmts {
    stmt_hash_found := is_stmt_hash_found_in_db(ctx, stmt)
    stmt_name_found := is_stmt_name_found_in_db(ctx, stmt)

    if (stmt_name_found && stmt_hash_found) || (!stmt_name_found && stmt_hash_found) {
      stmt.status = UNCHANGED
    } else if stmt_name_found && !stmt_hash_found {
      stmt.prev_version_stmt = get_prev_stmt_version(ctx, stmt)

      fmt.Printf("stmt.prev_version_stmt: %v\n", stmt.prev_version_stmt)
      stmt.status = CHANGED
    } else {
      stmt.status = NEW
    }
  }

  return stmts
}

func do_any_stmts_require_migration(ctx *Context, stmts []*ParsedStmt) bool {
  for _, s := range stmts {
    if s.removed {
      continue
    }

    if s.status == CHANGED || s.status == NEW {
      return true
    }
  }

  return false
}

func create_next_migration(ctx *Context) {
  migrations := get_all_executed_migration_files(ctx)

  next_migration_file := path.Join(MIGRATION_FOLDER, "0.sql") 

  fmt.Printf("next_migration_file: %v\n", next_migration_file)

  if len(migrations) > 0 {
    next_migration_file =  path.Join(MIGRATION_FOLDER, fmt.Sprintf("%d.sql", extract_number_from_migration_file_name(migrations[len(migrations)-1]) + 1))
  }

  ctx.migration_file_path = next_migration_file

  f, e := os.Create(next_migration_file)
  perr(e)

  ctx.migration_file = f
}

func update_statements_in_db(ctx *Context, stmts []*ParsedStmt) {
  for _, stmt := range stmts {
    if stmt.status == UNCHANGED {
      continue
    }

    if !stmt.has_name {
      _, e := ctx.db_tx.Exec("insert into morph.statements (stmt, stmt_hash, stmt_type) values ($1, $2, $3) on conflict (stmt_hash) do nothing", stmt.deparsed, stmt.hash, stmt.stmt_type)
      perr(e)
    } else {
      _, e := ctx.db_tx.Exec("insert into morph.statements (stmt, stmt_hash, stmt_name, stmt_type) values ($1, $2, $3, $4) on conflict (stmt_hash) do nothing", stmt.deparsed, stmt.hash, stmt.name, stmt.stmt_type)
      perr(e)
    }
  }
}

func execute_migrations(ctx *Context) {
  for _, m := range get_all_unexecuted_migration_files(ctx) {
    fmt.Printf("Executing %s\n", m)
    perr(execute_sql_file(ctx, m))
    perr(mark_migration_as_executed(ctx, m))
  }
}

func clear_removed_statements_from_db(ctx *Context, stmts []*ParsedStmt) {
  var hash string
  
  query, err := ctx.db.Query("select stmt_hash from morph.statements")
  defer query.Close()
  perr(err)

  for query.Next() {
    perr(query.Scan(&hash))

    found := false

    for _, stmt := range stmts {
      if stmt.hash == hash {
        found = true
        stmt.removed = true
        break
      }
    }

    if !found {
      log.Printf("Removing %s\n", hash);
      ctx.db_tx.Exec("delete from morph.statements where stmt_hash=$1", hash)
    }
  }

}

func clear_migrations(ctx *Context) {
  os.RemoveAll(MIGRATION_FOLDER)
  _, e := ctx.db.Exec("drop schema if exists morph cascade");
  perr(e)
  _, e = ctx.db.Exec("drop schema if exists public cascade");
  perr(e)
  _, e = ctx.db.Exec("create schema if not exists public")
  perr(e)
}

func main() {
  ctx := parse_args()
  db := create_db_connection(ctx.db_context)
  defer db.Close()
  ctx.db = db

  tx, te := ctx.db.Begin()
  perr(te)

  ctx.db_tx = tx

  if strings.Compare(ctx.cmd, CLEAR_MIGRATIONS_CMD) == 0 {
    log.Println("Clearing...");
    clear_migrations(ctx);
    ctx.cmd = MAKE_MIGRATIONS_CMD;
  }

  init_migration_schema(ctx);
  init_migrations_folder(ctx)

  verify_all_migration_files(ctx);

  if strings.Compare(ctx.cmd, MAKE_MIGRATIONS_CMD) == 0 {
    if !have_all_migrations_been_executed(ctx) {
      log.Fatal("There are migrations that have not yet been executed.");
    }

    log.Println("Processing SQL Files...")
    stmts := process_sql_files(ctx)

    log.Println("Setting statuses...")
    set_stmt_status(ctx, stmts)

    log.Println("Sorting statements...")
    stmts = sort_stmts_by_priority(stmts)

    log.Println("Clearing removed statements...")
    clear_removed_statements_from_db(ctx, stmts)

    if !do_any_stmts_require_migration(ctx, stmts) {
      log.Fatal("No migrations required.")
    }
    
    log.Println("Creating migrations...")
    create_next_migration(ctx)

    defer ctx.migration_file.Close()

    log.Println("Writing migrations...")
    write_migrations_to_next_migration_file(ctx, stmts)

    log.Println("Updating statements in db...")
    update_statements_in_db(ctx, stmts)

    log.Printf("New migrations have been created in %s.\n", ctx.migration_file_path)
  } else if strings.Compare(ctx.cmd, MIGRATE_CMD) == 0 {
    if have_all_migrations_been_executed(ctx) {
      log.Fatal("No migrations to run.")
    }

    if !have_all_migrations_been_resolved(ctx) {
      log.Fatal("Not all migrations have been resolved.")
    }

    log.Println("Executing migrations...")
    execute_migrations(ctx)
    log.Println("Migrations run successfully.")
  } else {
    log.Fatalf("Unknown command: %s\n", ctx.cmd)
  }

  perr(ctx.db_tx.Commit())
}
