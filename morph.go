package main

import (
	"crypto/sha1"
	sql "database/sql"
	"encoding/hex"
	"path"
	"sort"
	"strconv"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/lib/pq"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

const MIGRATION_FOLDER = "migrations"
const MIGRATION_REQUIRED = "-- MIGRATION REQUIRED"
const MAKE_MIGRATIONS_CMD = "makemigrations"
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

const (
  TABLE StmtType = iota
  VIEW
  MATERIALIZED_VIEW
  INDEX
  SEQUENCE
  SCHEMA
  FUNCTION
  PROCEDURE
  TRIGGER
  RULE
  CONSTRAINT
  TYPE
  DOMAIN
  AGGREGATE
  COLLATION
  EXTENSION
  LANGUAGE
  TABLESPACE
  ROLE
  USER
  GROUP
  POLICY
  PUBLICATION
  SUBSCRIPTION
  INSERT
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

type ParsedStmt struct {
  stmt *pg_query.RawStmt
  has_name bool
  stmt_name string
  deparsed string
  hash string
  status StmtStatus
  stmt_type StmtType
  depends_on []string
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

func set_stmt_type_and_name(x *pg_query.RawStmt, ps *ParsedStmt) {
  stmt := x.GetStmt()

  create_table := stmt.GetCreateStmt();
  create_view := stmt.GetViewStmt()
  create_index := stmt.GetIndexStmt()
  create_sequence := stmt.GetCreateSeqStmt()
  create_schema := stmt.GetCreateSchemaStmt()
  create_function := stmt.GetCreateFunctionStmt()
  create_trigger := stmt.GetCreateTrigStmt()
  create_domain := stmt.GetCreateDomainStmt()
  create_extension := stmt.GetCreateExtensionStmt()
  create_tablespace := stmt.GetCreateTableSpaceStmt()
  create_role := stmt.GetCreateRoleStmt()
  create_policy := stmt.GetCreatePolicyStmt()
  create_publication := stmt.GetCreatePublicationStmt()
  create_subscription := stmt.GetCreateSubscriptionStmt()

  insert_stmt := stmt.GetInsertStmt()

  if create_table != nil {
    ps.stmt_type = TABLE
    ps.stmt_name = create_table.GetRelation().Relname
  } else if create_view != nil {
    ps.stmt_type = VIEW
    ps.stmt_name = create_view.GetView().Relname
  } else if create_index != nil {
    ps.stmt_type = INDEX
    ps.stmt_name = create_index.Idxname
  } else if create_sequence != nil {
    ps.stmt_type = SEQUENCE 
  } else if create_schema != nil {
    ps.stmt_type = SCHEMA
    ps.stmt_name = create_schema.Schemaname
  } else if create_function != nil {
    ps.stmt_type = FUNCTION
  } else if create_trigger != nil {
    ps.stmt_type = TRIGGER
    ps.stmt_name = create_trigger.Trigname
  } else if create_domain != nil {
    ps.stmt_type = DOMAIN
  } else if create_extension != nil {
    ps.stmt_type = EXTENSION
    ps.stmt_name = create_extension.GetExtname()
  } else if create_tablespace != nil {
    ps.stmt_type = TABLESPACE
    ps.stmt_name = create_table.Tablespacename
  } else if create_role != nil {
    ps.stmt_type = ROLE
  } else if create_policy != nil {
    ps.stmt_type = POLICY
    ps.stmt_name = create_policy.PolicyName
  } else if create_publication != nil {
    ps.stmt_type = PUBLICATION
    ps.stmt_name = create_publication.Pubname
  } else if create_subscription != nil {
    ps.stmt_type = SUBSCRIPTION
    ps.stmt_name = create_subscription.Subname
  } else if insert_stmt != nil {
    ps.stmt_type = INSERT
  } else {
    fmt.Printf("Unknown statement %v\n", ps);
    os.Exit(1)
  }
}

func extract_stmts(pr *pg_query.ParseResult) []*ParsedStmt {
  var ps []*ParsedStmt

  for _, x := range pr.Stmts {
    var depends_on []string
    dp, err := deparse_raw_stmt(x)
    perr(err)
    nps := &ParsedStmt{ 
      x, 
      false,
      "",
      dp, 
      hash_string(dp), 
      UNKNOWN, 
      UNKNOWN_TYPE,
      depends_on,
    }
    set_stmt_type_and_name(x, nps)
    ps = append(ps, nps) 
  }

  return ps
}

func parse_sql(code string) (*pg_query.ParseResult, error) {
  pr, err := pg_query.Parse(code) 
  return pr, err
}

func process_sql_files(ctx *Context) []*ParsedStmt{
  var ps []*ParsedStmt

  err := filepath.Walk(ctx.sql_path, func(path string, info fs.FileInfo, err error) error {
    perr(err)

    if !strings.HasSuffix(info.Name(), ".sql") {
      return nil
    }

    fdata, err := os.ReadFile(path)

    perr(err)

    parsed_file, parse_err := parse_sql(string(fdata))

    if parse_err != nil {
      fmt.Printf("Syntax Error in %v:\n\n %v\n", path, parse_err)
      os.Exit(3)
    }

    extracted := extract_stmts(parsed_file)

    ps = append(ps, extracted...)

    return nil
  })

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
      fmt.Printf("Migration file %s has does not match the checksum in the database.\n", f)
      os.Exit(1)
    }
  }
}

func is_stmt_hash_found_in_db(ctx *Context, stmt *ParsedStmt) bool {
  r, e := ctx.db.Query("select * from morph.statements where stmt_hash=$1", stmt.hash)
  perr(e)
  return r.Next()
}

func set_stmt_status(ctx *Context, stmts []*ParsedStmt) []*ParsedStmt {
  for _, stmt := range stmts {
    if is_stmt_hash_found_in_db(ctx, stmt) {
      stmt.status = UNCHANGED
    } else {
      stmt.status = CHANGED
    }
  }

  return stmts
}

func do_any_stmts_require_migration(ctx *Context, stmts []*ParsedStmt) bool {
  for _, s := range stmts {
    if s.status == CHANGED {
      return true
    }
  }

  return false
}

func copy_changed_statements_to_next_migration_file(ctx *Context, stmts []*ParsedStmt) {
  for _, stmt := range stmts {
    if stmt.status == UNCHANGED {
      continue
    }
  
    ctx.migration_file.Write([]byte(MIGRATION_REQUIRED + "\n"))
    ctx.migration_file.Write([]byte(stmt.deparsed + "\n"))
  }
}

func create_next_migration(ctx *Context) {
  migrations := get_all_executed_migration_files(ctx)

  next_migration_file := path.Join(MIGRATION_FOLDER, "0.sql") 

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
    if stmt.status != CHANGED {
      continue
    }

    if stmt.stmt_name == "" {
      _, e := ctx.db_tx.Exec("insert into morph.statements (stmt, stmt_hash) values ($1, $2)", stmt.deparsed, stmt.hash)
      perr(e)
    } else {
      _, e := ctx.db_tx.Exec("insert into morph.statements (stmt, stmt_hash, stmt_name) values ($1, $2, $3)", stmt.deparsed, stmt.hash, stmt.stmt_name)
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

func main() {
  ctx := parse_args()
  db := create_db_connection(ctx.db_context)
  defer db.Close()
  ctx.db = db

  tx, te := ctx.db.Begin()
  perr(te)

  ctx.db_tx = tx

  init_migration_schema(ctx);
  init_migrations_folder(ctx)

  verify_all_migration_files(ctx);

  if strings.Compare(ctx.cmd, MAKE_MIGRATIONS_CMD) == 0 {
    if !have_all_migrations_been_executed(ctx) {
      fmt.Println("There are migrations that have not yet been executed.");
      os.Exit(3)
    }

    stmts := process_sql_files(ctx)
    set_stmt_status(ctx, stmts)

    if !do_any_stmts_require_migration(ctx, stmts) {
      fmt.Println("No migrations required.")
      os.Exit(3)
    }
    
    create_next_migration(ctx)
    defer ctx.migration_file.Close()

    copy_changed_statements_to_next_migration_file(ctx, stmts)
    update_statements_in_db(ctx, stmts)

    fmt.Printf("New migrations have been created in %s.\n", ctx.migration_file_path)
  } else if strings.Compare(ctx.cmd, MIGRATE_CMD) == 0 {
    if have_all_migrations_been_executed(ctx) {
      fmt.Println("No migrations to run.")
      os.Exit(0)
    }

    if !have_all_migrations_been_resolved(ctx) {
      fmt.Println("Not all migrations have been resolved.")
      os.Exit(1)
    }

    execute_migrations(ctx)
    fmt.Println("Migrations run successfully.")
  }

  perr(ctx.db_tx.Commit())
}
