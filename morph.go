package main

import (
	sql "database/sql"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
        _ "github.com/lib/pq"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)


type DbContext struct { 
  pg_host string 
  pg_port int
  pg_user string
  pg_password string 
  pg_db_name string
}

type Context struct {
  db_context DbContext
  db *sql.DB
  sql_path string
}

type ParsedStmts struct {
  stmt *pg_query.RawStmt
  deparsed string
}

func perr(e error) {
  if e != nil {
    panic(e)
  }
}

func create_db_connection(db_ctx DbContext) *sql.DB {
  conn_str := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", db_ctx.pg_host, db_ctx.pg_port, db_ctx.pg_user, db_ctx.pg_password, db_ctx.pg_db_name);
  dc, err := sql.Open("postgres", conn_str)

  perr(err)
  perr(dc.Ping())

  return dc
}

func parse_args() *Context {
  fmt.Println("Morph migration tool.");

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

    return deparsed, err
}

func extract_stmts(pr *pg_query.ParseResult) []ParsedStmts {
  var ps []ParsedStmts

  for _, x := range pr.Stmts {
    dp, err := deparse_raw_stmt(x)
    perr(err)
    ps = append(ps, ParsedStmts{ x, dp }) 
  }

  return ps
}

func parse_sql(code string) (*pg_query.ParseResult, error) {
  pr, err := pg_query.Parse(code) 
  return pr, err
}

func process_sql_files(ctx *Context) []ParsedStmts{
  var ps []ParsedStmts

  err := filepath.Walk(".", func(path string, info fs.FileInfo, err error) error {
    perr(err)

    if !strings.HasSuffix(info.Name(), ".sql") {
      return nil
    }

    fdata, err := os.ReadFile(path)

    perr(err)

    parsed_file, parse_err := parse_sql(string(fdata))

    if parse_err != nil {
      fmt.Printf("Syntax Error in %v:\n\n %v\n", path, parse_err)
      os.Exit(1)
    }

    extracted := extract_stmts(parsed_file)

    ps = append(ps, extracted...)

    return nil
  })

  perr(err)

  return ps
}

func main() {
  ctx := parse_args()
  // parsed := process_sql_files(ctx)
  db := create_db_connection(ctx.db_context)

  defer db.Close()

  ctx.db = db
}
