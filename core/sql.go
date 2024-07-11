package core

import (
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func DeparseRawStmt(x *pg_query.RawStmt) (string, error) {
  pr := new(pg_query.ParseResult)
  pr.Stmts = make([]*pg_query.RawStmt, 1)
  pr.Stmts[0] = x 

  deparsed, err := pg_query.Deparse(pr)

  return deparsed + ";", err
}


func ParseSql(code string) (*pg_query.ParseResult, error) {
  pr, err := pg_query.Parse(code) 
  return pr, err
}

func ProcessSqlFiles(ctx *Context) []*ParsedStmt {
  var ps []*ParsedStmt

  err := filepath.Walk(ctx.SqlPath, func(path string, info fs.FileInfo, err error) error {
    perr(err)

    if !strings.HasSuffix(info.Name(), ".sql") {
      return nil
    }

    log.Printf("Processing file %s\n", path)

    fdata, err := os.ReadFile(path)

    perr(err)

    parsed_file, parse_err := ParseSql(string(fdata))

    if parse_err != nil {
      log.Panicf("Syntax Error in %v:\n\n %v\n", path, parse_err)
    }

    extracted := ExtractStmts(ctx, parsed_file)

    ps = append(ps, extracted...)

    return nil
  })


  log.Println("Building dependency graph...");
  hydrateDependencies(ps)

  perr(err)

  return ps
}
