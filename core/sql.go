package core

import (
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func deparseRawStmt(x *pg_query.RawStmt) (string, error) {
  pr := new(pg_query.ParseResult)
  pr.Stmts = make([]*pg_query.RawStmt, 1)
  pr.Stmts[0] = x 

  deparsed, err := pg_query.Deparse(pr)

  return deparsed + ";", err
}


func parseSql(code string) (*pg_query.ParseResult, error) {
  pr, err := pg_query.Parse(code) 
  return pr, err
}
