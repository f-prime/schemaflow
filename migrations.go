package main

import (
	"fmt"
	"strings"
)

func build_sql_stmt_for_migration_file(acc *[]string, stmt string, add_migration_required_line bool)  {
  if add_migration_required_line {
    *acc = append(*acc, MIGRATION_REQUIRED + "\n")
  }

  if !strings.HasSuffix(stmt, ";") {
    stmt += ";"
  }

  *acc = append(*acc, stmt + "\n")
}

func build_sql_stmt(sql string, args ...any) string {
  return fmt.Sprintf(sql, args...)
}

func get_migration_for_stmt(ctx *Context, stmt *ParsedStmt) []string {
  var sql_stmt []string

  _ = stmt.stmt.GetStmt()
  
  switch stmt.stmt_type {
    case FUNCTION: {
      drop_fn := build_sql_stmt("DROP FUNCTION IF EXISTS %s", stmt.name)
      build_sql_stmt_for_migration_file(&sql_stmt, drop_fn, false)
      build_sql_stmt_for_migration_file(&sql_stmt, stmt.deparsed, false)
    }

    case VIEW: {
      drop_view := build_sql_stmt("DROP VIEW IF EXISTS %s", stmt.name)
      build_sql_stmt_for_migration_file(&sql_stmt, drop_view, false)
      build_sql_stmt_for_migration_file(&sql_stmt, stmt.deparsed, false)
    }

    case TABLE: {
    }

    default: {
      if ctx == nil {
        build_sql_stmt_for_migration_file(&sql_stmt, stmt.deparsed, true)
        break
      }

      cv, e := get_current_version_of_stmt(ctx, stmt)

      if e != nil {
        // This means it's a new statement so just throw it in.
        build_sql_stmt_for_migration_file(&sql_stmt, stmt.deparsed, true)
      } else {
        deparsed, err := deparse_raw_stmt(cv)
        perr(err)
        sql_stmt = append(sql_stmt, fmt.Sprintf(`/*
%s

Currently:
%s

Changed to:
%s
*/
`, MIGRATION_REQUIRED, deparsed, stmt.deparsed)) 
      }
    }
  }

  return sql_stmt
}

func write_sql_stmt_to_migration_file(ctx *Context, sql string) {
  ctx.migration_file.WriteString(sql);
}

func write_migration_for_stmt(ctx *Context, stmt *ParsedStmt) {
  sql := get_migration_for_stmt(ctx, stmt)
  write_sql_stmt_to_migration_file(ctx, strings.Join(sql, "\n"))
}

func write_migrations_to_next_migration_file(ctx *Context, stmts []*ParsedStmt) {
  for _, stmt := range stmts {
    if stmt.status == UNCHANGED {
      continue
    }

    if stmt.status == NEW {
      write_sql_stmt_to_migration_file(ctx, stmt.deparsed)
    } else {
      write_migration_for_stmt(ctx, stmt)
    }
  }
}
