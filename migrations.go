package main

import (
	"fmt"
	"strings"
)

func write_sql_stmt_to_migration_file(ctx *Context, stmt string, add_migration_required_line bool) {
  if add_migration_required_line {
    ctx.migration_file.WriteString(MIGRATION_REQUIRED + "\n")
  }

  if !strings.HasSuffix(stmt, ";") {
    stmt += ";"
  }

  ctx.migration_file.WriteString(stmt + "\n")
}

func write_migration_for_stmt(ctx *Context, stmt *ParsedStmt) {
  switch stmt.stmt_type {
    case FUNCTION: {
      drop_fn := fmt.Sprintf("DROP FUNCTION IF EXISTS %s", stmt.name)
      write_sql_stmt_to_migration_file(ctx, drop_fn, false)
      write_sql_stmt_to_migration_file(ctx, stmt.deparsed, false)
    }

    //case TABLE: {
    //  create_tbl_stmt := stmt.stmt.GetStmt().GetCreateStmt()
    //  cv, e := get_current_version_of_stmt(ctx, stmt)

    //  fmt.Printf("ctx: %v %v\n", cv, e)

    //  for _, col := range create_tbl_stmt.TableElts {
    //    c := col.GetColumnDef();
    //    fmt.Printf("c: %v\n", c)
    //  }
    //  panic("ASD")
    //}

    default: {
      cv, e := get_current_version_of_stmt(ctx, stmt)

      if e != nil {
        write_sql_stmt_to_migration_file(ctx, stmt.deparsed, true)
      } else {
        deparsed, err := deparse_raw_stmt(cv)
        perr(err)
        block := fmt.Sprintf(`/*
%s

Currently:
%s

Changed to:
%s
*/
`, MIGRATION_REQUIRED, deparsed, stmt.deparsed) 
        ctx.migration_file.WriteString(block)
      }
    }
  }
}

func write_migrations_to_next_migration_file(ctx *Context, stmts []*ParsedStmt) {
  for _, stmt := range stmts {
    if stmt.status == UNCHANGED {
      continue
    }

    if stmt.status == NEW {
      write_sql_stmt_to_migration_file(ctx, stmt.deparsed, false)
    } else {
      write_migration_for_stmt(ctx, stmt)
    }
  }
}
