package main

import (
	"fmt"
	"log"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func does_column_exist(column string, node *pg_query.Node) bool {
  c := node.GetCreateStmt()
  te := c.GetTableElts()

  for _, c := range te {
    cd := c.GetColumnDef()
    if cd.GetColname() != column {
      return false
    }
  }

  return true
}

func table_migrations(sql_stmts *[]string, new_stmt *pg_query.Node, prev_stmt *pg_query.Node, new_ps *ParsedStmt) {
  switch new_stmt.Node.(type) {
    case *pg_query.Node_CreateStmt: {
      new_table := new_stmt.GetCreateStmt()
      prev_table := prev_stmt.GetCreateStmt()

      relation := new_table.GetRelation()
      old_relation := prev_table.GetRelation()

      schema := relation.GetSchemaname()
      old_schema := old_relation.GetSchemaname()

      tablespace := new_table.GetTablespacename()
      old_tablespace := prev_table.GetTablespacename()

      constraints := new_table.GetConstraints()
      partition := new_table.GetPartspec()

      fmt.Printf("schema: %v\n", schema)

      fmt.Printf("tablespace: %v\n", tablespace)
      fmt.Printf("partition: %v\n", partition)

      access_method := new_table.GetAccessMethod()
      old_access_method := prev_table.GetAccessMethod()

      fmt.Printf("access_method: %v\n", access_method)

      if tablespace != old_tablespace {
        log.Fatalln("Tablespace change not implemented.")
      }

      if schema != old_schema {
        log.Fatalln("Schema change is not implemented.")
      }

      if access_method != old_access_method {
        log.Fatalln("Access method change not implemented")
      }


      inherit_rels := new_table.GetInhRelations()

      columns := new_table.GetTableElts()

      for _, inherit := range inherit_rels {
        table_migrations(sql_stmts, inherit, prev_stmt, new_ps)
      }

      for _, column  := range columns {
        table_migrations(sql_stmts, column, prev_stmt, new_ps)
      }

      for _, constraint := range constraints {
        table_migrations(sql_stmts, constraint, prev_stmt, new_ps)
      }
    }

    case *pg_query.Node_ColumnDef: {
      cd := new_stmt.GetColumnDef()

      name := cd.GetColname()
      type_obj := cd.GetTypeName()

      type_mod := type_obj.GetTypmods()

      var final_type []string

      for _, ty := range type_mod {
        final_type = append(final_type, partial_deparse(ty))
      }

      full_column_name := fmt.Sprintf("%s %s", name, pg_nodes_to_string(type_obj.GetNames()))

      if len(final_type) > 0 {
        full_column_name += "(" + strings.Join(final_type, ", ") + ")"
      }


      col_clause := cd.GetCollClause()
      col_name := pg_nodes_to_string(col_clause.GetCollname())
      constraints := cd.GetConstraints()

      fmt.Printf("constraints: %v\n", constraints)

      //col_arg := partial_deparse(col_clause.GetArg())

      //is_not_null := cd.GetIsNotNull()
      //is_from_type := cd.GetIsFromType()
      //storage := cd.GetStorage()
      //storage_name := cd.GetStorageName()
      //raw_default := cd.GetRawDefault()
      //cooked_default := cd.GetCookedDefault()
      //identity := cd.GetIdentity()
      //identity_sequence := cd.GetIdentitySequence()
      //generated := cd.GetGenerated()
      //fdw_options := cd.GetFdwoptions()
      //local := cd.GetIsLocal()
      //compression := cd.GetCompression()

      //_type := cd.GetTypeName()
      //type_name := pg_nodes_to_string(_type.GetNames())

      //mods := _type.GetTypmods()

      //if len(mods) > 0 {
      //  var tmods []string
      //  for _, mod := range mods  {
      //    tmods = append(tmods, pg_aconst_to_string(mod.GetAConst()))
      //  }
      //  type_name += "(" + strings.Join(tmods, ", ") + ")"
      //}

      //return col_name + " " + type_name


      if !does_column_exist(name, prev_stmt) {
        alter_stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", new_ps.name, full_column_name)

        if col_clause != nil {
          alter_stmt += fmt.Sprintf(" COLLATE %s", col_name)
        }

        for _, constraint := range constraints {
          alter_stmt = fmt.Sprintf("%s %s", alter_stmt, partial_deparse(constraint))
        }

        fmt.Printf("alter_stmt: %v\n", alter_stmt)

        build_sql_stmt_for_migration_file(sql_stmts, alter_stmt)
      }
    }

    default: {
      log.Fatalf("Unknown node type %v\n", new_stmt) 
    }

  }
}
