package main

import (
	"fmt"
	"log"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func partial_deparse(node *pg_query.Node) string {
  switch node.Node.(type) {
    case *pg_query.Node_TypeName: {
      tn := node.GetTypeName()

      var name []string

      for _, n := range tn.GetNames() {
        name = append(name, partial_deparse(n))
      }

      type_name := strings.Join(name, ".")

      type_mods := tn.GetTypmods()

      if len(type_mods) > 0 {
        var type_defs []string

        for _, tm := range type_mods {
          type_defs = append(type_defs, partial_deparse(tm))
        }

        type_name = fmt.Sprintf("%s(%s)", type_name, strings.Join(type_defs, ", "))
      }

      return type_name
    }

    case *pg_query.Node_String_: {
      strings := node.GetString_()
      return strings.GetSval()
    }

    case *pg_query.Node_NullTest: {
      nt := node.GetNullTest()
      arg_name := partial_deparse(nt.GetArg())
      switch nt.GetNulltesttype() {
        case pg_query.NullTestType_IS_NOT_NULL: {
          return fmt.Sprintf("%s IS NOT NULL", arg_name)
        }

        case pg_query.NullTestType_IS_NULL: {
          return fmt.Sprintf("%s IS NULL", arg_name)
        }
      }
    }

    case *pg_query.Node_BoolExpr: {
      bool_expr := node.GetBoolExpr() 
      op := bool_expr.GetBoolop()
      args := bool_expr.GetArgs()

      var deparsed_args []string


      for _, arg := range args {
        deparsed_args = append(deparsed_args, partial_deparse(arg))
      }

      switch op {
        case pg_query.BoolExprType_AND_EXPR: {
          return strings.Join(deparsed_args, " AND ") 
        }

        case pg_query.BoolExprType_NOT_EXPR: {
          return fmt.Sprintf("NOT (%s)", strings.Join(deparsed_args, " "))
        }

        case pg_query.BoolExprType_OR_EXPR: {
          return strings.Join(deparsed_args, " OR ")
        }
      }
    }

    case *pg_query.Node_AConst: {        
      return pg_aconst_to_string(node.GetAConst())
    }

    case *pg_query.Node_AExpr: {
      expr := node.GetAExpr()    

      lexpr := partial_deparse(expr.GetLexpr())
      op := pg_nodes_to_string(expr.GetName())
      rexpr := partial_deparse(expr.GetRexpr())

      return fmt.Sprintf("%s %s %s", lexpr, op, rexpr) 
    }

    case *pg_query.Node_ColumnRef: {
      cr := node.GetColumnRef()
      return pg_nodes_to_string(cr.GetFields())
    }

    case *pg_query.Node_Constraint: {
      constraint := node.GetConstraint()
      ctype := constraint.GetContype()

      switch ctype {
        case pg_query.ConstrType_CONSTR_ATTR_DEFERRABLE: { 
          return "DEFERRABLE"
        }
        case pg_query.ConstrType_CONSTR_ATTR_DEFERRED: { 
          return "INITIALLY DEFERRED"
        }
        case pg_query.ConstrType_CONSTR_ATTR_IMMEDIATE: { 
          return "INITIALLY IMMEDIATE"
        }
        case pg_query.ConstrType_CONSTR_ATTR_NOT_DEFERRABLE: { 
          return "NOT DEFERRABLE"
        }
        case pg_query.ConstrType_CONSTR_IDENTITY: { log.Panicln("NOT IMPLEMENTED") }
        case pg_query.ConstrType_CONSTR_CHECK: { 
          check := constraint.GetRawExpr()
          return fmt.Sprintf("CHECK (%s)", partial_deparse(check))
        }
        case pg_query.ConstrType_CONSTR_DEFAULT: { 
          default_ := constraint.GetRawExpr()
          return "DEFAULT " + partial_deparse(default_)
        }
        case pg_query.ConstrType_CONSTR_NOTNULL: { return "NOT NULL" }
        case pg_query.ConstrType_CONSTR_EXCLUSION: { log.Panicln("NOT IMPLEMENTED") }
        case pg_query.ConstrType_CONSTR_FOREIGN: { log.Panicln("NOT IMPLEMENTED") }
        case pg_query.ConstrType_CONSTR_GENERATED: { log.Panicln("NOT IMPLEMENTED") }
        case pg_query.ConstrType_CONSTR_NULL: { return "NULL" }
        case pg_query.ConstrType_CONSTR_PRIMARY: {
          return "PRIMARY KEY"
        }
        case pg_query.ConstrType_CONSTR_TYPE_UNDEFINED: { log.Panicln("NOT IMPLEMENTED") }
        case pg_query.ConstrType_CONSTR_UNIQUE: {
          return "UNIQUE"
        }
      }
    }

    default: {
      log.Panicf("Could not deparse %v\n", node)
    }

  }

  return ""
}
