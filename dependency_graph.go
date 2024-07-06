package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

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

func pg_aconst_to_string(ac *pg_query.A_Const) string {
  if ac.GetIsnull() {
    return "NULL"
  } else {
    s_val := ac.GetSval()
    i_val := ac.GetIval()
    f_val := ac.GetFval()
    b_val := ac.GetBsval()
    bool_val := ac.GetBoolval()

    if s_val != nil {
      return fmt.Sprintf("'%s'", s_val.GetSval())
    } else if i_val != nil {
      return fmt.Sprintf("%d", i_val.GetIval())
    } else if f_val != nil {
      return fmt.Sprintf("'%s'", b_val.GetBsval())
    } else if bool_val != nil {
      if bool_val.GetBoolval() {
        return fmt.Sprintf("TRUE")
      } else {
        return fmt.Sprintf("FALSE")
      }
    } else {
      return ""
    }
  }
}

func pg_nodes_to_string(nodes []*pg_query.Node) string {
  if len(nodes) == 0 {
    return ""
  }

  return nodes[len(nodes) - 1].GetString_().Sval

  //var name []string 

  //for _, node := range nodes {
  //  name = append(name, node.GetString_().Sval)
  //}

  //return strings.Join(name, ".")
}

func pg_rangevar_to_string(rv *pg_query.RangeVar) string {
  //sn := rv.GetSchemaname()

  //if len(sn) == 0 {
  //  return rv.GetRelname()
  //}

  return rv.GetRelname()
}

func pg_typename_to_string(tn *pg_query.TypeName) string {
  names := tn.GetNames()

  var name []string;

  for _, n := range names {
    str := n.GetString_()

    if str != nil {
      sval := str.GetSval()

      if len(sval) > 0 {
        name = append(name, sval)
      }
    }
  }

  return strings.Join(name, ".")
}

func pg_list_to_string(tn *pg_query.List) string {
  items := tn.GetItems()

  return pg_nodes_to_string(items)
}

func build_name(names ...string) string {
  var cleaned_names []string

  for _, name := range names {
    if len(name) > 0 {
      cleaned_names = append(cleaned_names, name)
    }
  }

  return strings.Join(cleaned_names, ".")
}

func build_dependency(t StmtType, name string) *Dependency {
  return &Dependency { t, name, nil }
}

func append_dependency(ps *ParsedStmt, t StmtType, name string) {
  if name == "" {
    return
  }

  for _, d := range ps.dependencies {
    if d.stmt_name == name && d.stmt_type == t {
      return
    }
  }

  ps.dependencies = append(ps.dependencies, build_dependency(t, name))
}

func append_rangevar_dependency(ps *ParsedStmt, rv *pg_query.RangeVar) {
  schema := rv.GetSchemaname()

  append_dependency(ps, SCHEMA, schema)
  append_dependency(ps, TABLE, pg_rangevar_to_string(rv))
}



func unroll_statement_dependencies(stmt *ParsedStmt, stmts []*ParsedStmt) []*ParsedStmt {
  unrolled := make([]*ParsedStmt, 0) 

  if stmt.handled {
    return unrolled
  }

  if stmt == nil {
    return unrolled
  }

  for _, dep := range stmt.dependencies {
    unrolled = append(unrolled, unroll_statement_dependencies(dep.dependency, stmts)...) 
  }

  stmt.handled = true
  unrolled = append(unrolled, stmt)

  return unrolled
}

func sort_stmts_by_priority(stmts []*ParsedStmt) []*ParsedStmt {
  sorted_stmts := make([]*ParsedStmt, 0)

  for _, sch := range stmts {
    if sch.stmt_type == SCHEMA {
      sorted_stmts = append(sorted_stmts, sch)
      sch.handled = true
    }
  }

  for _, ext := range stmts {
    if ext.stmt_type == EXTENSION {
      sorted_stmts = append(sorted_stmts, ext)
      ext.handled = true
    }
  }

  for _, s := range stmts {
    if !s.handled {
      sorted_stmts = append(sorted_stmts, unroll_statement_dependencies(s, stmts)...)
    }
  }

  return sorted_stmts
}

func hydrate_dependencies(stmts []*ParsedStmt) {
  for _, p1 := range stmts {
    var valid_deps []*Dependency
    for _, dep := range p1.dependencies {
      for _, p2 := range stmts {
        if p2.name == dep.stmt_name {
          if p2.stmt_type == dep.stmt_type {
            dep.dependency = p2
            break
          } else if p2.stmt_type == GENERIC_TYPE {
            if dep.stmt_type == DOMAIN || dep.stmt_type == TYPE || dep.stmt_type == ENUM {
              dep.dependency = p2
              break
            }
          } else if dep.stmt_type == GENERIC_TYPE {
            if p2.stmt_type == DOMAIN || p2.stmt_type == TYPE || p2.stmt_type == ENUM {
              dep.dependency = p2
              break
            }
          }
        }
      }

      if dep.dependency != nil {
        valid_deps = append(valid_deps, dep)
      }
    }

    p1.dependencies = valid_deps
  }
}

func object_type_to_stmt_type(ot pg_query.ObjectType) StmtType {
  switch ot {
    case pg_query.ObjectType_OBJECT_ACCESS_METHOD:
        return ACCESS_METHOD
    case pg_query.ObjectType_OBJECT_AGGREGATE:
        return AGGREGATE
    case pg_query.ObjectType_OBJECT_CAST:
        return CAST
    case pg_query.ObjectType_OBJECT_COLLATION:
        return COLLATION
    case pg_query.ObjectType_OBJECT_COLUMN:
        return COLUMN
    case pg_query.ObjectType_OBJECT_CONVERSION:
        return CONVERSION
    case pg_query.ObjectType_OBJECT_TABCONSTRAINT:
        return TABLE_CONSTRAINT
    case pg_query.ObjectType_OBJECT_DOMCONSTRAINT:
        return DOMAIN_CONSTRAINT
    case pg_query.ObjectType_OBJECT_DATABASE:
        return DATABASE
    case pg_query.ObjectType_OBJECT_DOMAIN:
        return DOMAIN
    case pg_query.ObjectType_OBJECT_EVENT_TRIGGER:
        return EVENT_TRIGGER
    case pg_query.ObjectType_OBJECT_EXTENSION:
        return EXTENSION
    case pg_query.ObjectType_OBJECT_FDW:
        return FOREIGN_DATA_WRAPPER
    case pg_query.ObjectType_OBJECT_FOREIGN_TABLE:
        return FOREIGN_TABLE
    case pg_query.ObjectType_OBJECT_FUNCTION:
        return FUNCTION
    case pg_query.ObjectType_OBJECT_INDEX:
        return INDEX
    case pg_query.ObjectType_OBJECT_LANGUAGE:
        return LANGUAGE
    case pg_query.ObjectType_OBJECT_LARGEOBJECT:
        return LARGE_OBJECT
    case pg_query.ObjectType_OBJECT_MATVIEW:
        return MATERIALIZED_VIEW
    case pg_query.ObjectType_OBJECT_OPERATOR:
        return OPERATOR
    case pg_query.ObjectType_OBJECT_OPCLASS:
        return OPERATOR_CLASS
    case pg_query.ObjectType_OBJECT_OPFAMILY:
        return OPERATOR_FAMILY
    case pg_query.ObjectType_OBJECT_POLICY:
        return POLICY
    case pg_query.ObjectType_OBJECT_PROCEDURE:
        return PROCEDURE
    case pg_query.ObjectType_OBJECT_PUBLICATION:
        return PUBLICATION
    case pg_query.ObjectType_OBJECT_ROLE:
        return ROLE
    case pg_query.ObjectType_OBJECT_ROUTINE:
        return ROUTINE
    case pg_query.ObjectType_OBJECT_RULE:
        return RULE
    case pg_query.ObjectType_OBJECT_SCHEMA:
        return SCHEMA
    case pg_query.ObjectType_OBJECT_SEQUENCE:
        return SEQUENCE
    case pg_query.ObjectType_OBJECT_FOREIGN_SERVER:
        return FOREIGN_SERVER
    case pg_query.ObjectType_OBJECT_STATISTIC_EXT:
        return STATISTICS
    case pg_query.ObjectType_OBJECT_SUBSCRIPTION:
        return SUBSCRIPTION
    case pg_query.ObjectType_OBJECT_TABLE:
        return TABLE
    case pg_query.ObjectType_OBJECT_TABLESPACE:
        return TABLESPACE
    case pg_query.ObjectType_OBJECT_TSCONFIGURATION:
        return TEXT_SEARCH_CONFIGURATION
    case pg_query.ObjectType_OBJECT_TSDICTIONARY:
        return TEXT_SEARCH_DICTIONARY
    case pg_query.ObjectType_OBJECT_TSPARSER:
        return TEXT_SEARCH_PARSER
    case pg_query.ObjectType_OBJECT_TSTEMPLATE:
        return TEXT_SEARCH_TEMPLATE
    case pg_query.ObjectType_OBJECT_TRANSFORM:
        return TRANSFORM
    case pg_query.ObjectType_OBJECT_TRIGGER:
        return TRIGGER
    case pg_query.ObjectType_OBJECT_TYPE:
        return TYPE
    case pg_query.ObjectType_OBJECT_VIEW:
        return VIEW
    default:
        return UNKNOWN_TYPE
  }
}

func hydrate_stmt_object(node *pg_query.Node, ps *ParsedStmt) {
  if node == nil || node.GetNode() == nil {
    return
  }

  switch n := node.Node.(type) {
    case *pg_query.Node_CreateStmt: {
      ps.stmt_type = TABLE

      relation := n.CreateStmt.GetRelation()
      ps.name = pg_rangevar_to_string(relation)

      append_dependency(ps, SCHEMA, relation.GetSchemaname())

      table_elts := n.CreateStmt.GetTableElts()
      constraints := n.CreateStmt.GetConstraints()
      inherited := n.CreateStmt.GetInhRelations()
      tablespace := n.CreateStmt.GetTablespacename()

      append_dependency(ps, TABLESPACE, tablespace)

      for _, elt := range table_elts {
        hydrate_stmt_object(elt, ps)
      }

      for _, constraint := range constraints {
        hydrate_stmt_object(constraint, ps)
      }

      for _, inherited := range inherited {
        hydrate_stmt_object(inherited, ps)
      }
    }

    case *pg_query.Node_CreateTableAsStmt: {
      rel := n.CreateTableAsStmt.GetInto().GetRel()  
      query := n.CreateTableAsStmt.GetQuery()

      ps.name = pg_rangevar_to_string(rel)
      ps.stmt_type = MATERIALIZED_VIEW

      append_dependency(ps, SCHEMA, rel.GetSchemaname())

      hydrate_stmt_object(query, ps)
    }

    case *pg_query.Node_ViewStmt: {
      schema_name := n.ViewStmt.View.GetSchemaname()
      rel_name := n.ViewStmt.View.GetRelname()

      ps.name = build_name(schema_name, rel_name)
      ps.stmt_type = VIEW

      append_dependency(ps, SCHEMA, schema_name)

      hydrate_stmt_object(n.ViewStmt.Query, ps)
    }

    case *pg_query.Node_CaseWhen: {
      cw := n.CaseWhen

      expr := cw.GetExpr()
      result := cw.GetResult()

      hydrate_stmt_object(expr, ps)
      hydrate_stmt_object(result, ps)
    }

    case *pg_query.Node_NullTest: {
      arg := n.NullTest.GetArg()
      hydrate_stmt_object(arg, ps)
    }

    case *pg_query.Node_CoalesceExpr: {
      ce := n.CoalesceExpr
      hydrate_stmt_object(ce.GetXpr(), ps)
      for _, arg := range ce.GetArgs() {
        hydrate_stmt_object(arg, ps)
      }
    }

    case *pg_query.Node_MinMaxExpr: {
      mme := n.MinMaxExpr
      for _, arg := range mme.GetArgs() {
        hydrate_stmt_object(arg, ps)
      }

      hydrate_stmt_object(mme.GetXpr(), ps)
    }

    case *pg_query.Node_RangeFunction: {
      rf := n.RangeFunction

      for _, cfl := range rf.GetColdeflist() {
        hydrate_stmt_object(cfl, ps)
      }

      for _, fun := range rf.GetFunctions() {
        hydrate_stmt_object(fun, ps)
      }
    }

    case *pg_query.Node_SubLink: {
      sl := n.SubLink 

      hydrate_stmt_object(sl.GetTestexpr(), ps)
      hydrate_stmt_object(sl.GetXpr(), ps)
      hydrate_stmt_object(sl.GetSubselect(), ps)

      for _, on := range sl.GetOperName() {
        hydrate_stmt_object(on, ps)
      }
    }

    case *pg_query.Node_CaseExpr: {
      ps.stmt_type = CASE

      ce := n.CaseExpr

      dr := ce.GetDefresult()

      hydrate_stmt_object(dr, ps)

      for _, arg := range ce.GetArgs() {
        hydrate_stmt_object(arg, ps)
      }

    }

    case *pg_query.Node_CommonTableExpr: {
      query := n.CommonTableExpr.GetCtequery()
      hydrate_stmt_object(query, ps)
    }

    case *pg_query.Node_VariableSetStmt: {
      ps.stmt_type = VARIABLE 
      ps.name = n.VariableSetStmt.GetName()

      args := n.VariableSetStmt.GetArgs()

      for _, arg := range args {
        hydrate_stmt_object(arg, ps)
      }
    }

    case *pg_query.Node_CreateSchemaStmt: {
      ps.stmt_type = SCHEMA
      ps.name = n.CreateSchemaStmt.GetSchemaname()
    }

    case *pg_query.Node_CreateFunctionStmt: {
      func_name := pg_nodes_to_string(n.CreateFunctionStmt.GetFuncname())
      rtype := pg_nodes_to_string(n.CreateFunctionStmt.GetReturnType().GetNames())

      ps.stmt_type = FUNCTION
      ps.name = func_name

      append_dependency(ps, GENERIC_TYPE, rtype)

      options := n.CreateFunctionStmt.GetOptions()
      parameters := n.CreateFunctionStmt.GetParameters()

      for _, option := range options {
        hydrate_stmt_object(option, ps)
      }

      for _, parameter := range parameters {
        hydrate_stmt_object(parameter, ps)
      }
    }

    case *pg_query.Node_FunctionParameter: {
      arg_type := n.FunctionParameter.GetArgType()

      for _, name := range arg_type.GetNames() {
        hydrate_stmt_object(name, ps)
      }
    }

    case *pg_query.Node_AccessPriv: {

    }

    case *pg_query.Node_RoleSpec: {
      name := n.RoleSpec.GetRolename()
      append_dependency(ps, ROLE, name)
    }

    case *pg_query.Node_CreateEnumStmt: {
      enum := n.CreateEnumStmt

      ps.stmt_type = ENUM
      ps.name = pg_nodes_to_string(enum.GetTypeName())
    }

    case *pg_query.Node_CreateDomainStmt: {
      dname := n.CreateDomainStmt.GetDomainname()
      constraints := n.CreateDomainStmt.GetConstraints()

      ps.stmt_type = DOMAIN
      ps.name = pg_nodes_to_string(dname)

      type_name := n.CreateDomainStmt.GetTypeName()

      append_dependency(ps, GENERIC_TYPE, pg_typename_to_string(type_name))

      for _, constraint := range constraints {
        hydrate_stmt_object(constraint, ps)
      }
      
    }

    case *pg_query.Node_DropStmt: {
      ps.stmt_type = DROP

      ds := n.DropStmt 
      ds.GetRemoveType()
      for _, object := range ds.GetObjects() {
        ln := pg_list_to_string(object.GetList())
        append_dependency(ps, object_type_to_stmt_type(ds.GetRemoveType()), ln)
      }
    }

    case *pg_query.Node_CreateExtensionStmt: {
      ps.stmt_type = EXTENSION
      ps.name = n.CreateExtensionStmt.GetExtname()
    }

    case *pg_query.Node_ObjectWithArgs: {
      objname := n.ObjectWithArgs.GetObjname()
      append_dependency(ps, FUNCTION, pg_nodes_to_string(objname))
    }

    case *pg_query.Node_GrantStmt: {
      ps.stmt_type = GRANT

      objs := n.GrantStmt.GetObjects()

      privs := n.GrantStmt.GetPrivileges()
      grantees := n.GrantStmt.GetGrantees()

      for _, obj := range objs {
        hydrate_stmt_object(obj, ps)
      }

      for _, priv := range privs {
        hydrate_stmt_object(priv, ps)
      }

      for _, grantee := range grantees {
        hydrate_stmt_object(grantee, ps)
      }
    }

    case *pg_query.Node_SqlvalueFunction: {

    }

    case *pg_query.Node_DefElem: {
       
    }

    case *pg_query.Node_CreateTrigStmt: {
      rel := n.CreateTrigStmt.GetRelation()

      ps.name = n.CreateTrigStmt.GetTrigname()
      ps.stmt_type = TRIGGER

      fname := pg_nodes_to_string(n.CreateTrigStmt.GetFuncname())
      append_rangevar_dependency(ps, rel)
      append_dependency(ps, FUNCTION, fname)
    }

    case *pg_query.Node_CreatePolicyStmt: {
      ps.name = n.CreatePolicyStmt.GetPolicyName()
      ps.stmt_type = POLICY

      table := n.CreatePolicyStmt.GetTable()

      roles := n.CreatePolicyStmt.GetRoles()
      qual := n.CreatePolicyStmt.GetQual()

      hydrate_stmt_object(qual, ps)

      append_rangevar_dependency(ps, table)

      for _, role := range roles {
        hydrate_stmt_object(role, ps)
      }
    }

    case *pg_query.Node_GrantRoleStmt: {
      ps.stmt_type = GRANT

      grs := n.GrantRoleStmt
      roles := grs.GetGrantedRoles()
      groles := grs.GetGranteeRoles()

      for _, role := range roles {
        hydrate_stmt_object(role, ps)
      }

      for _, grole := range groles {
        hydrate_stmt_object(grole, ps)
      }
    }

    case *pg_query.Node_AlterTableCmd: {

    }

    case *pg_query.Node_AlterDefaultPrivilegesStmt: {
      ps.stmt_type = ALTER_DEFAULT_PRIVILEGES
      adps := n.AlterDefaultPrivilegesStmt
      action := adps.GetAction()

      grantees := action.GetGrantees()
      privs := action.GetPrivileges()

      for _, g := range grantees {
        hydrate_stmt_object(g, ps)
      }

      for _, p := range privs {
        hydrate_stmt_object(p, ps)
      }


    }

    case *pg_query.Node_AlterTableStmt: {
      ps.stmt_type = ALTER_TABLE

      relation := n.AlterTableStmt.GetRelation()
      schema_name := relation.GetSchemaname()
      table := relation.GetRelname()

      cmds := n.AlterTableStmt.GetCmds()

      for _, cmd := range cmds {
        hydrate_stmt_object(cmd, ps)
      }

      append_dependency(ps, SCHEMA, schema_name)
      append_dependency(ps, TABLE, table)
    }

    case *pg_query.Node_IndexElem: {

    }

    case *pg_query.Node_IndexStmt: {
      ps.name = n.IndexStmt.GetIdxname()
      ps.stmt_type = INDEX

      relation := n.IndexStmt.GetRelation()
      append_dependency(ps, SCHEMA, relation.GetSchemaname())

      for _, ip := range n.IndexStmt.GetIndexParams() {
        hydrate_stmt_object(ip, ps)
      }
    }

    case *pg_query.Node_DoStmt: {
      ps.stmt_type = DO

      for _, arg := range n.DoStmt.GetArgs() {
        hydrate_stmt_object(arg, ps)
      }

    }

    case *pg_query.Node_String_: {

    }

    case *pg_query.Node_List: {
      items := n.List.GetItems()

      for _, item := range items {
        hydrate_stmt_object(item, ps)
      }
    }

    case *pg_query.Node_CompositeTypeStmt: {
      tv := n.CompositeTypeStmt.GetTypevar()
      name := pg_rangevar_to_string(tv)

      ps.name = name
      ps.stmt_type = TYPE

      append_dependency(ps, SCHEMA, tv.GetSchemaname())

      for _, cd := range n.CompositeTypeStmt.GetColdeflist() {
        hydrate_stmt_object(cd, ps)
      }
    }

    case *pg_query.Node_CommentStmt: {
      cmt := n.CommentStmt.GetObject()

      name := pg_list_to_string(cmt.GetList())

      ps.name = n.CommentStmt.GetComment()
      ps.stmt_type = COMMENT

      append_dependency(ps, object_type_to_stmt_type(n.CommentStmt.GetObjtype()), name)

      hydrate_stmt_object(cmt, ps)
    }

    case *pg_query.Node_SelectStmt: {
      ps.stmt_type = SELECT

      targets := n.SelectStmt.GetTargetList()
      from_clauses := n.SelectStmt.GetFromClause()
      having_clause := n.SelectStmt.GetHavingClause()
      where_clause := n.SelectStmt.GetWhereClause()
      with_clause := n.SelectStmt.GetWithClause()

      hydrate_stmt_object(where_clause, ps)

      for _, target := range targets {
        hydrate_stmt_object(target, ps)
      }

      for _, from_clause := range from_clauses {
        hydrate_stmt_object(from_clause, ps)
      }

      for _, cte := range with_clause.GetCtes() {
        hydrate_stmt_object(cte, ps)
      }

      hydrate_stmt_object(having_clause, ps)
    }

    case *pg_query.Node_ResTarget: {
      hydrate_stmt_object(n.ResTarget.GetVal(), ps)
    }

    case *pg_query.Node_ColumnRef: {
      
    }

    case *pg_query.Node_BoolExpr: {
      args := n.BoolExpr.GetArgs()

      for _, arg := range args {
        hydrate_stmt_object(arg, ps)
      }
    }

    case *pg_query.Node_AExpr: {
      lexpr := n.AExpr.GetLexpr()
      rexpr := n.AExpr.GetRexpr()

      hydrate_stmt_object(lexpr, ps)
      hydrate_stmt_object(rexpr, ps)
    }

    case *pg_query.Node_AConst: {

    }

    case *pg_query.Node_TypeCast: {
      type_name := n.TypeCast.GetTypeName()
      name_as_string := pg_nodes_to_string(type_name.GetNames())

      if !strings.HasPrefix(name_as_string, "pg_catalog") {
        append_dependency(ps, GENERIC_TYPE, name_as_string)
      }
    }

    case *pg_query.Node_RangeVar: {
      append_rangevar_dependency(ps, n.RangeVar)
    }

    case *pg_query.Node_FuncCall: {
      name := pg_nodes_to_string(n.FuncCall.GetFuncname()) 
      args := n.FuncCall.GetArgs()

      if name == "nextval" {
        if len(args) == 1 {
          seq_name := args[0].GetAConst().GetSval()
          append_dependency(ps, SEQUENCE, seq_name.GetSval())
        }

      } else {
        append_dependency(ps, FUNCTION, name)
        
        for _, arg := range args {
          hydrate_stmt_object(arg, ps)
        }
      }
    }

    case *pg_query.Node_RangeSubselect: {
      subquery := n.RangeSubselect.GetSubquery()
      hydrate_stmt_object(subquery, ps)
    }

    case *pg_query.Node_ColumnDef: {
      cd := n.ColumnDef
      type_name := cd.GetTypeName()
      names := type_name.GetNames()
      column_type_name := pg_nodes_to_string(names)
      constraints := cd.GetConstraints()

      colc := cd.GetCollClause()

      append_dependency(ps, COLLATION, pg_nodes_to_string(colc.GetCollname()))

      if !strings.HasPrefix(column_type_name, "pg_catalog") {
        append_dependency(ps, GENERIC_TYPE, column_type_name)
      }

      for _, constraint := range constraints {
        hydrate_stmt_object(constraint, ps)
      }
    }

    case *pg_query.Node_Constraint: {
      pktable := n.Constraint.GetPktable()  
      pktable_name := pg_rangevar_to_string(pktable)
      raw_expr := n.Constraint.GetRawExpr()

      append_dependency(ps, TABLE, pktable_name)

      hydrate_stmt_object(raw_expr, ps)
    }

    case *pg_query.Node_UpdateStmt: {
      relation := n.UpdateStmt.GetRelation()
      schema := relation.GetSchemaname()
      table := relation.GetRelname()

      log.Printf("table: %v\n", table)

      append_dependency(ps, SCHEMA, schema)
      append_dependency(ps, TABLE, table)

      from := n.UpdateStmt.GetFromClause()
      targets := n.UpdateStmt.GetTargetList()
      where := n.UpdateStmt.GetWhereClause()

      for _, f := range from {
        hydrate_stmt_object(f, ps)
      }

      for _, t := range targets {
        hydrate_stmt_object(t, ps)
      }

      hydrate_stmt_object(where, ps)
    }

    case *pg_query.Node_JoinExpr: {
      je := n.JoinExpr
      larg := je.GetLarg()
      rarg := je.GetRarg()
      hydrate_stmt_object(larg, ps)
      hydrate_stmt_object(rarg, ps)
    }

    case *pg_query.Node_InsertStmt: {
      relation := n.InsertStmt.GetRelation()

      append_dependency(ps, SCHEMA, relation.GetSchemaname())
      append_dependency(ps, TABLE, pg_rangevar_to_string(relation))

      cols := n.InsertStmt.GetCols()

      select_stmt := n.InsertStmt.GetSelectStmt()

      for _, c := range cols {
        hydrate_stmt_object(c, ps)
      }

      hydrate_stmt_object(select_stmt, ps)
    }

    default: {
      log.Printf("PARSE TREE: %v\n\n", ps.json)
      log.Fatalf("Unknown node type %v\n", node) 
    }
  }

  ps.has_name = ps.name != ""
}

func extract_stmts(pr *pg_query.ParseResult) []*ParsedStmt {
  var ps []*ParsedStmt
  dependencies := make([]*Dependency, 0)

  for _, x := range pr.Stmts {
    dp, err := deparse_raw_stmt(x)
    perr(err)
    json, err := pg_query.ParseToJSON(dp)
    perr(err)
    nps := &ParsedStmt{ 
      stmt: x, 
      has_name: false,
      name: "",
      deparsed: dp, 
      json: json,
      hash: hash_string(dp), 
      stmt_type: UNKNOWN_TYPE,
      dependencies: dependencies,
      handled: false,
      removed: false,
    }
    hydrate_stmt_object(x.GetStmt(), nps)
    ps = append(ps, nps) 
  }

  return ps
}
