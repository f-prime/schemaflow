package core

import (
	"fmt"
	"log"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func pgAconstToString(ac *pg_query.A_Const) string {
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

func pgNodesToString(nodes []*pg_query.Node) string {
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

func pgRangevarToString(rv *pg_query.RangeVar) string {
  //sn := rv.GetSchemaname()

  //if len(sn) == 0 {
  //  return rv.GetRelname()
  //}

  return rv.GetRelname()
}

func pgTypenameToString(tn *pg_query.TypeName) string {
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

func pgListToString(tn *pg_query.List) string {
  items := tn.GetItems()

  return pgNodesToString(items)
}

func buildName(names ...string) string {
  var cleaned_names []string

  for _, name := range names {
    if len(name) > 0 {
      cleaned_names = append(cleaned_names, name)
    }
  }

  return strings.Join(cleaned_names, ".")
}

func buildDependency(t StmtType, name string) *Dependency {
  return &Dependency { t, name, nil }
}

func appendDependency(ps *ParsedStmt, t StmtType, name string) {
  if name == "" {
    return
  }

  for _, d := range ps.Dependencies {
    if d.StmtName == name && d.StmtType == t {
      return
    }
  }

  ps.Dependencies = append(ps.Dependencies, buildDependency(t, name))
}

func appendRangevarDependency(ps *ParsedStmt, rv *pg_query.RangeVar) {
  schema := rv.GetSchemaname()

  appendDependency(ps, SCHEMA, schema)
  appendDependency(ps, TABLE, pgRangevarToString(rv))
}



func unrollStatementDependencies(stmt *ParsedStmt, stmts []*ParsedStmt) []*ParsedStmt {
  unrolled := make([]*ParsedStmt, 0) 

  if stmt.Handled {
    return unrolled
  }

  if stmt == nil {
    return unrolled
  }

  for _, dep := range stmt.Dependencies {
    unrolled = append(unrolled, unrollStatementDependencies(dep.Dependency, stmts)...) 
  }

  stmt.Handled = true
  unrolled = append(unrolled, stmt)

  return unrolled
}

func hydrateDependencies(stmts []*ParsedStmt) {
  for _, p1 := range stmts {
    var valid_deps []*Dependency
    for _, dep := range p1.Dependencies {
      for _, p2 := range stmts {
        if p2.Name == dep.StmtName {
          if p2.StmtType == dep.StmtType {
            dep.Dependency = p2
            break
          } else if p2.StmtType == GENERIC_TYPE {
            if dep.StmtType == DOMAIN || dep.StmtType == TYPE || dep.StmtType == ENUM {
              dep.Dependency = p2
              break
            }
          } else if dep.StmtType == GENERIC_TYPE {
            if p2.StmtType == DOMAIN || p2.StmtType == TYPE || p2.StmtType == ENUM {
              dep.Dependency = p2
              break
            }
          }
        }
      }

      if dep.Dependency != nil {
        valid_deps = append(valid_deps, dep)
      }
    }

    p1.Dependencies = valid_deps
  }
}

func objectTypeToStmtType(ot pg_query.ObjectType) StmtType {
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

func hydrateStmtObject(node *pg_query.Node, ps *ParsedStmt) {
  if node == nil || node.GetNode() == nil {
    return
  }

  switch n := node.Node.(type) {
    case *pg_query.Node_CreateStmt: {
      ps.StmtType = TABLE

      relation := n.CreateStmt.GetRelation()
      ps.Name = pgRangevarToString(relation)

      appendDependency(ps, SCHEMA, relation.GetSchemaname())

      table_elts := n.CreateStmt.GetTableElts()
      constraints := n.CreateStmt.GetConstraints()
      inherited := n.CreateStmt.GetInhRelations()
      tablespace := n.CreateStmt.GetTablespacename()

      appendDependency(ps, TABLESPACE, tablespace)

      for _, elt := range table_elts {
        hydrateStmtObject(elt, ps)
      }

      for _, constraint := range constraints {
        hydrateStmtObject(constraint, ps)
      }

      for _, inherited := range inherited {
        hydrateStmtObject(inherited, ps)
      }
    }

    case *pg_query.Node_CreateTableAsStmt: {
      rel := n.CreateTableAsStmt.GetInto().GetRel()  
      query := n.CreateTableAsStmt.GetQuery()

      ps.Name = pgRangevarToString(rel)
      ps.StmtType = MATERIALIZED_VIEW

      appendDependency(ps, SCHEMA, rel.GetSchemaname())

      hydrateStmtObject(query, ps)
    }

    case *pg_query.Node_ViewStmt: {
      schema_name := n.ViewStmt.View.GetSchemaname()
      rel_name := n.ViewStmt.View.GetRelname()

      ps.Name = buildName(schema_name, rel_name)
      ps.StmtType = VIEW

      appendDependency(ps, SCHEMA, schema_name)

      hydrateStmtObject(n.ViewStmt.Query, ps)
    }

    case *pg_query.Node_CaseWhen: {
      cw := n.CaseWhen

      expr := cw.GetExpr()
      result := cw.GetResult()

      hydrateStmtObject(expr, ps)
      hydrateStmtObject(result, ps)
    }

    case *pg_query.Node_NullTest: {
      arg := n.NullTest.GetArg()
      hydrateStmtObject(arg, ps)
    }

    case *pg_query.Node_CoalesceExpr: {
      ce := n.CoalesceExpr
      hydrateStmtObject(ce.GetXpr(), ps)
      for _, arg := range ce.GetArgs() {
        hydrateStmtObject(arg, ps)
      }
    }

    case *pg_query.Node_MinMaxExpr: {
      mme := n.MinMaxExpr
      for _, arg := range mme.GetArgs() {
        hydrateStmtObject(arg, ps)
      }

      hydrateStmtObject(mme.GetXpr(), ps)
    }

    case *pg_query.Node_RangeFunction: {
      rf := n.RangeFunction

      for _, cfl := range rf.GetColdeflist() {
        hydrateStmtObject(cfl, ps)
      }

      for _, fun := range rf.GetFunctions() {
        hydrateStmtObject(fun, ps)
      }
    }

    case *pg_query.Node_SubLink: {
      sl := n.SubLink 

      hydrateStmtObject(sl.GetTestexpr(), ps)
      hydrateStmtObject(sl.GetXpr(), ps)
      hydrateStmtObject(sl.GetSubselect(), ps)

      for _, on := range sl.GetOperName() {
        hydrateStmtObject(on, ps)
      }
    }

    case *pg_query.Node_CaseExpr: {
      ps.StmtType = CASE

      ce := n.CaseExpr

      dr := ce.GetDefresult()

      hydrateStmtObject(dr, ps)

      for _, arg := range ce.GetArgs() {
        hydrateStmtObject(arg, ps)
      }

    }

    case *pg_query.Node_CommonTableExpr: {
      query := n.CommonTableExpr.GetCtequery()
      hydrateStmtObject(query, ps)
    }

    case *pg_query.Node_VariableSetStmt: {
      ps.StmtType = VARIABLE 
      ps.Name = n.VariableSetStmt.GetName()

      args := n.VariableSetStmt.GetArgs()

      for _, arg := range args {
        hydrateStmtObject(arg, ps)
      }
    }

    case *pg_query.Node_CreateSchemaStmt: {
      ps.StmtType = SCHEMA
      ps.Name = n.CreateSchemaStmt.GetSchemaname()
    }

    case *pg_query.Node_CreateFunctionStmt: {
      func_name := pgNodesToString(n.CreateFunctionStmt.GetFuncname())
      rtype := pgNodesToString(n.CreateFunctionStmt.GetReturnType().GetNames())

      ps.StmtType = FUNCTION
      ps.Name = func_name

      appendDependency(ps, GENERIC_TYPE, rtype)

      options := n.CreateFunctionStmt.GetOptions()
      parameters := n.CreateFunctionStmt.GetParameters()

      for _, option := range options {
        hydrateStmtObject(option, ps)
      }

      for _, parameter := range parameters {
        hydrateStmtObject(parameter, ps)
      }
    }

    case *pg_query.Node_FunctionParameter: {
      arg_type := n.FunctionParameter.GetArgType()

      for _, name := range arg_type.GetNames() {
        hydrateStmtObject(name, ps)
      }
    }

    case *pg_query.Node_AccessPriv: {

    }

    case *pg_query.Node_RoleSpec: {
      name := n.RoleSpec.GetRolename()
      appendDependency(ps, ROLE, name)
    }

    case *pg_query.Node_CreateEnumStmt: {
      enum := n.CreateEnumStmt

      ps.StmtType = ENUM
      ps.Name = pgNodesToString(enum.GetTypeName())
    }

    case *pg_query.Node_CreateDomainStmt: {
      dname := n.CreateDomainStmt.GetDomainname()
      constraints := n.CreateDomainStmt.GetConstraints()

      ps.StmtType = DOMAIN
      ps.Name = pgNodesToString(dname)

      type_name := n.CreateDomainStmt.GetTypeName()

      appendDependency(ps, GENERIC_TYPE, pgTypenameToString(type_name))

      for _, constraint := range constraints {
        hydrateStmtObject(constraint, ps)
      }
      
    }

    case *pg_query.Node_DropStmt: {
      ps.StmtType = DROP

      ds := n.DropStmt 
      ds.GetRemoveType()
      for _, object := range ds.GetObjects() {
        ln := pgListToString(object.GetList())
        appendDependency(ps, objectTypeToStmtType(ds.GetRemoveType()), ln)
      }
    }

    case *pg_query.Node_CreateExtensionStmt: {
      ps.StmtType = EXTENSION
      ps.Name = n.CreateExtensionStmt.GetExtname()
    }

    case *pg_query.Node_ObjectWithArgs: {
      objname := n.ObjectWithArgs.GetObjname()
      appendDependency(ps, FUNCTION, pgNodesToString(objname))
    }

    case *pg_query.Node_GrantStmt: {
      ps.StmtType = GRANT

      objs := n.GrantStmt.GetObjects()

      privs := n.GrantStmt.GetPrivileges()
      grantees := n.GrantStmt.GetGrantees()

      for _, obj := range objs {
        hydrateStmtObject(obj, ps)
      }

      for _, priv := range privs {
        hydrateStmtObject(priv, ps)
      }

      for _, grantee := range grantees {
        hydrateStmtObject(grantee, ps)
      }
    }

    case *pg_query.Node_SqlvalueFunction: {

    }

    case *pg_query.Node_DefElem: {
       
    }

    case *pg_query.Node_CreateTrigStmt: {
      rel := n.CreateTrigStmt.GetRelation()

      ps.Name = n.CreateTrigStmt.GetTrigname()
      ps.StmtType = TRIGGER

      fname := pgNodesToString(n.CreateTrigStmt.GetFuncname())
      appendRangevarDependency(ps, rel)
      appendDependency(ps, FUNCTION, fname)
    }

    case *pg_query.Node_CreatePolicyStmt: {
      ps.Name = n.CreatePolicyStmt.GetPolicyName()
      ps.StmtType = POLICY

      table := n.CreatePolicyStmt.GetTable()

      roles := n.CreatePolicyStmt.GetRoles()
      qual := n.CreatePolicyStmt.GetQual()

      hydrateStmtObject(qual, ps)

      appendRangevarDependency(ps, table)

      for _, role := range roles {
        hydrateStmtObject(role, ps)
      }
    }

    case *pg_query.Node_CreateRoleStmt: {
      cs := n.CreateRoleStmt

      ps.Name = n.CreateRoleStmt.GetRole()
      ps.StmtType = ROLE

      for _, o := range cs.GetOptions() {
        hydrateStmtObject(o, ps)
      }
    
    }

    case *pg_query.Node_GrantRoleStmt: {
      ps.StmtType = GRANT

      grs := n.GrantRoleStmt
      roles := grs.GetGrantedRoles()
      groles := grs.GetGranteeRoles()

      for _, role := range roles {
        hydrateStmtObject(role, ps)
      }

      for _, grole := range groles {
        hydrateStmtObject(grole, ps)
      }
    }

    case *pg_query.Node_AlterTableCmd: {

    }

    case *pg_query.Node_AlterDefaultPrivilegesStmt: {
      ps.StmtType = ALTER_DEFAULT_PRIVILEGES
      adps := n.AlterDefaultPrivilegesStmt
      action := adps.GetAction()

      grantees := action.GetGrantees()
      privs := action.GetPrivileges()

      for _, g := range grantees {
        hydrateStmtObject(g, ps)
      }

      for _, p := range privs {
        hydrateStmtObject(p, ps)
      }


    }

    case *pg_query.Node_AlterTableStmt: {
      ps.StmtType = ALTER_TABLE

      relation := n.AlterTableStmt.GetRelation()
      schema_name := relation.GetSchemaname()
      table := relation.GetRelname()

      cmds := n.AlterTableStmt.GetCmds()

      for _, cmd := range cmds {
        hydrateStmtObject(cmd, ps)
      }

      appendDependency(ps, SCHEMA, schema_name)
      appendDependency(ps, TABLE, table)
    }

    case *pg_query.Node_IndexElem: {

    }

    case *pg_query.Node_IndexStmt: {
      ps.Name = n.IndexStmt.GetIdxname()
      ps.StmtType = INDEX

      relation := n.IndexStmt.GetRelation()
      appendDependency(ps, SCHEMA, relation.GetSchemaname())

      for _, ip := range n.IndexStmt.GetIndexParams() {
        hydrateStmtObject(ip, ps)
      }
    }

    case *pg_query.Node_DoStmt: {
      ps.StmtType = DO

      for _, arg := range n.DoStmt.GetArgs() {
        hydrateStmtObject(arg, ps)
      }

    }

    case *pg_query.Node_String_: {

    }

    case *pg_query.Node_List: {
      items := n.List.GetItems()

      for _, item := range items {
        hydrateStmtObject(item, ps)
      }
    }

    case *pg_query.Node_CompositeTypeStmt: {
      tv := n.CompositeTypeStmt.GetTypevar()
      name := pgRangevarToString(tv)

      ps.Name = name
      ps.StmtType = TYPE

      appendDependency(ps, SCHEMA, tv.GetSchemaname())

      for _, cd := range n.CompositeTypeStmt.GetColdeflist() {
        hydrateStmtObject(cd, ps)
      }
    }

    case *pg_query.Node_CommentStmt: {
      cmt := n.CommentStmt.GetObject()

      name := pgListToString(cmt.GetList())

      ps.Name = n.CommentStmt.GetComment()
      ps.StmtType = COMMENT

      appendDependency(ps, objectTypeToStmtType(n.CommentStmt.GetObjtype()), name)

      hydrateStmtObject(cmt, ps)
    }

    case *pg_query.Node_SelectStmt: {
      ps.StmtType = SELECT

      targets := n.SelectStmt.GetTargetList()
      from_clauses := n.SelectStmt.GetFromClause()
      having_clause := n.SelectStmt.GetHavingClause()
      where_clause := n.SelectStmt.GetWhereClause()
      with_clause := n.SelectStmt.GetWithClause()

      hydrateStmtObject(where_clause, ps)

      for _, target := range targets {
        hydrateStmtObject(target, ps)
      }

      for _, from_clause := range from_clauses {
        hydrateStmtObject(from_clause, ps)
      }

      for _, cte := range with_clause.GetCtes() {
        hydrateStmtObject(cte, ps)
      }

      hydrateStmtObject(having_clause, ps)
    }

    case *pg_query.Node_ResTarget: {
      hydrateStmtObject(n.ResTarget.GetVal(), ps)
    }

    case *pg_query.Node_ColumnRef: {
      
    }

    case *pg_query.Node_BoolExpr: {
      args := n.BoolExpr.GetArgs()

      for _, arg := range args {
        hydrateStmtObject(arg, ps)
      }
    }

    case *pg_query.Node_AExpr: {
      lexpr := n.AExpr.GetLexpr()
      rexpr := n.AExpr.GetRexpr()

      hydrateStmtObject(lexpr, ps)
      hydrateStmtObject(rexpr, ps)
    }

    case *pg_query.Node_AConst: {

    }

    case *pg_query.Node_TypeCast: {
      type_name := n.TypeCast.GetTypeName()
      name_as_string := pgNodesToString(type_name.GetNames())

      if !strings.HasPrefix(name_as_string, "pg_catalog") {
        appendDependency(ps, GENERIC_TYPE, name_as_string)
      }
    }

    case *pg_query.Node_RangeVar: {
      appendRangevarDependency(ps, n.RangeVar)
    }

    case *pg_query.Node_FuncCall: {
      name := pgNodesToString(n.FuncCall.GetFuncname()) 
      args := n.FuncCall.GetArgs()

      if name == "nextval" {
        if len(args) == 1 {
          seq_name := args[0].GetAConst().GetSval()
          appendDependency(ps, SEQUENCE, seq_name.GetSval())
        }

      } else {
        appendDependency(ps, FUNCTION, name)
        
        for _, arg := range args {
          hydrateStmtObject(arg, ps)
        }
      }
    }

    case *pg_query.Node_RangeSubselect: {
      subquery := n.RangeSubselect.GetSubquery()
      hydrateStmtObject(subquery, ps)
    }

    case *pg_query.Node_ColumnDef: {
      cd := n.ColumnDef
      type_name := cd.GetTypeName()
      names := type_name.GetNames()
      column_type_name := pgNodesToString(names)
      constraints := cd.GetConstraints()

      colc := cd.GetCollClause()

      appendDependency(ps, COLLATION, pgNodesToString(colc.GetCollname()))

      if !strings.HasPrefix(column_type_name, "pg_catalog") {
        appendDependency(ps, GENERIC_TYPE, column_type_name)
      }

      for _, constraint := range constraints {
        hydrateStmtObject(constraint, ps)
      }
    }

    case *pg_query.Node_Constraint: {
      pktable := n.Constraint.GetPktable()  
      pktable_name := pgRangevarToString(pktable)
      raw_expr := n.Constraint.GetRawExpr()

      appendDependency(ps, TABLE, pktable_name)

      hydrateStmtObject(raw_expr, ps)
    }

    case *pg_query.Node_UpdateStmt: {
      relation := n.UpdateStmt.GetRelation()
      schema := relation.GetSchemaname()
      table := relation.GetRelname()

      appendDependency(ps, SCHEMA, schema)
      appendDependency(ps, TABLE, table)

      from := n.UpdateStmt.GetFromClause()
      targets := n.UpdateStmt.GetTargetList()
      where := n.UpdateStmt.GetWhereClause()

      for _, f := range from {
        hydrateStmtObject(f, ps)
      }

      for _, t := range targets {
        hydrateStmtObject(t, ps)
      }

      hydrateStmtObject(where, ps)
    }

    case *pg_query.Node_JoinExpr: {
      je := n.JoinExpr
      larg := je.GetLarg()
      rarg := je.GetRarg()
      hydrateStmtObject(larg, ps)
      hydrateStmtObject(rarg, ps)
    }

    case *pg_query.Node_InsertStmt: {
      relation := n.InsertStmt.GetRelation()

      appendDependency(ps, SCHEMA, relation.GetSchemaname())
      appendDependency(ps, TABLE, pgRangevarToString(relation))

      cols := n.InsertStmt.GetCols()

      select_stmt := n.InsertStmt.GetSelectStmt()

      for _, c := range cols {
        hydrateStmtObject(c, ps)
      }

      hydrateStmtObject(select_stmt, ps)
    }

    default: {
      // log.Printf("PARSE TREE: %v\n\n", ps.Json)
      log.Printf("WARNING: Unknown node type %v this warning should be reported.\n", node) 
      ps.StmtType = UNKNOWN_TYPE
    }
  }

  ps.HasName = ps.Name != ""
}

func sortStmtsByPriority(stmts []*ParsedStmt) []*ParsedStmt {
  seen := make(map[string]bool)

  sorted_stmts := make([]*ParsedStmt, 0)

  for _, sch := range stmts {
    if seen[sch.Hash] {
      continue
    }

    if sch.StmtType == SCHEMA {
      sorted_stmts = append(sorted_stmts, sch)
      sch.Handled = true
    }
  }

  for _, ext := range stmts {
    if seen[ext.Hash] {
      continue
    }

    if ext.StmtType == EXTENSION {
      sorted_stmts = append(sorted_stmts, ext)
      ext.Handled = true
    }
  }

  for _, s := range stmts {
    if seen[s.Hash] {
      continue
    }

    if !s.Handled {
      sorted_stmts = append(sorted_stmts, unrollStatementDependencies(s, stmts)...)
    }
  }

  return sorted_stmts
}

func setStmtStatus(ctx *Context, stmt *ParsedStmt) {
  if ctx == nil {
    return
  }

  stmt_hash_found := isStmtHashFoundInDb(ctx, stmt)
  stmt_name_found := isStmtNameFoundInDb(ctx, stmt)

  if (stmt_name_found && stmt_hash_found) || (!stmt_name_found && stmt_hash_found) {
    stmt.Status = UNCHANGED
  } else if stmt_name_found && !stmt_hash_found {
    stmt.PrevStmt = getPrevStmtVersion(ctx, stmt)
    stmt.Status = CHANGED
  } else {
    stmt.Status = NEW
  }
}

func buildParsedStmts(ctx *Context) *[]*ParsedStmt {
  var ps []*ParsedStmt

  err := filepath.Walk(ctx.SqlPath, func(path string, info fs.FileInfo, err error) error {
    perr(err)

    if !strings.HasSuffix(info.Name(), ".sql") {
      return nil
    }

    log.Printf("Processing file %s\n", path)

    fdata, err := os.ReadFile(path)

    perr(err)

    parsed_file, parse_err := parseSql(string(fdata))

    if parse_err != nil {
      log.Panicf("Syntax Error in %v:\n\n %v\n", path, parse_err)
    }

    extracted := extractStmts(ctx, parsed_file)

    ps = append(ps, extracted...)

    return nil
  })

  log.Println("Building dependency graph...");
  hydrateDependencies(ps)

  perr(err)

  sorted_stmts := sortStmtsByPriority(ps)
  return &sorted_stmts
}

func extractStmts(ctx *Context, pr *pg_query.ParseResult) []*ParsedStmt {
  var ps []*ParsedStmt
  dependencies := make([]*Dependency, 0)

  for _, x := range pr.Stmts {
    dp, err := deparseRawStmt(x)
    perr(err)
    json, err := pg_query.ParseToJSON(dp)
    perr(err)
    nps := &ParsedStmt{ 
      Stmt: x, 
      PrevStmt: nil,
      HasName: false,
      Name: "",
      Deparsed: dp, 
      Json: json,
      Hash: HashString(dp), 
      StmtType: UNKNOWN_TYPE,
      Dependencies: dependencies,
      Handled: false,
      Removed: false,
      Status: UNKNOWN,
    }

    hydrateStmtObject(x.GetStmt(), nps)
    setStmtStatus(ctx, nps)

    ps = append(ps, nps) 
  }

  return ps
}
