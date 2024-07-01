package main

import (
	"crypto/sha1"
	sql "database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

const MIGRATION_FOLDER = "migrations"
const MIGRATION_REQUIRED = "-- MIGRATION REQUIRED"
const MAKE_MIGRATIONS_CMD = "makemigrations"
const CLEAR_MIGRATIONS_CMD = "clearmigrations"
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
  stmt_type integer not null,
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

// THE ORDER OF THIS ENUM IS THE SORT BUCKET PRIORITY ORDER
const (
  DATABASE StmtType = iota
  SERVER
  SCHEMA 
  EXTENSION
  USER
  ROLE
  VARIABLE
  CAST
  ACCESS_METHOD
  FOREIGN_SERVER
  OPERATOR
  OPERATOR_CLASS
  OPERATOR_FAMILY
  STATISTICS
  TEXT_SEARCH_CONFIGURATION
  TEXT_SEARCH_DICTIONARY
  TEXT_SEARCH_PARSER
  TEXT_SEARCH_TEMPLATE
  FUNCTION
  DOMAIN
  DOMAIN_CONSTRAINT
  TYPE
  COLUMN_TYPE // This is an ambiguous type. Could be a domain or a type.
  AGGREGATE
  COLLATION
  LANGUAGE
  ENUM

  FOREIGN_DATA_WRAPPER
  FOREIGN_TABLE
  TABLE 
  VIEW
  MATERIALIZED_VIEW
  INDEX
  COLUMN
  CASE
  CONVERSION
  SEQUENCE
  LARGE_OBJECT
  ROUTINE
  TRANSFORM
  SELECT
  PROCEDURE
  COMMENT
  GRANT
  GRANT_ROLE
  UPDATE
  ALTER_DEFAULT_PRIVILEGES
  ALTER_POLICY
  ALTER_TABLE
  EVENT_TRIGGER
  TRIGGER
  RULE
  CONSTRAINT
  TABLE_CONSTRAINT
  TABLESPACE
  GROUP
  POLICY
  PUBLICATION
  SUBSCRIPTION
  INSERT
  DROP_OWNED
  DROP
  DO
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

type Dependency struct {
  stmt_type StmtType
  stmt_name string
}

type ParsedStmt struct {
  stmt *pg_query.RawStmt
  has_name bool
  name string
  deparsed string
  sort_priority int
  json string
  hash string
  status StmtStatus
  stmt_type StmtType
  dependencies []Dependency
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
  dc.SetMaxOpenConns(10)

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

func pg_nodes_to_string(nodes []*pg_query.Node) string {
  var name []string 

  for _, node := range nodes {
    name = append(name, node.GetString_().Sval)
  }

  return strings.Join(name, ".")
}

func pg_rangevar_to_string(rv *pg_query.RangeVar) string {
  sn := rv.GetSchemaname()

  if len(sn) == 0 {
    return rv.GetRelname()
  }

  return sn + "." + rv.GetRelname()
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

func build_dependency(t StmtType, name string) Dependency {
  return Dependency { t, name }
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
      ps.stmt_type = TABLE

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

      if schema_name != "" {
        append_dependency(ps, SCHEMA, schema_name)
      }

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

      append_dependency(ps, TYPE, rtype)

      options := n.CreateFunctionStmt.GetOptions()

      for _, option := range options {
        hydrate_stmt_object(option, ps)
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

      append_dependency(ps, TYPE, pg_typename_to_string(type_name))

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
        append_dependency(ps, COLUMN_TYPE, name_as_string)
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
        append_dependency(ps, COLUMN_TYPE, column_type_name)
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
      fmt.Printf("PARSE TREE: %v\n\n", ps.json)
      log.Fatalf("Unknown node type %v\n", node) 
    }
  }

  ps.has_name = ps.name != ""
  ps.sort_priority = int(ps.stmt_type)
}

func get_current_version_of_stmt(ctx *Context, stmt *ParsedStmt) (*pg_query.RawStmt, error) {
  if stmt.has_name {
    var code string
    err := ctx.db.QueryRow("select stmt from morph.statements where stmt_type=$1 and stmt_name=$2", stmt.stmt_type, stmt.name).Scan(&code); 
    if err != nil {
      return nil, err 
    }

    result, err2 := pg_query.Parse(code)

    if err2 != nil {
      return nil, err2 
    }

    stmts := result.GetStmts()

    if len(stmts) == 0 {
      return nil, errors.New("Statements array is empty.")
    }

    return result.GetStmts()[0], nil
  }

  return nil, errors.New(fmt.Sprintf("Could not get current version of statement. %v\n", stmt));
}

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

func extract_stmts(pr *pg_query.ParseResult) []*ParsedStmt {
  var ps []*ParsedStmt
  var dependencies []Dependency

  for _, x := range pr.Stmts {
    dp, err := deparse_raw_stmt(x)
    perr(err)
    json, err := pg_query.ParseToJSON(dp)
    perr(err)
    nps := &ParsedStmt{ 
      x, 
      false,
      "",
      dp, 
      0,
      json,
      hash_string(dp), 
      UNKNOWN, 
      UNKNOWN_TYPE,
      dependencies,
    }
    hydrate_stmt_object(x.GetStmt(), nps)
    ps = append(ps, nps) 
  }

  return ps
}

func parse_sql(code string) (*pg_query.ParseResult, error) {
  pr, err := pg_query.Parse(code) 
  return pr, err
}

func sort_stmts_by_priority(ctx *Context, stmts []*ParsedStmt) []*ParsedStmt {
  sorted_ps := make([]*ParsedStmt, 0)

  for _, s := range stmts {
    fmt.Printf("\n\n-----\n%v\nNAME: %s\nDEPENDS ON: %v\n\n", s.deparsed, s.name, s.dependencies)
  }

  return sorted_ps
}

func process_sql_files(ctx *Context) []*ParsedStmt {
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
      log.Panicf("Syntax Error in %v:\n\n %v\n", path, parse_err)
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
      log.Panicf("Migration file %s has does not match the checksum in the database.\n", f)
    }
  }
}

func is_stmt_hash_found_in_db(ctx *Context, stmt *ParsedStmt) bool {
  r, e := ctx.db.Query("select * from morph.statements where stmt_hash=$1", stmt.hash)
  perr(e)
  return r.Next()
}

func is_stmt_name_found_in_db(ctx *Context, stmt *ParsedStmt) bool {
  if !stmt.has_name {
    return false
  }

  r, e := ctx.db.Query("select * from morph.statements where stmt_name=$1 and stmt_type=$2", stmt.name, stmt.stmt_type);
  perr(e)
  return r.Next()
}

func set_stmt_status(ctx *Context, stmts []*ParsedStmt) []*ParsedStmt {
  for _, stmt := range stmts {
    stmt_hash_found := is_stmt_hash_found_in_db(ctx, stmt)
    stmt_name_found := is_stmt_name_found_in_db(ctx, stmt)

    if (stmt_name_found && stmt_hash_found) || (!stmt_name_found && stmt_hash_found) {
      stmt.status = UNCHANGED
    } else if stmt_name_found && !stmt_hash_found {
      stmt.status = CHANGED
    } else {
      stmt.status = NEW
    }
  }

  return stmts
}

func do_any_stmts_require_migration(ctx *Context, stmts []*ParsedStmt) bool {
  for _, s := range stmts {
    switch s.status {
      case CHANGED:
        return true
      case NEW:
        return true
    }
  }

  return false
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
    if stmt.status == UNCHANGED {
      continue
    }

    if !stmt.has_name {
      _, e := ctx.db_tx.Exec("insert into morph.statements (stmt, stmt_hash, stmt_type) values ($1, $2, $3) on conflict (stmt_hash) do nothing", stmt.deparsed, stmt.hash, stmt.stmt_type)
      perr(e)
    } else {
      _, e := ctx.db_tx.Exec("insert into morph.statements (stmt, stmt_hash, stmt_name, stmt_type) values ($1, $2, $3, $4) on conflict (stmt_hash) do nothing", stmt.deparsed, stmt.hash, stmt.name, stmt.stmt_type)
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

func clear_removed_statements_from_db(ctx *Context, stmts []*ParsedStmt) {
  var hash string
  
  query, err := ctx.db.Query("select stmt_hash from morph.statements")
  perr(err)
  defer query.Close()

  for query.Next() {
    perr(query.Scan(&hash))

    found := false

    for _, stmt := range stmts {
      if stmt.hash == hash {
        found = true
        break
      }
    }

    if !found {
      log.Printf("Removing %s\n", hash);
      ctx.db_tx.Exec("delete from morph.statements where stmt_hash=$1", hash)
    }
  }

}

func clear_migrations(ctx *Context) {
  os.RemoveAll(MIGRATION_FOLDER)
  _, e := ctx.db.Exec("drop schema if exists morph cascade");
  perr(e)
  _, e = ctx.db.Exec("drop schema if exists public cascade");
  perr(e)
  _, e = ctx.db.Exec("create schema if not exists public")
  perr(e)
}

func main() {
  ctx := parse_args()
  db := create_db_connection(ctx.db_context)
  defer db.Close()
  ctx.db = db

  tx, te := ctx.db.Begin()
  perr(te)

  ctx.db_tx = tx

  if strings.Compare(ctx.cmd, CLEAR_MIGRATIONS_CMD) == 0 {
    log.Println("Clearing...");
    clear_migrations(ctx);
    ctx.cmd = MAKE_MIGRATIONS_CMD;
  }

  init_migration_schema(ctx);
  init_migrations_folder(ctx)

  verify_all_migration_files(ctx);

  if strings.Compare(ctx.cmd, MAKE_MIGRATIONS_CMD) == 0 {
    if !have_all_migrations_been_executed(ctx) {
      log.Fatal("There are migrations that have not yet been executed.");
    }

    log.Println("Processing SQL Files...")
    stmts := process_sql_files(ctx)
    set_stmt_status(ctx, stmts)

    stmts = sort_stmts_by_priority(ctx, stmts)

    if !do_any_stmts_require_migration(ctx, stmts) {
      log.Fatal("No migrations required.")
    }
    
    log.Println("Creating migrations...")
    create_next_migration(ctx)
    clear_removed_statements_from_db(ctx, stmts)
    defer ctx.migration_file.Close()

    write_migrations_to_next_migration_file(ctx, stmts)

    log.Println("Updating statements in db...")
    update_statements_in_db(ctx, stmts)

    log.Printf("New migrations have been created in %s.\n", ctx.migration_file_path)
  } else if strings.Compare(ctx.cmd, MIGRATE_CMD) == 0 {
    if have_all_migrations_been_executed(ctx) {
      log.Fatal("No migrations to run.")
    }

    if !have_all_migrations_been_resolved(ctx) {
      log.Fatal("Not all migrations have been resolved.")
    }

    log.Println("Executing migrations...")
    execute_migrations(ctx)
    log.Println("Migrations run successfully.")
  } else {
    log.Fatalf("Unknown command: %s\n", ctx.cmd)
  }

  perr(ctx.db_tx.Commit())
}
