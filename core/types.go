package core

import (
	"database/sql"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

const VALIDATE_MIGRATIONS_STRING = "--- REMOVE WHEN MIGRATION RESOLVED ---"

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
  GENERIC_TYPE // This is an ambiguous type. Could be a domain or a type.
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

const ACTION_CLEAN = "clean"
const ACTION_MIGRATE = "migrate"
const ACTION_MAKE_MIGRATIONS = "make"

type ActionType int

const (
  MIGRATE ActionType = iota
  MAKEMIGRATIONS
  CLEAN
)

type StmtStatus int

const (
  UNKNOWN = iota
  NEW
  CHANGED
  UNCHANGED
)

type DbContext struct { 
  PgHost string 
  PgPort int
  PgUser string
  PgPassword string 
  PgDbName string
}

type Context struct {
  DbContext *DbContext
  DbTx *sql.Tx
  Db *sql.DB
  SqlPath string
  MigrationPath string
  Action ActionType
  Stmts *[]*ParsedStmt
}

type Dependency struct {
  StmtType StmtType
  StmtName string
  Dependency *ParsedStmt
}

type ParsedStmt struct {
  Stmt *pg_query.RawStmt
  PrevStmt *pg_query.RawStmt
  HasName bool
  Name string
  Deparsed string
  Json string
  Hash string
  StmtType StmtType
  Dependencies []*Dependency
  Handled bool
  Removed bool
  Status StmtStatus
}

const MIGRATIONS_DB = "schemaflow_ephemeral_migration_db"
