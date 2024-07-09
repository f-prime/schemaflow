package main

import (
	"database/sql"
	"fmt"
)

type DiffableTable struct {
  current_tables []PgTable
  current_tables_map map[string]*PgTable
  new_tables []PgTable
  new_tables_map map[string]*PgTable
}

type PgTable struct {
  relname string
  relnamespace string
  reltype string
  reloftype string
  relam string
  reltablespace string
  reltoastrelid string
  relowner string
}

func (x *DiffableTable) GetCurrentNames() []string {
  return build_string_array[PgTable](x.current_tables, func(t PgTable) string {
    return t.relnamespace + "." + t.relname
  })
}

func (x *DiffableTable) GetNewNames() []string {
  return build_string_array[PgTable](x.new_tables, func(t PgTable) string {
    return t.relnamespace + "." + t.relname
  })
}

func (x *DiffableTable) GenerateAddStmts(ctx *Context) []string {
  return build_string_array[string](compute_added_objects(x), func (x string) string {
    return fmt.Sprintf("CREATE TABLE %s()", x)
  })
}

func (x *DiffableTable) GenerateDropStmts(ctx *Context) []string {
  return build_string_array[string](compute_removed_objects(x), func (x string) string {
    return fmt.Sprintf("DROP TABLE %s", x)
  })
}

func (x *DiffableTable) GenerateUpdateStmts(ctx *Context) []string {
  var stmts []string 

  for _, nt := range x.GetNewNames() {
    for _, ct := range x.GetCurrentNames() {
      if nt != ct {
        continue
      }

      fmt.Printf("PROCESS %s %v %v\n", nt, x.new_tables_map[nt], x.current_tables_map[ct])
    }
  }

  return stmts
}

func get_list_of_tables(db *sql.DB) []PgTable {
  var tables []PgTable

  r, err := db.Query(`
SELECT
  pc.relname relname
  ,pn.nspname relnamespace
  ,pt.typname reltype
  ,pto.typname reloftype
  ,po.rolname relowner
  ,pam.amname relam
  ,pts.spcname reltablespace
  ,ptoast.relname reltoastrelid
FROM pg_class pc
JOIN pg_namespace pn ON pn.oid=pc.relnamespace
JOIN pg_type pt ON pt.oid=pc.reltype
LEFT JOIN pg_type pto ON pto.oid=pc.reloftype
JOIN pg_authid po ON po.oid=pc.relowner
JOIN pg_am pam ON pam.oid=pc.relam
LEFT JOIN pg_tablespace pts ON pts.oid=pc.reltablespace
LEFT JOIN pg_class ptoast ON ptoast.oid=pc.reltoastrelid
WHERE pc.relkind='r' AND pn.nspname != 'pg_catalog' AND pn.nspname != 'information_schema'`);
  perr(err)
  defer r.Close()

  for r.Next() {
    var 
      relname, 
      relnamespace, 
      reltype, 
      reloftype, 
      relam,
      reltablespace,
      reltoastrelid,
      relowner string
    
    r.Scan(&relname, &relnamespace, &reltype, &reloftype, &relam, &reltablespace, &reltoastrelid, &relowner)
    tables = append(tables, PgTable { relname, relnamespace, reltype, reloftype, relam, reltablespace, reltoastrelid, relowner })
  }

  return tables
}
