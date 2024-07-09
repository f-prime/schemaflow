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

      old_obj := x.current_tables_map[ct]
      new_obj := x.new_tables_map[nt]

      if old_obj.reltablespace != new_obj.reltablespace {
        new_ts := new_obj.reltablespace

        if new_ts != "" {
          stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s SET TABLESPACE %s;", nt, new_ts))
        } else {
          stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s SET TABLESPACE pg_default;", nt))
        }
      } 

      if old_obj.relowner != new_obj.relowner {
        new_o := new_obj.relowner

        stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s OWNER TO %s;", nt, new_o))
      }
    }
  }

  return stmts
}

func get_list_of_tables(db *sql.DB) []PgTable {
  var tables []PgTable

  r, err := db.Query(`
SELECT
  coalesce(pc.relname, '') relname
  ,coalesce(pn.nspname, '') relnamespace
  ,coalesce(pt.typname, '') reltype
  ,coalesce(pto.typname, '') reloftype
  ,coalesce(po.rolname, '') relowner
  ,coalesce(pam.amname, '') relam
  ,coalesce(pts.spcname, '') reltablespace
  ,coalesce(ptoast.relname, '') reltoastrelid
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
    
    err := r.Scan(
      &relname, 
      &relnamespace, 
      &reltype, 
      &reloftype, 
      &relowner,
      &relam, 
      &reltablespace, 
      &reltoastrelid, 
    )
    perr(err)

    tables = append(tables, PgTable { 
      relname: relname, 
      relnamespace: relnamespace, 
      reltype: reltype, 
      reloftype: reloftype, 
      relam: relam, 
      reltablespace: reltablespace, 
      reltoastrelid: reltoastrelid, 
      relowner: relowner,
    })
  }

  return tables
}
