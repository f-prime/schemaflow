package main

import (
	"database/sql"
	"fmt"
)

type DiffableRoles struct {
  current_roles []PgRole
  new_roles []PgRole
  current_roles_map map[string]*PgRole
  new_roles_map map[string]*PgRole
}

type PgRole struct {
  rolname string
  rolsuper bool
  rolinherit bool
  rolcreaterole bool
  rolcreatedb bool
  rolcanlogin bool
  rolreplication bool
  rolconnlimit int
  rolbypassrls bool 
}

func (x *DiffableRoles) generate_alter_stmts_for(name string) []string {
  var stmts []string

  fmt.Printf("x.new_roles_map[name]: %v\n", x.new_roles_map[name])

  return stmts
}

func (x *DiffableRoles) GetCurrentNames() []string {
  return build_string_array[PgRole](x.current_roles, func(r PgRole) string {
    return r.rolname
  })
}

func (x *DiffableRoles) GetNewNames() []string {
  return build_string_array[PgRole](x.new_roles, func(r PgRole) string {
    return r.rolname
  })
}

func (x *DiffableRoles) GenerateAddStmts(ctx *Context) []string {
  added_roles := compute_added_objects(x)
  added := build_string_array[string](added_roles, func(r string) string {
    return fmt.Sprintf("CREATE ROLE %s;", r)
  })

  for _, role := range added_roles {
    x.generate_alter_stmts_for(role)
  }

  return added
}

func (x *DiffableRoles) GenerateDropStmts(ctx *Context) []string {
  return build_string_array[string](compute_removed_objects(x), func(r string) string {
    return fmt.Sprintf("DROP ROLE IF EXISTS %s;", r);
  })
}

func (x *DiffableRoles) GenerateUpdateStmts(ctx *Context) []string {
  var updated []string
  return updated
}

func get_list_of_roles(db *sql.DB) []PgRole {
  var extensions []PgRole
  r, err := db.Query(`
    select 
      rolname,
      rolsuper,
      rolinherit,
      rolcreaterole,
      rolcreatedb,
      rolcanlogin,
      rolreplication,
      rolconnlimit,
      rolbypassrls
    from pg_catalog.pg_roles
  `)

  defer r.Close()

  perr(err)

  for r.Next() {
    var rolname string
    var rolsuper bool
    var rolinherit bool
    var rolcreaterole bool
    var rolcreatedb bool 
    var rolcanlogin bool 
    var rolreplication bool
    var rolconnlimit int
    var rolbypassrls bool

    r.Scan(
      &rolname,
      &rolsuper,
      &rolinherit,
      &rolcreatedb,
      &rolcreatedb,
      &rolcanlogin,
      &rolreplication,
      &rolconnlimit,
      &rolbypassrls,
    )

    extensions = append(extensions, PgRole { 
      rolname: rolname,
      rolsuper: rolsuper,
      rolinherit: rolinherit,
      rolcreaterole: rolcreaterole,
      rolcreatedb: rolcreatedb,
      rolcanlogin: rolcanlogin,
      rolreplication: rolreplication,
      rolconnlimit: rolconnlimit,
      rolbypassrls: rolbypassrls,
    })

    fmt.Printf("extensions: %v\n", extensions)
  }

  return extensions
}
