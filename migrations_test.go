package main

import (
	"reflect"
	"testing"
)

func TestDomainMigration(t *testing.T) {
  //domain_example := `
  //create or replace view abc.some_view as 
  //  select *, x::my_domain 
  //  from 
  //  a_cool_table, b_cool_table
  //  join c_cool_table k on k.id=25 
  //  left join lateral (
  //    select * from d_cool_table
  //  ) x on true
  //  having some_other_function(d)
  //;
  //`

  t.Run("domain migration", func(t *testing.T) {
  });
}

func TestTableMigration(t *testing.T) {
  prev := `
  create table abc (
    id integer primary key,
    state varhchar(2)
  );
  ;
  `

  pr, e := parse_sql(prev)
  perr(e)

  next := `
    create table abc (
      id integer primary key check ( NOT (id > 0 AND id <> -1 AND id is not null) ),
      name text COLLATE "en_us" not null,
      state varchar(2) default 'NJ' not null
    );
  `

  nr, e := parse_sql(next)
  perr(e)

  t.Run("table migration", func(t *testing.T) {
    stmt := extract_stmts(nr)[0] 
    stmt.prev_version_stmt = pr.Stmts[0]

    migration := get_migration_for_stmt(nil, stmt)

    correct := []string{
      "ALTER TABLE abc ADD COLUMN name text;\n",
      "ALTER TABLE abc ALTER COLUMN state SET DEFAULT 'NJ';\n",
    }

    if !reflect.DeepEqual(correct, migration) {
      test_failed(t, migration, correct)
    }
  });
}

func TestFunctionMigration(t *testing.T) {
  prev := `
  create function okay(x text) returns jsonb as $$ $$ language sql;
  `

  pr, e := parse_sql(prev)
  perr(e)

  next := `
  create function okay(x text) returns text as $$ $$ language sql;
  `

  nr, e := parse_sql(next)
  perr(e)

  t.Run("function migration", func(t *testing.T) {
    stmt := extract_stmts(nr)[0] 
    stmt.prev_version_stmt = pr.Stmts[0]

    stmt.stmt_type = FUNCTION

    migration := get_migration_for_stmt(nil, stmt)

    correct := []string{
      "DROP FUNCTION IF EXISTS okay;\n",
      "CREATE FUNCTION okay(x text) RETURNS text AS $$ $$ LANGUAGE sql;\n",
    }

    if !reflect.DeepEqual(correct, migration) {
      test_failed(t, migration, correct)
    }
  });
}

func TestViewMigration(t *testing.T) {
  prev := `
  create view abc as select * from okay;
  `

  pr, e := parse_sql(prev)
  perr(e)

  next := `
  create view abc as select a, b, c from okay;
  `

  nr, e := parse_sql(next)
  perr(e)

  t.Run("view migration", func(t *testing.T) {
    stmt := extract_stmts(nr)[0] 
    stmt.prev_version_stmt = pr.Stmts[0]

    stmt.stmt_type = VIEW

    migration := get_migration_for_stmt(nil, stmt)

    correct := []string{
      "DROP VIEW IF EXISTS abc;\n",
      "CREATE VIEW abc AS SELECT a, b, c FROM okay;\n",
    }

    if !reflect.DeepEqual(correct, migration) {
      test_failed(t, migration, correct)
    }
  });
}

