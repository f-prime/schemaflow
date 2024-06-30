package main

import (
	"reflect"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func dependency_test_failed(t *testing.T, deps any, correct any) {
  t.Errorf("Dependencies error\n\nRECEIVED:  %v\nCORRECT:   %v", deps, correct) 
}

func TestViewDependency(t *testing.T) {
  domain_example := `
  create or replace view abc.some_view as 
    select *, x::my_domain 
    from 
    a_cool_table, b_cool_table
    join c_cool_table k on k.id=25 
    left join lateral (
      select * from d_cool_table
    ) x on true
    having some_other_function(d)
  ;
  `

  t.Run("view dependency", func(t *testing.T) {
    ite_parsed, e := pg_query.Parse(domain_example)
    perr(e)
    stmts := extract_stmts(ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      { SCHEMA, "abc" },
      { COLUMN_TYPE, "my_domain" },
      { TABLE, "a_cool_table" },
      { TABLE, "b_cool_table" },
      { TABLE, "c_cool_table" },
      { TABLE, "d_cool_table" },
      { FUNCTION, "some_other_function" },
    }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      dependency_test_failed(t, ps.dependencies, correct) 
    }
  });
}

func TestTableForeignKeyDependency(t *testing.T) {
  foreign_key_example := `
    CREATE TABLE child_table (
        parent_id INTEGER REFERENCES parent_table(id)
    );`

  t.Run("table foreign key dependency", func(t *testing.T) {
    fke_parsed, e := pg_query.Parse(foreign_key_example)
    perr(e)
    stmts := extract_stmts(fke_parsed)
    ps := stmts[0]

    correct := []Dependency {
      { TABLE, "parent_table" },
    }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      dependency_test_failed(t, ps.dependencies, correct) 
    }
  });
}

func TestTableInheritedDependency(t *testing.T) {
  inherited_table_example := `
    CREATE TABLE child_table (
        age INTEGER
    ) INHERITS (parent_table);  
  `

  t.Run("table inherited dependency", func(t *testing.T) {
    ite_parsed, e := pg_query.Parse(inherited_table_example)
    perr(e)
    stmts := extract_stmts(ite_parsed)
    ps := stmts[0]

    correct := []Dependency {
      { TABLE, "parent_table" },
    }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      dependency_test_failed(t, ps.dependencies, correct) 
    }
  });
}

func TestTablePartitionDependency(t *testing.T) {
  partition_table_example := `
    CREATE TABLE partition_table_2023 PARTITION OF parent_table
      FOR VALUES FROM ('2023-01-01') TO ('2023-12-31');
  `

  t.Run("table partition dependency", func(t *testing.T) {
    ite_parsed, e := pg_query.Parse(partition_table_example)
    perr(e)
    stmts := extract_stmts(ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      { TABLE, "parent_table" },
    }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      dependency_test_failed(t, ps.dependencies, correct) 
    }
  });
}

func TestTableFunctionDependency(t *testing.T) {
  default_function_example := `CREATE TABLE example_table (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4()
  );
  `

  t.Run("table function dependency", func(t *testing.T) {
    ite_parsed, e := pg_query.Parse(default_function_example)
    perr(e)
    stmts := extract_stmts(ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      { COLUMN_TYPE, "uuid" },
      { FUNCTION, "uuid_generate_v4" },
    }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      dependency_test_failed(t, ps.dependencies, correct) 
    }
  });
}

func TestTableSequenceDependency(t *testing.T) {
  default_function_example := `
  CREATE TABLE example_table (
    id INTEGER PRIMARY KEY DEFAULT nextval('example_sequence')
  );
  `

  t.Run("table sequence dependency", func(t *testing.T) {
    ite_parsed, e := pg_query.Parse(default_function_example)
    perr(e)
    stmts := extract_stmts(ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      { SEQUENCE, "example_sequence" },
    }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      dependency_test_failed(t, ps.dependencies, correct) 
    }
  });
}

func TestTableCustomTypeDependency(t *testing.T) {
  domain_example := `
    CREATE TABLE example_table (
      age positive_integer
    );  
  `

  t.Run("table domain dependency", func(t *testing.T) {
    ite_parsed, e := pg_query.Parse(domain_example)
    perr(e)
    stmts := extract_stmts(ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      { COLUMN_TYPE, "positive_integer" },
    }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      dependency_test_failed(t, ps.dependencies, correct) 
    }
  });
}

func TestTableCollateDependency(t *testing.T) {
  domain_example := `
    CREATE TABLE example_table (
      name text COLLATE romanian_phonebook 
    );  
  `

  t.Run("table collate dependency", func(t *testing.T) {
    ite_parsed, e := pg_query.Parse(domain_example)
    perr(e)
    stmts := extract_stmts(ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      { COLLATION, "romanian_phonebook" },
      { COLUMN_TYPE, "text" },
    }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      dependency_test_failed(t, ps.dependencies, correct) 
    }
  });
}

func TestTableSchemaDependency(t *testing.T) {
  domain_example := `
    CREATE TABLE my_schema.example_table (
      name integer
    );  
  `

  t.Run("table schema dependency", func(t *testing.T) {
    ite_parsed, e := pg_query.Parse(domain_example)
    perr(e)
    stmts := extract_stmts(ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      { SCHEMA, "my_schema" },
    }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      dependency_test_failed(t, ps.dependencies, correct) 
    }
  });
}

func TestTableTablespaceDependency(t *testing.T) {
  domain_example := `
    CREATE TABLE example_table (
        id integer
    ) TABLESPACE example_tablespace;  
  `

  t.Run("table tablespace dependency", func(t *testing.T) {
    ite_parsed, e := pg_query.Parse(domain_example)
    perr(e)
    stmts := extract_stmts(ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      { TABLESPACE, "example_tablespace" },
    }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      dependency_test_failed(t, ps.dependencies, correct) 
    }
  });
}

func TestInsertDependency(t *testing.T) {
  insert_example := `
    insert into cc.abc (a, b, c) select x.a, x.b, x.c from some_other_table
    where omg=123 and xyz=call_this_func(with_this_nested_call(123::MY_CUSTOM_NUMBER_TYPE))
  `

  t.Run("table insert dependency", func(t *testing.T) {
    ite_parsed, e := pg_query.Parse(insert_example)
    perr(e)
    stmts := extract_stmts(ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      { TABLE, "cc.abc" },
      { FUNCTION, "call_this_func" },
      { FUNCTION, "with_this_nested_call" },
      { COLUMN_TYPE, "my_custom_number_type" },
      { TABLE, "some_other_table" },
    }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      dependency_test_failed(t, ps.dependencies, correct) 
    }
  });
 
}

func TestWithDependency(t *testing.T) {
  example := `
    with first as (select * from qvc), second as (select a::CT from abc) select my_func(25), * from first, second;
  `

  t.Run("with dependency", func(t *testing.T) {
    ite_parsed, e := pg_query.Parse(example)
    perr(e)
    stmts := extract_stmts(ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      { FUNCTION, "my_func" },
      { TABLE, "first" },
      { TABLE, "second" },
      { TABLE, "qvc" },
      { COLUMN_TYPE, "ct" },
      { TABLE, "abc" },
    }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      dependency_test_failed(t, ps.dependencies, correct) 
    }
  });
 
}
