package main

import (
	"reflect"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func test_failed(t *testing.T, deps any, correct any) {
  t.Errorf("FAILED\n\nRECEIVED:  %v\nCORRECT:   %v", deps, correct) 
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
      *build_dependency(SCHEMA, "abc" ),
      *build_dependency(GENERIC_TYPE, "my_domain"),
      *build_dependency(TABLE, "a_cool_table"),
      *build_dependency(TABLE, "b_cool_table"),
      *build_dependency(TABLE, "c_cool_table"),
      *build_dependency(TABLE, "d_cool_table"),
      *build_dependency(FUNCTION, "some_other_function" ),
    }

    var checked []Dependency

    for _, c := range ps.dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
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
      *build_dependency(GENERIC_TYPE, "int4" ),
      *build_dependency(TABLE, "parent_table" ),
    }

    var checked []Dependency

    for _, c := range ps.dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
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
      *build_dependency(GENERIC_TYPE, "int4"),
      *build_dependency(TABLE, "parent_table"),
    }

    var checked []Dependency

    for _, c := range ps.dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
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
      *build_dependency(TABLE, "parent_table"),
    }

    var checked []Dependency

    for _, c := range ps.dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
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
      *build_dependency(GENERIC_TYPE, "uuid"),
      *build_dependency(FUNCTION, "uuid_generate_v4"),
    }

    var checked []Dependency

    for _, c := range ps.dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
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
      *build_dependency(GENERIC_TYPE, "int4"),
      *build_dependency(SEQUENCE, "example_sequence"),
    }

    var checked []Dependency

    for _, c := range ps.dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
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
      *build_dependency(GENERIC_TYPE, "positive_integer"),
    }

    var checked []Dependency

    for _, c := range ps.dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
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
      *build_dependency(COLLATION, "romanian_phonebook"),
      *build_dependency(GENERIC_TYPE, "text"),
    }

    var checked []Dependency

    for _, c := range ps.dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
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
      *build_dependency(SCHEMA, "my_schema"),
      *build_dependency(GENERIC_TYPE, "int4"),
    }

    var checked []Dependency

    for _, c := range ps.dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
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
      *build_dependency(TABLESPACE, "example_tablespace"),
      *build_dependency(GENERIC_TYPE, "int4"),
    }

    var checked []Dependency

    for _, c := range ps.dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
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
      *build_dependency(SCHEMA, "cc"),
      *build_dependency(TABLE, "abc"),
      *build_dependency(FUNCTION, "call_this_func"),
      *build_dependency(FUNCTION, "with_this_nested_call"),
      *build_dependency(GENERIC_TYPE, "my_custom_number_type"),
      *build_dependency(TABLE, "some_other_table"),
    }

    var checked []Dependency

    for _, c := range ps.dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
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
      *build_dependency(FUNCTION, "my_func"),
      *build_dependency(TABLE, "first"),
      *build_dependency(TABLE, "second"),
      *build_dependency(TABLE, "qvc"),
      *build_dependency(GENERIC_TYPE, "ct"),
      *build_dependency(TABLE, "abc"),
    }

    var checked []Dependency

    for _, c := range ps.dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
    }
  });
 
}

func TestCommentDependency(t *testing.T) {
  example := `
    comment on table some_other_table is 'This is a comment';
  `

  t.Run("with dependency", func(t *testing.T) {
    ite_parsed, e := pg_query.Parse(example)
    perr(e)
    stmts := extract_stmts(ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      *build_dependency(TABLE, "some_other_table"),
    }

    var checked []Dependency

    for _, c := range ps.dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
    }
  });
 
}
