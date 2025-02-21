package core

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
    stmts := extractStmts(nil, ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      *buildDependency(SCHEMA, "abc" ),
      *buildDependency(GENERIC_TYPE, "my_domain"),
      *buildDependency(TABLE, "a_cool_table"),
      *buildDependency(TABLE, "b_cool_table"),
      *buildDependency(TABLE, "c_cool_table"),
      *buildDependency(TABLE, "d_cool_table"),
      *buildDependency(FUNCTION, "some_other_function" ),
    }

    var checked []Dependency

    for _, c := range ps.Dependencies {
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
    stmts := extractStmts(nil, fke_parsed)
    ps := stmts[0]

    correct := []Dependency {
      *buildDependency(GENERIC_TYPE, "int4" ),
      *buildDependency(TABLE, "parent_table" ),
    }

    var checked []Dependency

    for _, c := range ps.Dependencies {
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
    stmts := extractStmts(nil, ite_parsed)
    ps := stmts[0]

    correct := []Dependency {
      *buildDependency(GENERIC_TYPE, "int4"),
      *buildDependency(TABLE, "parent_table"),
    }

    var checked []Dependency

    for _, c := range ps.Dependencies {
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
    stmts := extractStmts(nil, ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      *buildDependency(TABLE, "parent_table"),
    }

    var checked []Dependency

    for _, c := range ps.Dependencies {
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
    stmts := extractStmts(nil, ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      *buildDependency(GENERIC_TYPE, "uuid"),
      *buildDependency(FUNCTION, "uuid_generate_v4"),
    }

    var checked []Dependency

    for _, c := range ps.Dependencies {
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
    stmts := extractStmts(nil, ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      *buildDependency(GENERIC_TYPE, "int4"),
      *buildDependency(SEQUENCE, "example_sequence"),
    }

    var checked []Dependency

    for _, c := range ps.Dependencies {
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
    stmts := extractStmts(nil, ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      *buildDependency(GENERIC_TYPE, "positive_integer"),
    }

    var checked []Dependency

    for _, c := range ps.Dependencies {
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
    stmts := extractStmts(nil, ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      *buildDependency(COLLATION, "romanian_phonebook"),
      *buildDependency(GENERIC_TYPE, "text"),
    }

    var checked []Dependency

    for _, c := range ps.Dependencies {
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
    stmts := extractStmts(nil, ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      *buildDependency(SCHEMA, "my_schema"),
      *buildDependency(GENERIC_TYPE, "int4"),
    }

    var checked []Dependency

    for _, c := range ps.Dependencies {
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
    stmts := extractStmts(nil, ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      *buildDependency(TABLESPACE, "example_tablespace"),
      *buildDependency(GENERIC_TYPE, "int4"),
    }

    var checked []Dependency

    for _, c := range ps.Dependencies {
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
    stmts := extractStmts(nil, ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      *buildDependency(SCHEMA, "cc"),
      *buildDependency(TABLE, "cc.abc"),
      *buildDependency(FUNCTION, "call_this_func"),
      *buildDependency(FUNCTION, "with_this_nested_call"),
      *buildDependency(GENERIC_TYPE, "my_custom_number_type"),
      *buildDependency(TABLE, "some_other_table"),
    }

    var checked []Dependency

    for _, c := range ps.Dependencies {
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
    stmts := extractStmts(nil, ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      *buildDependency(FUNCTION, "my_func"),
      *buildDependency(TABLE, "first"),
      *buildDependency(TABLE, "second"),
      *buildDependency(TABLE, "qvc"),
      *buildDependency(GENERIC_TYPE, "ct"),
      *buildDependency(TABLE, "abc"),
    }

    var checked []Dependency

    for _, c := range ps.Dependencies {
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
    stmts := extractStmts(nil, ite_parsed)
    ps := stmts[0]

    correct := []Dependency{
      *buildDependency(TABLE, "some_other_table"),
    }

    var checked []Dependency

    for _, c := range ps.Dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
    }
  });
 
}

func TestRuleDependency(t *testing.T) {
  example := `
    create rule test_rule as on delete to test.test_table do instead nothing
  `

  t.Run("rule", func(t *testing.T) {
    ite_parsed, e := pg_query.Parse(example)
    perr(e)
    result := extractStmts(nil, ite_parsed)
    ps := result[0]

    correct := []Dependency{
      *buildDependency(SCHEMA, "test"),
      *buildDependency(TABLE, "test.test_table"),
    }

    var checked []Dependency

    for _, c := range ps.Dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
    }

  })
}

func TestRuleWithWhereDependency(t *testing.T) {
  example := `
    create rule test_rule as on delete to foo.test_table where exists (select 1 from bar.dep_tab where x=1) do instead nothing
  `

  t.Run("rule", func(t *testing.T) {
    ite_parsed, e := pg_query.Parse(example)
    perr(e)
    result := extractStmts(nil, ite_parsed)
    ps := result[0]

    correct := []Dependency{
      *buildDependency(SCHEMA, "foo"),
      *buildDependency(TABLE, "foo.test_table"),
      *buildDependency(SCHEMA, "bar"),
      *buildDependency(TABLE, "bar.dep_tab"),
    }

    var checked []Dependency

    for _, c := range ps.Dependencies {
      checked = append(checked, *c)
    }

    if !reflect.DeepEqual(correct, checked) {
      test_failed(t, checked, correct) 
    }

  })
}
