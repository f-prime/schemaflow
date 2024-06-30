package main

import (
	"reflect"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v5"
)

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

    correct := make([]Dependency, 1)
    correct[0] = Dependency{ TABLE, "parent_table" }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      t.Errorf("Table dependencies incorrect %v should be %v", ps.dependencies, correct) 
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

    correct := make([]Dependency, 1)
    correct[0] = Dependency{ TABLE, "parent_table" }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      t.Errorf("Table dependencies incorrect %v should be %v", ps.dependencies, correct) 
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

    correct := make([]Dependency, 1)
    correct[0] = Dependency{ TABLE, "parent_table" }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      t.Errorf("Table dependencies incorrect %v should be %v", ps.dependencies, correct) 
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

    correct := make([]Dependency, 2)
    correct[0] = Dependency{ FUNCTION, "uuid_generate_v4" }
    correct[1] = Dependency{ COLUMN_TYPE, "uuid" }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      t.Errorf("Table dependencies incorrect %v should be %v", ps.dependencies, correct) 
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

    correct := make([]Dependency, 1)
    correct[0] = Dependency{ SEQUENCE, "example_sequence" }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      t.Errorf("Table dependencies incorrect %v should be %v", ps.dependencies, correct) 
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

    correct := make([]Dependency, 1)
    correct[0] = Dependency{ COLUMN_TYPE, "positive_integer" }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      t.Errorf("Table dependencies incorrect %v should be %v", ps.dependencies, correct) 
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

    correct := make([]Dependency, 2)
    correct[0] = Dependency{ COLUMN_TYPE, "text" }
    correct[1] = Dependency{ COLLATION, "romanian_phonebook" }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      t.Errorf("Table dependencies incorrect %v should be %v", ps.dependencies, correct) 
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

    correct := make([]Dependency, 1)
    correct[0] = Dependency{ SCHEMA, "my_schema" }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      t.Errorf("Table dependencies incorrect %v should be %v", ps.dependencies, correct) 
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

    correct := make([]Dependency, 1)
    correct[0] = Dependency{ TABLESPACE, "example_tablespace" }

    if !reflect.DeepEqual(correct, ps.dependencies) {
      t.Errorf("Table dependencies incorrect %v should be %v", ps.dependencies, correct) 
    }
  });
}
