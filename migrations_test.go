package main

import (
	"testing"

	//pg_query "github.com/pganalyze/pg_query_go/v5"
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
