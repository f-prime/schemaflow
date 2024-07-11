create or replace function public.insert_person(i_name text, i_age integer, i_something int) returns bigint as $$
declare
  i_id integer; 
  i_some integer := i_something;
begin
  insert into person default values returning id into i_id; 
  insert into age (id, age) values (i_id, i_age); 
  insert into name (id, name) values (i_id, i_name);
  return i_id * i_some;
end;
$$ language plpgsql;

create or replace function calc_comething(a int, b int) returns integer as $$ 
  select (a * b); 
$$ language sql;
