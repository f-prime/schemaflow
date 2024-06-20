create or replace function public.insert_person(i_name text, i_age integer) returns integer as $$
declare
  i_id integer; 
begin
  insert into person default values returning id into i_id; 
  insert into age (id, age) values (i_id, i_age); 
  insert into name (id, name) values (i_id, i_name);
  return i_id;
end;
$$ language plpgsql;
