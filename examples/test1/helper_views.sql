create or replace view person_ages as 
  select p.id, a.age from person p, age a
  where p.id=a.id
;
