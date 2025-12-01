select * from sondaggio.progetto_andromeda pa where pa.genere 

select fascia_eta from sondaggio.progetto_andromeda 


drop table if exists dim_genere;

create table dim_genere as 
select row_number() over () as ids_genere, * from (
	select distinct pa.genere 
	from sondaggio.progetto_andromeda pa
);

drop table if exists dim_fascia_eta;

create table dim_fascia_eta as 
 select row_number() over () as ids_fascia_eta, * from (
   select distinct pa.fascia_eta 
from sondaggio.progetto_andromeda pa

) t 
order by fascia_eta;

select * from dim_fascia_eta dfe

select * from dim_genere dg  
