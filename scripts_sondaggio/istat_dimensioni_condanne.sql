drop table if exists istat_transformation.dim_tipo_reato;

create table istat_transformation.dim_tipo_reato
as
select 
ROW_Number () over (order by tipo_di_reato) as ids_reato,
tipo_di_reato,
NOW() as load_timestamp,
'ETL' as source_system
from  
(select distinct tipo_di_reato 	
from lt_condanne lc
);

drop table if exists istat_transformation.dim_eta_condannato;

create table istat_transformation.dim_eta_condannato
as
select 
ROW_Number () over (order by eta_del_condannato_al_momento_del_reato) as ids_eta_c,
eta_del_condannato_al_momento_del_reato,
NOW() as load_timestamp,
'ETL' as source_system
from  
(select distinct eta_del_condannato_al_momento_del_reato 	
from istat_landing.lt_condanne lc
where eta_del_condannato_al_momento_del_reato != 'Totale'
);