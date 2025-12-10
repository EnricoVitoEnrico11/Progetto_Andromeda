set search_path to istat_transformation;

SELECT * FROM istat_landing.lt_condanne_reati_violenti_sesso_reg;

select distinct tipo_di_reato from istat_landing.lt_condanne_reati_violenti_sesso_reg;

drop table if exists dim_tipo_reato;

create table istat_transformation.dim_tipo_reato
as
select 
ROW_Number () over (order by tipo_di_reato) as ids_reato,
tipo_di_reato,
NOW() as load_timestamp,
'ETL' as source_system
from  
(select distinct tipo_di_reato 	
from istat_landing.lt_condanne_reati_violenti_sesso_reg ltcrv
);


-- per la dim_anno usare istat_transformation.dim_anno
-- per la dim_regione usare istat_transformation.dim_regione


