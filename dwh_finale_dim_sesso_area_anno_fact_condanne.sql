set search_path to istat_transformation, progetto_andromeda;

SELECT * FROM istat_landing.lt_condanne_reati_violenti_sesso_reg;

select distinct tipo_di_reato from istat_landing.lt_condanne_reati_violenti_sesso_reg;

drop table if exists dim_tipo_reato;

create table dwh_progettoandromeda.dim_tipo_reato
as
select 
ROW_Number () over (order by tipo_di_reato) as ids_reato,
tipo_di_reato,
NOW() as load_timestamp,
'Landing' as source_system
from  
(select distinct tipo_di_reato 	
from istat_landing.lt_condanne_reati_violenti_sesso_reg ltcrv
);


-- per la dim_anno creo con una union

drop table if exists dwh_progettoandromeda.dim_anno;

create table if not exists dwh_progettoandromeda.dim_anno
as
select
ROW_Number () over (order by time_period) as ids_anno,
time_period as anno,
now () as load_timestamp,
'Landing' as source_system
from (
select distinct time_period from istat_landing.lt_chiamate_vittime
union
select distinct time_period from istat_landing.lt_condanne_reati_violenti_sesso_reg lcrvsr
order by time_period asc);


DROP TABLE dwh_progettoandromeda.dim_area;

create table dwh_progettoandromeda.dim_area
as
select 
ROW_Number () over (order by territorio) as ids_area,
territorio as nome_area,
NOW() as load_timestamp,
'Landing' as source_system
from  --istat_landing.lt_chiamate_vittime
(select distinct territorio 
from istat_landing.lt_chiamate_vittime
where territorio in 
('Nord-ovest',
'Nord-est',
'Centro',
'Sud',
'Isole'
)
);


-- creazione dim_sesso

drop table if exists dwh_progettoandromeda.dim_sesso;

create table dwh_progettoandromeda.dim_sesso
as
select 
ROW_Number () over (order by sesso) as ids_sesso,
sesso,
NOW() as load_timestamp,
'ETL' as source_system
from  
(select distinct sesso
from istat_landing.lt_condanne_reati_violenti_sesso_reg ltcrv
where sesso != 'Totale'
);

-- per controllo select sum(osservazione) from istat_landing.lt_condanne_reati_violenti_sesso_reg where territorio = 'Italia' ;

-- procedo con la creazione del fatto

drop table if exists dwh_progettoandromeda.fact_condanne_reati_violenti_sesso_eta_reg;

create table if not exists dwh_progettoandromeda.fact_condanne_reati_violenti_sesso_eta_reg as
select row_number() over() as 
	ids, 
	ids_nazione,
	ids_reato, 
	ids_sesso,  
	ids_anno, 
	osservazione as numero_condanne
from istat_landing.lt_condanne_reati_violenti_sesso_reg crv
join istat_transformation.dim_nazione itdn on itdn.nazione=crv.territorio
join istat_transformation.dim_tipo_reato dtr on dtr.tipo_di_reato=crv.tipo_di_reato
join istat_transformation.dim_sesso ds on ds.sesso=crv.sesso
join istat_transformation.dim_anno da on da.time_period=crv.time_period
order by ids asc;

/*
select count (*) from istat_dwh.fact_condanne_reati fcr
union all
select count (*) from istat_landing.lt_condanne_reati_violenti_sesso_reg
where sesso !='Totale';
*/
