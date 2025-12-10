drop schema if exists istat_landing cascade;
create schema istat_landing;

drop schema if exists istat_transformation cascade;
create schema istat_transformation;

set search_path to istat_transformation;

create table if not exists dim_territorio as select row_number() over() as ids_territorio, territorio from (select distinct territorio from istat_landing.lt_vittime);

create table if not exists dim_sesso as select row_number() over() as ids_sesso, sesso from (select distinct sesso from istat_landing.lt_vittime);

create table if not exists dim_motivi_chiamata as select row_number() over() as ids_motivi_chiamata, motivi_della_chiamata from
(select distinct motivi_della_chiamata from istat_landing.lt_vittime);

create table if not exists dim_anno as select row_number() over() as ids_anno, time_period from (select distinct time_period from istat_landing.lt_vittime);

drop schema if exists istat_dwh cascade;
create schema istat_dwh;

set search_path to istat_dwh;

create table if not exists fact_vittime as
select row_number() over() as ids, ids_territorio, ids_sesso, ids_motivi_chiamata, ids_anno, osservazione as numero_chiamate
from istat_landing.lt_vittime lv
join istat_transformation.dim_territorio dt on dt.territorio=lv.territorio
join istat_transformation.dim_sesso ds on ds.sesso=lv.sesso
join istat_transformation.dim_motivi_chiamata mc on mc.motivi_della_chiamata=lv.motivi_della_chiamata
join istat_transformation.dim_anno da on da.time_period=lv.time_period
order by ids asc;


set search_path to istat_transformation;

create table if not exists dim_eta as
select row_number() over() as ids_fascia_eta, età from 
(select distinct età from istat_landing.lt_denunce_delitti);

create table if not exists dim_indicatore as 
select row_number() over() as ids_indicatore, indicatore from
(select distinct indicatore from istat_landing.lt_denunce_delitti);

create table if not exists dim_tipo_delitto as 
select row_number() over() as ids_tipo_delitto, tipo_di_delitto from
(select distinct tipo_di_delitto from istat_landing.lt_denunce_delitti);

set search_path to istat_dwh;

create table if not exists fact_denunce_delitti as
select row_number() over() as ids, ids_territorio, ids_indicatore, ids_tipo_delitto, ids_sesso, ids_fascia_eta, ids_anno, osservazione as numero_denunce
from istat_landing.lt_denunce_delitti dd
join istat_transformation.mapping_città_regione mpc on mpc.territorio=dd.territorio
join istat_transformation.dim_indicatore di on di.indicatore=dd.indicatore
join istat_transformation.dim_tipo_delitto dtd on dtd.tipo_di_delitto=dd.tipo_di_delitto
join istat_transformation.dim_sesso ds on ds.sesso=dd.sesso
join istat_transformation.dim_fascia_eta dfe on dfe.età=dd.età
join istat_transformation.dim_anno da on da.time_period=dd.time_period
order by ids asc;

set search_path to istat_transformation;

create table if not exists dim_nazione (ids_nazione integer, nazione varchar(3000));

insert into dim_nazione (ids_nazione, nazione) values (1,'Italia'), (2,'Estero');

create table if not exists dim_area (ids_area integer, area varchar(3000));

insert into dim_area (ids_area, area) values (1, 'Nord-est'), (2, 'Nord-ovest'), (3, 'Centro'), (4, 'Sud'), (5, 'Isole'), (6, 'Non indicato');
