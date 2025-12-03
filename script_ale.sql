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