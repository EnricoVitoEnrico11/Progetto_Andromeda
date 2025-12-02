drop schema if exists stereotipi_transformation;
create schema if not exists stereotipi_transformation; 

drop table if exists stereotipi_transformation.dim_territorio;
create table if not exists stereotipi_transformation.dim_territorio as
select row_number() over() as ids_territorio, territorio from
(select distinct territorio 
from stereotipi_landing.accettabilita_stereotipi t );

drop table if exists stereotipi_transformation.dim_frequenza;
create table if not exists stereotipi_transformation.dim_frequenza as
select row_number() over() as ids_frequenza, frequenza from
(select distinct frequenza 
from stereotipi_landing.accettabilita_stereotipi t );

drop table if exists stereotipi_transformation.dim_sesso;
create table if not exists stereotipi_transformation.dim_sesso as
select row_number() over() as ids_sesso, sesso from
(select distinct sesso 
from stereotipi_landing.accettabilita_stereotipi t );

drop table if exists stereotipi_transformation.dim_time;
create table if not exists stereotipi_transformation.dim_time as
select row_number() over() as ids_time, time_period from
(select distinct time_period 
from stereotipi_landing.accettabilita_stereotipi t );

drop table if exists fact_accettabilita_stereotipi;
create table if not exists fact_accettabilita_stereotipi as
select ids_frequenza, ids_territorio, indicatore, ids_sesso, 
"stereotipi_sui_ruoli_di_genere,_comportamenti_nella_coppia",
ids_time, osservazione, "stato_dell'osservazione" 
from stereotipi_landing.accettabilita_stereotipi ast
join stereotipi_transformation.dim_frequenza df on df.frequenza=ast.frequenza
join stereotipi_transformation.dim_territorio dt on dt.territorio=ast.territorio 
join stereotipi_transformation.dim_sesso ds on ast.sesso=ds.sesso 
join stereotipi_transformation.dim_time dt2 on ast.time_period=dt2.time_period; 


drop table if exists fact_accettabilita_violenza_nella_coppia;
create table if not exists fact_accettabilita_violenza_nella_coppia as
select ids_frequenza, ids_territorio, indicatore, ids_sesso, 
"stereotipi_sui_ruoli_di_genere,_comportamenti_nella_coppia",grado_di_accettabilità,
ids_time, osservazione, "stato_dell_osservazione" 
from stereotipi_landing.accettabilita_violenza_nella_coppia ast
join stereotipi_transformation.dim_frequenza df on df.frequenza=ast.frequenza
join stereotipi_transformation.dim_territorio dt on dt.territorio=ast.territorio 
join stereotipi_transformation.dim_sesso ds on ast.sesso=ds.sesso 
join stereotipi_transformation.dim_time dt2 on ast.time_period=dt2.time_period;

drop table if exists fact_cosa_si_consiglierebbe_a_una_donna;
create table if not exists fact_cosa_si_consiglierebbe_a_una_donna as
select ids_frequenza, ids_territorio, indicatore, ids_sesso, 
consigli,
ids_time, osservazione, ast."stato_dell'osservazione" 
from stereotipi_landing.cosa_si_consiglierebbe_a_una_donna ast
join stereotipi_transformation.dim_frequenza df on df.frequenza=ast.frequenza
join stereotipi_transformation.dim_territorio dt on dt.territorio=ast.territorio 
join stereotipi_transformation.dim_sesso ds on ast.sesso=ds.sesso 
join stereotipi_transformation.dim_time dt2 on ast.time_period=dt2.time_period;

drop table if exists fact_indicazione_di_alcune_cause_della_violenza_nella_coppia_reg;
create table if not exists fact_indicazione_di_alcune_cause_della_violenza_nella_coppia_reg as
select ids_frequenza, ids_territorio, ast."Indicatore", ids_sesso, 
"Possibili cause",
ids_time, ast."Osservazione", ast."Stato_dell'osservazione" 
from stereotipi_landing.indicazione_di_alcune_cause_della_violenza_nella_coppia_reg ast
join dim_frequenza df on df.frequenza=ast."Frequenza"
join dim_territorio dt on dt.territorio=ast."Territorio"
join dim_sesso ds on ds.sesso=ast."Sesso"
join dim_time dt2 on dt2.time_period=ast."TIME_PERIOD"

drop table if exists fact_opinioni_ruoli_tradizionali;
create table if not exists fact_opinioni_ruoli_tradizionali as
select ids_frequenza, ids_territorio, indicatore, ids_sesso, 
"stereotipi_sui_ruoli_di_genere_comportamenti_nella_coppia", grado_di_accordo
ids_time, osservazione, "stato_dell_osservazione" 
from stereotipi_landing.opinioni_ruoli_tradizionali ast
join stereotipi_transformation.dim_frequenza df on df.frequenza=ast.frequenza
join stereotipi_transformation.dim_territorio dt on dt.territorio=ast.territorio 
join stereotipi_transformation.dim_sesso ds on ast.sesso=ds.sesso 
join stereotipi_transformation.dim_time dt2 on ast.time_period=dt2.time_period;

drop table if exists fact_opinioni_violenza_sessuali;
create table if not exists fact_opinioni_violenza_sessuali as
select ids_frequenza, ids_territorio, indicatore, ids_sesso, 
"stereotipi_sui_ruoli_di_genere_comportamenti_nella_coppia", grado_di_accordo
ids_time, osservazione, "stato_dell_osservazione" 
from stereotipi_landing.opinioni_violenza_sessuali ast
join stereotipi_transformation.dim_frequenza df on df.frequenza=ast.frequenza
join stereotipi_transformation.dim_territorio dt on dt.territorio=ast.territorio 
join stereotipi_transformation.dim_sesso ds on ast.sesso=ds.sesso 
join stereotipi_transformation.dim_time dt2 on ast.time_period=dt2.time_period;

drop table if exists fact_percezione_diffusione_violenza_nella_coppia;
create table if not exists fact_percezione_diffusione_violenza_nella_coppia as
select ids_frequenza, ids_territorio, indicatore, ids_sesso, 
"livello_di_diffusione",
ids_time, osservazione, "stato_dell_osservazione" 
from stereotipi_landing.percezione_diffusione_violenza_nella_coppia ast
join stereotipi_transformation.dim_frequenza df on df.frequenza=ast.frequenza
join stereotipi_transformation.dim_territorio dt on dt.territorio=ast.territorio 
join stereotipi_transformation.dim_sesso ds on ast.sesso=ds.sesso 
join stereotipi_transformation.dim_time dt2 on ast.time_period=dt2.time_period;

