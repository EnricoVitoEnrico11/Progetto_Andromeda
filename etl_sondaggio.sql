 

drop table if exists dim_genere cascade;
create table dim_genere as 
select row_number() over () as ids_genere, * from (
	select distinct "Genere" 
	from sondaggio.progetto_andromeda pa
);

drop table if exists dim_fascia_eta cascade;
create table dim_fascia_eta as 
 select row_number() over (order by fascia_eta) as ids_fascia_eta, * from (
   select distinct pa.fascia_eta 
from sondaggio.progetto_andromeda pa

) t; 



-- adesso procediamo con la "dim_grandezza_azienda"

drop table if exists dim_grandezza_azienda cascade;

create table dim_grandezza_azienda as 
 select row_number() over () as ids_grandezza_azienda, * from (
   select distinct pa.grandezza_azienda 
from sondaggio.progetto_andromeda pa
) t
order by grandezza_azienda; -- ho inserito l'order by perchè c'è una variazione di numeri

-- creazione della "dim_durata_lavoro_in_azienda"




drop table if exists dim_durata_lavoro_in_azienda cascade;

create table dim_durata_lavoro_in_azienda  as
select
	row_number() over(order by t.durata_lavoro_in_azienda) as 
	ids_durata_lavoro_in_azienda, t.* from (
	select distinct coalesce(nullif(trim(pa.durata_lavoro_in_azienda), ''),
	'Non indicato') as durata_lavoro_in_azienda
	from sondaggio.progetto_andromeda pa
	) t;


-- creazione dim_vittima_o_testimone_in_azienda

drop table if exists dim_vittima_o_testimone_di_discriminazione_in_azienda cascade;

create table dim_vittima_o_testimone_di_discriminazione_in_azienda   as 
 select row_number() over () as ids_vittima_o_testimone_di_discriminazione_in_azienda, * from (
   select distinct "vittima_o_testimone_discriminazione_in_azienda"
from sondaggio.progetto_andromeda pa

) t 
order by vittima_o_testimone_discriminazione_in_azienda;

--dim tipo discriminazione

drop table if exists dim_tipo_discriminazione CASCADE;
create table if not exists dim_tipo_discriminazione as
 select row_number() over() as ids_tipo_discriminazione, * from (
SELECT DISTINCT
    COALESCE(trim(elem), 'Nessuna discriminazione indicata') AS tipo_discriminazione
FROM sondaggio.progetto_andromeda pa
LEFT JOIN LATERAL unnest(string_to_array(pa.tipo_discriminazione, ',')) AS elem ON true);

--
drop table if exists dim_vittima_o_testimone_di_violenza_in_azienda cascade;

create table dim_vittima_o_testimone_di_violenza_in_azienda   as 
 select row_number() over () as ids_vittima_o_testimone_di_violenza_in_azienda, * from (
   select distinct "vittima_o_testimone_violenza_in_azienda"
from sondaggio.progetto_andromeda pa

) t 
order by vittima_o_testimone_violenza_in_azienda;

-- qui sotto la query che funziona con i dati puliti della dim_tipo_violenza

drop table if exists dim_tipo_violenza cascade;
create table if not exists dim_tipo_violenza as
 select row_number() over() as ids_tipo_violenza, * from (
SELECT DISTINCT
    COALESCE(trim(elem), 'Nessuna violenza indicata') AS tipo_violenza
FROM sondaggio.progetto_andromeda pa
LEFT JOIN LATERAL unnest(string_to_array(pa.tipo_violenza, ',')) AS elem ON true);

-- creazione dim_presenza_formazione_antidiscriminazione_in_azienda

drop table if exists dim_presenza_formazione_antidiscriminazione_in_azienda cascade;

create table dim_presenza_formazione_antidiscriminazione_in_azienda as 
select row_number() over () ids_presenza_formazione_antidiscriminazione_in_azienda, * from (
select distinct pa.presenza_formazione_antidiscriminazione_in_azienda
from sondaggio.progetto_andromeda pa

) t; 


drop table if exists dim_presenza_regolamenti_antidiscriminazione cascade;

create table dim_presenza_regolamenti_antidiscriminazione as
	select
	row_number() over(order by t.presenza_regolamenti_antidiscriminazione) as 
	ids_presenza_regolamenti_antidiscriminazione, t.* from (
	select distinct coalesce(nullif(trim(pa.presenza_regolamenti_antidiscriminazione), ''),
	'Non indicato') as presenza_regolamenti_antidiscriminazione
	from sondaggio.progetto_andromeda pa
	) t;

	

--- tabella manuale per andare a correggere valori delle province
create table if not exists sondaggio_transformation.man_mapping_province ( originale text,corretto text);

drop table if exists sondaggio_transformation.tt_provincia_validated_v01 cascade;


--- cerca province nella tabella istat con tutte le province
--- MANCA DA COLLEGARE LA DIM_REGIONE

create table sondaggio_transformation.tt_provincia_validated_v01 as 
select distinct coalesce(prov."Provincia",coalesce(prov_s."Provincia",son.provincia )) as provincia, 
 prov."Provincia" is null and prov_s."Provincia" is null as err
from
(
	select lower(trim(provincia_domicilio)) as provincia
	FROM sondaggio.progetto_andromeda
	union 
	select lower(trim(provincia_ultimo_lavoro)) as provincia
	FROM sondaggio.progetto_andromeda
) son
left join sondaggio_transformation.province_italiane prov
	on son.provincia=lower(prov."Provincia")
left join sondaggio_transformation.province_italiane prov_s
	on lower(son.provincia)=lower(prov_s."Sigla" )
order by provincia asc;

-- dimensione provincia con valori corretti da valori sondaggio
-- e da valori di tabella manuale corretti
--- MANCA DA COLLEGARE LA DIM_REGIONE


drop table if exists sondaggio_transformation.dim_provincia cascade;
create table sondaggio_transformation.dim_provincia as 
select row_number() over( order by provincia asc) as ids_provincia, provincia
from
(
	select provincia
	from sondaggio_transformation.tt_provincia_validated_v01
	where err=false 
	union
	(
		select corretto as provincia from(
		select provincia 
		from sondaggio_transformation.tt_provincia_validated_v01
		where err=true 
	) rotti
	join sondaggio_transformation.man_mapping_province mapping
		on rotti.provincia=mapping.originale
	)
)
union all
select -1 as ids_provincia,'Provincia sconosciuta' as provincia
;

drop table if exists sondaggio_transformation.et_dim_provincia cascade;


-- tabella errori provincia
drop table if exists et_dim_provincia cascade;
create table if not exists et_dim_provincia as 
select FORMAT('Valore provincia "%s" non valido',rotti.provincia ) as messaggio from(
select provincia 
from sondaggio_transformation.tt_provincia_validated_v01
where err=true 
) rotti
left join sondaggio_transformation.man_mapping_province mapping
	on rotti.provincia=mapping.originale
where mapping.corretto is null
;


-- toglie spazi da campi provincia domicilio e ultimo lavoro

drop table if exists sondaggio_transformation.tt_sondaggio_province_trim_v1 cascade;
create table sondaggio_transformation.tt_sondaggio_province_trim_v1 as
select "Timestamp", "Genere", fascia_eta, trim(provincia_domicilio) as provincia_domicilio, grandezza_azienda, durata_lavoro_in_azienda, trim(provincia_ultimo_lavoro) as provincia_ultimo_lavoro ,vittima_o_testimone_discriminazione_in_azienda, tipo_discriminazione, vittima_o_testimone_violenza_in_azienda, tipo_violenza, presenza_formazione_antidiscriminazione_in_azienda, presenza_regolamenti_antidiscriminazione from 
sondaggio.progetto_andromeda pa;


-- trasformazione di tutte le province domicilio nella versione estesa 
-- se sono delle sigle (se sono rotte rimangono rotte)

drop table if exists sondaggio_transformation.tt_sondaggio_province_domicilio_ok_v2 cascade;
create table sondaggio_transformation.tt_sondaggio_province_domicilio_ok_v2 as
select "Timestamp", "Genere", fascia_eta, t."Provincia" as provincia_domicilio, grandezza_azienda, durata_lavoro_in_azienda,  provincia_ultimo_lavoro, vittima_o_testimone_discriminazione_in_azienda, tipo_discriminazione, vittima_o_testimone_violenza_in_azienda, tipo_violenza, presenza_formazione_antidiscriminazione_in_azienda, presenza_regolamenti_antidiscriminazione from 
sondaggio_transformation.tt_sondaggio_province_trim_v1 pa
join sondaggio_transformation.province_italiane t
on lower(trim(pa.provincia_domicilio))=lower(t."Sigla")
union 
select "Timestamp", "Genere", fascia_eta, provincia_domicilio, grandezza_azienda, durata_lavoro_in_azienda,  provincia_ultimo_lavoro, "vittima_o_testimone_discriminazione_in_azienda", tipo_discriminazione, vittima_o_testimone_violenza_in_azienda, tipo_violenza, presenza_formazione_antidiscriminazione_in_azienda, presenza_regolamenti_antidiscriminazione from 
sondaggio_transformation.tt_sondaggio_province_trim_v1 pa
left join sondaggio_transformation.province_italiane t
on lower(trim(pa.provincia_domicilio))=lower(t."Sigla")
where t."Provincia" is null;


-- trasformazione di tutte le province ultimo lavoro nella versione estesa 
-- se sono delle sigle (se sono rotte rimangono rotte)

drop table if exists sondaggio_transformation.tt_sondaggio_province_ultimo_lavoro_ok_v3 cascade;

create table sondaggio_transformation.tt_sondaggio_province_ultimo_lavoro_ok_v3 as
select 
    pa."Timestamp", 
    pa."Genere", 
    pa.fascia_eta, 
    pa.provincia_domicilio, 
    pa.grandezza_azienda, 
    pa.durata_lavoro_in_azienda,  
    t."Provincia" as provincia_ultimo_lavoro, 
    pa.vittima_o_testimone_discriminazione_in_azienda, 
    pa.tipo_discriminazione, 
    pa.vittima_o_testimone_violenza_in_azienda, 
    pa.tipo_violenza, 
    pa.presenza_formazione_antidiscriminazione_in_azienda, 
    pa.presenza_regolamenti_antidiscriminazione
from sondaggio_transformation.tt_sondaggio_province_domicilio_ok_v2 pa
join sondaggio_transformation.province_italiane t
    on lower(trim(pa.provincia_ultimo_lavoro)) = lower(t."Sigla")

union 

select 
    pa."Timestamp", 
    pa."Genere", 
    pa.fascia_eta, 
    pa.provincia_domicilio, 
    pa.grandezza_azienda, 
    pa.durata_lavoro_in_azienda,  
    pa.provincia_ultimo_lavoro, 
    pa.vittima_o_testimone_discriminazione_in_azienda, 
    pa.tipo_discriminazione, 
    pa.vittima_o_testimone_violenza_in_azienda, 
    pa.tipo_violenza, 
    pa.presenza_formazione_antidiscriminazione_in_azienda, 
    pa.presenza_regolamenti_antidiscriminazione
from sondaggio_transformation.tt_sondaggio_province_domicilio_ok_v2 pa
left join sondaggio_transformation.province_italiane t
    on lower(trim(pa.provincia_ultimo_lavoro)) = lower(t."Sigla")
where t."Provincia" is null;


--creazione del fatto 
drop table if exists fact_sondaggio cascade;
create table if not exists fact_sondaggio as 
select 
    row_number() over() as ids_risposta,
    *
from (
    select
        coalesce(dg.ids_genere, -1) as ids_genere, 
        coalesce(d.ids_fascia_eta, -1) as ids_fascia_eta,
        coalesce(dp.ids_provincia, coalesce(dp2.ids_provincia, -1)) as ids_provincia_domicilio,
        coalesce(ga.ids_grandezza_azienda, -1) as ids_grandezza_azienda,
        coalesce(ddlia.ids_durata_lavoro_in_azienda, -1) as ids_durata_lavoro_in_azienda,
        coalesce(dp_l.ids_provincia, coalesce(dp_l2.ids_provincia, -1)) as ids_provincia_ultimo_lavoro,
        coalesce(ids_vittima_o_testimone_di_discriminazione_in_azienda, -1) as ids_vittima_o_testimone,
        coalesce(ids_tipo_discriminazione, -1) as ids_tipo_discriminazione,
        coalesce(ids_vittima_o_testimone_di_violenza, -1) as ids_vittima_o_testimone_violenza,
        coalesce(ids_tipo_violenza, -1) as ids_tipo_violenza,
        coalesce(ids_presenza_formazione_antidiscriminazione_in_azienda, -1) as ids_presenza_formazione,
        coalesce(ids_presenza_regolamenti_antidiscriminazione, -1) as ids_presenza_regolamenti
from sondaggio_transformation.tt_sondaggio_province_ultimo_lavoro_ok_v3 pa 
left join sondaggio_transformation.dim_genere dg 
        on dg.genere = pa."Genere"
left join sondaggio_transformation.dim_fascia_eta d 
        on d.fascia_eta = pa.fascia_eta
left join sondaggio_transformation.dim_provincia dp
        on lower(pa.provincia_domicilio) = lower(dp.provincia)
left join sondaggio_transformation.man_mapping_province mmp
        on lower(pa.provincia_domicilio) = lower(mmp.originale)
left join sondaggio_transformation.dim_provincia dp2
        on lower(mmp.corretto) = lower(dp2.provincia)
left join sondaggio_transformation.dim_provincia dp_l
        on lower(pa.provincia_ultimo_lavoro) = lower(dp_l.provincia)
left join sondaggio_transformation.man_mapping_province mmpl
        on lower(pa.provincia_ultimo_lavoro) = lower(mmpl.originale)
left join sondaggio_transformation.dim_provincia dp_l2
        on lower(mmpl.corretto) = lower(dp_l2.provincia)
left join sondaggio_transformation.dim_grandezza_azienda ga
        on ga.grandezza_azienda = pa.grandezza_azienda 
left join sondaggio_transformation.dim_durata_lavoro_in_azienda ddlia 
        on ddlia.durata_lavoro_in_azienda = pa.durata_lavoro_in_azienda 
left join sondaggio_transformation.dim_vittima_o_testimone_di_discriminazione_in_azienda dvotddia 
        on dvotddia.vittima_o_testimone_di_discriminazione_in_azienda = 
           pa.vittima_o_testimone_discriminazione_in_azienda 
left join sondaggio_transformation.dim_tipo_discriminazione dtd 
        on dtd.tipo_discriminazione = pa.tipo_discriminazione 
 left join sondaggio_transformation.dim_vittima_o_testimone_di_violenza dvot
 on dvot.vittima_o_testimone_di_violenza = pa.vittima_o_testimone_violenza_in_azienda
left join sondaggio_transformation.dim_tipo_violenza dtv 
        on dtv.tipo_violenza = pa.tipo_violenza 
left join sondaggio_transformation.dim_presenza_formazione_antidiscriminazione_in_azienda dpfaia 
        on dpfaia.presenza_formazione_antidiscriminazione_in_azienda = 
           pa.presenza_formazione_antidiscriminazione_in_azienda 
left join sondaggio_transformation.dim_presenza_regolamenti_antidiscriminazione dpra
        on dpra.presenza_regolamenti_antidiscriminazione = 
           pa.presenza_regolamenti_antidiscriminazione 
) t;


--CREAZIONE DELLE CHIAVI 
ALTER TABLE dim_genere
ADD CONSTRAINT dim_genere2 UNIQUE (ids_genere);

alter table fact_sondaggio
add constraint fact_2 FOREIGN KEY (ids_genere) references dim_genere (ids_genere);



ALTER TABLE dim_fascia_eta 
ADD CONSTRAINT dim_fascia_eta2 UNIQUE (ids_fascia_eta);

alter table fact_sondaggio
add constraint fact_3 FOREIGN KEY (ids_fascia_eta) references dim_fascia_eta (ids_fascia_eta);

--ERRORE per mancata corrispondenza con fittizio
ALTER TABLE dim_provincia_domicilio
ADD CONSTRAINT dim_provincia2 UNIQUE (ids_provincia_domicilio);

insert into dim_provincia_domicilio (ids_provincia_domicilio, "Sigla") values (-1, 'Provincia fittizia');

alter table fact_sondaggio
add constraint fact_4 FOREIGN KEY (ids_provincia_domicilio) references dim_provincia_domicilio (ids_provincia_domicilio);


ALTER TABLE dim_grandezza_azienda
ADD CONSTRAINT dim_grandezza2 UNIQUE (ids_grandezza_azienda);

alter table fact_sondaggio
add constraint fact_5 FOREIGN KEY (ids_grandezza_azienda) references dim_grandezza_azienda (ids_grandezza_azienda);

--ERRORE PER MANCATA CORRISPONDENZA CON FITTIZIO
ALTER TABLE dim_durata_lavoro_in_azienda
ADD CONSTRAINT dim_durata2 UNIQUE (ids_durata_lavoro_in_azienda);

insert into dim_durata_lavoro_in_azienda (ids_durata_lavoro_in_azienda, durata_lavoro_in_azienda)
values (-1, 'Durata fittizia')

alter table fact_sondaggio
add constraint fact_ FOREIGN KEY (ids_durata_lavoro_in_azienda) references dim_durata_lavoro_in_azienda (ids_durata_lavoro_in_azienda);

--ERRORE MANCANZA FITTIZIO

ALTER TABLE dim_provincia_ultimo_lavoro
ADD CONSTRAINT dim_ultimolavoro2 UNIQUE (id);

insert into dim_provincia_ultimo_lavoro (id, provincia_clean) values (-1, 'Provincia fittizia');

alter table fact_sondaggio
add constraint fact8 FOREIGN KEY (ids_provincia_ultimo_lavoro) references dim_provincia_ultimo_lavoro (id);



ALTER TABLE dim_vittima_o_testimone_di_discriminazione_in_azienda
ADD CONSTRAINT dim_vittima UNIQUE (ids_vittima_o_testimone_di_discriminazione_in_azienda);

alter table fact_sondaggio
add constraint fact9 FOREIGN KEY (ids_vittima_o_testimone) references dim_vittima_o_testimone_di_discriminazione_in_azienda (ids_vittima_o_testimone_di_discriminazione_in_azienda);

--MANCATA CORRISPONDENZA CON FITTIZIO
ALTER TABLE dim_tipo_discriminazione
ADD CONSTRAINT dim_discriminazione UNIQUE (ids_tipo_discriminazione);

insert into dim_tipo_discriminazione (ids_tipo_discriminazione, tipo_discriminazione) values (-1, 'Discriminazione fittizia');
alter table fact_sondaggio
add constraint fact20 FOREIGN KEY (ids_tipo_discriminazione) references dim_tipo_discriminazione (ids_tipo_discriminazione);


ALTER TABLE dim_vittima_o_testimone_di_violenza
ADD CONSTRAINT dim_violenza UNIQUE (ids_vittima_o_testimone_di_violenza);

alter table fact_sondaggio
add constraint fact10 FOREIGN KEY (ids_vittima_o_testimone) references dim_vittima_o_testimone_di_violenza (ids_vittima_o_testimone_di_violenza);

--MANCATA CORRISPONDENZA CON FITTIZIO
ALTER TABLE dim_tipo_violenza
ADD CONSTRAINT dim_violenza2 UNIQUE (ids_tipo_violenza);

insert into dim_tipo_violenza (ids_tipo_violenza, tipo_violenza) values (-1, 'Violenza fittizia');
alter table fact_sondaggio
add constraint fact11 FOREIGN KEY (ids_tipo_violenza) references dim_tipo_violenza (ids_tipo_violenza);


ALTER TABLE dim_presenza_formazione_antidiscriminazione_in_azienda 
ADD CONSTRAINT dim_presenza_formazione2 UNIQUE (ids_presenza_formazione_antidiscriminazione_in_azienda);

alter table fact_sondaggio
add constraint fact12 FOREIGN KEY (ids_presenza_formazione) references dim_presenza_formazione_antidiscriminazione_in_azienda (ids_presenza_formazione_antidiscriminazione_in_azienda) ;

--MANCATA CORRISPONDENZA CON FITTIZIO
ALTER TABLE dim_presenza_regolamenti_antidiscriminazione 
ADD CONSTRAINT dim_presenza_antidiscrim2 UNIQUE (ids_presenza_regolamenti_antidiscriminazione);

insert into dim_presenza_regolamenti_antidiscriminazione (ids_presenza_regolamenti_antidiscriminazione, presenza_regolamenti_antidiscriminazione) values (-1, 'Valore fittizio');
alter table fact_sondaggio
add constraint fact13 FOREIGN KEY (ids_presenza_regolamenti) references dim_presenza_regolamenti_antidiscriminazione  (ids_presenza_regolamenti_antidiscriminazione) ;

--chiavi provincia e regione
ALTER TABLE dim_regioni  
ADD CONSTRAINT dim_presenza_regioni2 UNIQUE (ids_regione);

alter table dim_provincia_regione 
add constraint regione FOREIGN KEY (ids_regione) references dim_regioni  (ids_regione) ;





