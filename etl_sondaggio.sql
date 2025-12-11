 

drop table if exists dim_genere cascade;

create table dim_genere as 
select row_number() over () as ids_genere, * from (
	select distinct pa.genere 
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
   select distinct pa.vittima_o_testimone_di_discriminazione_in_azienda
from sondaggio.progetto_andromeda pa

) t 
order by vittima_o_testimone_di_discriminazione_in_azienda;

--dim tipo discriminazione

drop table if exists dim_tipo_discriminazione cascade;

create table dim_tipo_discriminazione as
SELECT 
    row_number() OVER (
        ORDER BY 
            (valore_discriminazione = 'Nessuna discriminazione indicata') DESC, 
            valore_discriminazione
    ) as ids_tipo_discriminazione,
    valore_discriminazione as tipo_discriminazione
FROM 
    (
    -- LIVELLO 2: Pulisce i dati, gestisce i NULL e rimuove i duplicati
    select distinct 
        COALESCE(NULLIF(TRIM(t1.singolo_elemento), ''), 'Nessuna discriminazione indicata') 
        as valore_discriminazione
    FROM 
        (
        -- LIVELLO 1 (Il cuore): Spacchetta le stringhe separate dalla virgola
        SELECT 
            unnest(string_to_array(pa.tipo_discriminazione, ',')) as singolo_elemento 
        FROM 
            sondaggio.progetto_andromeda pa
        ) t1
    ) t2;


-- qui sotto la query che funziona con i dati puliti della dim_tipo_violenza

drop table if exists dim_tipo_violenza cascade;

create table dim_tipo_violenza as
SELECT 
    row_number() OVER (
        ORDER BY 
            (valore_violenza = 'Nessuna violenza indicata') DESC, 
            valore_violenza
    ) as ids_tipo_violenza,
    valore_violenza as tipo_violenza
FROM 
    (
    -- LIVELLO 2: Pulisce gli spazi, gestisce i NULL/vuoti e rimuove i duplicati
    SELECT DISTINCT 
        COALESCE(NULLIF(TRIM(t1.singolo_elemento), ''), 'Nessuna violenza indicata') 
        as valore_violenza
    FROM 
        (
        -- LIVELLO 1: Spacchetta le stringhe separate da virgola
        SELECT 
            unnest(string_to_array(pa.tipo_violenza, ',')) as singolo_elemento 
        FROM 
            sondaggio.progetto_andromeda pa
        ) t1
    ) t2;

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

	

create table if not exists sondaggio_transformation.man_mapping_province ( originale text,corretto text);

drop table if exists sondaggio_transformation.tt_provincia_validated_v01;

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

drop table if exists sondaggio_transformation.dim_provincia;

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
)order by provincia;

drop table if exists sondaggio_transformation.et_dim_provincia;

create table et_dim_provincia as 
select FORMAT('Valore provincia "%s" non valido',rotti.provincia ) as messaggio from(
select provincia 
from sondaggio_transformation.tt_provincia_validated_v01
where err=true 
) rotti
left join sondaggio_transformation.man_mapping_province mapping
	on rotti.provincia=mapping.originale
where mapping.corretto is null
;

--creazione del fatto 
select row_number() over() as ids, *
from (
    select 
        dg.ids_genere, 
        d.ids_fascia_eta, 
        coalesce(dpr.ids_provincia, et.ids_provincia) as ids_provincia
    from sondaggio.progetto_andromeda pa
    left join dim_genere dg on dg.genere = pa.genere
    left join dim_fascia_eta d on pa.fascia_eta = d.fascia_eta
    left join dim_provincia_regione dpr  
        on pa.provincia_domicilio = dpr."territorio" 
        or pa.provincia_domicilio = dpr."sigla_territorio"
    left join sondaggio_transformation.et_dim_provincia_domicilio_mapping et 
        on et.sigla = dpr.sigla_territorio ) sub;

select coalesce(dpr.ids_provincia, edpdm.ids_provincia) from sondaggio.progetto_andromeda pa 
left join dim_provincia_regione dpr on (lower(pa.provincia_domicilio) = lower(dpr.sigla_territorio) or lower(pa.provincia_domicilio) = lower(dpr.territorio) )
left join sondaggio_transformation.et_dim_provincia_domicilio_mapping edpdm on lower(edpdm.provincia_domicilio) = lower(pa.provincia_domicilio) 
