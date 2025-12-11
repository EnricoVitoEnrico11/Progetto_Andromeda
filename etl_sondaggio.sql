 

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

 

CREATE TABLE dim_provincia_domicilio AS
SELECT 
    ROW_NUMBER() OVER() as ids_provincia_domicilio,
    final_view.*
FROM (
    SELECT DISTINCT 
        -- LA LOGICA MAGICA:
        -- 1. Se p."Provincia" non è vuoto (trovato match), usa quello (es. Bologna).
        -- 2. Altrimenti, scrivi 'Altro'.
        COALESCE(p."Provincia", 'Altro') as provincia_domicilio
        
    FROM (
        -- PRIMA PULIZIA: Gestisci solo le eccezioni 'sporche' manuali
        SELECT 
            CASE 
               -- I tuoi casi specifici irrisolvibili con automazione
               WHEN TRIM(LOWER(provincia_domicilio)) LIKE '%via del giardinetto%' THEN 'lucca'
               WHEN TRIM(LOWER(provincia_domicilio)) = 'villamaina(av)' THEN 'avellino'
               -- Rimuovi tutti gli altri WHEN (bo, rm, mn)... ci pensa la JOIN dopo!
               ELSE TRIM(LOWER(provincia_domicilio))
            END as input_clean
        FROM sondaggio.progetto_andromeda
    ) t
    
    -- QUI CAMBIA TUTTO: Usiamo LEFT JOIN invece di JOIN
    LEFT JOIN province_italiane p 
        ON LOWER(p."Sigla") = t.input_clean 
        OR LOWER(p."Provincia") = t.input_clean

) final_view;
	

-- questa qua sotto è la dim_provincia_ultimo_lavoro

drop table if exists dim_provincia_ultimo_lavoro CASCADE;

create table dim_provincia_ultimo_lavoro as 
select 
    row_number() over() as id,
    provincia_clean
from (
    select distinct
        trim(initcap(
            case
                when provincia_ultimo_lavoro = 'vr' then 'verona'
                when provincia_ultimo_lavoro = 'ra' then 'ravenna'
                when provincia_ultimo_lavoro = 'bo' then 'bologna'
                when provincia_ultimo_lavoro = 'villamaina(av)' then 'avellino'
                when provincia_ultimo_lavoro = 'marlia' then 'lucca'
                when provincia_ultimo_lavoro = 'germania' then 'estero'
                when provincia_ultimo_lavoro = 'non lavoro' then 'altro'
                when provincia_ultimo_lavoro = 'rm' then 'roma'
                else provincia_ultimo_lavoro
            end
        )) as provincia_clean
    from (
        select trim(lower(provincia_ultimo_lavoro)) as provincia_ultimo_lavoro
        from sondaggio.progetto_andromeda pa
        order by provincia_ultimo_lavoro
    ) t_interna
) t_esterna;

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
	
select * from et_dim_provincia

--ET 
drop table if exists sondaggio_transformation.et_dim_provincia_domicilio;
create table if not exists sondaggio_transformation.et_dim_provincia_domicilio as
SELECT DISTINCT lower(trim(pa.provincia_domicilio)) as provincia_domicilio, 'Mapping provincia mancante' as errore
FROM sondaggio.progetto_andromeda pa
left JOIN sondaggio_transformation.province_italiane t
ON (pa."provincia_domicilio" = t."Provincia" or pa."provincia_domicilio" = t."Sigla")
WHERE t."Provincia" IS null
AND pa.provincia_domicilio  IS NOT NULL;


--tapella et di mapping a mano 
DROP TABLE IF EXISTS sondaggio_transformation.et_dim_provincia_domicilio_mapping CASCADE;

CREATE TABLE sondaggio_transformation.et_dim_provincia_domicilio_mapping AS
WITH mapped AS (
    SELECT 
        provincia_domicilio,
        CASE 
            WHEN LOWER(provincia_domicilio) IN ('bo', 'bologna') THEN 'BO'
            WHEN LOWER(provincia_domicilio) = 'estero' THEN 'Estero'
            WHEN LOWER(provincia_domicilio) IN ('forlì cesena', 'forli cesena') THEN 'FC'
            WHEN LOWER(provincia_domicilio) = 'lucca' THEN 'LU'
            WHEN LOWER(provincia_domicilio) = 'milano' THEN 'MI'
            WHEN LOWER(provincia_domicilio) = 'modena' THEN 'MO'
            WHEN LOWER(provincia_domicilio) = 'padova' THEN 'PD'
            WHEN LOWER(provincia_domicilio) IN ('rm', 'roma') THEN 'RM'
            WHEN LOWER(provincia_domicilio) = 'torino' THEN 'TO'
            WHEN LOWER(provincia_domicilio) = 'trento' THEN 'TN'
            WHEN LOWER(provincia_domicilio) = 'tunisi' THEN 'Estero'  
            WHEN LOWER(provincia_domicilio) = 'venezia' THEN 'VE'
            WHEN LOWER(provincia_domicilio) LIKE '%via del giardinetto%' THEN 'LU'  
            WHEN LOWER(provincia_domicilio) LIKE '%villamaina%' THEN 'AV'
            ELSE NULL
        END AS sigla
    FROM sondaggio_transformation.et_dim_provincia_domicilio
)
SELECT 
    provincia_domicilio,
    sigla,
    CASE 
        WHEN sigla = 'BO' THEN 16
        WHEN sigla = 'Estero' THEN NULL  
        WHEN sigla = 'FC' THEN 37
        WHEN sigla = 'LU' THEN 50
        WHEN sigla = 'MI' THEN 57
        WHEN sigla = 'MO' THEN 58
        WHEN sigla = 'PD' THEN 64
        WHEN sigla = 'RM' THEN 83
        WHEN sigla = 'TO' THEN 95
        WHEN sigla = 'TN' THEN 97
        WHEN sigla = 'VE' THEN 102
        WHEN sigla = 'AV' THEN 9
        ELSE NULL
    END AS ids_provincia
FROM mapped;


  --et mapping a mano 
DROP TABLE IF EXISTS sondaggio_transformation.et_dim_provincia_mapping CASCADE;

CREATE TABLE sondaggio_transformation.et_dim_provincia_mapping AS
WITH mapped AS (
    SELECT 
        provincia_domicilio,
        CASE 
            WHEN LOWER(provincia_domicilio) IN ('bo', 'bologna') THEN 'BO'
            WHEN LOWER(provincia_domicilio) = 'estero' THEN 'Estero'
            WHEN LOWER(provincia_domicilio) IN ('forlì cesena', 'forli cesena') THEN 'FC'
            WHEN LOWER(provincia_domicilio) = 'lucca' THEN 'LU'
            WHEN LOWER(provincia_domicilio) = 'milano' THEN 'MI'
            WHEN LOWER(provincia_domicilio) = 'modena' THEN 'MO'
            WHEN LOWER(provincia_domicilio) = 'padova' THEN 'PD'
            WHEN LOWER(provincia_domicilio) IN ('rm', 'roma') THEN 'RM'
            WHEN LOWER(provincia_domicilio) = 'torino' THEN 'TO'
            WHEN LOWER(provincia_domicilio) = 'trento' THEN 'TN'
            WHEN LOWER(provincia_domicilio) = 'tunisi' THEN 'Estero'  
            WHEN LOWER(provincia_domicilio) = 'venezia' THEN 'VE'
            WHEN LOWER(provincia_domicilio) LIKE '%via del giardinetto%' THEN 'LU'  
            WHEN LOWER(provincia_domicilio) LIKE '%villamaina%' THEN 'AV'
            ELSE NULL
        END AS sigla
    FROM sondaggio_transformation.et_dim_provincia_domicilio
)
SELECT 
    provincia_domicilio,
    sigla,
    CASE 
        WHEN sigla = 'BO' THEN 16
        WHEN sigla = 'Estero' THEN NULL
        WHEN sigla = 'FC' THEN 37
        WHEN sigla = 'LU' THEN 50
        WHEN sigla = 'MI' THEN 57
        WHEN sigla = 'MO' THEN 58
        WHEN sigla = 'PD' THEN 64
        WHEN sigla = 'RM' THEN 83
        WHEN sigla = 'TO' THEN 95
        WHEN sigla = 'TN' THEN 97
        WHEN sigla = 'VE' THEN 102
        WHEN sigla = 'AV' THEN 9
        ELSE NULL
    END AS ids_provincia
FROM mapped;

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
