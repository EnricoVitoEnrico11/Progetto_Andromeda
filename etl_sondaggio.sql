 

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


	
	-- tabella pulita con i case when
	drop table if exists dim_provincia_domicilio cascade;
	
	create table dim_provincia_domicilio as 
	select row_number () over() , * from (select distinct 
	trim (INITCAP (
		case
        when provincia_domicilio = 'bo'
        	then 'bologna'
        when provincia_domicilio = 'rm'
        	then 'roma'
        when provincia_domicilio = 'mn'
        	then 'mantova'
        when provincia_domicilio = 'villamaina(av)'
        	then 'avellino'
        when provincia_domicilio = 'via del giardinetto 30 marlia'
        	then 'lucca'
        when provincia_domicilio = 'tunisi'
        	then 'estero'
        when provincia_domicilio = 'forlì cesena'
        	then 'forlì-cesena'
        else provincia_domicilio 
        end)
                
	) as provincia_domicilio from (select trim(lower(provincia_domicilio)) as provincia_domicilio
	from sondaggio.progetto_andromeda pa)
	order by provincia_domicilio) t;
	
	
	
	

	

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

 

drop table if exists dim_provincia_domicilio CASCADE;
create table dim_provincia_domicilio as
	select row_number () over() , * from (
	select distinct p."Sigla" from sondaggio.progetto_andromeda a
	join sondaggio_transformation.province_italiane p on (p."Sigla"=a."provincia_domicilio"
	or p."Provincia"=a."provincia_domicilio"));
	
	
drop table if exists dim_provincia_ultimo_lavoro CASCADE;
	
create table dim_provincia_ultimo_lavoro as
	select row_number () over() , * from (
	select distinct p."Sigla" from sondaggio.progetto_andromeda a
	join sondaggio_transformation.province_italiane p on (p."Sigla"=a."provincia_ultimo_lavoro"
	or p."Provincia"=a."provincia_ultimo_lavoro"));
	
	drop table if exists dim_provincia_domicilio CASCADE;
	
	create table dim_provincia_domicilio as 
	select row_number () over() , * from (select distinct 
	trim (INITCAP (
		case
        when provincia_domicilio = 'bo'
        	then 'bologna'
        when provincia_domicilio = 'rm'
        	then 'roma'
        when provincia_domicilio = 'mn'
        	then 'mantova'
        when provincia_domicilio = 'villamaina(av)'
        	then 'avellino'
        when provincia_domicilio = 'via del giardinetto 30 marlia'
        	then 'lucca'
        when provincia_domicilio = 'tunisi'
        	then 'estero'
        when provincia_domicilio = 'forlì cesena'
        	then 'forlì-cesena'
        else provincia_domicilio 
        end   )
                
	) as provincia_domicilio from (select trim(lower(provincia_domicilio)) as provincia_domicilio
	from sondaggio.progetto_andromeda pa)
	order by provincia_domicilio) t;
	
	
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
