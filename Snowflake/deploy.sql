create database if not exists POKEMONS;
use database POKEMONS;

create warehouse if not exists DEPLOYMENT_WH with warehouse_size=XSMALL max_cluster_count=1;
create warehouse if not exists TASKS_WH with warehouse_size=XSMALL max_cluster_count=1;

use warehouse DEPLOYMENT_WH;

create schema if not exists STAGING;
use schema STAGING;

create or replace stage STAGING
  url='s3://de-school-snowflake/snowpipe/Sharaeva/'
  credentials=(aws_key_id='aws_key_id' aws_secret_key='aws_secret_key');


create or replace file format CSV type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  trim_space = false skip_header=1
  error_on_column_count_mismatch = false escape = 'none'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for csv files';

--fact-tables
create or replace table POKEMONS (
    name string,
    url string,
    filename string,
    editor string,
    modify_date timestamp);

create or replace table TYPES (
    name string,
    url string,
    filename string,
    editor string,
    modify_date timestamp);

create or replace table MOVES (
    name string,
    url string,
    filename string,
    editor string,
    modify_date timestamp);

create or replace table GENERATIONS (
    name string,
    url string,
    filename string,
    editor string,
    modify_date timestamp);

--dimensions
create or replace table POKEMONS_TYPES (
    pokemon_name string,
    pokemon_type string,
    filename string,
    editor string,
    modify_date timestamp);

create or replace table POKEMONS_MOVES (
    pokemon_name string,
    move_name string,
    filename string,
    editor string,
    modify_date timestamp);

create or replace table POKEMONS_STATS (
    power string,
    characteristic string,
    pokemon_name string,
    filename string,
    editor string,
    modify_date timestamp);

create or replace table GENERATIONS_SPECIES (
    specie_name string,
    specie_url string,
    generation_name string,
    specie_id integer,
    filename string,
    editor string,
    modify_date timestamp);

create or replace table POKEMONS_SPECIES (
    pokemon_name string,
    specie_id integer,
    filename string,
    editor string,
    modify_date timestamp);

--creating streams
create or replace stream STG_POKEMONS on table POKEMONS;
create or replace stream STG_TYPES on table TYPES;
create or replace stream STG_MOVES on table MOVES;
create or replace stream STG_GENERATIONS on table GENERATIONS;
create or replace stream STG_POKEMONS_STATS on table POKEMONS_STATS;
create or replace stream STG_POKEMONS_MOVES on table POKEMONS_MOVES;
create or replace stream STG_POKEMONS_TYPES on table POKEMONS_TYPES;
create or replace stream STG_GENERATIONS_SPECIES on table GENERATIONS_SPECIES;
create or replace stream STG_POKEMONS_SPECIES on table POKEMONS_SPECIES;

--copying data via pipes
create or replace pipe LOAD_POKEMONS auto_ingest=true as
    copy into POKEMONS from (
        select
            p.$1,
            p.$2,
            metadata$filename,
            current_user(),
            sysdate()
        from @staging (file_format => CSV, pattern => 'pokemons.csv') p);

create or replace pipe LOAD_TYPES auto_ingest=true as
    copy into TYPES from (
        select
            p.$1,
            p.$2,
            metadata$filename,
            current_user(),
            sysdate()
        from @staging (file_format => CSV, pattern => 'types.csv') p);


create or replace pipe LOAD_MOVES auto_ingest=true as
    copy into MOVES from (
        select
            p.$1,
            p.$2,
            metadata$filename,
            current_user(),
            sysdate()
        from @staging (file_format => CSV, pattern => 'moves.csv') p);


create or replace pipe LOAD_GENERATIONS auto_ingest=true as
    copy into GENERATIONS from (
        select
            p.$1,
            p.$2,
            metadata$filename,
            current_user(),
            sysdate()
        from @staging (file_format => CSV, pattern => 'generations.csv') p);


create or replace pipe LOAD_POKEMONS_STATS auto_ingest=true as
    copy into POKEMONS_STATS from (
        select
            p.$1,
            p.$2,
            p.$3,
            metadata$filename,
            current_user(),
            sysdate()
        from @staging (file_format => CSV, pattern => 'pokemons_stats.csv') p);


create or replace pipe LOAD_POKEMONS_MOVES auto_ingest=true as
    copy into POKEMONS_MOVES from (
        select
            p.$1,
            p.$2,
            metadata$filename,
            current_user(),
            sysdate()
        from @staging (file_format => CSV, pattern => 'pokemons_moves.csv') p);


create or replace pipe LOAD_POKEMONS_TYPES auto_ingest=true as
    copy into POKEMONS_TYPES from (
        select
            p.$1,
            p.$2,
            metadata$filename,
            current_user(),
            sysdate()
        from @staging (file_format => CSV, pattern => 'pokemons_types.csv') p);


create or replace pipe LOAD_GENERATIONS_SPECIES auto_ingest=true as
    copy into GENERATIONS_SPECIES from (
        select
            p.$1,
            p.$2,
            p.$3,
            p.$4,
            metadata$filename,
            current_user(),
            sysdate()
        from @staging (file_format => CSV, pattern => 'generations_species.csv') p);

create or replace pipe LOAD_POKEMONS_SPECIES auto_ingest=true as
    copy into POKEMONS_SPECIES from (
        select
            p.$1,
            p.$2,
            metadata$filename,
            current_user(),
            sysdate()
        from @staging (file_format => CSV, pattern => 'pokemons_species.csv') p);


-- warehouse creation
create schema if not exists WAREHOUSE;
use schema WAREHOUSE;

create or replace table POKEMONS (
    name string);

create or replace table TYPES (
    name string);

create or replace table MOVES (
    name string);

create or replace table GENERATIONS (
    name string);

create or replace table POKEMONS_TYPES (
    pokemon_name string,
    pokemon_type string);

create or replace table POKEMONS_MOVES (
    pokemon_name string,
    move_name string);

create or replace table POKEMONS_STATS (
    power string,
    characteristic string,
    pokemon_name string);

create or replace table GENERATIONS_SPECIES (
    generation_name string,
    specie_id integer);

create or replace table POKEMONS_SPECIES (
    pokemon_name string,
    specie_id integer);

-- tasks to load sata from streams
create or replace task INSERT_POKEMONS
warehouse='DEPLOYMENT_WH'
schedule='5 minute'
when system$stream_has_data('STAGING.STG_POKEMONS') as
    insert into POKEMONS(name)
        select distinct
            p.name::string
        from STAGING.STG_POKEMONS p
        where p.metadata$action = 'INSERT';


create or replace task INSERT_TYPES
warehouse='DEPLOYMENT_WH'
schedule='5 minute'
when system$stream_has_data('STAGING.STG_TYPES') as
    insert into TYPES(name)
        select distinct
            t.name::string
        from STAGING.STG_TYPES t
        where t.metadata$action = 'INSERT';


create or replace task INSERT_MOVES
warehouse='DEPLOYMENT_WH'
schedule='5 minute'
when system$stream_has_data('STAGING.STG_MOVES') as
    insert into MOVES(name)
        select distinct
            m.name::string
        from STAGING.STG_MOVES m
        where m.metadata$action = 'INSERT';


create or replace task INSERT_GENERATIONS
warehouse='DEPLOYMENT_WH'
schedule='5 minute'
when system$stream_has_data('STAGING.STG_GENERATIONS') as
    insert into GENERATIONS(name)
        select distinct
            g.name::string
        from STAGING.STG_GENERATIONS g
        where g.metadata$action = 'INSERT';


create or replace task INSERT_POKEMONS_STATS
warehouse='DEPLOYMENT_WH'
schedule='5 minute'
when system$stream_has_data('STAGING.STG_POKEMONS_STATS') as
    insert into POKEMONS_STATS(power, characteristic, pokemon_name)
        select distinct
            ps.power::integer,
            ps.characteristic::string,
            ps.pokemon_name::string
        from STAGING.STG_POKEMONS_STATS ps
        where ps.metadata$action = 'INSERT';


create or replace task INSERT_POKEMONS_MOVES
warehouse='DEPLOYMENT_WH'
schedule='5 minute'
when system$stream_has_data('STAGING.STG_POKEMONS_MOVES') as
    insert into POKEMONS_MOVES(pokemon_name,move_name)
        select distinct
            pm.pokemon_name::string,
            pm.move_name::string
        from STAGING.STG_POKEMONS_MOVES pm
        where pm.metadata$action = 'INSERT';


create or replace task INSERT_POKEMONS_TYPES
warehouse='DEPLOYMENT_WH'
schedule='5 minute'
when system$stream_has_data('STAGING.STG_POKEMONS_TYPES') as
    insert into POKEMONS_TYPES(pokemon_name, pokemon_type)
        select distinct
            pt.pokemon_name::string,
            pt.pokemon_type::string
        from STAGING.STG_POKEMONS_TYPES pt
        where pt.metadata$action = 'INSERT';


create or replace task INSERT_GENERATIONS_SPECIES
warehouse='DEPLOYMENT_WH'
schedule='5 minute'
when system$stream_has_data('STAGING.STG_GENERATIONS_SPECIES') as
    insert into GENERATIONS_SPECIES(generation_name, specie_id)
        select distinct
            gs.generation_name::string,
            gs.specie_id::number
        from STAGING.STG_GENERATIONS_SPECIES gs
        where gs.metadata$action = 'INSERT';


create or replace task INSERT_POKEMONS_SPECIES
warehouse='DEPLOYMENT_WH'
schedule='5 minute'
when system$stream_has_data('STAGING.STG_POKEMONS_SPECIES') as
    insert into POKEMONS_SPECIES(pokemon_name,specie_id)
        select distinct
            ps.pokemon_name::string,
            ps.specie_id::number
        from STAGING.STG_POKEMONS_SPECIES ps
        where ps.metadata$action = 'INSERT';


--data marts to answer questions
create schema if not exists DATA_MARTS;
use schema DATA_MARTS;

use warehouse TASKS_WH;

-- Сколько покемонов в каждом типе (type в терминах API), насколько это меньше чем у следующего по рангу типа? А насколько больше, чем у предыдущего?~~
create or replace view TYPES_STATISTICS as
select
    pokemon_type as "Pokemon Type",
    count(*) as "Number of pokemons",
    lead("Number of pokemons") over (order by "Number of pokemons", "Pokemon Type") - "Number of pokemons" as "Delta from next rank",
    "Number of pokemons" - lag("Number of pokemons") over (order by "Number of pokemons", "Pokemon Type") as "Delta from previous rank"
from WAREHOUSE.POKEMONS_TYPES
group by pokemon_type
order by "Number of pokemons" desc;

-- Сколько у разных атак (moves в терминах API) использующих их покемонов? + показать дельту от следующей и предыдущей по популярности атаки.
create or replace view MOVES_STATISTICS as
select
    move_name as "Move name",
    count(*) as "Attack usage",
    lead("Attack usage") over (order by "Attack usage", "Move name") - "Attack usage" as "Delta from next rank",
    "Attack usage" - lag("Attack usage") over (order by "Attack usage", "Move name") as "Delta from previous rank"
from WAREHOUSE.POKEMONS_MOVES
group by move_name
order by "Attack usage" desc;

-- Составить рейтинг покемонов по сумме их характеристик (stats в терминах API). Например, если у покемона есть только 2 статы: HP 20 & attack 25, то в финальный рейтинг идёт сумма характеристик: 20 + 25 = 45.
create or replace view STATS_STATISTICS as
select
    pokemon_name as "Pokemon",
    sum(power) as "Total power"
from WAREHOUSE.POKEMONS_STATS
group by pokemon_name
order by "Total power" desc;

-- Показать количество покемонов по типам (строки таблицы, type в терминах API) и поколениям (колонки таблицы, generations в терминах API).
create or replace view TYPES_GENERATIONS_STATISTICS as (
with POKEMONS_TYPES_GENERATIONS as (
    select
        pokemon_type,
        generation_name,
        pt.pokemon_name
    from WAREHOUSE.POKEMONS_TYPES pt
    inner join WAREHOUSE.POKEMONS_SPECIES ps on pt.pokemon_name = ps.pokemon_name
    inner join WAREHOUSE.GENERATIONS_SPECIES gs on ps.specie_id = gs.specie_id
) select *
from POKEMONS_TYPES_GENERATIONS
pivot(count(pokemon_name) for generation_name in ('generation-i', 'generation-ii', 'generation-iii', 'generation-iv', 'generation-v', 'generation-vi', 'generation-vii', 'generation-viii'))
      as p ("Pokemon Type", "I", "II", "III", "IV", "V", "VI", "VII", "VIII")
order by 1);